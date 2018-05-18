/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.client;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.client.ClientResponseContext;
import javax.ws.rs.client.ClientResponseFilter;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * A client filter for Jersey client which supports SPNEGO authentication.
 *
 * Currently only "Negotiate" scheme is supported which will do auth using Kerberos.
 *
 * A user can use his/her keytab and userprincipal in lens-client-site.xml
 * using config "lens.client.authentication.kerberos.keytab" and "lens.client.authentication.kerberos.principal"
 * respectively. If these config is not provided then kerberos credential cache is used by default.
 */
@Slf4j
@RequiredArgsConstructor
public class SpnegoClientFilter implements ClientRequestFilter, ClientResponseFilter{
  private static final String REQUEST_PROPERTY_FILTER_REUSED =
          "org.glassfish.jersey.client.authentication.HttpAuthenticationFilter.reused";
  private static final String SPNEGO_OID = "1.3.6.1.5.5.2";
  private static final String NEGOTIATE_SCHEME = "Negotiate";

  private static final LensClientConfig CONF = new LensClientConfig();
  private final String keyTabLocation = CONF.get(LensClientConfig.KERBEROS_KEYTAB);
  private final String userPrincipal = CONF.get(LensClientConfig.KERBEROS_PRINCIPAL);
  private final String realm = CONF.get(LensClientConfig.KERBEROS_REALM);

  private String servicePrincipalName;
  private boolean useCanonicalHostname;

  @Override
  public void filter(ClientRequestContext requestContext) throws IOException {

  }

  @Override
  public void filter(ClientRequestContext request, ClientResponseContext response) throws IOException {
    if ("true".equals(request.getProperty(REQUEST_PROPERTY_FILTER_REUSED))) {
      return;
    }
    boolean authenticate;

    if (response.getStatus() == Response.Status.UNAUTHORIZED.getStatusCode()) {
      String authString = response.getHeaders().getFirst(HttpHeaders.WWW_AUTHENTICATE);
      if (authString != null) {
        if (authString.trim().startsWith(NEGOTIATE_SCHEME)) {
          authenticate = true;
        } else {
          return;
        }
      } else {
        authenticate = false;
      }

      if (authenticate) {
        String authorization = getAuthorization(request.getUri());
        repeatRequest(request, response, authorization);
      }
    }
  }



  private String getAuthorization(URI currentURI) {
    try {
      String spn = getCompleteServicePrincipalName(currentURI);

      Oid oid = new Oid(SPNEGO_OID);

      byte[] token = getToken(spn, oid);
      String encodedToken = new String(Base64.getEncoder().encode(token), StandardCharsets.UTF_8);
      return NEGOTIATE_SCHEME + " " + encodedToken;
    } catch (LoginException e) {
      throw new RuntimeException(e.getMessage(), e);
    } catch (GSSException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }


  private byte[] getToken(String spn, Oid oid) throws GSSException, LoginException {
    LoginContext lc = buildLoginContext();
    lc.login();
    Subject subject = lc.getSubject();

    GSSManager manager = GSSManager.getInstance();
    GSSName serverName = manager.createName(spn, null); // 2nd oid

    GSSContext context = manager
            .createContext(serverName.canonicalize(oid), oid, null, GSSContext.DEFAULT_LIFETIME);

    final byte[] token = new byte[0];

    try {
      return Subject.doAs(subject, new CreateServiceTicketAction(context, token));
    } catch (PrivilegedActionException e) {
      if (e.getCause() instanceof GSSException) {
        throw (GSSException) e.getCause();
      }
      log.error("initSecContext", e);
      return null;
    }
  }


  private String getCompleteServicePrincipalName(URI currentURI) {
    String name;

    if (servicePrincipalName == null) {
      String host = currentURI.getHost();
      if (useCanonicalHostname) {
        host = getCanonicalHostname(host);
      }
      name = "HTTP/" + host;
    } else {
      name = servicePrincipalName;
    }
    if (realm != null) {
      name += "@" + realm;
    }

    return name;
  }

  private String getCanonicalHostname(String hostname) {
    String canonicalHostname = hostname;
    try {
      InetAddress in = InetAddress.getByName(hostname);
      canonicalHostname = in.getCanonicalHostName();
      log.debug("resolved hostname=" + hostname + " to canonicalHostname=" + canonicalHostname);
    } catch (Exception e) {
      log.warn("unable to resolve canonical hostname", e);
    }
    return canonicalHostname;
  }


  private static final class CreateServiceTicketAction implements PrivilegedExceptionAction<byte[]> {
    private final GSSContext context;
    private final byte[] token;

    private CreateServiceTicketAction(GSSContext context, byte[] token) {
      this.context = context;
      this.token = token;
    }

    public byte[] run() throws GSSException {
      byte[] data =  context.initSecContext(token, 0, token.length);
      return data;
    }
  }

  @RequiredArgsConstructor
  static class ClientLoginConfig extends Configuration {
    private final String keyTabLocation;
    private final String userPrincipal;

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      Map<String, Object> options = new HashMap<String, Object>();

      // if we don't have keytab or principal only option is to rely on
      // credentials cache.
      if (StringUtils.isEmpty(keyTabLocation) || StringUtils.isEmpty(userPrincipal)) {
        // cache
        options.put("useTicketCache", "true");
      } else {
        // keytab
        options.put("useKeyTab", "true");
        options.put("keyTab", keyTabLocation);
        options.put("principal", userPrincipal);
        options.put("storeKey", "true");
      }

      options.put("doNotPrompt", "true");
      options.put("isInitiator", "true");

      return new AppConfigurationEntry[] { new AppConfigurationEntry(
              "com.sun.security.auth.module.Krb5LoginModule",
              AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options), };
    }

  }

  private LoginContext buildLoginContext() throws LoginException {
    ClientLoginConfig loginConfig = new ClientLoginConfig(keyTabLocation, userPrincipal);

    Subject subject =  null;
    if (StringUtils.isNotBlank(keyTabLocation) && StringUtils.isNotBlank(userPrincipal)) {
      Set<Principal> princ = new HashSet<>(1);
      princ.add(new KerberosPrincipal(userPrincipal));
      subject = new Subject(false, princ, new HashSet<>(), new HashSet<>());
    }
    LoginContext lc = new LoginContext("", subject, null, loginConfig);
    return lc;
  }


  /**
   * Repeat the {@code request} with provided {@code newAuthorizationHeader}
   * and update the {@code response} with newest response data.
   *
   * @param request                Request context.
   * @param response               Response context (will be updated with the new response data).
   * @param newAuthorizationHeader {@code Authorization} header that should be added to the new request.
   * @return {@code true} is the authentication was successful ({@code true} if 401 response code was not returned;
   * {@code false} otherwise).
   */
  private boolean repeatRequest(ClientRequestContext request,
                               ClientResponseContext response,
                               String newAuthorizationHeader) {
    Client client = ClientBuilder.newClient(request.getConfiguration());
    String method = request.getMethod();
    MediaType mediaType = request.getMediaType();
    URI lUri = request.getUri();

    WebTarget resourceTarget = client.target(lUri);

    Invocation.Builder builder = resourceTarget.request(mediaType);

    MultivaluedMap<String, Object> newHeaders = new MultivaluedHashMap<String, Object>();

    for (Map.Entry<String, List<Object>> entry : request.getHeaders().entrySet()) {
      if (HttpHeaders.AUTHORIZATION.equals(entry.getKey())) {
        continue;
      }
      newHeaders.put(entry.getKey(), entry.getValue());
    }

    newHeaders.add(HttpHeaders.AUTHORIZATION, newAuthorizationHeader);
    builder.headers(newHeaders);

    builder.property(REQUEST_PROPERTY_FILTER_REUSED, "true");

    Invocation invocation;
    if (request.getEntity() == null) {
      invocation = builder.build(method);
    } else {
      invocation = builder.build(method,
              Entity.entity(request.getEntity(), request.getMediaType()));
    }
    Response nextResponse = invocation.invoke();

    if (nextResponse.hasEntity()) {
      response.setEntityStream(nextResponse.readEntity(InputStream.class));
    }
    MultivaluedMap<String, String> headers = response.getHeaders();
    headers.clear();
    headers.putAll(nextResponse.getStringHeaders());
    response.setStatus(nextResponse.getStatus());

    return response.getStatus() != Response.Status.UNAUTHORIZED.getStatusCode();
  }
}

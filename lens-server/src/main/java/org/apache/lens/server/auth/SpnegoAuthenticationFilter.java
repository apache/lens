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

package org.apache.lens.server.auth;

import java.io.File;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Priority;
import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import org.apache.lens.api.auth.AuthScheme;
import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.api.LensConfConstants;

import org.apache.commons.lang3.StringUtils;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import lombok.extern.slf4j.Slf4j;

/**
 * A JAX-RS filter for SPNEGO authentication.
 *
 * <p>Currently only "Negotiate" scheme is supported which will do auth using Kerberos.</p>
 *
 * <p>This filter can be enabled by adding an entry in {@code lens.server.ws.filternames} property and providing
 * the impl class.</p>
 *
 * <pre>The following configuration is needed for the filter to function
 * {@code lens.server.authentication.scheme} : NEGOTIATE (other values which are not supported are listed
 * in {@link AuthScheme})
 * {@code lens.server.authentication.kerberos.principal} : The SPN (in format HTTP/fqdn)
 * {@code lens.server.authentication.kerberos.keytab} : Keytab of lens SPN
 * </pre>
 */
@Priority(20)
@Slf4j
public class SpnegoAuthenticationFilter implements ContainerRequestFilter {
  private static final String SPNEGO_OID = "1.3.6.1.5.5.2";
  private static final String KERBEROS_LOGIN_MODULE_NAME =
          "com.sun.security.auth.module.Krb5LoginModule";

  private static final org.apache.hadoop.conf.Configuration CONF = LensServerConf.getHiveConf();
  private static final AuthScheme AUTH_SCHEME = AuthScheme.valueOf(CONF.get(LensConfConstants.AUTH_SCHEME));

  static {
    if (AUTH_SCHEME != AuthScheme.NEGOTIATE) {
      log.error("Lens server currently only supports NEGOTIATE auth scheme");
      throw new RuntimeException("Lens server currently only supports NEGOTIATE auth scheme");
    }
  }

  private String servicePrincipalName = CONF.get(LensConfConstants.KERBEROS_PRINCIPAL);
  private String realm = CONF.get(LensConfConstants.KERBEROS_REALM);
  private Configuration loginConfig = getJaasKrb5TicketConfig(servicePrincipalName,
          new File(CONF.get(LensConfConstants.KERBEROS_KEYTAB)));

  private HttpHeaders headers;

  private UriInfo uriInfo;

  private ResourceInfo resourceInfo;

  @Context
  public void setHeaders(HttpHeaders headers) {
    this.headers = headers;
  }

  @Context
  public void setUriInfo(UriInfo uriInfo) {
    this.uriInfo = uriInfo;
  }

  @Context
  public void setResourceInfo(ResourceInfo resourceInfo) {
    this.resourceInfo = resourceInfo;
  }

  @Override
  public void filter(ContainerRequestContext context) {
    Principal userPrincipal = context.getSecurityContext().getUserPrincipal();
    if (userPrincipal != null) {
      log.info("Authentication already done for principal {}, skipping this filter...", userPrincipal.getName());
      return;
    }
    // only authenticate when @Authenticate is present on resource
    if (resourceInfo.getResourceClass() == null || resourceInfo.getResourceMethod() == null) {
      return;
    }
    if (!(resourceInfo.getResourceClass().isAnnotationPresent(Authenticate.class)
            || resourceInfo.getResourceMethod().isAnnotationPresent(Authenticate.class))) {
      return;
    }
    List<String> authHeaders = headers
            .getRequestHeader(HttpHeaders.AUTHORIZATION);
    if (authHeaders == null || authHeaders.size() != 1) {
      log.info("No Authorization header is available");
      throw toNotAuthorizedException(null, getFaultResponse());
    }
    String[] authPair = StringUtils.split(authHeaders.get(0), " ");
    if (authPair.length != 2 || !AuthScheme.NEGOTIATE.getName().equalsIgnoreCase(authPair[0])) {
      log.info("Negotiate Authorization scheme is expected");
      throw toNotAuthorizedException(null, getFaultResponse());
    }

    byte[] serviceTicket = getServiceTicket(authPair[1]);

    try {
      Subject serviceSubject = loginAndGetSubject();

      GSSContext gssContext = createGSSContext();

      Subject.doAs(serviceSubject, new ValidateServiceTicketAction(gssContext, serviceTicket));

      final GSSName srcName = gssContext.getSrcName();
      if (srcName == null) {
        throw toNotAuthorizedException(null, getFaultResponse());
      }

      String complexUserName = srcName.toString();

      String simpleUserName = complexUserName;
      int index = simpleUserName.lastIndexOf('@');
      if (index > 0) {
        simpleUserName = simpleUserName.substring(0, index);
      }
      context.setSecurityContext(createSecurityContext(simpleUserName, AUTH_SCHEME.getName()));
      if (!gssContext.getCredDelegState()) {
        gssContext.dispose();
        gssContext = null;
      }

    } catch (LoginException e) {
      log.info("Unsuccessful JAAS login for the service principal: " + e.getMessage());
      throw toNotAuthorizedException(e, getFaultResponse());
    } catch (GSSException e) {
      log.info("GSS API exception: " + e.getMessage());
      throw toNotAuthorizedException(e, getFaultResponse());
    } catch (PrivilegedActionException e) {
      log.info("PrivilegedActionException: " + e.getMessage());
      throw toNotAuthorizedException(e, getFaultResponse());
    }
  }

  private SecurityContext createSecurityContext(String simpleUserName, String authScheme) {
    return new LensSecurityContext(simpleUserName, authScheme);
  }

  private GSSContext createGSSContext() throws GSSException {
    Oid oid = new Oid(SPNEGO_OID);
    GSSManager gssManager = GSSManager.getInstance();

    String spn = getCompleteServicePrincipalName();
    GSSName gssService = gssManager.createName(spn, null);

    return gssManager.createContext(gssService.canonicalize(oid),
            oid, null, GSSContext.DEFAULT_LIFETIME);
  }

  private Subject loginAndGetSubject() throws LoginException {

    // The login without a callback can work if
    // - Kerberos keytabs are used with a principal name set in the JAAS config
    // - Kerberos is integrated into the OS logon process
    //   meaning that a process which runs this code has the
    //   user identity

    LoginContext lc = null;
    if (loginConfig != null) {
      lc = new LoginContext("", null, null, loginConfig);
    } else {
      log.info("LoginContext can not be initialized");
      throw new LoginException();
    }
    lc.login();
    return lc.getSubject();
  }

  private byte[] getServiceTicket(String encodedServiceTicket) {
    try {
      return java.util.Base64.getDecoder().decode(encodedServiceTicket);
    } catch (IllegalArgumentException ex) {
      throw toNotAuthorizedException(null, getFaultResponse());
    }
  }

  private static Response getFaultResponse() {
    return Response.status(401).header(HttpHeaders.WWW_AUTHENTICATE, AuthScheme.NEGOTIATE.getName()).build();
  }

  private String getCompleteServicePrincipalName() {
    String name = servicePrincipalName == null
            ? "HTTP/" + uriInfo.getBaseUri().getHost() : servicePrincipalName;
    if (realm != null) {
      name += "@" + realm;
    }
    return name;
  }

  private static final class ValidateServiceTicketAction implements PrivilegedExceptionAction<byte[]> {
    private final GSSContext context;
    private final byte[] token;

    private ValidateServiceTicketAction(GSSContext context, byte[] token) {
      this.context = context;
      this.token = token;
    }

    public byte[] run() throws GSSException {
      byte[] data = context.acceptSecContext(token, 0, token.length);
      return data;
    }
  }

  private static Configuration getJaasKrb5TicketConfig(
          final String principal, final File keytab) {
    return new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        Map<String, String> options = new HashMap<>();
        options.put("principal", principal);
        options.put("keyTab", keytab.getAbsolutePath());
        options.put("doNotPrompt", "true");
        options.put("useKeyTab", "true");
        options.put("storeKey", "true");
        options.put("isInitiator", "false");

        return new AppConfigurationEntry[] {
          new AppConfigurationEntry(KERBEROS_LOGIN_MODULE_NAME,
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options),
        };
      }
    };
  }

  private WebApplicationException toNotAuthorizedException(Throwable cause, Response resp) {
    return new NotAuthorizedException(resp, cause);
  }

}


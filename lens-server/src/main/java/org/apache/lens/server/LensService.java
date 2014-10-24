package org.apache.lens.server;

/*
 * #%L
 * Lens Server
 * %%
 * Copyright (C) 2014 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */



import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.auth.AuthenticationProviderFactory;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HandleIdentifier;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.SessionManager;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensException;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.session.LensSessionImpl;
import org.apache.lens.server.user.UserConfigLoaderFactory;
import org.apache.lens.server.util.UtilityMethods;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class LensService extends CompositeService implements Externalizable {
  public static final Log LOG = LogFactory.getLog(LensService.class);
  private final CLIService cliService;

  protected boolean stopped = false;

  //Static session map which is used by query submission thread to get the
  //lens session before submitting a query to hive server
  protected static ConcurrentHashMap<String, LensSessionHandle> sessionMap =
      new ConcurrentHashMap<String, LensSessionHandle>();

  protected LensService(String name, CLIService cliService) {
    super(name);
    this.cliService = cliService;
  }

  /**
   * @return the cliService
   */
  public CLIService getCliService() {
    return cliService;
  }

  public String getServerDomain() {
    return cliService.getHiveConf().get(LensConfConstants.SERVER_DOMAIN);
  }

  public LensSessionHandle openSession(String username, String password, Map<String, String> configuration)
      throws LensException {
    if (StringUtils.isBlank(username)) {
      throw new BadRequestException("User name cannot be null or empty");
    }
    SessionHandle sessionHandle;
    username = UtilityMethods.removeDomain(username);
    doPasswdAuth(username, password);
    try {
      Map<String, String> sessionConf = new HashMap<String, String>();
      sessionConf.putAll(LensSessionImpl.DEFAULT_HIVE_SESSION_CONF);
      if (configuration != null) {
        sessionConf.putAll(configuration);
      }
      Map<String, String> userConfig = UserConfigLoaderFactory.getUserConfig(username);
      UtilityMethods.mergeMaps(sessionConf, userConfig, false);
      sessionConf.put(LensConfConstants.SESSION_LOGGEDIN_USER, username);
      if(sessionConf.get(LensConfConstants.SESSION_CLUSTER_USER) == null) {
        LOG.info("Didn't get cluster user from user config loader. Setting same as logged in user: " + username);
        sessionConf.put(LensConfConstants.SESSION_CLUSTER_USER, username);
      }
      String clusterUser = sessionConf.get(LensConfConstants.SESSION_CLUSTER_USER);
      password = "useless";
      if (
          cliService.getHiveConf().getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION)
          .equals(HiveAuthFactory.AuthTypes.KERBEROS.toString())
          &&
          cliService.getHiveConf().
          getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS)
          )
      {
        String delegationTokenStr = null;
        try {
          delegationTokenStr = cliService.getDelegationTokenFromMetaStore(username);
        } catch (UnsupportedOperationException e) {
          // The delegation token is not applicable in the given deployment mode
        }
        sessionHandle = cliService.openSessionWithImpersonation(clusterUser, password,
            sessionConf, delegationTokenStr);
      } else {
        sessionHandle = cliService.openSession(clusterUser, password,
            sessionConf);
      }
    } catch (Exception e) {
      throw new LensException (e);
    }
    LensSessionHandle lensSession = new LensSessionHandle(
        sessionHandle.getHandleIdentifier().getPublicId(),
        sessionHandle.getHandleIdentifier().getSecretId());
    sessionMap.put(lensSession.getPublicId().toString(), lensSession);
    return lensSession;
  }

  /**
   * Restore session from previous instance of lens server
   */
  public void restoreSession(LensSessionHandle sessionHandle,
      String userName,
      String password) throws LensException {
    HandleIdentifier handleIdentifier = new HandleIdentifier(sessionHandle.getPublicId(), sessionHandle.getSecretId());
    SessionHandle hiveSessionHandle = new SessionHandle(new TSessionHandle(handleIdentifier.toTHandleIdentifier()));
    try {
      SessionHandle restoredHandle =
          cliService.restoreSession(hiveSessionHandle, userName, password, new HashMap<String, String>());
      LensSessionHandle restoredSession = new LensSessionHandle(
          restoredHandle.getHandleIdentifier().getPublicId(),
          restoredHandle.getHandleIdentifier().getSecretId());
      sessionMap.put(restoredSession.getPublicId().toString(), restoredSession);
    } catch (HiveSQLException e) {
      throw new LensException("Error restoring session " + sessionHandle, e);
    }
  }

  private void doPasswdAuth(String userName, String password){
    // Lens confs to Hive Confs.
    for(ConfVars var: new ConfVars[]{ConfVars.HIVE_SERVER2_PLAIN_LDAP_DOMAIN}) {
      if(cliService.getHiveConf().getVar(var) == null) {
        cliService.getHiveConf().setVar(var,
            cliService.getHiveConf().get(LensConfConstants.SERVER_DOMAIN));
      }
    }
    String authType = cliService.getHiveConf().getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION);
    // No-op when authType is NOSASL
    if (!authType.equalsIgnoreCase(HiveAuthFactory.AuthTypes.NOSASL.toString())) {
      try {
        AuthenticationProviderFactory.AuthMethods authMethod =
            AuthenticationProviderFactory.AuthMethods.getValidAuthMethod(authType);
        PasswdAuthenticationProvider provider =
            AuthenticationProviderFactory.getAuthenticationProvider(authMethod, cliService.getHiveConf());
        provider.Authenticate(userName, password);
      } catch (Exception e) {
        LOG.error("Auth error: " + e);
        throw new NotAuthorizedException(e);
      }
    }
  }

  public void closeSession(LensSessionHandle sessionHandle)
      throws LensException {
    try {
      cliService.closeSession(getHiveSessionHandle(sessionHandle));
      sessionMap.remove(sessionHandle.getPublicId().toString());
    } catch (Exception e) {
      throw new LensException (e);
    }
  }

  public SessionManager getSessionManager() {
    return cliService.getSessionManager();
  }

  public LensSessionImpl getSession(LensSessionHandle sessionHandle) {
    try {
      return ((LensSessionImpl)getSessionManager().getSession(getHiveSessionHandle(sessionHandle)));
    } catch (HiveSQLException exc) {
      LOG.warn("Session " + sessionHandle.getPublicId() + " not found", exc);
      throw new NotFoundException("Session " + sessionHandle.getPublicId() + " not found " + sessionHandle);
    }
  }

  public void acquire(LensSessionHandle sessionHandle) {
    LOG.debug("Acquiring lens session:" + sessionHandle.getPublicId());
    getSession(sessionHandle).acquire();
  }

  /**
   * Acquire a lens session specified by the public UUID
   * @param sessionHandle public UUID of the session
   * @throws LensException if session cannot be acquired
   */
  public void acquire(String sessionHandle) {
    acquire(sessionMap.get(sessionHandle));
  }

  public void release(LensSessionHandle sessionHandle) {
    getSession(sessionHandle).release();
    LOG.debug("Released lens session:" + sessionHandle.getPublicId());
  }

  /**
   * Releases a lens session specified by the public UUID
   * @throws LensException if session cannot be released
   */
  public void release(String sessionHandle) throws LensException {
    getSession(sessionMap.get(sessionHandle)).release();
  }

  protected LensSessionHandle getSessionHandle(String sessionid) {
    return sessionMap.get(sessionid);
  }

  public void addResource(LensSessionHandle sessionHandle, String type,
      String path) throws LensException {
  }

  public void deleteResource(LensSessionHandle sessionHandle, String type,
      String path) throws LensException {
  }

  public static SessionHandle getHiveSessionHandle(LensSessionHandle lensHandle) {
    return new SessionHandle(
        new HandleIdentifier(lensHandle.getPublicId(), lensHandle.getSecretId()), CLIService.SERVER_VERSION);
  }

  public Configuration getLensConf(LensSessionHandle sessionHandle, LensConf conf) throws LensException {
    Configuration qconf = new Configuration(false);
    for (Map.Entry<String, String> entry : getSession(sessionHandle).getSessionConf()) {
      qconf.set(entry.getKey(), entry.getValue());
    }

    if (conf != null && !conf.getProperties().isEmpty()) {
      for (Map.Entry<String, String> entry : conf.getProperties().entrySet()) {
        qconf.set(entry.getKey(), entry.getValue());
      }
    }
    return qconf;
  }

  public Configuration getLensConf(LensConf conf) throws LensException {
    Configuration qconf = LensSessionImpl.createDefaultConf();

    if (conf != null && !conf.getProperties().isEmpty()) {
      for (Map.Entry<String, String> entry : conf.getProperties().entrySet()) {
        qconf.set(entry.getKey(), entry.getValue());
      }
    }
    return qconf;
  }

  public void prepareStopping() {
    this.stopped = true;
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException,
  ClassNotFoundException {
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
  }

}

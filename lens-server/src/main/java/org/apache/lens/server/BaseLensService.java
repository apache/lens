/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server;

import static org.apache.lens.server.error.LensServerErrorCode.SESSION_CLOSED;
import static org.apache.lens.server.error.LensServerErrorCode.SESSION_ID_NOT_PROVIDED;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.session.UserSessionInfo;
import org.apache.lens.api.util.PathValidator;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.LensService;
import org.apache.lens.server.api.SessionValidator;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.LensEvent;
import org.apache.lens.server.api.events.LensEventService;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.error.LensServerErrorCode;
import org.apache.lens.server.query.QueryExecutionServiceImpl;
import org.apache.lens.server.session.LensSessionImpl;
import org.apache.lens.server.user.UserConfigLoaderFactory;
import org.apache.lens.server.util.UtilityMethods;

import org.apache.commons.lang3.StringUtils;
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
import org.apache.hive.service.rpc.thrift.TSessionHandle;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class LensService.
 */
@Slf4j
public abstract class BaseLensService extends CompositeService implements Externalizable, LensService,
  SessionValidator {

  /** The cli service. */
  private final CLIService cliService;

  /** The stopped. */
  protected boolean stopped = false;

  /** Utility to validate and get valid paths for input paths **/
  private PathValidator pathValidator;

  // Static session map which is used by query submission thread to get the
  // lens session before submitting a query to hive server
  /** The session map. */
  protected static final ConcurrentHashMap<String, LensSessionHandle> SESSION_MAP
    = new ConcurrentHashMap<>();

  /**
   * This map maintains active session count for each user
   * Key: userName
   * Value: number of sessions opened
   */
  private static final Map<String, Integer> SESSIONS_PER_USER = new ConcurrentHashMap<>();

  /**
   * Maintains a map with user to SessionUser instance.
   * This map is used for acquiring a lock on specific user for while opening & closing sessions
   */
  private static final Map<String, SessionUser> SESSION_USER_INSTANCE_MAP = new HashMap<>();

  private final int maxNumSessionsPerUser;

  /**
   * Instantiates a new lens service.
   *
   * @param name       the name
   * @param cliService the cli service
   */
  protected BaseLensService(String name, CLIService cliService) {
    super(name);
    this.cliService = cliService;
    maxNumSessionsPerUser = getMaximumNumberOfSessionsPerUser();
  }

  private static class SessionUser {
    private String sessionUser;

    public SessionUser(String user) {
      this.sessionUser = user;
    }
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

  public static int getNumberOfSessions() {
    return BaseLensService.SESSION_MAP.size();
  }

  private static int getMaximumNumberOfSessionsPerUser() {
    return LensServerConf.getHiveConf().getInt(LensConfConstants.MAX_SESSIONS_PER_USER,
      LensConfConstants.DEFAULT_MAX_SESSIONS_PER_USER);
  }

  private boolean isMaxSessionsLimitReachedPerUser(String userName) {
    Integer numSessions = SESSIONS_PER_USER.get(userName);
    return numSessions != null && numSessions >= maxNumSessionsPerUser;
  }


  /**
   * Open session.
   *
   * @param username      the username
   * @param password      the password
   * @param configuration the configuration
   * @return the lens session handle
   * @throws LensException the lens exception
   */
  public LensSessionHandle openSession(String username, String password, Map<String, String> configuration)
    throws LensException {
    return openSession(username, password, configuration, true);
  }

  public LensSessionHandle openSession(String username, String password, Map<String, String> configuration,
      boolean auth) throws LensException {
    if (StringUtils.isBlank(username)) {
      throw new BadRequestException("User name cannot be null or empty");
    }
    SessionHandle sessionHandle;
    username = UtilityMethods.removeDomain(username);
    if (auth) {
      doPasswdAuth(username, password);
    }
    SessionUser sessionUser = SESSION_USER_INSTANCE_MAP.get(username);
    if (sessionUser == null) {
      sessionUser = new SessionUser(username);
      SESSION_USER_INSTANCE_MAP.put(username, sessionUser);
    }
    synchronized (sessionUser) {
      if (isMaxSessionsLimitReachedPerUser(username)) {
        log.error("Can not open new session as session limit {} is reached already for {} user",
            maxNumSessionsPerUser, username);
        throw new LensException(LensServerErrorCode.TOO_MANY_OPEN_SESSIONS.getLensErrorInfo(), username,
            maxNumSessionsPerUser);
      }
      try {
        Map<String, String> sessionConf = new HashMap<String, String>();
        sessionConf.putAll(LensSessionImpl.DEFAULT_HIVE_SESSION_CONF);
        if (configuration != null) {
          sessionConf.putAll(configuration);
        }
        Map<String, String> userConfig = UserConfigLoaderFactory.getUserConfig(username);
        log.info("Got user config: {}", userConfig);
        UtilityMethods.mergeMaps(sessionConf, userConfig, false);
        sessionConf.put(LensConfConstants.SESSION_LOGGEDIN_USER, username);
        if (sessionConf.get(LensConfConstants.SESSION_CLUSTER_USER) == null) {
          log.info("Didn't get cluster user from user config loader. Setting same as logged in user: {}", username);
          sessionConf.put(LensConfConstants.SESSION_CLUSTER_USER, username);
        }
        String clusterUser = sessionConf.get(LensConfConstants.SESSION_CLUSTER_USER);
        password = "useless";
        if (cliService.getHiveConf().getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION)
            .equals(HiveAuthFactory.AuthTypes.KERBEROS.toString())
            && cliService.getHiveConf().getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS)) {
          String delegationTokenStr = null;
          try {
            delegationTokenStr = cliService.getDelegationTokenFromMetaStore(username);
          } catch (UnsupportedOperationException e) {
            // The delegation token is not applicable in the given deployment mode
          }
          sessionHandle = cliService.openSessionWithImpersonation(clusterUser, password, sessionConf,
              delegationTokenStr);
        } else {
          sessionHandle = cliService.openSession(clusterUser, password, sessionConf);
        }
      } catch (Exception e) {
        throw new LensException(e);
      }
      LensSessionHandle lensSessionHandle = new LensSessionHandle(sessionHandle.getHandleIdentifier().getPublicId(),
          sessionHandle.getHandleIdentifier().getSecretId());
      SESSION_MAP.put(lensSessionHandle.getPublicId().toString(), lensSessionHandle);
      updateSessionsPerUser(username);
      return lensSessionHandle;
    }
  }

  private void updateSessionsPerUser(String userName) {
    Integer numOfSessions = SESSIONS_PER_USER.get(userName);
    if (null == numOfSessions) {
      SESSIONS_PER_USER.put(userName, 1);
    } else {
      SESSIONS_PER_USER.put(userName, ++numOfSessions);
    }
  }

  protected LensEventService getEventService() {
    LensEventService eventService = LensServices.get().getService(LensEventService.NAME);
    if (eventService == null) {
      throw new NullPointerException("Could not get event service");
    }
    return eventService;
  }

  protected void notifyEvent(LensEvent event) throws LensException {
    getEventService().notifyEvent(event);
  }

  /**
   * Restore session from previous instance of lens server.
   *
   * @param sessionHandle the session handle
   * @param userName      the user name
   * @param password      the password
   * @throws LensException the lens exception
   */
  public void restoreSession(LensSessionHandle sessionHandle, String userName, String password) throws LensException {
    HandleIdentifier handleIdentifier = new HandleIdentifier(sessionHandle.getPublicId(), sessionHandle.getSecretId());
    SessionHandle hiveSessionHandle = new SessionHandle(new TSessionHandle(handleIdentifier.toTHandleIdentifier()));
    try {
      cliService.createSessionWithSessionHandle(hiveSessionHandle, userName, password,
        new HashMap<String, String>());
      LensSessionHandle restoredSession = new LensSessionHandle(hiveSessionHandle.getHandleIdentifier().getPublicId(),
        hiveSessionHandle.getHandleIdentifier().getSecretId());
      SESSION_MAP.put(restoredSession.getPublicId().toString(), restoredSession);
      updateSessionsPerUser(userName);
    } catch (HiveSQLException e) {
      throw new LensException("Error restoring session " + sessionHandle, e);
    }
  }

  /**
   * Do passwd auth.
   *
   * @param userName the user name
   * @param password the password
   */
  private void doPasswdAuth(String userName, String password) {
    // Lens confs to Hive Confs.
    for (ConfVars var : new ConfVars[]{ConfVars.HIVE_SERVER2_PLAIN_LDAP_DOMAIN}) {
      if (cliService.getHiveConf().getVar(var) == null) {
        cliService.getHiveConf().setVar(var, cliService.getHiveConf().get(LensConfConstants.SERVER_DOMAIN));
      }
    }
    String authType = getHiveConf().getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION);
    // No-op when authType is NOSASL
    if (!authType.equalsIgnoreCase(HiveAuthFactory.AuthTypes.NOSASL.toString())) {
      try {
        AuthenticationProviderFactory.AuthMethods authMethod = AuthenticationProviderFactory.AuthMethods
          .getValidAuthMethod(authType);
        PasswdAuthenticationProvider provider = AuthenticationProviderFactory
          .getAuthenticationProvider(authMethod, getHiveConf());
        provider.Authenticate(userName, password);
      } catch (Exception e) {
        log.error("Auth error: ", e);
        throw new NotAuthorizedException(e);
      }
    }
  }

  /**
   * Close session.
   *
   * @param sessionHandle the session handle
   * @throws LensException the lens exception
   */
  public void closeSession(LensSessionHandle sessionHandle) throws LensException {
    try {
      LensSessionImpl session = getSession(sessionHandle);
      boolean shouldDecrementOpenedSessionCount = !session.getLensSessionPersistInfo().isMarkedForClose();
      if (session.activeOperationsPresent()) {
        session.markForClose();
      } else {
        cliService.closeSession(getHiveSessionHandle(sessionHandle));
        SESSION_MAP.remove(sessionHandle.getPublicId().toString());
        log.info("Closed session {} for {} user", sessionHandle, session.getLoggedInUser());
      }
      if (shouldDecrementOpenedSessionCount) {
        decrementSessionCountForUser(sessionHandle, session.getLoggedInUser());
      }
      if (!SESSION_MAP.containsKey(sessionHandle.getPublicId().toString())) {
        // Inform query service
        BaseLensService svc = LensServices.get().getService(QueryExecutionService.NAME);
        if (svc instanceof QueryExecutionServiceImpl) {
          ((QueryExecutionServiceImpl) svc).closeDriverSessions(sessionHandle);
        }
      }
    } catch (HiveSQLException e) {
      throw new LensException(e);
    }
  }

  private void decrementSessionCountForUser(LensSessionHandle sessionHandle, String userName) {
    SessionUser sessionUser = SESSION_USER_INSTANCE_MAP.get(userName);
    if (sessionUser == null) {
      log.info("Trying to close invalid session {} for user {}", sessionHandle, userName);
      return;
    }
    synchronized (sessionUser) {
      Integer sessionCount = SESSIONS_PER_USER.get(userName);
      if (sessionCount == 1) {
        SESSIONS_PER_USER.remove(userName);
      } else {
        SESSIONS_PER_USER.put(userName, --sessionCount);
      }
    }
  }

  public SessionManager getSessionManager() {
    return cliService.getSessionManager();
  }

  /**
   * Gets the session.
   *
   * @param sessionHandle the session handle
   * @return the session
   */
  public LensSessionImpl getSession(LensSessionHandle sessionHandle) {
    if (sessionHandle == null) {
      throw new ClientErrorException("Session is null", 400);
    }
    try {
      return ((LensSessionImpl) getSessionManager().getSession(getHiveSessionHandle(sessionHandle)));
    } catch (HiveSQLException exc) {
      log.warn("Session {} not found", sessionHandle.getPublicId(), exc);
      // throw resource gone exception (410)
      throw new ClientErrorException("Session " + sessionHandle.getPublicId() + " is invalid " + sessionHandle,
        Response.Status.GONE, exc);
    }
  }

  /**
   * Acquire.
   *
   * @param sessionHandle the session handle
   */
  public void acquire(LensSessionHandle sessionHandle) {
    if (sessionHandle != null) {
      log.debug("Acquiring lens session:{}", sessionHandle.getPublicId());
      getSession(sessionHandle).acquire();
    }
  }

  /**
   * Acquire a lens session specified by the public UUID.
   *
   * @param sessionHandle public UUID of the session
   */
  public void acquire(String sessionHandle) {
    LensSessionHandle handle = SESSION_MAP.get(sessionHandle);

    if (handle == null) {
      throw new NotFoundException("Session handle not found " + sessionHandle);
    }

    acquire(handle);
  }

  /**
   * Release.
   *
   * @param sessionHandle the session handle
   */
  public void release(LensSessionHandle sessionHandle) {
    if (sessionHandle != null) {
      getSession(sessionHandle).release();
      log.debug("Released lens session:{}", sessionHandle.getPublicId());
    }
  }

  /**
   * Releases a lens session specified by the public UUID.
   *
   * @param sessionHandle the session handle
   * @throws LensException if session cannot be released
   */
  public void release(String sessionHandle) throws LensException {
    LensSessionHandle handle = SESSION_MAP.get(sessionHandle);
    if (handle != null) {
      getSession(handle).release();
    }
  }

  /**
   * Gets the session handle.
   *
   * @param sessionid the sessionid
   * @return the session handle
   */
  protected LensSessionHandle getSessionHandle(String sessionid) {
    return SESSION_MAP.get(sessionid);
  }

  /**
   * Gets the hive session handle.
   *
   * @param lensHandle the lens handle
   * @return the hive session handle
   */
  public static SessionHandle getHiveSessionHandle(LensSessionHandle lensHandle) {
    return new SessionHandle(new HandleIdentifier(lensHandle.getPublicId(), lensHandle.getSecretId()),
      CLIService.SERVER_VERSION);
  }

  /**
   * Gets the lens conf.
   *
   * @param sessionHandle the session handle
   * @param conf          the conf
   * @return the lens conf
   * @throws LensException the lens exception
   */
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
    qconf.setClassLoader(getSession(sessionHandle).getClassLoader());
    return qconf;
  }

  /**
   * Gets the lens conf.
   *
   * @param conf the conf
   * @return the lens conf
   * @throws LensException the lens exception
   */
  public Configuration getLensConf(LensConf conf) throws LensException {
    Configuration qconf = LensSessionImpl.createDefaultConf();

    if (conf != null && !conf.getProperties().isEmpty()) {
      for (Map.Entry<String, String> entry : conf.getProperties().entrySet()) {
        qconf.set(entry.getKey(), entry.getValue());
      }
    }
    return qconf;
  }

  /**
   * Prepare stopping.
   */
  public void prepareStopping() {
    this.stopped = true;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
   */
  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
   */
  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
  }


  /**
   * Method that uses PathValidator to get appropriate path.
   *
   * @param path
   * @param shouldBeDirectory
   * @param shouldExist
   * @return
   */
  public String getValidPath(File path, boolean shouldBeDirectory, boolean shouldExist) {
    if (pathValidator == null) {
      LensConf conf = new LensConf();
      pathValidator = new PathValidator(conf);
    }
    return pathValidator.getValidPath(path, shouldBeDirectory, shouldExist);
  }

  /**
   * Method to remove unrequired prefix from path.
   *
   * @param path
   * @return
   */
  public String removePrefixBeforeURI(String path) {
    if (pathValidator == null) {
      LensConf conf = new LensConf();
      pathValidator = new PathValidator(conf);
    }
    return pathValidator.removePrefixBeforeURI(path);
  }

  @Override
  public void validateSession(LensSessionHandle handle) throws LensException {
    if (handle == null) {
      throw new LensException(SESSION_ID_NOT_PROVIDED.getLensErrorInfo());
    }
    if (!getSession(handle).isActive()) {
      throw new LensException(SESSION_CLOSED.getLensErrorInfo(), handle);
    }
  }

  public class SessionContext implements AutoCloseable {
    private LensSessionHandle sessionHandle;

    public SessionContext(LensSessionHandle sessionHandle) {
      this.sessionHandle = sessionHandle;
      acquire(sessionHandle);
    }
    @Override
    public void close() {
      release(sessionHandle);
    }
  }

  public List<UserSessionInfo> getSessionInfo() {
    List<UserSessionInfo> userSessionInfoList = new ArrayList<>();
    for (LensSessionHandle handle : SESSION_MAP.values()) {
      LensSessionImpl session = getSession(handle);
      UserSessionInfo sessionInfo = new UserSessionInfo();
      sessionInfo.setHandle(handle.getPublicId().toString());
      sessionInfo.setUserName(session.getLoggedInUser());
      sessionInfo.setActiveQueries(session.getActiveQueries());
      sessionInfo.setCreationTime(session.getCreationTime());
      sessionInfo.setLastAccessTime(session.getLastAccessTime());
      userSessionInfoList.add(sessionInfo);
    }
    return userSessionInfoList;
  }
}


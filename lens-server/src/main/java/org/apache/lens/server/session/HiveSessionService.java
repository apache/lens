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
package org.apache.lens.server.session;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import java.util.concurrent.*;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.server.BaseLensService;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.health.HealthStatus;
import org.apache.lens.server.api.session.*;
import org.apache.lens.server.session.LensSessionImpl.ResourceEntry;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.SystemVariables;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;

import com.google.common.collect.Maps;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * The Class HiveSessionService.
 */
@Slf4j
public class HiveSessionService extends BaseLensService implements SessionService {


  /** The restorable sessions. */
  private List<LensSessionImpl.LensSessionPersistInfo> restorableSessions;

  /** The session expiry thread. */
  private ScheduledExecutorService sessionExpiryThread;

  /** The session expiry runnable. */
  private Runnable sessionExpiryRunnable = new SessionExpiryRunnable();

  /** Service to manage database specific resources */
  @Getter(AccessLevel.PROTECTED)
  private DatabaseResourceService databaseResourceService;

  /**
   * The conf.
   */
  private Configuration conf;

  /**
   * Instantiates a new hive session service.
   *
   * @param cliService the cli service
   */
  public HiveSessionService(CLIService cliService) {
    super(NAME, cliService);
  }

  @Override
  public List<String> listAllResources(LensSessionHandle sessionHandle, String type) {
    if (!isValidResouceType(type)) {
      throw new BadRequestException("Bad resource type is passed. Please pass jar or file as source type");
    }
    List<ResourceEntry> resources = getSession(sessionHandle).getResources();
    List<String> allResources = new ArrayList<String>();
    for (ResourceEntry resource : resources) {
      if (type == null || resource.getType().equalsIgnoreCase(type)) {
        allResources.add(resource.toString());
      }
    }
    return allResources;
  }

  private boolean isValidResouceType(String type) {
    return (type == null || type.equalsIgnoreCase("jar") || type.equalsIgnoreCase("file"));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addResource(LensSessionHandle sessionid, String type, String path) {
    try {
      acquire(sessionid);
      SessionState ss = getSession(sessionid).getSessionState();
      String finalLocation = ss.add_resource(SessionState.ResourceType.valueOf(type.toUpperCase()), path);
      getSession(sessionid).addResource(type, path, finalLocation);
    } catch (RuntimeException e) {
      log.error("Failed to add resource type:" + type + " path:" + path + " in session", e);
      throw new WebApplicationException(e);
    } finally {
      release(sessionid);
    }
  }

  private void addResourceUponRestart(LensSessionHandle sessionid, ResourceEntry resourceEntry) {
    try {
      acquire(sessionid);
      SessionState ss = getSession(sessionid).getSessionState();
      resourceEntry.location = ss.add_resource(SessionState.ResourceType.valueOf(resourceEntry.getType()),
        resourceEntry.getUri());
      if (resourceEntry.location == null) {
        throw new NullPointerException("Resource's final location cannot be null");
      }
    } finally {
      release(sessionid);
    }
  }
  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteResource(LensSessionHandle sessionid, String type, String path) {
    String command = "delete " + type.toLowerCase() + " " + path;
    try {
      acquire(sessionid);
      closeCliServiceOp(getCliService().executeStatement(getHiveSessionHandle(sessionid), command, null));
      getSession(sessionid).removeResource(type, path);
    } catch (HiveSQLException e) {
      throw new WebApplicationException(e);
    } finally {
      release(sessionid);
    }
  }

  /**
   * Gets the session param.
   *
   * @param sessionConf the session conf
   * @param ss          the ss
   * @param varname     the varname
   * @return the session param
   */
  private String getSessionParam(Configuration sessionConf, SessionState ss, String varname) {
    if (varname.indexOf(SystemVariables.HIVEVAR_PREFIX) == 0) {
      String var = varname.substring(SystemVariables.HIVEVAR_PREFIX.length());
      if (ss.getHiveVariables().get(var) != null) {
        return SystemVariables.HIVEVAR_PREFIX + var + "=" + ss.getHiveVariables().get(var);
      } else {
        throw new NotFoundException(varname + " is undefined as a hive variable");
      }
    } else {
      String var;
      if (varname.indexOf(SystemVariables.HIVECONF_PREFIX) == 0) {
        var = varname.substring(SystemVariables.HIVECONF_PREFIX.length());
      } else {
        var = varname;
      }
      if (sessionConf.get(var) != null) {
        return varname + "=" + sessionConf.get(var);
      } else {
        throw new NotFoundException(varname + " is undefined");
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LensSessionHandle openSession(String username, String password, String database,
    Map<String, String> configuration)
    throws LensException {
    LensSessionHandle sessionid = super.openSession(username, password, configuration);
    log.info("Opened session " + sessionid + " for user " + username);
    notifyEvent(new SessionOpened(System.currentTimeMillis(), sessionid, username));

    // Set current database
    if (StringUtils.isNotBlank(database)) {
      try {
        if (!Hive.get(getSession(sessionid).getHiveConf()).databaseExists(database)) {
          closeSession(sessionid);
          log.info("Closed session " + sessionid.getPublicId().toString() + " as db " + database + " does not exist");
          throw new NotFoundException("Database " + database + " does not exist");
        }
      } catch (Exception e) {
        if (!(e instanceof NotFoundException)) {
          try {
            closeSession(sessionid);
          } catch (LensException e2) {
            log.error("Error closing session " + sessionid.getPublicId().toString(), e2);
          }

          log.error("Error in checking if database exists " + database, e);
          throw new LensException("Error in checking if database exists" + database, e);
        } else {
          throw (NotFoundException) e;
        }
      }

      getSession(sessionid).setCurrentDatabase(database);
      log.info("Set database to " + database + " for session " + sessionid.getPublicId());
    }

    // add auxuiliary jars
    String[] auxJars = getSession(sessionid).getSessionConf().getStrings(LensConfConstants.AUX_JARS);

    if (auxJars != null) {
      for (String jar : auxJars) {
        log.info("Adding aux jar:" + jar);
        addResource(sessionid, "jar", jar);
      }
    }
    return sessionid;
  }

  @Override
  public boolean isOpen(LensSessionHandle sessionHandle) {
    return SESSION_MAP.containsKey(sessionHandle.getPublicId().toString());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<String> getAllSessionParameters(LensSessionHandle sessionid, boolean verbose, String key)
    throws LensException {
    List<String> result = new ArrayList<String>();
    acquire(sessionid);
    try {
      SessionState ss = getSession(sessionid).getSessionState();
      if (!StringUtils.isBlank(key)) {
        result.add(getSessionParam(getSession(sessionid).getSessionConf(), ss, key));
      } else {
        SortedMap<String, String> sortedMap = new TreeMap<String, String>();
        sortedMap.put("silent", (ss.getIsSilent() ? "on" : "off"));
        for (String s : ss.getHiveVariables().keySet()) {
          sortedMap.put(SystemVariables.HIVEVAR_PREFIX + s, ss.getHiveVariables().get(s));
        }
        for (Map.Entry<String, String> entry : getSession(sessionid).getSessionConf()) {
          sortedMap.put(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, String> entry : sortedMap.entrySet()) {
          result.add(entry.toString());
        }
      }
    } finally {
      release(sessionid);
    }
    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setSessionParameter(LensSessionHandle sessionid, String key, String value) {
    setSessionParameter(sessionid, key, value, true);
  }
  /**
   * Sets the session parameter.
   *
   * @param sessionid    the sessionid
   * @param config       map of string-string. each entry represents key and the value to be set for that key
   * @param addToSession the add to session
   */

  protected void setSessionParameters(LensSessionHandle sessionid, Map<String, String> config, boolean addToSession) {
    log.info("Request to Set params:" + config);
    try {
      acquire(sessionid);
      // set in session conf
      for(Map.Entry<String, String> entry: config.entrySet()) {
        String var = entry.getKey();
        if (var.indexOf(SystemVariables.HIVECONF_PREFIX) == 0) {
          var = var.substring(SystemVariables.HIVECONF_PREFIX.length());
        }
        getSession(sessionid).getSessionConf().set(var, entry.getValue());
        if (addToSession) {
          String command = "set" + " " + entry.getKey() + "= " + entry.getValue();
          closeCliServiceOp(getCliService().executeStatement(getHiveSessionHandle(sessionid), command, null));
        } else {
          getSession(sessionid).getHiveConf().set(entry.getKey(), entry.getValue());
        }
      }
      // add to persist
      if (addToSession) {
        getSession(sessionid).setConfig(config);
      }
      log.info("Set params:" + config);
    } catch (HiveSQLException e) {
      throw new WebApplicationException(e);
    } finally {
      release(sessionid);
    }
  }
    /**
     * Sets the session parameter.
     *
     * @param sessionid    the sessionid
     * @param key          the key
     * @param value        the value
     * @param addToSession the add to session
     */
  protected void setSessionParameter(LensSessionHandle sessionid, String key, String value, boolean addToSession) {
    HashMap<String, String> config = Maps.newHashMap();
    config.put(key, value);
    setSessionParameters(sessionid, config, addToSession);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hive.service.CompositeService#init()
   */
  @Override
  public synchronized void init(HiveConf hiveConf) {
    this.databaseResourceService = new DatabaseResourceService(DatabaseResourceService.NAME);
    addService(this.databaseResourceService);
    this.conf = hiveConf;
    super.init(hiveConf);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hive.service.CompositeService#start()
   */
  @Override
  public synchronized void start() {
    super.start();

    sessionExpiryThread = Executors.newSingleThreadScheduledExecutor();
    int sessionExpiryInterval = getSessionExpiryInterval();
    sessionExpiryThread.scheduleWithFixedDelay(sessionExpiryRunnable, sessionExpiryInterval,
        sessionExpiryInterval, TimeUnit.SECONDS);

    // Restore sessions if any
    if (restorableSessions == null || restorableSessions.size() <= 0) {
      log.info("No sessions to restore");
      return;
    }

    for (LensSessionImpl.LensSessionPersistInfo persistInfo : restorableSessions) {
      try {
        LensSessionHandle sessionHandle = persistInfo.getSessionHandle();
        restoreSession(sessionHandle, persistInfo.getUsername(), persistInfo.getPassword());
        LensSessionImpl session = getSession(sessionHandle);
        session.setLastAccessTime(persistInfo.getLastAccessTime());
        session.getLensSessionPersistInfo().setConfig(persistInfo.getConfig());
        session.getLensSessionPersistInfo().setResources(persistInfo.getResources());
        session.setCurrentDatabase(persistInfo.getDatabase());
        session.getLensSessionPersistInfo().setMarkedForClose(persistInfo.isMarkedForClose());

        // Add resources for restored sessions
        for (LensSessionImpl.ResourceEntry resourceEntry : session.getResources()) {
          try {
            addResourceUponRestart(sessionHandle, resourceEntry);
          } catch (Exception e) {
            log.error("Failed to restore resource for session: " + session + " resource: " + resourceEntry, e);
          }
        }

        // Add config for restored sessions
        try{
          setSessionParameters(sessionHandle, session.getConfig(), false);
        } catch (Exception e) {
          log.error("Error setting parameters " + session.getConfig()
            + " for session: " + session, e);
        }
        log.info("Restored session " + persistInfo.getSessionHandle().getPublicId());
        notifyEvent(new SessionRestored(System.currentTimeMillis(), sessionHandle));
      } catch (LensException e) {
        throw new RuntimeException(e);
      }
    }
    log.info("Session service restored " + restorableSessions.size() + " sessions");
  }

  private int getSessionExpiryInterval() {
    return conf.getInt(LensConfConstants.SESSION_EXPIRY_SERVICE_INTERVAL_IN_SECS,
        LensConfConstants.DEFAULT_SESSION_EXPIRY_SERVICE_INTERVAL_IN_SECS);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hive.service.CompositeService#stop()
   */
  @Override
  public synchronized void stop() {
    super.stop();
    if (sessionExpiryThread != null) {
      sessionExpiryThread.shutdownNow();
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.LensService#writeExternal(java.io.ObjectOutput)
   */
  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // Write out all the sessions
    out.writeInt(SESSION_MAP.size());
    for (LensSessionHandle sessionHandle : SESSION_MAP.values()) {
      LensSessionImpl session = getSession(sessionHandle);
      session.getLensSessionPersistInfo().writeExternal(out);
    }
    log.info("Session service pesristed " + SESSION_MAP.size() + " sessions");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HealthStatus getHealthStatus() {
    return this.getServiceState().equals(STATE.STARTED)
        ? new HealthStatus(true, "Hive session service is healthy.")
        : new HealthStatus(false, "Hive session service is down.");
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.LensService#readExternal(java.io.ObjectInput)
   */
  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    int numSessions = in.readInt();
    restorableSessions = new ArrayList<LensSessionImpl.LensSessionPersistInfo>();

    for (int i = 0; i < numSessions; i++) {
      LensSessionImpl.LensSessionPersistInfo persistInfo = new LensSessionImpl.LensSessionPersistInfo();
      persistInfo.readExternal(in);
      restorableSessions.add(persistInfo);
      SESSION_MAP.put(persistInfo.getSessionHandle().getPublicId().toString(), persistInfo.getSessionHandle());
    }
    log.info("Session service recovered " + SESSION_MAP.size() + " sessions");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void closeSession(LensSessionHandle sessionHandle) throws LensException {
    closeInternal(sessionHandle);
    notifyEvent(new SessionClosed(System.currentTimeMillis(), sessionHandle));
  }

  @Override
  public void cleanupIdleSessions() throws LensException {
    ScheduledFuture<?> schedule = sessionExpiryThread.schedule(sessionExpiryRunnable, 0, TimeUnit.MILLISECONDS);
    // wait till completion
    try {
      schedule.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new LensException(e);
    }
  }

  /**
   * Close a Lens server session
   * @param sessionHandle session handle
   * @throws LensException
   */
  private void closeInternal(LensSessionHandle sessionHandle) throws LensException {
    super.closeSession(sessionHandle);
  }

  /**
   * Close operation created for underlying CLI service
   * @param op operation handle
   */
  private void closeCliServiceOp(OperationHandle op) {
    if (op != null) {
      try {
        getCliService().closeOperation(op);
      } catch (HiveSQLException e) {
        log.error("Error closing operation " + op.getHandleIdentifier(), e);
      }
    }
  }

  Runnable getSessionExpiryRunnable() {
    return sessionExpiryRunnable;
  }

  /**
   * The Class SessionExpiryRunnable.
   */
  public class SessionExpiryRunnable implements Runnable {

    /**
     * Run internal.
     */
    public void runInternal() {
      List<LensSessionHandle> sessionsToRemove = new ArrayList<LensSessionHandle>(SESSION_MAP.values());
      Iterator<LensSessionHandle> itr = sessionsToRemove.iterator();
      while (itr.hasNext()) {
        LensSessionHandle sessionHandle = itr.next();
        try {
          LensSessionImpl session = getSession(sessionHandle);
          if (session.isActive()) {
            itr.remove();
          }
        } catch (ClientErrorException nfe) {
          itr.remove();
        }
      }

      // Now close all inactive sessions
      for (LensSessionHandle sessionHandle : sessionsToRemove) {
        try {
          long lastAccessTime = getSession(sessionHandle).getLastAccessTime();
          closeInternal(sessionHandle);
          log.info("Closed inactive session " + sessionHandle.getPublicId() + " last accessed at "
            + new Date(lastAccessTime));
          notifyEvent(new SessionExpired(System.currentTimeMillis(), sessionHandle));
        } catch (ClientErrorException nfe) {
          // Do nothing
        } catch (LensException e) {
          log.error("Error closing session " + sessionHandle.getPublicId() + " reason " + e.getMessage(), e);
        }
      }
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
      try {
        runInternal();
      } catch (Exception e) {
        log.warn("Unknown error while checking for inactive sessions - " + e.getMessage());
      }
    }
  }

}

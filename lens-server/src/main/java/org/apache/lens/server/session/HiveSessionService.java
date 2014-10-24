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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.processors.SetProcessor;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.*;
import org.apache.lens.api.LensException;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.server.LensService;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.query.QueryExecutionServiceImpl;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The Class HiveSessionService.
 */
public class HiveSessionService extends LensService {

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(HiveSessionService.class);

  /** The Constant NAME. */
  public static final String NAME = "session";

  /** The restorable sessions. */
  private List<LensSessionImpl.LensSessionPersistInfo> restorableSessions;

  /** The session expiry thread. */
  private ScheduledExecutorService sessionExpiryThread;

  /** The session expiry runnable. */
  private Runnable sessionExpiryRunnable = new SessionExpiryRunnable();

  /**
   * Instantiates a new hive session service.
   *
   * @param cliService
   *          the cli service
   */
  public HiveSessionService(CLIService cliService) {
    super(NAME, cliService);
  }

  /**
   * Adds the resource to all services.
   *
   * @param sessionid
   *          the sessionid
   * @param type
   *          the type
   * @param path
   *          the path
   * @return the int
   */
  public int addResourceToAllServices(LensSessionHandle sessionid, String type, String path) {
    int numAdded = 0;
    boolean error = false;
    for (LensService service : LensServices.get().getLensServices()) {
      try {
        service.addResource(sessionid, type, path);
        numAdded++;
      } catch (LensException e) {
        LOG.error("Failed to add resource type:" + type + " path:" + path + " in service:" + service, e);
        error = true;
        break;
      }
    }
    if (!error) {
      getSession(sessionid).addResource(type, path);
    }
    return numAdded;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.LensService#addResource(org.apache.lens.api.LensSessionHandle, java.lang.String,
   * java.lang.String)
   */
  public void addResource(LensSessionHandle sessionid, String type, String path) {
    String command = "add " + type.toLowerCase() + " " + path;
    try {
      acquire(sessionid);
      getCliService().executeStatement(getHiveSessionHandle(sessionid), command, null);
    } catch (HiveSQLException e) {
      throw new WebApplicationException(e);
    } finally {
      release(sessionid);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.LensService#deleteResource(org.apache.lens.api.LensSessionHandle, java.lang.String,
   * java.lang.String)
   */
  public void deleteResource(LensSessionHandle sessionid, String type, String path) {
    String command = "delete " + type.toLowerCase() + " " + path;
    try {
      acquire(sessionid);
      getCliService().executeStatement(getHiveSessionHandle(sessionid), command, null);
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
   * @param sessionConf
   *          the session conf
   * @param ss
   *          the ss
   * @param varname
   *          the varname
   * @return the session param
   */
  private String getSessionParam(Configuration sessionConf, SessionState ss, String varname) {
    if (varname.indexOf(SetProcessor.HIVEVAR_PREFIX) == 0) {
      String var = varname.substring(SetProcessor.HIVEVAR_PREFIX.length());
      if (ss.getHiveVariables().get(var) != null) {
        return SetProcessor.HIVEVAR_PREFIX + var + "=" + ss.getHiveVariables().get(var);
      } else {
        throw new NotFoundException(varname + " is undefined as a hive variable");
      }
    } else {
      String var;
      if (varname.indexOf(SetProcessor.HIVECONF_PREFIX) == 0) {
        var = varname.substring(SetProcessor.HIVECONF_PREFIX.length());
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

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.LensService#openSession(java.lang.String, java.lang.String, java.util.Map)
   */
  public LensSessionHandle openSession(String username, String password, Map<String, String> configuration)
      throws LensException {
    LensSessionHandle sessionid = super.openSession(username, password, configuration);
    LOG.info("Opened session " + sessionid + " for user " + username);
    // add auxuiliary jars
    String[] auxJars = getSession(sessionid).getSessionConf().getStrings(LensConfConstants.AUX_JARS);
    if (auxJars != null) {
      LOG.info("Adding aux jars:" + auxJars);
      for (String jar : auxJars) {
        addResourceToAllServices(sessionid, "jar", jar);
      }
    }
    return sessionid;
  }

  /**
   * Gets the all session parameters.
   *
   * @param sessionid
   *          the sessionid
   * @param verbose
   *          the verbose
   * @param key
   *          the key
   * @return the all session parameters
   * @throws LensException
   *           the lens exception
   */
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
          sortedMap.put(SetProcessor.HIVEVAR_PREFIX + s, ss.getHiveVariables().get(s));
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
   * Sets the session parameter.
   *
   * @param sessionid
   *          the sessionid
   * @param key
   *          the key
   * @param value
   *          the value
   */
  public void setSessionParameter(LensSessionHandle sessionid, String key, String value) {
    setSessionParameter(sessionid, key, value, true);
  }

  /**
   * Sets the session parameter.
   *
   * @param sessionid
   *          the sessionid
   * @param key
   *          the key
   * @param value
   *          the value
   * @param addToSession
   *          the add to session
   */
  protected void setSessionParameter(LensSessionHandle sessionid, String key, String value, boolean addToSession) {
    LOG.info("Request to Set param key:" + key + " value:" + value);
    String command = "set" + " " + key + "= " + value;
    try {
      acquire(sessionid);
      // set in session conf
      String var;
      if (key.indexOf(SetProcessor.HIVECONF_PREFIX) == 0) {
        var = key.substring(SetProcessor.HIVECONF_PREFIX.length());
      } else {
        var = key;
      }
      getSession(sessionid).getSessionConf().set(var, value);
      // set in underlying cli session
      getCliService().executeStatement(getHiveSessionHandle(sessionid), command, null);
      // add to persist
      if (addToSession) {
        getSession(sessionid).setConfig(key, value);
      }
      LOG.info("Set param key:" + key + " value:" + value);
    } catch (HiveSQLException e) {
      throw new WebApplicationException(e);
    } finally {
      release(sessionid);
    }
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
    sessionExpiryThread.scheduleWithFixedDelay(sessionExpiryRunnable, 60, 60, TimeUnit.MINUTES);

    // Restore sessions if any
    if (restorableSessions == null || restorableSessions.size() <= 0) {
      LOG.info("No sessions to restore");
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

        // Add resources for restored sessions
        for (LensSessionImpl.ResourceEntry resourceEntry : session.getResources()) {
          try {
            addResource(sessionHandle, resourceEntry.getType(), resourceEntry.getLocation());
          } catch (Exception e) {
            LOG.error("Failed to restore resource for session: " + session + " resource: " + resourceEntry);
            throw new RuntimeException(e);
          }
        }

        // Add config for restored sessions
        for (Map.Entry<String, String> cfg : session.getConfig().entrySet()) {
          try {
            setSessionParameter(sessionHandle, cfg.getKey(), cfg.getValue(), false);
          } catch (Exception e) {
            LOG.error("Error setting parameter " + cfg.getKey() + "=" + cfg.getValue() + " for session: " + session);
          }
        }
        LOG.info("Restored session " + persistInfo.getSessionHandle().getPublicId());
      } catch (LensException e) {
        throw new RuntimeException(e);
      }
    }
    LOG.info("Session service restoed " + restorableSessions.size() + " sessions");
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
    out.writeInt(sessionMap.size());
    for (LensSessionHandle sessionHandle : sessionMap.values()) {
      LensSessionImpl session = getSession(sessionHandle);
      session.getLensSessionPersistInfo().writeExternal(out);
    }
    LOG.info("Session service pesristed " + sessionMap.size() + " sessions");
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
      sessionMap.put(persistInfo.getSessionHandle().getPublicId().toString(), persistInfo.getSessionHandle());
    }
    LOG.info("Session service recovered " + sessionMap.size() + " sessions");
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.LensService#closeSession(org.apache.lens.api.LensSessionHandle)
   */
  @Override
  public void closeSession(LensSessionHandle sessionHandle) throws LensException {
    super.closeSession(sessionHandle);
    // Inform query service
    LensService svc = LensServices.get().getService(QueryExecutionServiceImpl.NAME);
    if (svc instanceof QueryExecutionServiceImpl) {
      ((QueryExecutionServiceImpl) svc).closeDriverSessions(sessionHandle);
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
      List<LensSessionHandle> sessionsToRemove = new ArrayList<LensSessionHandle>(sessionMap.values());
      Iterator<LensSessionHandle> itr = sessionsToRemove.iterator();
      while (itr.hasNext()) {
        LensSessionHandle sessionHandle = itr.next();
        try {
          LensSessionImpl session = getSession(sessionHandle);
          if (session.isActive()) {
            itr.remove();
          }
        } catch (NotFoundException nfe) {
          itr.remove();
        }
      }

      // Now close all inactive sessions
      for (LensSessionHandle sessionHandle : sessionsToRemove) {
        try {
          long lastAccessTime = getSession(sessionHandle).getLastAccessTime();
          closeSession(sessionHandle);
          LOG.info("Closed inactive session " + sessionHandle.getPublicId() + " last accessed at "
              + new Date(lastAccessTime));
        } catch (NotFoundException nfe) {
          // Do nothing
        } catch (LensException e) {
          LOG.error("Error closing session " + sessionHandle.getPublicId() + " reason " + e.getMessage());
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
        LOG.warn("Unknown error while checking for inactive sessions - " + e.getMessage());
      }
    }
  }

}

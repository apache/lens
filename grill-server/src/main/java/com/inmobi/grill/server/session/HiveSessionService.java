package com.inmobi.grill.server.session;

/*
 * #%L
 * Grill Server
 * %%
 * Copyright (C) 2014 Inmobi
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

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.server.GrillService;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.query.QueryExecutionServiceImpl;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.processors.SetProcessor;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.*;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HiveSessionService extends GrillService {
  public static final Log LOG = LogFactory.getLog(HiveSessionService.class);
  public static final String NAME = "session";
  private List<GrillSessionImpl.GrillSessionPersistInfo> restorableSessions;
  private ScheduledExecutorService sessionExpiryThread;
  private Runnable sessionExpiryRunnable = new SessionExpiryRunnable();

  public HiveSessionService(CLIService cliService) {
    super(NAME, cliService);
  }

  public void addResource(GrillSessionHandle sessionid, String type, String path) {
    addResource(sessionid, type, path, true);
  }

  private void addResource(GrillSessionHandle sessionid, String type, String path, boolean addToSession) {
    String command = "add " + type.toLowerCase() + " " + path;
    try {
      acquire(sessionid);
      getCliService().executeStatement(getHiveSessionHandle(sessionid), command, null);

      if (addToSession) {
        getSession(sessionid).addResource(type, path);
      }

    } catch (HiveSQLException e) {
      throw new WebApplicationException(e);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    } finally {
      try {
        release(sessionid);
      } catch (GrillException e) {
        throw new WebApplicationException(e);
      }
    }
  }

  public void deleteResource(GrillSessionHandle sessionid, String type, String path) {
    String command = "delete " + type.toLowerCase() + " " + path;
    try {
      acquire(sessionid);
      getCliService().executeStatement(getHiveSessionHandle(sessionid), command, null);
      getSession(sessionid).removeResource(type, path);
    } catch (HiveSQLException e) {
      throw new WebApplicationException(e);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    } finally {
      try {
        release(sessionid);
      } catch (GrillException e) {
        throw new WebApplicationException(e);
      }
    }
  }

  private String getSessionParam(Configuration sessionConf, SessionState ss, String varname) {
    if (varname.indexOf(SetProcessor.HIVEVAR_PREFIX) == 0) {
      String var = varname.substring(SetProcessor.HIVEVAR_PREFIX.length());
      if (ss.getHiveVariables().get(var) != null){
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
      if (sessionConf.get(var) != null){
        return varname + "=" + sessionConf.get(var);
      } else {
        throw new NotFoundException(varname + " is undefined");
      }
    }
  }

  public GrillSessionHandle openSession(String username, String password, Map<String, String> configuration)
      throws GrillException {
    GrillSessionHandle sessionid = super.openSession(username, password, configuration);
    // add auxuiliary jars
    String[] auxJars = getSession(sessionid).getSessionConf().getStrings(GrillConfConstants.AUX_JARS);
    if (auxJars != null) {
      LOG.info("Adding aux jars:" + auxJars);
      for (String jar : auxJars) {
        addResource(sessionid, "jar", jar);
      }
    }
    return sessionid;
  }

  public List<String> getAllSessionParameters(GrillSessionHandle sessionid,
      boolean verbose, String key) throws GrillException {
    List<String> result = new ArrayList<String>();
    acquire(sessionid);
    SessionState ss = getSession(sessionid).getSessionState();
    if (!StringUtils.isBlank(key)) {
      result.add(getSessionParam(getSession(sessionid).getSessionConf(), ss, key));
    } else {
      try {
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
      } catch (GrillException e) {
        throw new WebApplicationException(e);
      } finally {
        try {
          release(sessionid);
        } catch (GrillException e) {
          throw new WebApplicationException(e);
        }
      }
    }
    return result;
  }

  public void setSessionParameter(GrillSessionHandle sessionid, String key, String value) {
    setSessionParameter(sessionid, key, value, true);
  }

  protected void setSessionParameter(GrillSessionHandle sessionid, String key, String value, boolean addToSession) {
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
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    } finally {
      try {
        release(sessionid);
      } catch (GrillException e) {
        throw new WebApplicationException(e);
      }
    }
  }

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

    for (GrillSessionImpl.GrillSessionPersistInfo persistInfo : restorableSessions) {
      try {
        GrillSessionHandle sessionHandle = persistInfo.getSessionHandle();
        restoreSession(sessionHandle, persistInfo.getUsername(), persistInfo.getPassword());
        GrillSessionImpl session = getSession(sessionHandle);
        session.setLastAccessTime(persistInfo.getLastAccessTime());
        session.getGrillSessionPersistInfo().setConfig(persistInfo.getConfig());
        session.getGrillSessionPersistInfo().setResources(persistInfo.getResources());
        session.setCurrentDatabase(persistInfo.getDatabase());

        // Add resources for restored sessions
        for (GrillSessionImpl.ResourceEntry resourceEntry : session.getResources()) {
          try {
            addResource(sessionHandle, resourceEntry.getType(), resourceEntry.getLocation(), false);
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
      } catch (GrillException e) {
        throw new RuntimeException(e);
      }
    }
    LOG.info("Session service restoed " + restorableSessions.size() + " sessions");
  }

  @Override
  public synchronized void stop() {
    super.stop();
    if (sessionExpiryThread != null) {
      sessionExpiryThread.shutdownNow();
    }
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // Write out all the sessions
    out.writeInt(sessionMap.size());
    for (GrillSessionHandle sessionHandle : sessionMap.values()) {
      try {
        GrillSessionImpl session = getSession(sessionHandle);
        session.getGrillSessionPersistInfo().writeExternal(out);
      } catch (GrillException e) {
        throw new IOException(e);
      }
    }
    LOG.info("Session service pesristed " + sessionMap.size() + " sessions");
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    int numSessions = in.readInt();
    restorableSessions = new ArrayList<GrillSessionImpl.GrillSessionPersistInfo>();

    for (int i = 0; i < numSessions; i++) {
      GrillSessionImpl.GrillSessionPersistInfo persistInfo = new GrillSessionImpl.GrillSessionPersistInfo();
      persistInfo.readExternal(in);
      restorableSessions.add(persistInfo);
      sessionMap.put(persistInfo.getSessionHandle().getPublicId().toString(), persistInfo.getSessionHandle());
    }
    LOG.info("Session service recovered " + sessionMap.size() + " sessions");
  }

  @Override
  public void closeSession(GrillSessionHandle sessionHandle) throws GrillException {
    super.closeSession(sessionHandle);
    // Inform query service
    GrillService svc = GrillServices.get().getService(QueryExecutionServiceImpl.NAME);
    if (svc instanceof QueryExecutionServiceImpl) {
      ((QueryExecutionServiceImpl) svc).closeDriverSessions(sessionHandle);
    }
  }

  Runnable getSessionExpiryRunnable() {
    return sessionExpiryRunnable;
  }

  public class SessionExpiryRunnable implements Runnable {
    public void runInternal() {
      List<GrillSessionHandle> sessionsToRemove = new ArrayList<GrillSessionHandle>(sessionMap.values());
      Iterator<GrillSessionHandle> itr = sessionsToRemove.iterator();
      while (itr.hasNext()) {
        GrillSessionHandle sessionHandle = itr.next();
        try {
          GrillSessionImpl session = getSession(sessionHandle);
          if (session.isActive()) {
            itr.remove();
          }
        } catch (NotFoundException nfe) {
          itr.remove();
        } catch (GrillException e) {
          itr.remove();
        }
      }

      // Now close all inactive sessions
      for (GrillSessionHandle sessionHandle : sessionsToRemove) {
        try {
          long lastAccessTime = getSession(sessionHandle).getLastAccessTime();
          closeSession(sessionHandle);
          LOG.info("Closed inactive session " + sessionHandle.getPublicId() + " last accessed at "
            + new Date(lastAccessTime));
        } catch (NotFoundException nfe) {
          // Do nothing
        } catch (GrillException e) {
          LOG.error("Error closing session " + sessionHandle.getPublicId() + " reason " + e.getMessage());
        }
      }
    }

    @Override
    public void run() {
      try {
        runInternal();
      } catch (Exception e) {
        LOG.warn("Unknown error while checking for inactive sessions - " +  e.getMessage());
      }
    }
  }

}

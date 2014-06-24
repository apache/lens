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
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hive.service.cli.*;
import org.apache.hive.service.cli.thrift.THandleIdentifier;
import org.apache.hive.service.cli.thrift.TSessionHandle;

import javax.ws.rs.WebApplicationException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

public class HiveSessionService extends GrillService {
  public static final Log LOG = LogFactory.getLog(HiveSessionService.class);
  public static final String NAME = "session";
  private List<GrillSessionImpl.GrillSessionPersistInfo> restorableSessions;

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

  public OperationHandle getAllSessionParameters(GrillSessionHandle sessionid,
      boolean verbose, String key) throws GrillException, HiveSQLException {
    String command = "set";
    if (verbose) {
      command += " -v ";
    }
    if (!StringUtils.isBlank(key)) {
      command += " " + key;
    }
    OperationHandle handle;
    try {
      acquire(sessionid);
      handle = getCliService().executeStatement(getHiveSessionHandle(sessionid), command, null);
    } finally {
      try {
        release(sessionid);
      } catch (GrillException e) {
        throw new WebApplicationException(e);
      }
    }
    return handle;
  }

  public void setSessionParameter(GrillSessionHandle sessionid, String key, String value) {
    setSessionParameter(sessionid, key, value, true);
  }

  protected void setSessionParameter(GrillSessionHandle sessionid, String key, String value, boolean addToSession) {
    String command = "set" + " " + key + "= " + value;
    try {
      acquire(sessionid);
      getCliService().executeStatement(getHiveSessionHandle(sessionid), command, null);
      if (addToSession) {
        getSession(sessionid).setConfig(key, value);
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

  @Override
  public synchronized void start() {
    super.start();

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
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    int numSessions = in.readInt();
    restorableSessions = new ArrayList<GrillSessionImpl.GrillSessionPersistInfo>();

    for (int i = 0; i < numSessions; i++) {
      GrillSessionImpl.GrillSessionPersistInfo persistInfo = new GrillSessionImpl.GrillSessionPersistInfo();
      persistInfo.readExternal(in);
      restorableSessions.add(persistInfo);
    }
  }

}

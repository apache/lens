package com.inmobi.grill.server;

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

import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.server.session.GrillSessionImpl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HandleIdentifier;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.SessionManager;
import org.apache.hive.service.cli.thrift.TSessionHandle;

import javax.ws.rs.NotFoundException;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class GrillService extends CompositeService implements Externalizable {
  public static final Log LOG = LogFactory.getLog(GrillService.class);
  private final CLIService cliService;
  protected boolean stopped = false;

  //Static session map which is used by query submission thread to get the
  //grill session before submitting a query to hive server
  protected static ConcurrentHashMap<String, GrillSessionHandle> sessionMap =
      new ConcurrentHashMap<String, GrillSessionHandle>();

  protected GrillService(String name, CLIService cliService) {
    super(name);
    this.cliService = cliService;
  }

  /**
   * @return the cliService
   */
  public CLIService getCliService() {
    return cliService;
  }

  public GrillSessionHandle openSession(String username, String password, Map<String, String> configuration)
      throws GrillException {
    SessionHandle sessionHandle = null;
    try {
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
        sessionHandle = cliService.openSessionWithImpersonation(username, password,
            configuration, delegationTokenStr);
      } else {
        sessionHandle = cliService.openSession(username, password,
            configuration);
      }
    } catch (Exception e) {
      throw new GrillException (e);
    }
    GrillSessionHandle grillSession = new GrillSessionHandle(
        sessionHandle.getHandleIdentifier().getPublicId(),
        sessionHandle.getHandleIdentifier().getSecretId());
    sessionMap.put(grillSession.getPublicId().toString(), grillSession);
    return grillSession;
  }

  /**
   * Restore session from previous instance of grill server
   */
  public void restoreSession(GrillSessionHandle sessionHandle,
                               String userName,
                               String password) throws GrillException {
    HandleIdentifier handleIdentifier = new HandleIdentifier(sessionHandle.getPublicId(), sessionHandle.getSecretId());
    SessionHandle hiveSessionHandle = new SessionHandle(new TSessionHandle(handleIdentifier.toTHandleIdentifier()));
    try {
      SessionHandle restoredHandle =
        cliService.restoreSession(hiveSessionHandle, userName, password, new HashMap<String, String>());
      GrillSessionHandle restoredSession = new GrillSessionHandle(
        restoredHandle.getHandleIdentifier().getPublicId(),
        restoredHandle.getHandleIdentifier().getSecretId());
      sessionMap.put(restoredSession.getPublicId().toString(), restoredSession);
    } catch (HiveSQLException e) {
      throw new GrillException("Error restoring session " + sessionHandle, e);
    }
  }

  public void closeSession(GrillSessionHandle sessionHandle)
      throws GrillException {
    try {
      cliService.closeSession(getHiveSessionHandle(sessionHandle));
      sessionMap.remove(sessionHandle.getPublicId().toString());
    } catch (Exception e) {
      throw new GrillException (e);
    }
  }

  public SessionManager getSessionManager() {
    return cliService.getSessionManager();
  }

  public GrillSessionImpl getSession(GrillSessionHandle sessionHandle) throws GrillException {
    try {
      return ((GrillSessionImpl)getSessionManager().getSession(getHiveSessionHandle(sessionHandle)));
    } catch (HiveSQLException exc) {
      LOG.warn("Session not found", exc);
      throw new NotFoundException("Session not found " + sessionHandle);
    } catch (Exception e) {
      throw new GrillException (e);
    }
  }

  public void acquire(GrillSessionHandle sessionHandle) throws GrillException {
    getSession(sessionHandle).acquire();
  }

  /**
   * Acquire a grill session specified by the public UUID
   * @param sessionHandle public UUID of the session
   * @throws GrillException if session cannot be acquired
   */
  public void acquire(String sessionHandle) throws GrillException {
    getSession(sessionMap.get(sessionHandle)).acquire();
  }

  public void release(GrillSessionHandle sessionHandle) throws GrillException {
    getSession(sessionHandle).release();
  }

  /**
   * Releases a grill session specified by the public UUID
   * @throws GrillException if session cannot be released
   */
  public void release(String sessionHandle) throws GrillException {
    getSession(sessionMap.get(sessionHandle)).release();
  }

  protected GrillSessionHandle getSessionHandle(String sessionid) {
    return sessionMap.get(sessionid);
  }

  public void addResource(GrillSessionHandle sessionHandle, String type,
      String path) throws GrillException {
  }

  public void deleteResource(GrillSessionHandle sessionHandle, String type,
      String path) throws GrillException {
  }

  public static SessionHandle getHiveSessionHandle(GrillSessionHandle grillHandle) {
    return new SessionHandle(
        new HandleIdentifier(grillHandle.getPublicId(), grillHandle.getSecretId()), CLIService.SERVER_VERSION);
  }

  public Configuration getGrillConf(GrillSessionHandle sessionHandle, GrillConf conf) throws GrillException {
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

  public Configuration getGrillConf(GrillConf conf) throws GrillException {
    Configuration qconf = GrillSessionImpl.createDefaultConf();

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

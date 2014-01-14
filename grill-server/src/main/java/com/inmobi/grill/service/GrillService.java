package com.inmobi.grill.service;

import java.io.IOException;
import java.util.Map;

import javax.security.auth.login.LoginException;
import javax.ws.rs.NotFoundException;

import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.SessionManager;

import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.service.session.GrillSessionImpl;

public abstract class GrillService extends CompositeService {

  private final CLIService cliService;

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

  public SessionHandle openSession(String username, String password, Map<String, String> configuration)
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
    return sessionHandle;
  }

  public void closeSession(SessionHandle sessionHandle)
      throws GrillException {
    try {
      cliService.closeSession(sessionHandle);
    } catch (Exception e) {
      throw new GrillException (e);
    }
  }

  public SessionManager getSessionManager() throws GrillException {
    return cliService.getSessionManager();
  }

  public GrillSessionImpl getSession(SessionHandle sessionHandle) throws GrillException {
    try {
      return ((GrillSessionImpl)getSessionManager().getSession(sessionHandle));
    } catch (HiveSQLException exc) {
      throw new NotFoundException("Session not found " + sessionHandle);
    } catch (Exception e) {
      throw new GrillException (e);
    }
  }

  public void acquire(SessionHandle sessionHandle) throws GrillException {
    try {
      getSession(sessionHandle).acquire();
    } catch (HiveSQLException e) {
      throw new GrillException (e);
    }
  }

  public void release(SessionHandle sessionHandle) throws GrillException {
    getSession(sessionHandle).release();
  }

}

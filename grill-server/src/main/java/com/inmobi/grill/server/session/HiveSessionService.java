package com.inmobi.grill.server.session;

import javax.ws.rs.WebApplicationException;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.server.GrillService;

public class HiveSessionService extends GrillService {

  public HiveSessionService(CLIService cliService) {
    super("session", cliService);
  }

  public void addResource(GrillSessionHandle sessionid, String type, String path) {
    String command = "add " + type.toLowerCase() + " " + path;
    try {
      acquire(sessionid);
      getCliService().executeStatement(
          getHiveSessionHandle(sessionid), command, null);
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
    String command = "set" + " " + key + "= " + value;
    try {
      acquire(sessionid);
      getCliService().executeStatement(getHiveSessionHandle(sessionid), command, null);
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
}

package com.inmobi.grill.hivecommand.service;

import javax.ws.rs.WebApplicationException;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;

import com.inmobi.grill.common.GrillSessionHandle;
import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.service.GrillService;
import com.inmobi.grill.service.GrillSessionImpl;

public class HiveCommandService extends GrillService {

  public HiveCommandService(CLIService cliService) {
    super("command", cliService);
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
      boolean verbose, String key) {
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

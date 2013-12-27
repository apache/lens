package com.inmobi.grill.hivecommand.service;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.WebApplicationException;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.thrift.TRow;

import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.service.GrillService;

public class HiveCommandService extends GrillService {

  public HiveCommandService(CLIService cliService) {
    super("command", cliService);
  }

  public void addResource(GrillSessionHandle sessionid, String type, String path) {
    String command = "add " + type.toLowerCase() + " " + path;
    try {
      getCliService().executeStatement(sessionid.getSessionHandle(), command, null);
    } catch (HiveSQLException e) {
      throw new WebApplicationException(e);
    }
  }


  public void deleteResource(GrillSessionHandle sessionid, String type, String path) {
    String command = "delete " + type.toLowerCase() + " " + path;
    try {
      getCliService().executeStatement(sessionid.getSessionHandle(), command, null);
    } catch (HiveSQLException e) {
      throw new WebApplicationException(e);
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
      try {
        System.out.println(key + " in sessionconf:" + getSessionManager().getSession(sessionid.getSessionHandle()).getHiveConf().get(key));
      } catch (Exception e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
    }
    OperationHandle handle;
    try {
      handle = getCliService().executeStatement(sessionid.getSessionHandle(), command, null);
    } catch (HiveSQLException e) {
      throw new WebApplicationException(e);
    }
    return handle;
  }

  public void setSessionParameter(GrillSessionHandle sessionid, String key, String value) {
    String command = "set" + " " + key + "= " + value;
    try {
      OperationHandle handle = getCliService().executeStatement(sessionid.getSessionHandle(), command, null);
      RowSet rows = null;
      try {
        rows = getCliService().fetchResults(handle);
      } catch (HiveSQLException e) {
        new WebApplicationException(e);
      }
      for (TRow row : rows.toTRowSet().getRows()) {
        System.out.println(row.getColVals().get(0).getStringVal().getValue()); 
      }

    } catch (HiveSQLException e) {
      throw new WebApplicationException(e);
    }
  }

}

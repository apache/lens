package com.inmobi.grill.service.session;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.SessionHandle;

import com.inmobi.grill.service.GrillService;

public class CubeMetastoreServiceExpt extends GrillService {
  public CubeMetastoreServiceExpt(CLIService cliService) {
    super("CubeServiceExperiment", cliService);
  }


  public OperationHandle getCubes(SessionHandle sessionHandle)
          throws HiveSQLException, HiveException {
    OperationHandle opHandle = ((GrillSessionImpl)getCliService().getSessionManager().getSession(sessionHandle)).getCubes();
    System.out.println(sessionHandle + ": getCubes()");
    return opHandle;
  }
}

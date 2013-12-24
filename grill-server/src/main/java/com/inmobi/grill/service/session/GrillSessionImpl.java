package com.inmobi.grill.service.session;

import java.util.Map;

import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.session.HiveSessionImpl;

public class GrillSessionImpl extends HiveSessionImpl {
  
  private CubeMetastoreClient cubeClient;
  private GrillOperationManager grillOperationManager;

  public GrillSessionImpl(String username, String password,
      Map<String, String> sessionConf) {
    super(username, password, sessionConf);
    grillOperationManager = new GrillOperationManager();
  }

 
  OperationHandle getCubes() throws HiveException, HiveSQLException {
    GetCubesOperation operation = new GetCubesOperation(this, OperationType.UNKNOWN_OPERATION);
    grillOperationManager.addGrillOperation(operation);
    OperationHandle opHandle = operation.getHandle();
    try {
      operation.run();
      return opHandle;
    } catch (HiveSQLException e) {
      grillOperationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release();
    }
  }

  public CubeMetastoreClient getCubeMetastoreClient() throws HiveException {
    if (cubeClient == null) {
      cubeClient = CubeMetastoreClient.getInstance(getHiveConf());
    }
    return cubeClient;
  }
}

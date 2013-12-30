package com.inmobi.grill.service.session;

import java.util.HashMap;
import java.util.Map;

import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.operation.Operation;
import org.apache.hive.service.cli.operation.OperationManager;

public class GrillOperationManager extends OperationManager {

  private final Map<OperationHandle, Operation> grillOperations =
      new HashMap<OperationHandle, Operation>();

  GrillOperationManager() {
    super();
  }

  public synchronized void addGrillOperation(Operation operation) {
    grillOperations.put(operation.getHandle(), operation);
  }

  public synchronized Operation removeGrillOperation(OperationHandle opHandle) {
    return grillOperations.remove(opHandle);
  }


}

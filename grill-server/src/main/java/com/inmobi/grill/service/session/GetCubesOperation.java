package com.inmobi.grill.service.session;

import java.util.List;

import org.apache.hadoop.hive.ql.cube.metadata.Cube;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.operation.MetadataOperation;
import org.apache.hive.service.cli.session.HiveSession;

public class GetCubesOperation extends MetadataOperation {
  private static final TableSchema RESULT_SET_SCHEMA = new TableSchema().addStringColumn("CUBE", "cube type");
  private final RowSet rowSet = new RowSet();

  protected GetCubesOperation(HiveSession parentSession, OperationType opType) {
    super(parentSession, opType);
  }

  @Override
  public void run() throws HiveSQLException {
    setState(OperationState.RUNNING);
    try {
      CubeMetastoreClient metastoreClient = ((GrillSessionImpl)getParentSession()).getCubeMetastoreClient();
      List<Cube> cubes = metastoreClient.getAllCubes();
      for (Cube cube : cubes) {
        Object[] row = new Object[] {cube};
        rowSet.addRow(RESULT_SET_SCHEMA, row);
      }
    } catch (Exception e) {
      setState(OperationState.ERROR);
      throw new HiveSQLException(e);
    }
  }

  @Override
  public TableSchema getResultSetSchema() throws HiveSQLException {
    assertState(OperationState.FINISHED);
    return RESULT_SET_SCHEMA;
  }

  @Override
  public RowSet getNextRowSet(FetchOrientation orientation, long maxRows)
      throws HiveSQLException {
    assertState(OperationState.FINISHED);
    return rowSet.extractSubset((int)maxRows);
  }

}

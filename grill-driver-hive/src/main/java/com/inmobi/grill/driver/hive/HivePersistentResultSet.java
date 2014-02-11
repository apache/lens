package com.inmobi.grill.driver.hive;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryResult;
import com.inmobi.grill.api.query.QueryResultSetMetadata;
import com.inmobi.grill.api.query.ResultColumn;
import com.inmobi.grill.server.api.driver.GrillResultSetMetadata;
import com.inmobi.grill.server.api.driver.PersistentResultSet;

public class HivePersistentResultSet extends PersistentResultSet {
  private final Path path;
  private final OperationHandle opHandle;
  private final ThriftCLIServiceClient client;
  private final QueryHandle queryHandle;
  private TableSchema metadata;

  public HivePersistentResultSet(Path resultSetPath, OperationHandle opHandle,
      ThriftCLIServiceClient client, QueryHandle queryHandle) {
    this.path = resultSetPath;
    this.client = client;
    this.opHandle = opHandle;
    this.queryHandle = queryHandle;
  }

  private TableSchema getTableSchema() throws HiveSQLException {
    if (metadata == null) {
      metadata = client.getResultSetMetadata(opHandle);
    }
    return metadata;
  }

  public QueryHandle getQueryHandle() {
    return queryHandle;
  }

  @Override
  public int size() throws GrillException {
    return -1;
  }

  @Override
  public String getOutputPath() throws GrillException {
    return path.toString();
  }

  @Override
  public GrillResultSetMetadata getMetadata() throws GrillException {
    return new GrillResultSetMetadata() {
      @Override
      public List<ResultColumn> getColumns() {
        List<ColumnDescriptor> descriptors;

        try {
          descriptors = getTableSchema().getColumnDescriptors();
        } catch (HiveSQLException e) {
          return null;
        }

        if (descriptors == null) {
          return null;
        }

        List<ResultColumn> columns = new ArrayList<ResultColumn>(descriptors.size());
        for (ColumnDescriptor colDesc : descriptors) {
          columns.add(new ResultColumn(colDesc.getName(), colDesc.getTypeName()));
        }
        return columns;
      }
    };
  }
}

package com.inmobi.grill.driver.hive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.ResultColumn;
import com.inmobi.grill.api.query.ResultRow;
import com.inmobi.grill.server.api.driver.GrillResultSetMetadata;
import com.inmobi.grill.server.api.driver.InMemoryResultSet;

public class HiveInMemoryResultSet extends InMemoryResultSet {
  private final CLIServiceClient client;
  private final OperationHandle opHandle;
  private TableSchema metadata;
  private RowSet rowSet;
  private int fetchSize = 100;
  private Iterator<Object[]> fetchedRowsItr;
  private boolean noMoreResults;

  public HiveInMemoryResultSet(OperationHandle hiveHandle, CLIServiceClient client) {
    this.client = client;
    this.opHandle = hiveHandle;
  }

  private TableSchema getTableSchema() throws GrillException {
    if (metadata == null) {
      try {
        metadata = client.getResultSetMetadata(opHandle);
      } catch (HiveSQLException e) {
        throw new GrillException(e);
      }
    }
    return metadata;
  }

  @Override
  public int size() throws GrillException {
    return -1;
  }

  @Override
  public GrillResultSetMetadata getMetadata() throws GrillException {
    return new GrillResultSetMetadata() {

      @Override
      public List<ResultColumn> getColumns() {
        try {
          List<ColumnDescriptor> descriptors = getTableSchema().getColumnDescriptors();

          if (descriptors == null) {
            return null;
          }

          List<ResultColumn> columns = new ArrayList<ResultColumn>(descriptors.size());

          for (ColumnDescriptor desc : descriptors) {
            columns.add(new ResultColumn(desc.getName(), desc.getTypeName()));
          }

          return columns;
        } catch (GrillException e) {
          return null;
        }
      }
    };
  }

  @Override
  public boolean hasNext() throws GrillException {
    if (fetchedRowsItr == null || !fetchedRowsItr.hasNext()) {
      try {
        rowSet = client.fetchResults(opHandle, FetchOrientation.FETCH_NEXT, fetchSize);
        noMoreResults = rowSet.numRows() == 0;
        if (noMoreResults) {
          return false;
        }
        fetchedRowsItr = rowSet.iterator();
      } catch (Exception e) {
        throw new GrillException(e);
      }
    }
    return fetchedRowsItr.hasNext();
  }

  @Override
  public ResultRow next() throws GrillException {
    List<ColumnDescriptor> descriptors = getTableSchema().getColumnDescriptors();
    int numColumns = descriptors.size();
    List<Object> results = new ArrayList<Object>(numColumns);
    Object[] row = fetchedRowsItr.next();
    results.addAll(Arrays.asList(row));

    return new ResultRow(results);
  }

  @Override
  public void setFetchSize(int size) throws GrillException {
    assert size >= 0;
    fetchSize = size == 0 ? Integer.MAX_VALUE : size;
  }
}

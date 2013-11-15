package com.inmobi.grill.driver.hive;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.Type;
import org.apache.hive.service.cli.thrift.TBoolValue;
import org.apache.hive.service.cli.thrift.TByteValue;
import org.apache.hive.service.cli.thrift.TColumnValue;
import org.apache.hive.service.cli.thrift.TDoubleValue;
import org.apache.hive.service.cli.thrift.TI16Value;
import org.apache.hive.service.cli.thrift.TI32Value;
import org.apache.hive.service.cli.thrift.TI64Value;
import org.apache.hive.service.cli.thrift.TRow;
import org.apache.hive.service.cli.thrift.TRowSet;
import org.apache.hive.service.cli.thrift.TStringValue;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;

import com.inmobi.grill.api.GrillResultSetMetadata;
import com.inmobi.grill.api.InMemoryResultSet;
import com.inmobi.grill.api.ResultColumn;
import com.inmobi.grill.exception.GrillException;

public class HiveInMemoryResultSet implements InMemoryResultSet {
  private final ThriftCLIServiceClient client;
  private final OperationHandle opHandle;
  private TableSchema metadata;
  private TRowSet rowSet;
  private int fetchSize = 100;
  private Iterator<TRow> rowItr;
  private boolean noMoreResults;

  public HiveInMemoryResultSet(OperationHandle hiveHandle, ThriftCLIServiceClient client) {
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
    if (noMoreResults) {
      return false;
    }

    if (rowItr == null || !rowItr.hasNext()) {
      try {
        rowSet = client.fetchResults(opHandle, FetchOrientation.FETCH_NEXT, fetchSize).toTRowSet();
        noMoreResults = rowSet.getRowsSize() == 0;
      } catch (Exception e) {
        throw new GrillException(e);
      }
      rowItr = rowSet.getRowsIterator();
    }
    return rowItr.hasNext();
  }

  @Override
  public List<Object> next() throws GrillException {
    TRow row = rowItr.next();
    List<ColumnDescriptor> descriptors = getTableSchema().getColumnDescriptors();

    List<Object> results = new ArrayList<Object>(row.getColValsSize());
    List<TColumnValue> thriftRow = row.getColVals();
    for (int i = 0 ; i < row.getColValsSize(); i++) {
      TColumnValue tColumnValue = thriftRow.get(i);
      Type type = descriptors.get(i).getType();
      Object value = null;
      switch (type) {
      case NULL_TYPE :
        value = null;
        break;
      case BOOLEAN_TYPE:
      {
        TBoolValue tValue = tColumnValue.getBoolVal();
        if (tValue.isSetValue()) {
        value = tValue.isValue();
        }
        break;
      }
      case TINYINT_TYPE:
      {
        TByteValue tValue = tColumnValue.getByteVal();
        if (tValue.isSetValue()) {
        value = tValue.getValue();
        }
        break;
      }
      case SMALLINT_TYPE: 
      {
        TI16Value tValue = tColumnValue.getI16Val();
        if (tValue.isSetValue()) {
        value = tValue.getValue();
        }
        break;
      }
      case INT_TYPE: 
      {
        TI32Value tValue = tColumnValue.getI32Val();
        if (tValue.isSetValue()) {
        value = tValue.getValue();
        }
        break;
      }
      case BIGINT_TYPE: 
      {
        TI64Value tValue = tColumnValue.getI64Val();
        if (tValue.isSetValue()) {
        value = tValue.getValue();
        }
        break;
      }
      case FLOAT_TYPE:
      case DOUBLE_TYPE:
      {
        TDoubleValue tValue = tColumnValue.getDoubleVal();
        if (tValue.isSetValue()) {
        value = tValue.getValue();
        }
        break;
      }
      case STRING_TYPE:
      case VARCHAR_TYPE:
      case DATE_TYPE:
      case TIMESTAMP_TYPE:
      case BINARY_TYPE:
      case DECIMAL_TYPE:
      case ARRAY_TYPE:
      case MAP_TYPE:
      case STRUCT_TYPE:
      case UNION_TYPE:
      case USER_DEFINED_TYPE:
      {
        TStringValue tValue = tColumnValue.getStringVal();
        if (tValue.isSetValue()) {
          value = tValue.getValue();
        }
        break;
      }
      default:
        value = null;
      }
      results.add(value);
    }

    return results;
  }

  @Override
  public void setFetchSize(int size) throws GrillException {
    assert size >= 0;
    fetchSize = size == 0 ? Integer.MAX_VALUE : size;
  }

}

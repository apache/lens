package com.inmobi.grill.driver.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.ResultColumn;
import com.inmobi.grill.api.query.ResultColumnType;
import com.inmobi.grill.api.query.ResultRow;
import com.inmobi.grill.server.api.driver.GrillResultSetMetadata;
import com.inmobi.grill.server.api.driver.InMemoryResultSet;

public class JdbcResultSet extends InMemoryResultSet {
  public static final Logger LOG = Logger.getLogger(JdbcResultSet.class);

  ResultSetMetaData resultMeta;
  private final ResultSet resultSet;
  
  public JdbcResultSet(ResultSet rs) {
    if (rs == null) {
      throw new NullPointerException("Resultset should not be null");
    }
    resultSet = rs;
  }
  
  
  private ResultSetMetaData getRsMetadata() throws GrillException {
    if (resultMeta == null) {
      try {
        resultMeta = resultSet.getMetaData();
      } catch (SQLException e) {
        throw new GrillException(e);
      }
    }
    return resultMeta;
  }
  
  @Override
  public int size() throws GrillException {
    LOG.warn("Size of result set is not supported");
    return -1;
  }
  
  @Override
  public GrillResultSetMetadata getMetadata() throws GrillException {
    return new GrillResultSetMetadata() {
      @Override
      public List<ResultColumn> getColumns() {
        try{
          ResultSetMetaData rsmeta = getRsMetadata();
          List<ResultColumn> columns = new ArrayList<ResultColumn>(rsmeta.getColumnCount());
          for (int i = 0; i < rsmeta.getColumnCount(); i++) {
            ResultColumnType colType;
            switch (rsmeta.getColumnType(i + 1)) {
            case Types.BIGINT:
              colType = ResultColumnType.BIGINT; break;
            case Types.TINYINT:
            case Types.BIT:
              colType = ResultColumnType.TINYINT; break;
            case Types.INTEGER:
              colType = ResultColumnType.INT; break;
            case Types.SMALLINT:
              colType = ResultColumnType.SMALLINT; break;
  
            case Types.BOOLEAN:
              colType = ResultColumnType.BOOLEAN; break;
            
            case Types.BLOB:
            case Types.VARBINARY:
            case Types.JAVA_OBJECT:
            case Types.LONGVARBINARY:
              colType = ResultColumnType.BINARY; break;
              
            case Types.DATALINK:
            case Types.CHAR:
            case Types.CLOB:
            case Types.VARCHAR:
            case Types.NCLOB:
            case Types.NCHAR:
            case Types.LONGNVARCHAR:
            case Types.NVARCHAR:
            case Types.SQLXML:
              colType = ResultColumnType.STRING; break;
              
            case Types.DATE:
              colType = ResultColumnType.DATE; break;
            case Types.TIME:
            case Types.TIMESTAMP:
              colType = ResultColumnType.TIMESTAMP; break;
            
            case Types.FLOAT:
              colType = ResultColumnType.FLOAT; break;
            case Types.DECIMAL:
              colType = ResultColumnType.DECIMAL; break;
            
            case Types.DOUBLE:
            case Types.REAL:
            case Types.NUMERIC:
              colType = ResultColumnType.DOUBLE; break;
            
            case Types.DISTINCT:
            case Types.NULL:
            case Types.OTHER:
            case Types.REF:
            case Types.ROWID:
              colType = ResultColumnType.USER_DEFINED; break;
            
            case Types.STRUCT:
              colType = ResultColumnType.STRUCT; break;
  
            case Types.ARRAY:
              colType = ResultColumnType.ARRAY; break;
            default:
              colType = ResultColumnType.USER_DEFINED;
            }
            
            columns.add(new ResultColumn(rsmeta.getColumnName(i + 1), colType.toString()));
          }
          return columns;
        } catch (Exception e) {
          LOG.error("Error getting JDBC type information: " + e.getMessage(), e);
          return null;
        }
      }
    };
  }
  
  @Override
  public void setFetchSize(int size) throws GrillException {
    try {
      resultSet.setFetchSize(size);
    } catch (SQLException e) {
      throw new GrillException(e);
    }
  }
  
  @Override
  public ResultRow next() throws GrillException {
    ResultSetMetaData meta = getRsMetadata();
    try {
      List<Object> row = new ArrayList<Object>(meta.getColumnCount());
      for (int i = 0; i < meta.getColumnCount(); i++) {
        row.add(resultSet.getObject(i + 1));
      }
      return new ResultRow(row);
    } catch (SQLException e) {
      throw new GrillException(e);
    }
  }
  
  @Override
  public boolean hasNext() throws GrillException {
    try {
      return resultSet.next();
    } catch (SQLException e) {
      throw new GrillException(e);
    }
  }
}

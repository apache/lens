package com.inmobi.grill.driver.jdbc;

/*
 * #%L
 * Grill Driver for JDBC
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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
import com.inmobi.grill.driver.jdbc.JDBCDriver.QueryResult;
import com.inmobi.grill.server.api.driver.GrillResultSetMetadata;
import com.inmobi.grill.server.api.driver.InMemoryResultSet;

public class JDBCResultSet extends InMemoryResultSet {
  public static final Logger LOG = Logger.getLogger(JDBCResultSet.class);

  ResultSetMetaData resultMeta;
  private final ResultSet resultSet;
  private final QueryResult queryResult;
  private GrillResultSetMetadata grillResultMeta;
  private final boolean closeAfterFetch;
  
  public JDBCResultSet(QueryResult queryResult, ResultSet resultSet,
      boolean closeAfterFetch) {
    this.queryResult = queryResult;
    this.resultSet = resultSet;;
    this.closeAfterFetch = closeAfterFetch;
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
  public synchronized GrillResultSetMetadata getMetadata() throws GrillException {
    if (grillResultMeta == null) {
        grillResultMeta =  new GrillResultSetMetadata() {
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
    return grillResultMeta;
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
  public synchronized ResultRow next() throws GrillException {
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
  public synchronized boolean hasNext() throws GrillException {
    try {
      boolean hasMore = resultSet.next();
      if (!hasMore && closeAfterFetch) {
        close();
      }
      return hasMore;
    } catch (SQLException e) {
      throw new GrillException(e);
    }
  }
  
  public void close() {
    queryResult.close();
  }
}

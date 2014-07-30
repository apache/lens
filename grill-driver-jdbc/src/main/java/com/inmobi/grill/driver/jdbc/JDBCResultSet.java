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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.Type;
import org.apache.hive.service.cli.TypeDescriptor;
import org.apache.hive.service.cli.TypeQualifiers;
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
        public List<ColumnDescriptor> getColumns() {
          try{
            ResultSetMetaData rsmeta = getRsMetadata();
            List<ColumnDescriptor> columns = new ArrayList<ColumnDescriptor>(rsmeta.getColumnCount());
            for (int i = 1; i <= rsmeta.getColumnCount(); i++) {
              FieldSchema col = new FieldSchema(rsmeta.getColumnName(i),
                TypeInfoUtils.getTypeInfoFromTypeString(
                  getHiveTypeForSQLType(i, rsmeta)).getTypeName(),
                rsmeta.getColumnTypeName(i));
              columns.add(new ColumnDescriptor(col, i));
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

  public static String getHiveTypeForSQLType(int index, ResultSetMetaData rsmeta) throws SQLException {
    TypeDescriptor hiveType;
    TypeQualifiers qualifiers;
    switch (rsmeta.getColumnType(index)) {
    case Types.BIGINT:
      hiveType = new TypeDescriptor(Type.BIGINT_TYPE); break;
    case Types.TINYINT:
    case Types.BIT:
      hiveType = new TypeDescriptor(Type.TINYINT_TYPE); break;
    case Types.INTEGER:
      hiveType = new TypeDescriptor(Type.INT_TYPE); break;
    case Types.SMALLINT:
      hiveType = new TypeDescriptor(Type.SMALLINT_TYPE); break;
    case Types.BOOLEAN:
      hiveType = new TypeDescriptor(Type.BOOLEAN_TYPE); break;
    case Types.BLOB:
    case Types.VARBINARY:
    case Types.JAVA_OBJECT:
    case Types.LONGVARBINARY:
      hiveType = new TypeDescriptor(Type.BINARY_TYPE); break;

    case Types.CHAR:
    case Types.NCHAR:
      hiveType = new TypeDescriptor(Type.CHAR_TYPE);
      qualifiers = new TypeQualifiers();
      qualifiers.setCharacterMaximumLength(rsmeta.getColumnDisplaySize(index));
      hiveType.setTypeQualifiers(qualifiers);
      break;
    case Types.VARCHAR:
    case Types.LONGNVARCHAR:
    case Types.NVARCHAR:
      hiveType = new TypeDescriptor(Type.VARCHAR_TYPE);
      qualifiers = new TypeQualifiers();
      qualifiers.setCharacterMaximumLength(rsmeta.getColumnDisplaySize(index));
      hiveType.setTypeQualifiers(qualifiers);
      break;

    case Types.NCLOB:
    case Types.CLOB:
    case Types.DATALINK:
    case Types.SQLXML:
      hiveType = new TypeDescriptor(Type.STRING_TYPE); break;

    case Types.DATE:
      hiveType = new TypeDescriptor(Type.DATE_TYPE); break;
    case Types.TIME:
    case Types.TIMESTAMP:
      hiveType = new TypeDescriptor(Type.TIMESTAMP_TYPE); break;

    case Types.FLOAT:
      hiveType = new TypeDescriptor(Type.TIMESTAMP_TYPE); break;
    case Types.DECIMAL:
      hiveType = new TypeDescriptor(Type.DECIMAL_TYPE);
      qualifiers = new TypeQualifiers();
      qualifiers.setPrecision(rsmeta.getPrecision(index));
      qualifiers.setScale(rsmeta.getScale(index));
      hiveType.setTypeQualifiers(qualifiers);
      break;

    case Types.DOUBLE:
    case Types.REAL:
    case Types.NUMERIC:
      hiveType = new TypeDescriptor(Type.DOUBLE_TYPE); break;

    case Types.DISTINCT:
    case Types.NULL:
    case Types.OTHER:
    case Types.REF:
    case Types.ROWID:
      hiveType = new TypeDescriptor(Type.USER_DEFINED_TYPE); break;

    case Types.STRUCT:
      hiveType = new TypeDescriptor(Type.STRUCT_TYPE); break;
    case Types.ARRAY:
      hiveType = new TypeDescriptor(Type.ARRAY_TYPE); break;
    default:
      hiveType = new TypeDescriptor(Type.USER_DEFINED_TYPE); break;
    }
    return GrillResultSetMetadata.getQualifiedTypeName(hiveType);
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

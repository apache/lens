/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.driver.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.Type;
import org.apache.hive.service.cli.TypeDescriptor;
import org.apache.hive.service.cli.TypeQualifiers;
import org.apache.lens.api.LensException;
import org.apache.lens.api.query.ResultRow;
import org.apache.lens.driver.jdbc.JDBCDriver.QueryResult;
import org.apache.lens.server.api.driver.LensResultSetMetadata;
import org.apache.lens.server.api.driver.InMemoryResultSet;
import org.apache.log4j.Logger;

/**
 * The Class JDBCResultSet.
 */
public class JDBCResultSet extends InMemoryResultSet {

  /** The Constant LOG. */
  public static final Logger LOG = Logger.getLogger(JDBCResultSet.class);

  /** The result meta. */
  ResultSetMetaData resultMeta;

  /** The result set. */
  private final ResultSet resultSet;

  /** The query result. */
  private final QueryResult queryResult;

  /** The lens result meta. */
  private LensResultSetMetadata lensResultMeta;

  /** The close after fetch. */
  private final boolean closeAfterFetch;

  /**
   * Instantiates a new JDBC result set.
   *
   * @param queryResult
   *          the query result
   * @param resultSet
   *          the result set
   * @param closeAfterFetch
   *          the close after fetch
   */
  public JDBCResultSet(QueryResult queryResult, ResultSet resultSet, boolean closeAfterFetch) {
    this.queryResult = queryResult;
    this.resultSet = resultSet;
    ;
    this.closeAfterFetch = closeAfterFetch;
  }

  private ResultSetMetaData getRsMetadata() throws LensException {
    if (resultMeta == null) {
      try {
        resultMeta = resultSet.getMetaData();
      } catch (SQLException e) {
        throw new LensException(e);
      }
    }
    return resultMeta;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensResultSet#size()
   */
  @Override
  public int size() throws LensException {
    LOG.warn("Size of result set is not supported");
    return -1;
  }

  @Override
  public synchronized LensResultSetMetadata getMetadata() throws LensException {
    if (lensResultMeta == null) {
      lensResultMeta = new LensResultSetMetadata() {
        @Override
        public List<ColumnDescriptor> getColumns() {
          try {
            ResultSetMetaData rsmeta = getRsMetadata();
            List<ColumnDescriptor> columns = new ArrayList<ColumnDescriptor>(rsmeta.getColumnCount());
            for (int i = 1; i <= rsmeta.getColumnCount(); i++) {
              FieldSchema col = new FieldSchema(rsmeta.getColumnName(i), TypeInfoUtils.getTypeInfoFromTypeString(
                  getHiveTypeForSQLType(i, rsmeta)).getTypeName(), rsmeta.getColumnTypeName(i));
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
    return lensResultMeta;
  }

  /**
   * Gets the hive type for sql type.
   *
   * @param index
   *          the index
   * @param rsmeta
   *          the rsmeta
   * @return the hive type for sql type
   * @throws SQLException
   *           the SQL exception
   */
  public static String getHiveTypeForSQLType(int index, ResultSetMetaData rsmeta) throws SQLException {
    TypeDescriptor hiveType;
    TypeQualifiers qualifiers;
    switch (rsmeta.getColumnType(index)) {
    case Types.BIGINT:
      hiveType = new TypeDescriptor(Type.BIGINT_TYPE);
      break;
    case Types.TINYINT:
    case Types.BIT:
      hiveType = new TypeDescriptor(Type.TINYINT_TYPE);
      break;
    case Types.INTEGER:
      hiveType = new TypeDescriptor(Type.INT_TYPE);
      break;
    case Types.SMALLINT:
      hiveType = new TypeDescriptor(Type.SMALLINT_TYPE);
      break;
    case Types.BOOLEAN:
      hiveType = new TypeDescriptor(Type.BOOLEAN_TYPE);
      break;
    case Types.BLOB:
    case Types.VARBINARY:
    case Types.JAVA_OBJECT:
    case Types.LONGVARBINARY:
      hiveType = new TypeDescriptor(Type.BINARY_TYPE);
      break;

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
    case Types.LONGVARCHAR:
    case Types.DATALINK:
    case Types.SQLXML:
      hiveType = new TypeDescriptor(Type.STRING_TYPE);
      break;

    case Types.DATE:
      hiveType = new TypeDescriptor(Type.DATE_TYPE);
      break;
    case Types.TIME:
    case Types.TIMESTAMP:
      hiveType = new TypeDescriptor(Type.TIMESTAMP_TYPE);
      break;

    case Types.FLOAT:
      hiveType = new TypeDescriptor(Type.TIMESTAMP_TYPE);
      break;
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
      hiveType = new TypeDescriptor(Type.DOUBLE_TYPE);
      break;

    case Types.DISTINCT:
    case Types.NULL:
    case Types.OTHER:
    case Types.REF:
    case Types.ROWID:
      hiveType = new TypeDescriptor(Type.USER_DEFINED_TYPE);
      break;

    case Types.STRUCT:
      hiveType = new TypeDescriptor(Type.STRUCT_TYPE);
      break;
    case Types.ARRAY:
      hiveType = new TypeDescriptor(Type.ARRAY_TYPE);
      break;
    default:
      hiveType = new TypeDescriptor(Type.USER_DEFINED_TYPE);
      break;
    }
    return LensResultSetMetadata.getQualifiedTypeName(hiveType);
  }

  @Override
  public void setFetchSize(int size) throws LensException {
    try {
      resultSet.setFetchSize(size);
    } catch (SQLException e) {
      throw new LensException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.InMemoryResultSet#next()
   */
  @Override
  public synchronized ResultRow next() throws LensException {
    ResultSetMetaData meta = getRsMetadata();
    try {
      List<Object> row = new ArrayList<Object>(meta.getColumnCount());
      for (int i = 0; i < meta.getColumnCount(); i++) {
        row.add(resultSet.getObject(i + 1));
      }
      return new ResultRow(row);
    } catch (SQLException e) {
      throw new LensException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.InMemoryResultSet#hasNext()
   */
  @Override
  public synchronized boolean hasNext() throws LensException {
    try {
      boolean hasMore = resultSet.next();
      if (!hasMore && closeAfterFetch) {
        close();
      }
      return hasMore;
    } catch (SQLException e) {
      throw new LensException(e);
    }
  }

  /**
   * Close.
   */
  public void close() {
    queryResult.close();
  }
}

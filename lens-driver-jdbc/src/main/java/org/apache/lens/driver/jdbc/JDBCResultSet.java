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

import org.apache.lens.api.query.ResultRow;
import org.apache.lens.driver.jdbc.JDBCDriver.QueryResult;
import org.apache.lens.server.api.driver.InMemoryResultSet;
import org.apache.lens.server.api.driver.LensResultSetMetadata;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.TypeDescriptor;
import org.apache.hive.service.cli.TypeQualifiers;

import org.codehaus.jackson.annotate.JsonIgnore;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class JDBCResultSet.
 */
@Slf4j
public class JDBCResultSet extends InMemoryResultSet {

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
   * @param queryResult     the query result
   * @param resultSet       the result set
   * @param closeAfterFetch the close after fetch
   */
  public JDBCResultSet(QueryResult queryResult, ResultSet resultSet, boolean closeAfterFetch) throws LensException {
    this.queryResult = queryResult;
    this.resultSet = resultSet;
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
  public Integer size() throws LensException {
    log.warn("Size of result set is not supported");
    return null;
  }

  @Override
  public synchronized LensResultSetMetadata getMetadata() throws LensException {
    if (lensResultMeta == null) {
      JDBCResultSetMetadata jdbcResultSetMetadata = new JDBCResultSetMetadata();
      jdbcResultSetMetadata.setFieldSchemas(new ArrayList<FieldSchemaData>());
      try {
        ResultSetMetaData rsmeta = getRsMetadata();

        for (int i = 1; i <= rsmeta.getColumnCount(); i++) {
          FieldSchemaData col = new FieldSchemaData(rsmeta.getColumnName(i),
            TypeInfoUtils.getTypeInfoFromTypeString(getHiveTypeForSQLType(i, rsmeta)).getTypeName(),
            rsmeta.getColumnTypeName(i));
          jdbcResultSetMetadata.getFieldSchemas().add(col);
        }
      } catch (Exception e) {
        log.error("Error getting JDBC type information: {}", e.getMessage(), e);
        jdbcResultSetMetadata.setFieldSchemas(null);
      }
      lensResultMeta = jdbcResultSetMetadata;
    }
    return lensResultMeta;
  }

  /**
   * FieldSchemaData is created so that we don't save FieldSchema as some classes used by it don't have
   * default constructors which are required by jackson
   */
  @Data
  @NoArgsConstructor
  public static class FieldSchemaData {
    private String name;
    private String type;
    private String comment;

    public FieldSchemaData(String name, String type, String comment) {
      this.name = name;
      this.type = type;
      this.comment = comment;
    }

    public FieldSchema toFieldSchema() {
      return new FieldSchema(name, type, comment);
    }
  }

  /**
   * Result set metadata of a JDBC query
   */
  public static class JDBCResultSetMetadata extends LensResultSetMetadata {
    @Getter @Setter
    private List<FieldSchemaData> fieldSchemas;

    @JsonIgnore
    @Override
    public List<ColumnDescriptor> getColumns() {
      if (fieldSchemas == null) {
        return null;
      }
      List<ColumnDescriptor> columns = new ArrayList<ColumnDescriptor>(fieldSchemas.size());

      for (int i = 0; i < fieldSchemas.size(); i++) {
        FieldSchema schema = fieldSchemas.get(i).toFieldSchema();
        columns.add(ColumnDescriptor.newPrimitiveColumnDescriptor(schema.getName(), schema.getComment(),
          Type.getType(schema.getType()), i + 1));
      }
      return columns;
    }
  }

  /**
   * Gets the hive type for sql type.
   *
   * @param index  the index
   * @param rsmeta the rsmeta
   * @return the hive type for sql type
   * @throws SQLException the SQL exception
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
      hiveType = new TypeDescriptor(Type.STRING_TYPE);
      break;
    case Types.VARCHAR:
    case Types.LONGNVARCHAR:
    case Types.NVARCHAR:
      hiveType = new TypeDescriptor(Type.STRING_TYPE);
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
      hiveType = new TypeDescriptor(Type.DOUBLE_TYPE);
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

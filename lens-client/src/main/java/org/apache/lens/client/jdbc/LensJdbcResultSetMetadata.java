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
package org.apache.lens.client.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lens.api.query.QueryResultSetMetadata;
import org.apache.lens.api.query.ResultColumn;

/**
 * The Class LensJdbcResultSetMetadata.
 */
public class LensJdbcResultSetMetadata implements ResultSetMetaData {

  /** The metadata. */
  private final QueryResultSetMetadata metadata;

  /** The col names. */
  private final List<String> colNames;

  /** The col types. */
  private final List<String> colTypes;

  /**
   * Instantiates a new lens jdbc result set metadata.
   *
   * @param metadata
   *          the metadata
   */
  public LensJdbcResultSetMetadata(QueryResultSetMetadata metadata) {
    this.metadata = metadata;
    colNames = new ArrayList<String>();
    colTypes = new ArrayList<String>();
    for (ResultColumn column : metadata.getColumns()) {
      colNames.add(column.getName());
      colTypes.add(column.getType().name());
    }
  }

  @Override
  public int getColumnCount() throws SQLException {
    return metadata.getColumns().size();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#isAutoIncrement(int)
   */
  @Override
  public boolean isAutoIncrement(int index) throws SQLException {
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#isCaseSensitive(int)
   */
  @Override
  public boolean isCaseSensitive(int index) throws SQLException {
    String type = colTypes.get(toZeroIndex(index));

    if ("string".equalsIgnoreCase(type)) {
      return true;
    } else {
      return false;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#isSearchable(int)
   */
  @Override
  public boolean isSearchable(int i) throws SQLException {
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#isCurrency(int)
   */
  @Override
  public boolean isCurrency(int i) throws SQLException {
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#isNullable(int)
   */
  @Override
  public int isNullable(int i) throws SQLException {
    return ResultSetMetaData.columnNullable;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#isSigned(int)
   */
  @Override
  public boolean isSigned(int i) throws SQLException {
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getColumnDisplaySize(int)
   */
  @Override
  public int getColumnDisplaySize(int i) throws SQLException {
    return getPrecision(i);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getColumnLabel(int)
   */
  @Override
  public String getColumnLabel(int i) throws SQLException {
    return colNames.get(toZeroIndex(i));
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getColumnName(int)
   */
  @Override
  public String getColumnName(int i) throws SQLException {
    return colNames.get(toZeroIndex(i));
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getSchemaName(int)
   */
  @Override
  public String getSchemaName(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getPrecision(int)
   */
  @Override
  public int getPrecision(int i) throws SQLException {
    return JDBCUtils.columnPrecision(getColumnType(i));
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getScale(int)
   */
  @Override
  public int getScale(int i) throws SQLException {
    return JDBCUtils.columnScale(getColumnType(i));
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getTableName(int)
   */
  @Override
  public String getTableName(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getCatalogName(int)
   */
  @Override
  public String getCatalogName(int i) throws SQLException {
    throw new SQLException("Opertation not supported!!!");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getColumnType(int)
   */
  @Override
  public int getColumnType(int index) throws SQLException {
    return JDBCUtils.getSQLType(colTypes.get(toZeroIndex(index)));
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getColumnTypeName(int)
   */
  @Override
  public String getColumnTypeName(int index) throws SQLException {
    return colTypes.get(toZeroIndex(index));
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#isReadOnly(int)
   */
  @Override
  public boolean isReadOnly(int i) throws SQLException {
    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#isWritable(int)
   */
  @Override
  public boolean isWritable(int i) throws SQLException {
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#isDefinitelyWritable(int)
   */
  @Override
  public boolean isDefinitelyWritable(int i) throws SQLException {
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getColumnClassName(int)
   */
  @Override
  public String getColumnClassName(int i) throws SQLException {
    return JDBCUtils.columnClassName(getColumnType(i));
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Wrapper#unwrap(java.lang.Class)
   */
  @Override
  public <T> T unwrap(Class<T> tClass) throws SQLException {
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
   */
  @Override
  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    return false;
  }

  /**
   * To zero index.
   *
   * @param column
   *          the column
   * @return the int
   * @throws SQLException
   *           the SQL exception
   */
  protected int toZeroIndex(int column) throws SQLException {
    if (colTypes.isEmpty()) {
      throw new SQLException("Could not determine column type name for ResultSet");
    }
    if (column < 1 || column > colTypes.size()) {
      throw new SQLException("Invalid column value: " + column);
    }
    return column - 1;
  }
}

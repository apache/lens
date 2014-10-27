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

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

import org.apache.lens.api.query.*;

/**
 * The Class LensJdbcResultSet.
 */
public class LensJdbcResultSet implements ResultSet {

  /** The result. */
  private final QueryResult result;

  /** The iterators. */
  private final Iterator<ResultRow> iterators;

  /** The current row. */
  private ResultRow currentRow;

  /** The metadata. */
  private final QueryResultSetMetadata metadata;

  /** The statement. */
  private final LensJdbcStatement statement;

  /** The col names. */
  private final List<String> colNames;

  /** The col types. */
  private final List<ResultColumnType> colTypes;

  /** The closed. */
  private boolean closed;

  /** The wasnull. */
  private boolean wasnull;

  /**
   * Instantiates a new lens jdbc result set.
   *
   * @param result
   *          the result
   * @param metadata
   *          the metadata
   * @param statement
   *          the statement
   */
  public LensJdbcResultSet(QueryResult result, QueryResultSetMetadata metadata, LensJdbcStatement statement) {
    this.result = result;
    this.metadata = metadata;
    this.statement = statement;
    colNames = new ArrayList<String>();
    colTypes = new ArrayList<ResultColumnType>();
    for (ResultColumn col : metadata.getColumns()) {
      colNames.add(col.getName());
      colTypes.add(col.getType());
    }
    if (result instanceof InMemoryQueryResult) {
      iterators = ((InMemoryQueryResult) result).getRows().iterator();
    } else {
      iterators = null;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#next()
   */
  @Override
  public boolean next() throws SQLException {
    if (closed) {
      throw new SQLException("You cannot iterate after resultset is closed");
    }

    if (iterators != null && iterators.hasNext()) {
      currentRow = iterators.next();
      return true;
    }
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#close()
   */
  @Override
  public void close() throws SQLException {
    closed = true;
    statement.closeResultSet();
    statement.close();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#wasNull()
   */
  @Override
  public boolean wasNull() throws SQLException {
    return wasnull;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getString(int)
   */
  @Override
  public String getString(int i) throws SQLException {
    return String.valueOf(getObject(i));
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getString(java.lang.String)
   */
  @Override
  public String getString(String colName) throws SQLException {
    return getString(findColumn(colName));
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBoolean(int)
   */
  @Override
  public boolean getBoolean(int i) throws SQLException {
    Object obj = getObject(i);
    if (Boolean.class.isInstance(obj)) {
      return (Boolean) obj;
    } else if (obj == null) {
      return false;
    } else if (Number.class.isInstance(obj)) {
      return ((Number) obj).intValue() != 0;
    } else if (String.class.isInstance(obj)) {
      return !((String) obj).equals("0");
    }
    throw new SQLException("Cannot convert column " + i + "to boolean");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBoolean(java.lang.String)
   */
  @Override
  public boolean getBoolean(String colName) throws SQLException {
    return getBoolean(findColumn(colName));
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getByte(int)
   */
  @Override
  public byte getByte(int i) throws SQLException {
    Object obj = getObject(i);
    if (Number.class.isInstance(obj)) {
      return ((Number) obj).byteValue();
    } else if (obj == null) {
      return 0;
    }
    throw new SQLException("Cannot covert column " + i + " to byte");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getByte(java.lang.String)
   */
  @Override
  public byte getByte(String colName) throws SQLException {
    return getByte(findColumn(colName));
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getShort(int)
   */
  @Override
  public short getShort(int i) throws SQLException {
    try {
      Object obj = getObject(i);
      if (Number.class.isInstance(obj)) {
        return ((Number) obj).shortValue();
      } else if (obj == null) {
        return 0;
      } else if (String.class.isInstance(obj)) {
        return Short.valueOf((String) obj);
      }
      throw new Exception("Illegal Conversion");
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + i + "to short:" + e.toString(), e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getShort(java.lang.String)
   */
  @Override
  public short getShort(String colName) throws SQLException {
    return getShort(findColumn(colName));
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getInt(int)
   */
  @Override
  public int getInt(int i) throws SQLException {
    try {
      Object obj = getObject(i);
      if (Number.class.isInstance(obj)) {
        return ((Number) obj).intValue();
      } else if (obj == null) {
        return 0;
      } else if (String.class.isInstance(obj)) {
        return Integer.valueOf((String) obj);
      }
      throw new Exception("Illegal Conversion");
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + i + "to short:" + e.toString(), e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getInt(java.lang.String)
   */
  @Override
  public int getInt(String colName) throws SQLException {
    return getInt(findColumn(colName));
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getLong(int)
   */
  @Override
  public long getLong(int i) throws SQLException {
    try {
      Object obj = getObject(i);
      if (Number.class.isInstance(obj)) {
        return ((Number) obj).longValue();
      } else if (obj == null) {
        return 0;
      } else if (String.class.isInstance(obj)) {
        return Long.valueOf((String) obj);
      }
      throw new Exception("Illegal Conversion");
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + i + "to short:" + e.toString(), e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getLong(java.lang.String)
   */
  @Override
  public long getLong(String colName) throws SQLException {
    return getLong(findColumn(colName));
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getFloat(int)
   */
  @Override
  public float getFloat(int i) throws SQLException {
    try {
      Object obj = getObject(i);
      if (Number.class.isInstance(obj)) {
        return ((Number) obj).floatValue();
      } else if (obj == null) {
        return 0;
      } else if (String.class.isInstance(obj)) {
        return Float.valueOf((String) obj);
      }
      throw new Exception("Illegal Conversion");
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + i + "to short:" + e.toString(), e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getFloat(java.lang.String)
   */
  @Override
  public float getFloat(String colName) throws SQLException {
    return getFloat(findColumn(colName));
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getDouble(int)
   */
  @Override
  public double getDouble(int i) throws SQLException {
    try {
      Object obj = getObject(i);
      if (Number.class.isInstance(obj)) {
        return ((Number) obj).doubleValue();
      } else if (obj == null) {
        return 0;
      } else if (String.class.isInstance(obj)) {
        return Double.valueOf((String) obj);
      }
      throw new Exception("Illegal Conversion");
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + i + "to short:" + e.toString(), e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getDouble(java.lang.String)
   */
  @Override
  public double getDouble(String colName) throws SQLException {
    return getDouble(findColumn(colName));
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBytes(int)
   */
  @Override
  public byte[] getBytes(int i) throws SQLException {
    Object obj = getObject(i);
    if (obj == null) {
      return null;
    }
    try {
      ByteArrayOutputStream boas = new ByteArrayOutputStream();
      ObjectOutputStream o = new ObjectOutputStream(boas);
      o.writeObject(obj);
      return boas.toByteArray();
    } catch (IOException e) {
      throw new SQLException("Unable to convert object" + i + " to byte array", e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBytes(java.lang.String)
   */
  @Override
  public byte[] getBytes(String s) throws SQLException {
    return getBytes(findColumn(s));
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getDate(int)
   */
  @Override
  public Date getDate(int i) throws SQLException {
    Object obj = getObject(i);
    if (obj == null) {
      return null;
    }
    if (obj instanceof Date) {
      return (Date) obj;
    }
    try {
      if (obj instanceof String) {
        return Date.valueOf((String) obj);
      }
    } catch (Exception e) {
      throw new SQLException("Cannot covert column " + i + " to date", e);
    }
    throw new SQLException("Cannot covert column " + i + " to date");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getDate(java.lang.String)
   */
  @Override
  public Date getDate(String colName) throws SQLException {
    return getDate(findColumn(colName));
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getTimestamp(int)
   */
  @Override
  public Timestamp getTimestamp(int i) throws SQLException {
    Object obj = getObject(i);
    if (obj instanceof Timestamp) {
      return (Timestamp) obj;
    }
    if (obj instanceof String) {
      return Timestamp.valueOf((String) obj);
    }
    throw new SQLException("Cannot convert column " + i + " to timestamp");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getTimestamp(java.lang.String)
   */
  @Override
  public Timestamp getTimestamp(String colName) throws SQLException {
    return getTimestamp(findColumn(colName));
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return new LensJdbcResultSetMetadata(metadata);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getObject(int)
   */
  @Override
  public Object getObject(int index) throws SQLException {
    if (closed) {
      throw new SQLException("Cannot read from closed resultset");
    }
    if (currentRow == null) {
      throw new SQLException("No row found.");
    }
    if (currentRow.getValues().isEmpty()) {
      throw new SQLException("Rowset does not contain any columns");
    }
    if (index > currentRow.getValues().size()) {
      throw new SQLException("Invalid column index: " + index);
    }
    Object obj = currentRow.getValues().get(toZeroIndex(index));
    if (obj == null) {
      wasnull = true;
    }
    return obj;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getObject(java.lang.String)
   */
  @Override
  public Object getObject(String colName) throws SQLException {
    int index = findColumn(colName);
    return getObject(index);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#findColumn(java.lang.String)
   */
  @Override
  public int findColumn(String colName) throws SQLException {
    int index = colNames.indexOf(colName);
    if (index == -1) {
      throw new SQLException("Column with " + colName + " not found");
    } else {
      return ++index;
    }
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#clearWarnings()
   */
  @Override
  public void clearWarnings() throws SQLException {

  }

  @Override
  public int getFetchDirection() throws SQLException {
    return FETCH_FORWARD;
  }

  @Override
  public int getType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public int getConcurrency() throws SQLException {
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public Statement getStatement() throws SQLException {
    return statement;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed;
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

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getTime(int)
   */
  @Override
  public Time getTime(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getTime(java.lang.String)
   */
  @Override
  public Time getTime(String s) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getAsciiStream(int)
   */
  @Override
  public InputStream getAsciiStream(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getUnicodeStream(int)
   */
  @Override
  public InputStream getUnicodeStream(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBinaryStream(int)
   */
  @Override
  public InputStream getBinaryStream(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBigDecimal(int, int)
   */
  @Override
  public BigDecimal getBigDecimal(int i, int scale) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBigDecimal(java.lang.String, int)
   */
  @Override
  public BigDecimal getBigDecimal(String s, int i) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBigDecimal(java.lang.String)
   */
  @Override
  public BigDecimal getBigDecimal(String s) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBigDecimal(int)
   */
  @Override
  public BigDecimal getBigDecimal(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getAsciiStream(java.lang.String)
   */
  @Override
  public InputStream getAsciiStream(String s) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getUnicodeStream(java.lang.String)
   */
  @Override
  public InputStream getUnicodeStream(String s) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBinaryStream(java.lang.String)
   */
  @Override
  public InputStream getBinaryStream(String s) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public String getCursorName() throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getCharacterStream(int)
   */
  @Override
  public Reader getCharacterStream(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getCharacterStream(java.lang.String)
   */
  @Override
  public Reader getCharacterStream(String s) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public boolean isFirst() throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public boolean isLast() throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#beforeFirst()
   */
  @Override
  public void beforeFirst() throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#afterLast()
   */
  @Override
  public void afterLast() throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#first()
   */
  @Override
  public boolean first() throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#last()
   */
  @Override
  public boolean last() throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public int getRow() throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#absolute(int)
   */
  @Override
  public boolean absolute(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#relative(int)
   */
  @Override
  public boolean relative(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#previous()
   */
  @Override
  public boolean previous() throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void setFetchDirection(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public int getFetchSize() throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void setFetchSize(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#rowUpdated()
   */
  @Override
  public boolean rowUpdated() throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#rowInserted()
   */
  @Override
  public boolean rowInserted() throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#rowDeleted()
   */
  @Override
  public boolean rowDeleted() throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNull(int)
   */
  @Override
  public void updateNull(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBoolean(int, boolean)
   */
  @Override
  public void updateBoolean(int i, boolean b) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateByte(int, byte)
   */
  @Override
  public void updateByte(int i, byte b) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateShort(int, short)
   */
  @Override
  public void updateShort(int i, short i2) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateInt(int, int)
   */
  @Override
  public void updateInt(int i, int i2) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateLong(int, long)
   */
  @Override
  public void updateLong(int i, long l) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateFloat(int, float)
   */
  @Override
  public void updateFloat(int i, float v) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateDouble(int, double)
   */
  @Override
  public void updateDouble(int i, double v) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBigDecimal(int, java.math.BigDecimal)
   */
  @Override
  public void updateBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateString(int, java.lang.String)
   */
  @Override
  public void updateString(int i, String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBytes(int, byte[])
   */
  @Override
  public void updateBytes(int i, byte[] bytes) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateDate(int, java.sql.Date)
   */
  @Override
  public void updateDate(int i, Date date) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateTime(int, java.sql.Time)
   */
  @Override
  public void updateTime(int i, Time time) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateTimestamp(int, java.sql.Timestamp)
   */
  @Override
  public void updateTimestamp(int i, Timestamp timestamp) throws SQLException {

    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream, int)
   */
  @Override
  public void updateAsciiStream(int i, InputStream inputStream, int i2) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream, int)
   */
  @Override
  public void updateBinaryStream(int i, InputStream inputStream, int i2) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader, int)
   */
  @Override
  public void updateCharacterStream(int i, Reader reader, int i2) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateObject(int, java.lang.Object, int)
   */
  @Override
  public void updateObject(int i, Object o, int i2) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateObject(int, java.lang.Object)
   */
  @Override
  public void updateObject(int i, Object o) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNull(java.lang.String)
   */
  @Override
  public void updateNull(String s) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBoolean(java.lang.String, boolean)
   */
  @Override
  public void updateBoolean(String s, boolean b) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateByte(java.lang.String, byte)
   */
  @Override
  public void updateByte(String s, byte b) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateShort(java.lang.String, short)
   */
  @Override
  public void updateShort(String s, short i) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateInt(java.lang.String, int)
   */
  @Override
  public void updateInt(String s, int i) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateLong(java.lang.String, long)
   */
  @Override
  public void updateLong(String s, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateFloat(java.lang.String, float)
   */
  @Override
  public void updateFloat(String s, float v) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateDouble(java.lang.String, double)
   */
  @Override
  public void updateDouble(String s, double v) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBigDecimal(java.lang.String, java.math.BigDecimal)
   */
  @Override
  public void updateBigDecimal(String s, BigDecimal bigDecimal) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateString(java.lang.String, java.lang.String)
   */
  @Override
  public void updateString(String s, String s2) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBytes(java.lang.String, byte[])
   */
  @Override
  public void updateBytes(String s, byte[] bytes) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateDate(java.lang.String, java.sql.Date)
   */
  @Override
  public void updateDate(String s, Date date) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateTime(java.lang.String, java.sql.Time)
   */
  @Override
  public void updateTime(String s, Time time) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateTimestamp(java.lang.String, java.sql.Timestamp)
   */
  @Override
  public void updateTimestamp(String s, Timestamp timestamp) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateAsciiStream(java.lang.String, java.io.InputStream, int)
   */
  @Override
  public void updateAsciiStream(String s, InputStream inputStream, int i) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBinaryStream(java.lang.String, java.io.InputStream, int)
   */
  @Override
  public void updateBinaryStream(String s, InputStream inputStream, int i) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateCharacterStream(java.lang.String, java.io.Reader, int)
   */
  @Override
  public void updateCharacterStream(String s, Reader reader, int i) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateObject(java.lang.String, java.lang.Object, int)
   */
  @Override
  public void updateObject(String s, Object o, int i) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateObject(java.lang.String, java.lang.Object)
   */
  @Override
  public void updateObject(String s, Object o) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#insertRow()
   */
  @Override
  public void insertRow() throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateRow()
   */
  @Override
  public void updateRow() throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#deleteRow()
   */
  @Override
  public void deleteRow() throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#refreshRow()
   */
  @Override
  public void refreshRow() throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#cancelRowUpdates()
   */
  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#moveToInsertRow()
   */
  @Override
  public void moveToInsertRow() throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#moveToCurrentRow()
   */
  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getObject(int, java.util.Map)
   */
  @Override
  public Object getObject(int i, Map<String, Class<?>> stringClassMap) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getRef(int)
   */
  @Override
  public Ref getRef(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBlob(int)
   */
  @Override
  public Blob getBlob(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getClob(int)
   */
  @Override
  public Clob getClob(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getArray(int)
   */
  @Override
  public Array getArray(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getObject(java.lang.String, java.util.Map)
   */
  @Override
  public Object getObject(String s, Map<String, Class<?>> stringClassMap) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getRef(java.lang.String)
   */
  @Override
  public Ref getRef(String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBlob(java.lang.String)
   */
  @Override
  public Blob getBlob(String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getClob(java.lang.String)
   */
  @Override
  public Clob getClob(String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getArray(java.lang.String)
   */
  @Override
  public Array getArray(String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getDate(int, java.util.Calendar)
   */
  @Override
  public Date getDate(int i, Calendar calendar) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getDate(java.lang.String, java.util.Calendar)
   */
  @Override
  public Date getDate(String s, Calendar calendar) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getTime(int, java.util.Calendar)
   */
  @Override
  public Time getTime(int i, Calendar calendar) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getTime(java.lang.String, java.util.Calendar)
   */
  @Override
  public Time getTime(String s, Calendar calendar) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getTimestamp(int, java.util.Calendar)
   */
  @Override
  public Timestamp getTimestamp(int i, Calendar calendar) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getTimestamp(java.lang.String, java.util.Calendar)
   */
  @Override
  public Timestamp getTimestamp(String s, Calendar calendar) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getURL(int)
   */
  @Override
  public URL getURL(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getURL(java.lang.String)
   */
  @Override
  public URL getURL(String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateRef(int, java.sql.Ref)
   */
  @Override
  public void updateRef(int i, Ref ref) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateRef(java.lang.String, java.sql.Ref)
   */
  @Override
  public void updateRef(String s, Ref ref) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBlob(int, java.sql.Blob)
   */
  @Override
  public void updateBlob(int i, Blob blob) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBlob(java.lang.String, java.sql.Blob)
   */
  @Override
  public void updateBlob(String s, Blob blob) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateClob(int, java.sql.Clob)
   */
  @Override
  public void updateClob(int i, Clob clob) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateClob(java.lang.String, java.sql.Clob)
   */
  @Override
  public void updateClob(String s, Clob clob) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateArray(int, java.sql.Array)
   */
  @Override
  public void updateArray(int i, Array array) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateArray(java.lang.String, java.sql.Array)
   */
  @Override
  public void updateArray(String s, Array array) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getRowId(int)
   */
  @Override
  public RowId getRowId(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getRowId(java.lang.String)
   */
  @Override
  public RowId getRowId(String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateRowId(int, java.sql.RowId)
   */
  @Override
  public void updateRowId(int i, RowId rowId) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateRowId(java.lang.String, java.sql.RowId)
   */
  @Override
  public void updateRowId(String s, RowId rowId) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNString(int, java.lang.String)
   */
  @Override
  public void updateNString(int i, String s) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNString(java.lang.String, java.lang.String)
   */
  @Override
  public void updateNString(String s, String s2) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNClob(int, java.sql.NClob)
   */
  @Override
  public void updateNClob(int i, NClob nClob) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNClob(java.lang.String, java.sql.NClob)
   */
  @Override
  public void updateNClob(String s, NClob nClob) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getNClob(int)
   */
  @Override
  public NClob getNClob(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getNClob(java.lang.String)
   */
  @Override
  public NClob getNClob(String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getSQLXML(int)
   */
  @Override
  public SQLXML getSQLXML(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getSQLXML(java.lang.String)
   */
  @Override
  public SQLXML getSQLXML(String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateSQLXML(int, java.sql.SQLXML)
   */
  @Override
  public void updateSQLXML(int i, SQLXML sqlxml) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateSQLXML(java.lang.String, java.sql.SQLXML)
   */
  @Override
  public void updateSQLXML(String s, SQLXML sqlxml) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getNString(int)
   */
  @Override
  public String getNString(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getNString(java.lang.String)
   */
  @Override
  public String getNString(String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getNCharacterStream(int)
   */
  @Override
  public Reader getNCharacterStream(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getNCharacterStream(java.lang.String)
   */
  @Override
  public Reader getNCharacterStream(String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNCharacterStream(int, java.io.Reader, long)
   */
  @Override
  public void updateNCharacterStream(int i, Reader reader, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNCharacterStream(java.lang.String, java.io.Reader, long)
   */
  @Override
  public void updateNCharacterStream(String s, Reader reader, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream, long)
   */
  @Override
  public void updateAsciiStream(int i, InputStream inputStream, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream, long)
   */
  @Override
  public void updateBinaryStream(int i, InputStream inputStream, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader, long)
   */
  @Override
  public void updateCharacterStream(int i, Reader reader, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateAsciiStream(java.lang.String, java.io.InputStream, long)
   */
  @Override
  public void updateAsciiStream(String s, InputStream inputStream, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBinaryStream(java.lang.String, java.io.InputStream, long)
   */
  @Override
  public void updateBinaryStream(String s, InputStream inputStream, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateCharacterStream(java.lang.String, java.io.Reader, long)
   */
  @Override
  public void updateCharacterStream(String s, Reader reader, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBlob(int, java.io.InputStream, long)
   */
  @Override
  public void updateBlob(int i, InputStream inputStream, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBlob(java.lang.String, java.io.InputStream, long)
   */
  @Override
  public void updateBlob(String s, InputStream inputStream, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateClob(int, java.io.Reader, long)
   */
  @Override
  public void updateClob(int i, Reader reader, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateClob(java.lang.String, java.io.Reader, long)
   */
  @Override
  public void updateClob(String s, Reader reader, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNClob(int, java.io.Reader, long)
   */
  @Override
  public void updateNClob(int i, Reader reader, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNClob(java.lang.String, java.io.Reader, long)
   */
  @Override
  public void updateNClob(String s, Reader reader, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNCharacterStream(int, java.io.Reader)
   */
  @Override
  public void updateNCharacterStream(int i, Reader reader) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNCharacterStream(java.lang.String, java.io.Reader)
   */
  @Override
  public void updateNCharacterStream(String s, Reader reader) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream)
   */
  @Override
  public void updateAsciiStream(int i, InputStream inputStream) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream)
   */
  @Override
  public void updateBinaryStream(int i, InputStream inputStream) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader)
   */
  @Override
  public void updateCharacterStream(int i, Reader reader) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateAsciiStream(java.lang.String, java.io.InputStream)
   */
  @Override
  public void updateAsciiStream(String s, InputStream inputStream) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBinaryStream(java.lang.String, java.io.InputStream)
   */
  @Override
  public void updateBinaryStream(String s, InputStream inputStream) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateCharacterStream(java.lang.String, java.io.Reader)
   */
  @Override
  public void updateCharacterStream(String s, Reader reader) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBlob(int, java.io.InputStream)
   */
  @Override
  public void updateBlob(int i, InputStream inputStream) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBlob(java.lang.String, java.io.InputStream)
   */
  @Override
  public void updateBlob(String s, InputStream inputStream) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateClob(int, java.io.Reader)
   */
  @Override
  public void updateClob(int i, Reader reader) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateClob(java.lang.String, java.io.Reader)
   */
  @Override
  public void updateClob(String s, Reader reader) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNClob(int, java.io.Reader)
   */
  @Override
  public void updateNClob(int i, Reader reader) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNClob(java.lang.String, java.io.Reader)
   */
  @Override
  public void updateNClob(String s, Reader reader) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /**
   * Gets the object.
   *
   * @param <T>
   *          the generic type
   * @param columnIndex
   *          the column index
   * @param type
   *          the type
   * @return the object
   * @throws SQLException
   *           the SQL exception
   */
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /**
   * Gets the object.
   *
   * @param <T>
   *          the generic type
   * @param columnLabel
   *          the column label
   * @param type
   *          the type
   * @return the object
   * @throws SQLException
   *           the SQL exception
   */
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    throw new SQLException("Operation not supported!!!");
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
}

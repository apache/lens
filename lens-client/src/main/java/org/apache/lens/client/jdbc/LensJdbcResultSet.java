package org.apache.lens.client.jdbc;

/*
 * #%L
 * Lens client
 * %%
 * Copyright (C) 2014 Apache Software Foundation
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


import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

import org.apache.lens.api.query.*;


public class LensJdbcResultSet implements ResultSet {


  private final QueryResult result;
  private final Iterator<ResultRow> iterators;
  private ResultRow currentRow;
  private final QueryResultSetMetadata metadata;
  private final LensJdbcStatement statement;
  private final List<String> colNames;
  private final List<ResultColumnType> colTypes;
  private boolean closed;
  private boolean wasnull;

  public LensJdbcResultSet(QueryResult result, QueryResultSetMetadata metadata,
                            LensJdbcStatement statement) {
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

  @Override
  public void close() throws SQLException {
    closed = true;
    statement.closeResultSet();
    statement.close();
  }

  @Override
  public boolean wasNull() throws SQLException {
    return wasnull;
  }

  @Override
  public String getString(int i) throws SQLException {
    return String.valueOf(getObject(i));
  }

  @Override
  public String getString(String colName) throws SQLException {
    return getString(findColumn(colName));
  }

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

  @Override
  public boolean getBoolean(String colName) throws SQLException {
    return getBoolean(findColumn(colName));
  }

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

  @Override
  public byte getByte(String colName) throws SQLException {
    return getByte(findColumn(colName));
  }

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
      throw new SQLException("Cannot convert column " + i + "to short:"
          + e.toString(), e);
    }
  }


  @Override
  public short getShort(String colName) throws SQLException {
    return getShort(findColumn(colName));
  }

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
      throw new SQLException("Cannot convert column " + i + "to short:"
          + e.toString(), e);
    }
  }

  @Override
  public int getInt(String colName) throws SQLException {
    return getInt(findColumn(colName));
  }

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
      throw new SQLException("Cannot convert column " + i + "to short:"
          + e.toString(), e);
    }
  }

  @Override
  public long getLong(String colName) throws SQLException {
    return getLong(findColumn(colName));
  }

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
      throw new SQLException("Cannot convert column " + i + "to short:"
          + e.toString(), e);
    }
  }

  @Override
  public float getFloat(String colName) throws SQLException {
    return getFloat(findColumn(colName));
  }


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
      throw new SQLException("Cannot convert column " + i + "to short:"
          + e.toString(), e);
    }
  }

  @Override
  public double getDouble(String colName) throws SQLException {
    return getDouble(findColumn(colName));
  }


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

  @Override
  public byte[] getBytes(String s) throws SQLException {
    return getBytes(findColumn(s));
  }

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

  @Override
  public Date getDate(String colName) throws SQLException {
    return getDate(findColumn(colName));
  }

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

  @Override
  public Timestamp getTimestamp(String colName) throws SQLException {
    return getTimestamp(findColumn(colName));
  }


  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return new LensJdbcResultSetMetadata(metadata);
  }

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

  @Override
  public Object getObject(String colName) throws SQLException {
    int index = findColumn(colName);
    return getObject(index);
  }

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


  protected int toZeroIndex(int column) throws SQLException {
    if (colTypes.isEmpty()) {
      throw new SQLException(
          "Could not determine column type name for ResultSet");
    }
    if (column < 1 || column > colTypes.size()) {
      throw new SQLException("Invalid column value: " + column);
    }
    return column - 1;
  }

  @Override
  public Time getTime(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }


  @Override
  public Time getTime(String s) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }


  @Override
  public InputStream getAsciiStream(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public InputStream getUnicodeStream(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public InputStream getBinaryStream(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public BigDecimal getBigDecimal(int i, int scale) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public BigDecimal getBigDecimal(String s, int i) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }


  @Override
  public BigDecimal getBigDecimal(String s) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public BigDecimal getBigDecimal(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }


  @Override
  public InputStream getAsciiStream(String s) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public InputStream getUnicodeStream(String s) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public InputStream getBinaryStream(String s) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }


  @Override
  public String getCursorName() throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }


  @Override
  public Reader getCharacterStream(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

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

  @Override
  public void beforeFirst() throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void afterLast() throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public boolean first() throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public boolean last() throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public int getRow() throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public boolean absolute(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public boolean relative(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

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


  @Override
  public boolean rowUpdated() throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public boolean rowInserted() throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void updateNull(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void updateBoolean(int i, boolean b) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void updateByte(int i, byte b) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void updateShort(int i, short i2) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void updateInt(int i, int i2) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void updateLong(int i, long l) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void updateFloat(int i, float v) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void updateDouble(int i, double v) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void updateBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void updateString(int i, String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void updateBytes(int i, byte[] bytes) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void updateDate(int i, Date date) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void updateTime(int i, Time time) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void updateTimestamp(int i, Timestamp timestamp) throws SQLException {

    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateAsciiStream(int i, InputStream inputStream, int i2) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateBinaryStream(int i, InputStream inputStream, int i2) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateCharacterStream(int i, Reader reader, int i2) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateObject(int i, Object o, int i2) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateObject(int i, Object o) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateNull(String s) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateBoolean(String s, boolean b) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateByte(String s, byte b) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateShort(String s, short i) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateInt(String s, int i) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateLong(String s, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateFloat(String s, float v) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateDouble(String s, double v) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateBigDecimal(String s, BigDecimal bigDecimal) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateString(String s, String s2) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateBytes(String s, byte[] bytes) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateDate(String s, Date date) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateTime(String s, Time time) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateTimestamp(String s, Timestamp timestamp) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateAsciiStream(String s, InputStream inputStream, int i) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateBinaryStream(String s, InputStream inputStream, int i) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateCharacterStream(String s, Reader reader, int i) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateObject(String s, Object o, int i) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateObject(String s, Object o) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void insertRow() throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateRow() throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void deleteRow() throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void refreshRow() throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new SQLException("Operation not supported");

  }


  @Override
  public Object getObject(int i, Map<String, Class<?>> stringClassMap) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public Ref getRef(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public Blob getBlob(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public Clob getClob(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public Array getArray(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public Object getObject(String s, Map<String, Class<?>> stringClassMap) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public Ref getRef(String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public Blob getBlob(String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public Clob getClob(String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public Array getArray(String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public Date getDate(int i, Calendar calendar) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public Date getDate(String s, Calendar calendar) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public Time getTime(int i, Calendar calendar) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public Time getTime(String s, Calendar calendar) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public Timestamp getTimestamp(int i, Calendar calendar) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public Timestamp getTimestamp(String s, Calendar calendar) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public URL getURL(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public URL getURL(String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void updateRef(int i, Ref ref) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateRef(String s, Ref ref) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateBlob(int i, Blob blob) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateBlob(String s, Blob blob) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateClob(int i, Clob clob) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateClob(String s, Clob clob) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateArray(int i, Array array) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateArray(String s, Array array) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public RowId getRowId(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public RowId getRowId(String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void updateRowId(int i, RowId rowId) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateRowId(String s, RowId rowId) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }


  @Override
  public void updateNString(int i, String s) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateNString(String s, String s2) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateNClob(int i, NClob nClob) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateNClob(String s, NClob nClob) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public NClob getNClob(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public NClob getNClob(String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public SQLXML getSQLXML(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public SQLXML getSQLXML(String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void updateSQLXML(int i, SQLXML sqlxml) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateSQLXML(String s, SQLXML sqlxml) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public String getNString(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public String getNString(String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public Reader getNCharacterStream(int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public Reader getNCharacterStream(String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void updateNCharacterStream(int i, Reader reader, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateNCharacterStream(String s, Reader reader, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateAsciiStream(int i, InputStream inputStream, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateBinaryStream(int i, InputStream inputStream, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateCharacterStream(int i, Reader reader, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateAsciiStream(String s, InputStream inputStream, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateBinaryStream(String s, InputStream inputStream, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateCharacterStream(String s, Reader reader, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateBlob(int i, InputStream inputStream, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateBlob(String s, InputStream inputStream, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateClob(int i, Reader reader, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateClob(String s, Reader reader, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateNClob(int i, Reader reader, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateNClob(String s, Reader reader, long l) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateNCharacterStream(int i, Reader reader) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateNCharacterStream(String s, Reader reader) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateAsciiStream(int i, InputStream inputStream) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateBinaryStream(int i, InputStream inputStream) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateCharacterStream(int i, Reader reader) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateAsciiStream(String s, InputStream inputStream) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateBinaryStream(String s, InputStream inputStream) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateCharacterStream(String s, Reader reader) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateBlob(int i, InputStream inputStream) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateBlob(String s, InputStream inputStream) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateClob(int i, Reader reader) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateClob(String s, Reader reader) throws SQLException {
    throw new SQLException("Operation not supported");

  }

  @Override
  public void updateNClob(int i, Reader reader) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void updateNClob(String s, Reader reader) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public <T> T unwrap(Class<T> tClass) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    return false;
  }
}

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

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.lens.client.LensConnection;
import org.apache.lens.client.LensConnectionParams;

/**
 * JDBC connection class which handles connection level operations to lens server.
 */
public class LensJdbcConnection implements Connection {

  /** The connection. */
  private final LensConnection connection;

  /**
   * Instantiates a new lens jdbc connection.
   *
   * @param uri  the uri
   * @param info the info
   */
  public LensJdbcConnection(String uri, Properties info) {
    LensConnectionParams params = JDBCUtils.parseUrl(uri);
    connection = new LensConnection(params);
    // TODO: should we prompt here?
    connection.open("");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createStatement()
   */
  @Override
  public Statement createStatement() throws SQLException {

    return createStatement(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_FORWARD_ONLY);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String)
   */
  @Override
  public PreparedStatement prepareStatement(String s) throws SQLException {
    return new LensJdbcPreparedStatement(this, s);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createStatement(int, int)
   */
  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw new SQLException("Statements with resultset concurrency :" + resultSetType + " is not supported");
    }
    if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE) {
      throw new SQLException("Statements with resultset type: " + resultSetType + " is not supported");
    }
    return new LensJdbcStatement(this);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String, int, int)
   */
  @Override
  public PreparedStatement prepareStatement(String sql, int rsType, int rsConcurrency) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return true;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#close()
   */
  @Override
  public void close() throws SQLException {

  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return null;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return false;
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#clearWarnings()
   */
  @Override
  public void clearWarnings() throws SQLException {

  }

  @Override
  public String getCatalog() throws SQLException {
    return "";
  }

  public LensConnection getConnection() {
    return this.connection;
  }

  /*
   * Start of not supported operations by JDBC driver.
   */

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createStatement(int, int, int)
   */
  @Override
  public Statement createStatement(int rsConcurrency, int rsType, int rsHoldability) throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareCall(java.lang.String)
   */
  @Override
  public CallableStatement prepareCall(String s) throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareCall(java.lang.String, int, int)
   */
  @Override
  public CallableStatement prepareCall(String s, int i, int i2) throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareCall(java.lang.String, int, int, int)
   */
  @Override
  public CallableStatement prepareCall(String s, int i, int i2, int i3) throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String, int)
   */
  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String, int[])
   */
  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String, java.lang.String[])
   */
  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String, int, int, int)
   */
  @Override
  public PreparedStatement prepareStatement(String s, int rsType, int rsConcurrency, int rsHoldability)
    throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#nativeSQL(java.lang.String)
   */
  @Override
  public String nativeSQL(String s) throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  @Override
  public void setAutoCommit(boolean b) throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#commit()
   */
  @Override
  public void commit() throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#rollback()
   */
  @Override
  public void rollback() throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  @Override
  public void setReadOnly(boolean b) throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  @Override
  public void setCatalog(String s) throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  @Override
  public void setTransactionIsolation(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> stringClassMap) throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  @Override
  public void setHoldability(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setSavepoint()
   */
  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setSavepoint(java.lang.String)
   */
  @Override
  public Savepoint setSavepoint(String s) throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#rollback(java.sql.Savepoint)
   */
  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#releaseSavepoint(java.sql.Savepoint)
   */
  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createClob()
   */
  @Override
  public Clob createClob() throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createBlob()
   */
  @Override
  public Blob createBlob() throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createNClob()
   */
  @Override
  public NClob createNClob() throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createSQLXML()
   */
  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#isValid(int)
   */
  @Override
  public boolean isValid(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setClientInfo(java.lang.String, java.lang.String)
   */
  @Override
  public void setClientInfo(String s, String s2) throws SQLClientInfoException {
    throw new SQLClientInfoException("Operation not supported!!!!", null);
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    throw new SQLClientInfoException("Operation not supported!!!!", null);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getClientInfo(java.lang.String)
   */
  @Override
  public String getClientInfo(String s) throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createArrayOf(java.lang.String, java.lang.Object[])
   */
  @Override
  public Array createArrayOf(String s, Object[] objects) throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createStruct(java.lang.String, java.lang.Object[])
   */
  @Override
  public Struct createStruct(String s, Object[] objects) throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  public void setSchema(String schema) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  public String getSchema() throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /**
   * Abort.
   *
   * @param executor the executor
   * @throws SQLException the SQL exception
   */
  public void abort(Executor executor) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /**
   * Sets the network timeout.
   *
   * @param executor     the executor
   * @param milliseconds the milliseconds
   * @throws SQLException the SQL exception
   */
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  public int getNetworkTimeout() throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Wrapper#unwrap(java.lang.Class)
   */
  @Override
  public <T> T unwrap(Class<T> tClass) throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
   */
  @Override
  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }
}

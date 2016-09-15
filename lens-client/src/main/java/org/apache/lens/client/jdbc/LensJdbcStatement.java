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

import org.apache.lens.api.LensConf;
import org.apache.lens.client.LensStatement;
import org.apache.lens.client.exceptions.LensAPIException;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class LensJdbcStatement.
 */
@Slf4j
public class LensJdbcStatement implements Statement {

  /** The connection. */
  private final LensJdbcConnection connection;

  /** The statement. */
  private final LensStatement statement;

  /** The closed. */
  private boolean closed;

  /**
   * Instantiates a new lens jdbc statement.
   *
   * @param connection the connection
   */
  public LensJdbcStatement(LensJdbcConnection connection) {
    this.connection = connection;
    statement = new LensStatement(connection.getConnection());
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#executeQuery(java.lang.String)
   */
  @Override
  public ResultSet executeQuery(String s) throws SQLException {
    try {
      statement.executeQuery(s, true, null, new LensConf());
    } catch (LensAPIException e) {
      log.error("Execution Failed for Statement:{}", s, e);
    }
    return new LensJdbcResultSet(statement.getResultSet(), statement.getResultSetMetaData(), this);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#close()
   */
  @Override
  public void close() throws SQLException {
    killUnderlyingLensQuery();
    this.closed = true;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#cancel()
   */
  @Override
  public void cancel() throws SQLException {
    killUnderlyingLensQuery();
  }

  /**
   * Kill underlying lens query.
   *
   * @throws SQLException the SQL exception
   */
  private void killUnderlyingLensQuery() throws SQLException {
    if (closed) {
      return;
    }
    if (statement.isIdle()) {
      return;
    }
    boolean status = statement.kill();
    if (!status) {
      throw new SQLException("Unable to close the Statement on lens server");
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#execute(java.lang.String)
   */
  @Override
  public boolean execute(String s) throws SQLException {
    if (closed) {
      throw new SQLException("Cannot execute statemes on closed statements");
    }
    try {
      statement.executeQuery(s, true, null, new LensConf());
    } catch (Throwable t) {
      throw new SQLException(t);
    }
    return statement.wasQuerySuccessful();
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    if (closed) {
      throw new SQLException("Cannot get resultset for closed statements");
    }
    return new LensJdbcResultSet(statement.getResultSet(), statement.getResultSetMetaData(), this);
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public int getResultSetType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return this.connection;
  }

  /**
   * Close result set.
   */
  void closeResultSet() {
    this.statement.closeResultSet();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#clearWarnings()
   */
  @Override
  public void clearWarnings() throws SQLException {
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

  @Override
  public void setCursorName(String s) throws SQLException {
    throw new SQLException("Operation not supported!!!!");

  }

  @Override
  public int getUpdateCount() throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  @Override
  public void setFetchDirection(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!!");

  }

  @Override
  public void setFetchSize(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!!");

  }

  @Override
  public int getFetchSize() throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#executeUpdate(java.lang.String)
   */
  @Override
  public int executeUpdate(String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#addBatch(java.lang.String)
   */
  @Override
  public void addBatch(String s) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#clearBatch()
   */
  @Override
  public void clearBatch() throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#executeBatch()
   */
  @Override
  public int[] executeBatch() throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#getMoreResults(int)
   */
  @Override
  public boolean getMoreResults(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#executeUpdate(java.lang.String, int)
   */
  @Override
  public int executeUpdate(String s, int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
   */
  @Override
  public int executeUpdate(String s, int[] ints) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#executeUpdate(java.lang.String, java.lang.String[])
   */
  @Override
  public int executeUpdate(String s, String[] strings) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#execute(java.lang.String, int)
   */
  @Override
  public boolean execute(String s, int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#execute(java.lang.String, int[])
   */
  @Override
  public boolean execute(String s, int[] ints) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
   */
  @Override
  public boolean execute(String s, String[] strings) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public void setPoolable(boolean b) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public boolean isPoolable() throws SQLException {
    return false;
  }

  /**
   * Close on completion.
   *
   * @throws SQLException the SQL exception
   */
  public void closeOnCompletion() throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  public boolean isCloseOnCompletion() throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public void setMaxFieldSize(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!");

  }

  @Override
  public int getMaxRows() throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public void setMaxRows(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public void setEscapeProcessing(boolean b) throws SQLException {
    throw new SQLException("Operation not supported!!!");

  }

  @Override
  public int getQueryTimeout() throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  @Override
  public void setQueryTimeout(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!!");

  }
}

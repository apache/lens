package com.inmobi.grill.client.jdbc;

/*
 * #%L
 * Grill client
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

import com.inmobi.grill.client.GrillStatement;

import java.sql.*;

public class GrillJdbcStatement implements Statement {


  private final GrillJdbcConnection connection;
  private final GrillStatement statement;

  private boolean closed;

  public GrillJdbcStatement(GrillJdbcConnection connection) {
    this.connection = connection;
    statement = new GrillStatement(connection.getConnection());
  }


  @Override
  public ResultSet executeQuery(String s) throws SQLException {
    statement.execute(s);
    return new GrillJdbcResultSet(statement.getResultSet(),
        statement.getResultSetMetaData(), this);
  }


  @Override
  public void close() throws SQLException {
    killUnderlyingGrillQuery();
    this.closed = true;
  }


  @Override
  public void cancel() throws SQLException {
    killUnderlyingGrillQuery();
  }

  private void killUnderlyingGrillQuery() throws SQLException {
    if (closed) {
      return;
    }
    if (statement.isIdle()) {
      return;
    }
    boolean status = statement.kill();
    if (!status) {
      throw new SQLException("Unable to close the Statement on grill server");
    }
  }



  @Override
  public boolean execute(String s) throws SQLException {
    if (closed) {
      throw new SQLException("Cannot execute statemes on closed statements");
    }
    try {
      statement.execute(s, true);
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
    return new GrillJdbcResultSet(statement.getResultSet(),
        statement.getResultSetMetaData(), this);
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

  void closeResultSet() {
    this.statement.closeResultSet();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
  }


  @Override
  public <T> T unwrap(Class<T> tClass) throws SQLException {
    return null;
  }

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


  @Override
  public int executeUpdate(String s) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public void addBatch(String s) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public void clearBatch() throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }


  @Override
  public boolean getMoreResults(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public int executeUpdate(String s, int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public int executeUpdate(String s, int[] ints) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public int executeUpdate(String s, String[] strings) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public boolean execute(String s, int i) throws SQLException {
    throw new SQLException("Operation not supported");
  }

  @Override
  public boolean execute(String s, int[] ints) throws SQLException {
    throw new SQLException("Operation not supported");
  }

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

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
package org.apache.lens.server.query;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.FailedAttempt;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.FinishedLensQuery;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.session.LensSessionImpl;
import org.apache.lens.server.util.UtilityMethods;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.dbutils.*;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

/**
 * Top level class which logs and retrieves finished query from Database.
 */
@Slf4j
public class LensServerDAO {

  /** The ds. */
  private DataSource ds;

  /**
   * Inits the.
   *
   * @param conf the conf
   */
  public void init(Configuration conf) {
    ds = UtilityMethods.getDataSourceFromConf(conf);
  }

  public Connection getConnection() throws SQLException {
    return ds.getConnection();
  }

  /**
   * Drop finished queries table.
   */
  public void dropFinishedQueriesTable() {
    QueryRunner runner = new QueryRunner(ds);
    try {
      runner.update("drop table finished_queries");
    } catch (SQLException e) {
      log.error("SQL exception while dropping finished queries table.", e);
    }
  }

  /**
   * Method to create finished queries table, this is required for embedded lens server. For production server we will
   * not be creating tables as it would be created upfront.
   *
   * @throws Exception the exception
   */
  public void createFinishedQueriesTable() throws Exception {
    String sql = "CREATE TABLE if not exists finished_queries (handle varchar(255) not null unique,"
      + "userquery varchar(20000) not null," + "submitter varchar(255) not null," + "priority varchar(255), "
      + "starttime bigint, " + "endtime bigint," + "result varchar(255)," + "status varchar(255), "
      + "metadata varchar(100000), " + "rows int, " + "filesize bigint, " + "errormessage varchar(10000), "
      + "driverstarttime bigint, " + "driverendtime bigint, " + "drivername varchar(10000), "
      + "queryname varchar(255), " + "submissiontime bigint, " + "driverquery varchar(1000000), "
      + "conf varchar(100000), numfailedattempts int)";
    try {
      QueryRunner runner = new QueryRunner(ds);
      runner.update(sql);
      log.info("Created finished queries table");
    } catch (SQLException e) {
      log.warn("Unable to create finished queries table", e);
    }
  }

  public void createPreparedQueriesTable() throws Exception {
    String sql = "CREATE TABLE if not exists prepared_queries (handle varchar(255) NOT NULL unique,  userquery "
      + "varchar(20000),  submitter varchar(255) NOT NULL,  timetaken bigint,  queryname varchar(255) DEFAULT NULL, "
      + "drivername varchar(10000) DEFAULT NULL,  driverquery varchar(1000000),  starttime bigint)";
    try {
      QueryRunner runner = new QueryRunner(ds);
      runner.update(sql);
      log.info("Created prepared_queries queries table");
    } catch (SQLException e) {
      log.warn("Unable to create prepared_queries queries table", e);
    }
  }

  public void createFailedAttemptsTable() throws Exception {
    String sql = "CREATE TABLE if not exists failed_attempts (handle varchar(255) not null,"
      + "attempt_number int, drivername varchar(10000), progress float, progressmessage varchar(10000), "
      + "errormessage varchar(10000), driverstarttime bigint, driverendtime bigint)";
    try {
      QueryRunner runner = new QueryRunner(ds);
      runner.update(sql);
      log.info("Created failed_attempts table");
    } catch (SQLException e) {
      log.error("Unable to create failed_attempts table", e);
    }
  }

  /**
   * DAO method to insert a new Finished query into Table.
   *
   * @param query to be inserted
   * @throws SQLException the exception
   */
  public void insertFinishedQuery(FinishedLensQuery query) throws SQLException {
    FinishedLensQuery alreadyExisting = getQuery(query.getHandle());
    if (alreadyExisting == null) {
      // The expected case
      String sql = "insert into finished_queries (handle, userquery, submitter, priority, "
        + "starttime,endtime,result,status,metadata,rows,filesize,"
        + "errormessage,driverstarttime,driverendtime, drivername, queryname, submissiontime, driverquery, conf, "
        + "numfailedattempts)"
        + " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
      Connection conn = null;
      try {
        conn = getConnection();
        conn.setAutoCommit(false);
        QueryRunner runner = new QueryRunner();
        runner.update(conn, sql, query.getHandle(), query.getUserQuery(), query.getSubmitter(), query.getPriority(),
            query.getStartTime(), query.getEndTime(), query.getResult(), query.getStatus(), query.getMetadata(),
            query.getRows(), query.getFileSize(), query.getErrorMessage(), query.getDriverStartTime(),
            query.getDriverEndTime(), query.getDriverName(), query.getQueryName(), query.getSubmissionTime(),
            query.getDriverQuery(), serializeConf(query.getConf()),
            query.getFailedAttempts() == null ? 0 : query.getFailedAttempts().size());
        if (query.getFailedAttempts() != null) {
          for (int i = 0; i < query.getFailedAttempts().size(); i++) {
            insertFailedAttempt(runner, conn, query.getHandle(), query.getFailedAttempts().get(i), i);
          }
        }
        conn.commit();
      } finally {
        DbUtils.closeQuietly(conn);
      }
    } else {
      log.warn("Re insert happening in purge: " + Thread.currentThread().getStackTrace());
      if (alreadyExisting.equals(query)) {
        // This is also okay
        log.warn("Skipping Re-insert. Finished Query found in DB while trying to insert, handle=" + query.getHandle());
      } else {
        String msg = "Found different value pre-existing in DB while trying to insert finished query. "
          + "Old = " + alreadyExisting + "\nNew = " + query;
        throw new SQLException(msg);
      }
    }
  }
  /**
   * DAO method to insert a new Finished query into Table.
   *
   *
   * @param runner
   * @param conn
   *@param handle to be inserted
   * @param index   @throws SQLException the exception
   */
  public void insertFailedAttempt(QueryRunner runner, Connection conn, String handle, FailedAttempt attempt, int index)
    throws SQLException {
    String sql = "insert into failed_attempts(handle, attempt_number, drivername, progress, progressmessage, "
      + "errormessage, driverstarttime, driverendtime) values (?, ?, ?, ?, ?, ?, ?, ?)";
    runner.update(conn, sql, handle, index, attempt.getDriverName(),
      attempt.getProgress(), attempt.getProgressMessage(), attempt.getErrorMessage(),
      attempt.getDriverStartTime(), attempt.getDriverFinishTime());
  }

  public void getFailedAttempts(final FinishedLensQuery query) {
    if (query != null) {
      String handle = query.getHandle();
      ResultSetHandler<List<FailedAttempt>> rsh = new BeanHandler<List<FailedAttempt>>(null) {
        @Override
        public List<FailedAttempt> handle(ResultSet rs) throws SQLException {
          List<FailedAttempt> attempts = Lists.newArrayList();
          while (rs.next()) {
            FailedAttempt attempt = new FailedAttempt(rs.getString(3), rs.getDouble(4), rs.getString(5),
              rs.getString(6), rs.getLong(7), rs.getLong(8));
            attempts.add(attempt);
          }
          return attempts;
        }
      };
      String sql = "select * from failed_attempts where handle=? order by attempt_number";
      QueryRunner runner = new QueryRunner(ds);
      try {
        query.setFailedAttempts(runner.query(sql, rsh, handle));
      } catch (SQLException e) {
        log.error("SQL exception while executing query.", e);
      }
    }
  }



  private String serializeConf(LensConf conf) {
    return Base64.encodeBase64String(conf.toXMLString().getBytes(Charset.defaultCharset()));
  }

  private LensConf deserializeConf(String serializedConf) {
    return LensConf.fromXMLString(new String(Base64.decodeBase64(serializedConf),
        Charset.defaultCharset()), LensConf.class);
  }

  /**
   * Fetch Finished query from Database.
   *
   * @param handle to be fetched
   * @return Finished query.
   */
  public FinishedLensQuery getQuery(String handle) {
    ResultSetHandler<FinishedLensQuery> rsh = new BeanHandler<>(FinishedLensQuery.class,
        new BasicRowProcessor(new FinishedLensQueryBeanProcessor()));
    String sql = "select * from finished_queries where handle=?";
    QueryRunner runner = new QueryRunner(ds);
    try {
      FinishedLensQuery finishedQuery = runner.query(sql, rsh, handle);
      getFailedAttempts(finishedQuery);
      return finishedQuery;
    } catch (SQLException e) {
      log.error("SQL exception while executing query.", e);
    }
    return null;
  }

  private class FinishedLensQueryBeanProcessor extends BeanProcessor {

    @Override
    protected Object processColumn(ResultSet rs, int index, Class<?> propType) throws SQLException {
      Object obj = super.processColumn(rs, index, propType);
      if (obj != null && propType.equals(LensConf.class) && obj instanceof String) {
        return deserializeConf((String) obj);
      }
      return obj;
    }
  }

  private class NestedResultHandler<T> implements ResultSetHandler<T> {

    private final Class<T> type;
    private final RowProcessor convert;

    public NestedResultHandler(Class<T> type, RowProcessor convert) {
      this.type = type;
      this.convert = convert;
    }

    @Override
    public T handle(ResultSet rs) throws SQLException {
      return this.convert.toBean(rs, this.type);
    }
  }

  private class QueryHandleNestedHandler implements ResultSetHandler<QueryHandle> {

    @Override
    public QueryHandle handle(ResultSet rs) throws SQLException {
      try {
        return QueryHandle.fromString(rs.getString(1));
      } catch (IllegalArgumentException exc) {
        log.warn("Warning invalid query handle found in DB " + rs.getString(1));
        return null;
      }
    }
  }

  /**
   * Find finished queries.
   *
   * @param states     the state
   * @param user      the user
   * @param driverName the driver's fully qualified Name
   * @param queryName the query name
   * @param fromDate  the from date
   * @param toDate    the to date
   * @return the list
   * @throws LensException the lens exception
   */
  public List<FinishedLensQuery> findFinishedQueryDetails(List<QueryStatus.Status> states, String user,
    String driverName, String queryName, long fromDate, long toDate) throws LensException {
    ResultSetHandler<FinishedLensQuery> handler = new NestedResultHandler<>(FinishedLensQuery.class,
        new BasicRowProcessor(new FinishedLensQueryBeanProcessor()));
    return findInternal(states, user, driverName, queryName, fromDate, toDate, handler, "*");
  }

  /**
   * Find finished queries.
   *
   * @param states     the state
   * @param user      the user
   * @param driverName the driver's fully qualified Name
   * @param queryName the query name
   * @param fromDate  the from date
   * @param toDate    the to date
   * @return the list
   * @throws LensException the lens exception
   */
  public List<QueryHandle> findFinishedQueries(List<QueryStatus.Status> states, String user, String driverName,
    String queryName, long fromDate, long toDate) throws LensException {

    ResultSetHandler<QueryHandle> handler = new QueryHandleNestedHandler();
    return findInternal(states, user, driverName, queryName, fromDate, toDate, handler, "handle");
  }

  private <T> List<T> findInternal(List<QueryStatus.Status> states, String user, String driverName, String queryName,
    long fromDate, long toDate, final ResultSetHandler<T> handler, String projection) throws LensException {
    StringBuilder builder = new StringBuilder("SELECT " + projection + " FROM finished_queries");
    List<Object> params = new ArrayList<>(3);
    builder.append(" WHERE ");
    List<String> filters = new ArrayList<>(3);

    if (states != null && !states.isEmpty()) {
      StringBuilder statusFilterBuilder = new StringBuilder("status in (");
      String sep = "";
      for(QueryStatus.Status status: states) {
        statusFilterBuilder.append(sep).append("?");
        sep = ", ";
        params.add(status.toString());
      }
      filters.add(statusFilterBuilder.append(")").toString());
    }

    if (StringUtils.isNotBlank(user)) {
      filters.add("submitter=?");
      params.add(user);
    }

    if (StringUtils.isNotBlank(queryName)) {
      filters.add("queryname like ?");
      params.add("%" + queryName + "%");
    }

    if (StringUtils.isNotBlank(driverName)) {
      filters.add("lower(drivername)=?");
      params.add(driverName.toLowerCase());
    }

    filters.add("submissiontime BETWEEN ? AND ?");
    params.add(fromDate);
    params.add(toDate);
    builder.append(StringUtils.join(filters, " AND "));

    ResultSetHandler<List<T>> resultSetHandler = new ResultSetHandler<List<T>>() {
      @Override
      public List<T> handle(ResultSet resultSet) throws SQLException {
        List<T> results = new ArrayList<T>();
        while (resultSet.next()) {
          try {
            results.add(handler.handle(resultSet));
          } catch (RuntimeException e) {
            log.warn("Unable to handle row " + LensServerDAO.toString(resultSet), e);
          }
        }
        return results;
      }
    };

    QueryRunner runner = new QueryRunner(ds);
    String query = builder.toString();
    try {
      return runner.query(query, resultSetHandler, params.toArray());
    } catch (SQLException e) {
      throw new LensException(e);
    }
  }

  private static String toString(ResultSet resultSet) {
    try {
      StringBuilder builder = new StringBuilder();
      for (int index = 1; index <= resultSet.getMetaData().getColumnCount(); index++) {
        builder.append(index > 1 ? ", " : "").append(resultSet.getString(index));
      }
      return builder.toString();
    } catch (SQLException e) {
      return "Error : " + e.getMessage();
    }
  }


  /**
   * Drop active session table.
   */
  public void dropActiveSessionsTable() {
    QueryRunner runner = new QueryRunner(ds);
    try {
      runner.update("drop table active_sessions");
    } catch (SQLException e) {
      log.error("SQL exception while dropping active sessions table.", e);
    }
  }

  /**
   * Drop active queries table.
   */
  public void dropActiveQueries() {
    QueryRunner runner = new QueryRunner(ds);
    try {
      runner.update("drop table active_queries");
    } catch (SQLException e) {
      log.error("SQL exception while dropping active queries table.", e);
    }
  }

  /**
   * Method to create active queries table, this is required for embedded lens server. For production server we will
   * not be creating tables as it would be created upfront.
   *
   */
  public void createActiveQueriesTable() {
    String sql = "CREATE TABLE if not exists active_queries ("
            + "queryid varchar(200) not null,"
            + "querycontext BLOB,"
            + "primary key (queryid)"
            + ")";
    try {
      QueryRunner runner = new QueryRunner(ds);
      runner.update(sql);
      log.info("Created active queries table");
    } catch (SQLException e) {
      log.warn("Unable to create active queries table", e);
    }
  }

  /**
   * Method to create active session table, this is required for embedded lens server. For production server we will
   * not be creating tables as it would be created upfront.
   *
   */
  public void createActiveSessionsTable() throws Exception {
    String sql = "CREATE TABLE if not exists active_sessions ("
            + "sessionid varchar(200) not null,"
            + "sessionobject BLOB,"
            + "primary key (sessionid)"
            + ")";
    try {
      QueryRunner runner = new QueryRunner(ds);
      runner.update(sql);
      log.info("Created active sessions table");
    } catch (SQLException e) {
      log.warn("Unable to create active sessions table", e);
    }
  }

  /**
   * Method to insert a new active query into Table.
   *
   * @param ctx query context
   *
   * @throws SQLException the exception
   *
   */
  public void insertActiveQuery(QueryContext ctx) throws LensException {

    String sql = "insert into active_queries (queryid, querycontext)"
            + " values (?,?)";
    Connection conn = null;
    PreparedStatement pstmt = null;
    try {
      conn = getConnection();
      pstmt = conn.prepareStatement(sql);

      // set input parameters
      pstmt.setString(1, ctx.getQueryHandleString());
      pstmt.setObject(2, SerializationUtils.serialize(ctx));
      pstmt.execute();

      log.info("Inserted query with query " + ctx.getQueryHandleString() + " in database.");
    } catch (SQLException e) {
      log.error("Failed to insert query " + ctx.getQueryHandleString() + " in database with error, " + e);
      throw new LensException(e);
    } finally {
      DbUtils.closeQuietly(pstmt);
      DbUtils.closeQuietly(conn);
    }
  }

  /**
   * Method to insert a new active session into Table.
   *
   * @param session LensSessionPersistInfo object that has to be serialized.
   *
   * @throws SQLException the exception
   *
   */
  public void insertActiveSession(LensSessionImpl.LensSessionPersistInfo session) throws LensException {
    String sql = "insert into active_sessions (sessionid, sessionobject)"
            + " values (?,?)";
    Connection conn = null;
    PreparedStatement pstmt = null;
    try {
      conn = getConnection();
      pstmt = conn.prepareStatement(sql);

      // set input parameters
      pstmt.setString(1, session.getSessionHandle().getPublicId().toString());
      pstmt.setObject(2, SerializationUtils.serialize(session));
      pstmt.execute();

      log.info("Inserted seesion " + session.getSessionHandle().getPublicId() + " in database.");
    } catch (SQLException e) {
      log.error("Failed to insert session " + session.getSessionHandle().getPublicId()
              + " in database with error, " + e);
      throw new LensException(e);
    } finally {
      DbUtils.closeQuietly(pstmt);
      DbUtils.closeQuietly(conn);
    }
  }

  /**
   * Method to update a new active query into Table.
   *
   * @param ctx query context
   *
   * @throws LensException the exception
   *
   */
  public void updateActiveQuery(QueryContext ctx) throws LensException {

    String sql = "UPDATE active_queries SET querycontext=? where queryid=?";
    Connection conn = null;
    PreparedStatement pstmt = null;
    try {
      conn = getConnection();
      pstmt = conn.prepareStatement(sql);

      pstmt.setObject(1, SerializationUtils.serialize(ctx));
      pstmt.setString(2, ctx.getQueryHandleString());
      pstmt.execute();

      log.info("Updated query with query " + ctx.getQueryHandleString() + " with query status as "
              + ctx.getStatus().getStatus() + " in database.");
    } catch (SQLException e) {
      log.error("Failed to update query " + ctx.getQueryHandleString()
              + " in database with error, " + e);
      throw new LensException(e);
    } finally {
      DbUtils.closeQuietly(pstmt);
      DbUtils.closeQuietly(conn);
    }
  }

  /**
   * Method to update a active session into Table.
   *
   * @param session query context object
   *
   * @throws LensException the exception
   *
   */
  public void updateActiveSession(LensSessionImpl.LensSessionPersistInfo session) throws LensException {
    String sql = "UPDATE active_sessions SET sessionobject=? where sessionid=?";
    Connection conn = null;
    PreparedStatement pstmt = null;
    try {
      conn = getConnection();
      pstmt = conn.prepareStatement(sql);

      // set input parameters
      pstmt.setObject(1, SerializationUtils.serialize(session));
      pstmt.setString(2, session.getSessionHandle().getPublicId().toString());
      pstmt.execute();

      log.info("Updated session " + session.getSessionHandle().getPublicId() + " in database.");
    } catch (SQLException e) {
      log.error("Failed to update session " + session.getSessionHandle().getPublicId()
              + " in database with error, " + e);
      throw new LensException(e);
    } finally {
      DbUtils.closeQuietly(pstmt);
      DbUtils.closeQuietly(conn);
    }
  }

  /**
   * Finds Active query.
   *
   * @param queryHandle     the state
   *
   * @return the QueryContext object
   *
   * @throws LensException the lens exception
   *
   */
  public QueryContext findActiveQueryDetails(QueryHandle queryHandle) throws LensException {
    QueryContext ctx = null;
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet rs = null;

    try {

      String sql = "SELECT querycontext FROM active_queries WHERE queryid = ?";

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, queryHandle.toString());

      rs = pstmt.executeQuery();

      if (rs != null) {
        rs.next();
        ctx = (QueryContext) SerializationUtils.deserialize(rs.getBytes(1));
      }
    } catch (SQLException e) {
      log.error("Failed to find active query " + queryHandle.getHandleIdString()
              + " in database with error, " + e);
      throw new LensException(e);
    } finally {
      DbUtils.closeQuietly(rs);
      DbUtils.closeQuietly(pstmt);
      DbUtils.closeQuietly(conn);
    }
    return ctx;
  }

  /**
   * Gets all active query.
   *
   * @return the list of QueryContext objects
   *
   * @throws LensException the lens exception
   *
   */
  public List<QueryContext> getAllActiveQueries() throws LensException {
    List<QueryContext> ctxs = new ArrayList<>();
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet rs = null;

    try {
      String sql = "SELECT querycontext FROM active_queries";
      conn = getConnection();
      pstmt = conn.prepareStatement(sql);

      rs = pstmt.executeQuery();

      while (rs.next()) {
        ctxs.add((QueryContext) SerializationUtils.deserialize(rs.getBytes(1)));
      }
    } catch (SQLException e) {
      log.error("Unable to find all active queries in database, Failed with error, " + e);
      throw new LensException(e);
    } finally {
      DbUtils.closeQuietly(rs);
      DbUtils.closeQuietly(pstmt);
      DbUtils.closeQuietly(conn);
    }
    return ctxs;
  }

  /**
   * Finds active session.
   *
   * @param sessionId     the state
   *
   * @return session object
   *
   * @throws LensException the lens exception
   *
   */
  public LensSessionImpl.LensSessionPersistInfo findActiveSessionDetails(LensSessionHandle sessionId)
          throws LensException {
    LensSessionImpl.LensSessionPersistInfo session = null;
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet rs = null;

    try {
      String sql = "SELECT sessionobject FROM active_sessions WHERE sessionid = '"
              + sessionId.getPublicId() + "'";
      conn = getConnection();
      pstmt = conn.prepareStatement(sql);

      rs = pstmt.executeQuery();

      if (rs != null) {
        rs.next();
        session =
                (LensSessionImpl.LensSessionPersistInfo) SerializationUtils.deserialize(rs.getBytes(1));
      }
    } catch (SQLException e) {
      log.error("Failed to find active session " + sessionId.getPublicId()
              + " in database with error, " + e);
      throw new LensException(e);
    } finally {
      DbUtils.closeQuietly(rs);
      DbUtils.closeQuietly(pstmt);
      DbUtils.closeQuietly(conn);
    }
    return session;
  }

  /**
   * Gets all active session.
   *
   * @return the list of LensSessionImpl.LensSessionPersistInfo objects
   *
   * @throws LensException the lens exception
   *
   */
  public List<LensSessionImpl.LensSessionPersistInfo> getAllActiveSessions() throws LensException {
    List<LensSessionImpl.LensSessionPersistInfo> ctxs = new ArrayList<>();
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet rs = null;

    try {

      String sql = "SELECT sessionobject FROM active_queries";

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      rs = pstmt.executeQuery();

      while (rs.next()) {
        ctxs.add((LensSessionImpl.LensSessionPersistInfo) SerializationUtils.deserialize(rs.getBytes(1)));
      }
    } catch (SQLException e) {
      log.error("Unable to find all active queries in database, Failed with error, " + e);
      throw new LensException(e);
    } finally {
      DbUtils.closeQuietly(rs);
      DbUtils.closeQuietly(pstmt);
      DbUtils.closeQuietly(conn);
    }
    return ctxs;
  }

  /**
   * Delete active query.
   *
   * @param ctx QueryContext object for query
   *
   * @return true on success, false otherwise.
   *
   * @throws LensException the lens exception
   *
   */
  public boolean deleteActiveQuery(QueryContext ctx) throws LensException {

    String sql = "DELETE FROM active_queries where queryid=?";
    Connection conn = null;
    PreparedStatement pstmt = null;
    boolean result = false;
    try {
      conn = getConnection();

      pstmt = conn.prepareStatement(sql);

      // set input parameters
      pstmt.setString(1, ctx.getQueryHandleString());
      result = pstmt.execute();

      log.info("deleted active query " + ctx.getQueryHandleString() + " with final status " + ctx.getStatus()
              + " from database.");
    } catch (SQLException e) {
      log.error("Failed to delete active query " + ctx.getQueryHandleString()
              + " in database with error, " + e);
      throw new LensException(e);
    } finally {
      DbUtils.closeQuietly(pstmt);
      DbUtils.closeQuietly(conn);
    }

    return result;
  }

  /**
   * Delete active session.
   *
   * @param sessionId session id to be deleted
   *
   * @return true on success, false otherwise.
   *
   * @throws LensException the lens exception
   *
   */
  public boolean deleteActiveSession(LensSessionHandle sessionId) throws LensException {

    String sql = "DELETE FROM active_sessions where sessionid=?";
    Connection conn = null;
    PreparedStatement pstmt = null;
    boolean result;

    try {
      conn = getConnection();

      pstmt = conn.prepareStatement(sql);

      // set input parameters
      pstmt.setString(1, sessionId.getPublicId().toString());
      result = pstmt.execute();

      log.info("deleted active session " + sessionId.getPublicId().toString() + " from database.");
    } catch (SQLException e) {
      log.error("Failed to delete active session " + sessionId.getPublicId().toString()
              + " in database with error, " + e);
      throw new LensException(e);
    } finally {
      DbUtils.closeQuietly(pstmt);
      DbUtils.closeQuietly(conn);
    }

    return result;
  }

  /**
   * DAO method to insert a new Prepared query into Table.
   *
   * @param preparedQueryContext to be inserted
   * @throws SQLException the exception
   */
  public void insertPreparedQuery(PreparedQueryContext preparedQueryContext) throws LensException {
    String sql =
        "insert into prepared_queries (handle, userquery, submitter, timetaken, queryname, drivername, "
            + "driverquery, starttime)" + " values (?,?,?,?,?,?,?,?)";
    Connection conn = null;
    try {
      conn = getConnection();
      conn.setAutoCommit(false);
      QueryRunner runner = new QueryRunner();

      long timeTaken =
          preparedQueryContext.getPrepareEndTime().getTime() - preparedQueryContext.getPrepareStartTime().getTime();

      runner.update(conn, sql, preparedQueryContext.getPrepareHandle().getQueryHandleString(),
          preparedQueryContext.getUserQuery(), preparedQueryContext.getSubmittedUser(), timeTaken,
          preparedQueryContext.getQueryName(), preparedQueryContext.getDriverContext().getSelectedDriver().toString(),
          preparedQueryContext.getSelectedDriverQuery(), preparedQueryContext.getPrepareStartTime().getTime());
      conn.commit();
    } catch (SQLException e) {
      log.error("Failed to insert prepared query into database with error, " + e);
      throw new LensException(e);
    } finally {
      DbUtils.closeQuietly(conn);
    }
  }
}

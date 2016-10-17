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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.query.FailedAttempt;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.FinishedLensQuery;
import org.apache.lens.server.util.UtilityMethods;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.dbutils.*;
import org.apache.commons.dbutils.handlers.BeanHandler;
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
            query.getDriverQuery(), serializeConf(query.getConf()), query.getFailedAttempts().size());
        for (int i = 0; i < query.getFailedAttempts().size(); i++) {
          insertFailedAttempt(runner, conn, query.getHandle(), query.getFailedAttempts().get(i), i);
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
}

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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.FinishedLensQuery;
import org.apache.lens.server.util.UtilityMethods;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

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
      + "userquery varchar(10000) not null," + "submitter varchar(255) not null," + "starttime bigint, "
      + "endtime bigint," + "result varchar(255)," + "status varchar(255), " + "metadata varchar(100000), "
      + "rows int, " + "filesize bigint, " + "errormessage varchar(10000), " + "driverstarttime bigint, "
      + "driverendtime bigint, " + "drivername varchar(10000), "
      + "queryname varchar(255), " + "submissiontime bigint" + ")";
    try {
      QueryRunner runner = new QueryRunner(ds);
      runner.update(sql);
      log.info("Created finished queries table");
    } catch (SQLException e) {
      log.warn("Unable to create finished queries table", e);
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
      Connection conn = null;
      String sql = "insert into finished_queries (handle, userquery,submitter,"
        + "starttime,endtime,result,status,metadata,rows,filesize,"
        + "errormessage,driverstarttime,driverendtime, drivername, queryname, submissiontime)"
        + " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
      try {
        conn = getConnection();
        QueryRunner runner = new QueryRunner();
        runner.update(conn, sql, query.getHandle(), query.getUserQuery(), query.getSubmitter(), query.getStartTime(),
          query.getEndTime(), query.getResult(), query.getStatus(), query.getMetadata(), query.getRows(),
          query.getFileSize(), query.getErrorMessage(), query.getDriverStartTime(), query.getDriverEndTime(),
          query.getDriverName(), query.getQueryName(), query.getSubmissionTime());
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
   * Fetch Finished query from Database.
   *
   * @param handle to be fetched
   * @return Finished query.
   */
  public FinishedLensQuery getQuery(String handle) {
    ResultSetHandler<FinishedLensQuery> rsh = new BeanHandler<FinishedLensQuery>(FinishedLensQuery.class);
    String sql = "select * from finished_queries where handle=?";
    QueryRunner runner = new QueryRunner(ds);
    try {
      return runner.query(sql, rsh, handle);
    } catch (SQLException e) {
      log.error("SQL exception while executing query.", e);
    }
    return null;
  }

  /**
   * Find finished queries.
   *
   * @param state     the state
   * @param user      the user
   * @param driverName the driver's fully qualified Name
   * @param queryName the query name
   * @param fromDate  the from date
   * @param toDate    the to date
   * @return the list
   * @throws LensException the lens exception
   */
  public List<QueryHandle> findFinishedQueries(String state, String user, String driverName, String queryName,
    long fromDate, long toDate) throws LensException {
    boolean addFilter = StringUtils.isNotBlank(state) || StringUtils.isNotBlank(user)
      || StringUtils.isNotBlank(queryName);
    StringBuilder builder = new StringBuilder("SELECT handle FROM finished_queries");
    List<Object> params = null;
    if (addFilter) {
      builder.append(" WHERE ");
      List<String> filters = new ArrayList<String>(3);
      params = new ArrayList<Object>(3);

      if (StringUtils.isNotBlank(state)) {
        filters.add("status=?");
        params.add(state);
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
    }

    ResultSetHandler<List<QueryHandle>> resultSetHandler = new ResultSetHandler<List<QueryHandle>>() {
      @Override
      public List<QueryHandle> handle(ResultSet resultSet) throws SQLException {
        List<QueryHandle> queryHandleList = new ArrayList<QueryHandle>();
        while (resultSet.next()) {
          String handle = resultSet.getString(1);
          try {
            queryHandleList.add(QueryHandle.fromString(handle));
          } catch (IllegalArgumentException exc) {
            log.warn("Warning invalid query handle found in DB " + handle);
          }
        }
        return queryHandleList;
      }
    };

    QueryRunner runner = new QueryRunner(ds);
    String query = builder.toString();
    try {
      if (addFilter) {
        return runner.query(query, resultSetHandler, params.toArray());
      } else {
        return runner.query(query, resultSetHandler);
      }
    } catch (SQLException e) {
      throw new LensException(e);
    }
  }

}

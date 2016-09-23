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
package org.apache.lens.server.api.query;

import java.util.List;

import javax.ws.rs.core.Response;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.*;
import org.apache.lens.server.api.LensService;
import org.apache.lens.server.api.SessionValidator;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.cost.QueryCost;

/**
 * The Interface QueryExecutionService.
 */
public interface QueryExecutionService extends LensService, SessionValidator {

  /**
   * The Constant NAME.
   */
  String NAME = "query";

  /**
   * Estimate the cost of given query.
   *
   * @param requestId     the request Id of request used to start estimate operation
   * @param sessionHandle the session handle
   * @param query         The query should be in HiveQL(SQL like)
   * @param conf          The query configuration
   *
   * @return The query cost
   *
   * @throws LensException thrown in case of failure
   */
  QueryCost estimate(final String requestId, LensSessionHandle sessionHandle, String query, LensConf conf)
    throws LensException;

  /**
   * Explain the given query.
   *
   * @param requestId     the request Id of request used to start explain operation
   * @param sessionHandle the session handle
   * @param query         The query should be in HiveQL(SQL like)
   * @param conf          The query configuration
   * @return The query plan;
   * @throws LensException the lens exception
   */
  QueryPlan explain(final String requestId, LensSessionHandle sessionHandle, String query, LensConf conf)
    throws LensException;

  /**
   * Prepare the query.
   *
   * @param sessionHandle the session handle
   * @param query         The query should be in HiveQL(SQL like)
   * @param conf          The query configuration
   * @param queryName     the query name
   * @return Prepare handle
   * @throws LensException the lens exception
   */
  QueryPrepareHandle prepare(LensSessionHandle sessionHandle, String query, LensConf conf, String queryName)
    throws LensException;

  /**
   * Explain the given query and prepare it as well.
   *
   * @param sessionHandle the session handle
   * @param query         The query should be in HiveQL(SQL like)
   * @param conf          The query configuration
   * @param queryName     the query name
   * @return The query plan; Query plan also consists of prepare handle, if it should be used to executePrepare
   * @throws LensException the lens exception
   */
  QueryPlan explainAndPrepare(LensSessionHandle sessionHandle, String query, LensConf conf, String queryName)
    throws LensException;

  /**
   * Execute already prepared query asynchronously. Query can be prepared with explain
   *
   * @param sessionHandle the session handle
   * @param prepareHandle The {@link QueryPrepareHandle}
   * @param conf          The configuration for the query to execute
   * @param queryName     the query name
   * @return Returns the query handle
   * @throws LensException the lens exception
   */
  QueryHandle executePrepareAsync(LensSessionHandle sessionHandle, QueryPrepareHandle prepareHandle,
    LensConf conf, String queryName) throws LensException;

  /**
   * Execute already prepared query with timeout. Query can be prepared with explain
   *
   * @param sessionHandle the session handle
   * @param prepareHandle The {@link QueryPrepareHandle}
   * @param timeoutmillis The timeout after which it will return handle, if query did not finish before.
   * @param conf          The configuration for the query to execute
   * @param queryName     the query name
   * @return the query handle with result set
   * @throws LensException the lens exception
   */
  QueryHandleWithResultSet executePrepare(LensSessionHandle sessionHandle, QueryPrepareHandle prepareHandle,
    long timeoutmillis, LensConf conf, String queryName) throws LensException;

  /**
   * Asynchronously execute the query.
   *
   * @param sessionHandle the session handle
   * @param query         The query should be in HiveQL(SQL like)
   * @param conf          The query configuration
   * @param queryName     the query name
   * @return a query handle, which can used to know the status.
   * @throws LensException the lens exception
   */
  QueryHandle executeAsync(LensSessionHandle sessionHandle, String query, LensConf conf, String queryName)
    throws LensException;

  /**
   * Update the query conf.
   *
   * @param sessionHandle the session handle
   * @param queryHandle   the query handle
   * @param newconf       the newconf
   * @return true if update is successful
   * @throws LensException the lens exception
   */
  boolean updateQueryConf(LensSessionHandle sessionHandle, QueryHandle queryHandle, LensConf newconf)
    throws LensException;

  /**
   * Execute the query with a timeout.
   *
   * @param sessionHandle the session handle
   * @param query         The query should be in HiveQL(SQL like)
   * @param timeoutmillis The timeout after which it will return handle, if query did not finish before.
   * @param conf          The query configuration
   * @param queryName     the query name
   * @return a query handle, if query did not finish within the timeout specified else result will also be returned.
   * @throws LensException the lens exception
   */
  QueryHandleWithResultSet execute(LensSessionHandle sessionHandle, String query, long timeoutmillis,
    LensConf conf, String queryName) throws LensException;

  /**
   * Get the query, specified by the handle.
   *
   * @param sessionHandle the session handle
   * @param queryHandle   The query handle
   * @return query status
   * @throws LensException the lens exception
   */
  LensQuery getQuery(LensSessionHandle sessionHandle, QueryHandle queryHandle) throws LensException;

  /**
   * Get the result set metadata - list of columns(names and types) and result size.
   *
   * @param sessionHandle the session handle
   * @param queryHandle   the query handle
   * @return The result set metadata
   * @throws LensException the lens exception
   */
  QueryResultSetMetadata getResultSetMetadata(LensSessionHandle sessionHandle, QueryHandle queryHandle)
    throws LensException;

  /**
   * Fetch the results of the query, specified by the handle.
   *
   * @param sessionHandle the session handle
   * @param queryHandle   The query handle
   * @param startIndex    The start Index from which result rows have to be fetched
   * @param fetchSize     Number of rows to be fetched
   * @return returns the result set
   * @throws LensException the lens exception
   */
  QueryResult fetchResultSet(LensSessionHandle sessionHandle, QueryHandle queryHandle, long startIndex,
    int fetchSize) throws LensException;

  /**
   * Get the http end point for the result set.
   *
   * @param sessionHandle The lens session handle
   * @param queryHandle   The query handle
   * @return returns javax.ws.rs.core.Response object
   * @throws LensException the lens exception
   */
  Response getHttpResultSet(LensSessionHandle sessionHandle, QueryHandle queryHandle) throws LensException;

  /**
   * Closes result set by releasing any resources used in serving the resultset.
   *
   * @param sessionHandle the session handle
   * @param queryHandle   the query handle
   * @throws LensException the lens exception
   */
  void closeResultSet(LensSessionHandle sessionHandle, QueryHandle queryHandle) throws LensException;

  /**
   * Cancel the execution of the query, specified by the handle.
   *
   * @param sessionHandle the session handle
   * @param queryHandle   The query handle.
   * @return true if cancel was successful, false otherwise
   * @throws LensException the lens exception
   */
  boolean cancelQuery(LensSessionHandle sessionHandle, QueryHandle queryHandle) throws LensException;

  /**
   * Returns all the queries in the specified state, for the given user and matching query name.
   *
   * @param sessionHandle the session handle
   * @param states        return queries in these state. if null, all queries will be returned. Multiple states can
   *                      be supplied separated by comma
   * @param user          Get queries submitted by a specific user.
   *                      If this set to "all", queries of all users are returned
   * @param driver        Get queries submitted on a specific driver.
   * @param queryName     return queries containing the query name. If null, all queries will be returned
   * @param fromDate      start date of time range interval
   * @param toDate        end date of the time range interval
   * @return List of query handles
   * @throws LensException the lens exception
   */
  List<QueryHandle> getAllQueries(LensSessionHandle sessionHandle, String states, String user, String driver,
    String queryName, String fromDate, String toDate) throws LensException;


  /**
   * Returns all the queries in the specified state, for the given user and matching query name.
   *
   * @param sessionHandle the session handle
   * @param states        return queries in these state. if null, all queries will be returned. Multiple states can
   *                      be supplied separated by comma
   * @param user          Get queries submitted by a specific user.
   *                      If this set to "all", queries of all users are returned
   * @param driver        Get queries submitted on a specific driver.
   * @param queryName     return queries containing the query name. If null, all queries will be returned
   * @param fromDate      start date of time range interval
   * @param toDate        end date of the time range interval
   * @return List of Lens Query object
   * @throws LensException the lens exception
   */
  List<LensQuery> getAllQueryDetails(LensSessionHandle sessionHandle, String states, String user, String driver,
    String queryName, String fromDate, String toDate) throws LensException;

  /**
   * Returns all the prepared queries for the specified user. If no user is passed, queries of all users will be
   * returned.
   *
   * @param sessionHandle the session handle
   * @param user          returns queries of the user. If set to "all", returns queries of all users.
   *                      By default returns the queries
   *                      of the current user.
   * @param queryName     returns queries matching the query name
   * @param fromDate      start time for filtering prepared queries by preparation time
   * @param toDate        end time for filtering prepared queries by preparation time
   * @return List of query prepare handles
   * @throws LensException the lens exception
   */
  List<QueryPrepareHandle> getAllPreparedQueries(LensSessionHandle sessionHandle, String user, String queryName,
    String fromDate, String toDate) throws LensException;

  /**
   * Destroy a prepared query.
   *
   * @param sessionHandle the session handle
   * @param prepared      the prepared
   * @return return true if successful, false otherwise
   * @throws LensException the lens exception
   */
  boolean destroyPrepared(LensSessionHandle sessionHandle, QueryPrepareHandle prepared) throws LensException;

  /**
   * Get prepared query.
   *
   * @param sessionHandle the session handle
   * @param prepareHandle the prepare handle
   * @return PreparedQueryContext object
   * @throws LensException the lens exception
   */
  LensPreparedQuery getPreparedQuery(LensSessionHandle sessionHandle, QueryPrepareHandle prepareHandle)
    throws LensException;

  /**
   * Update configuration for prepared query.
   *
   * @param sessionHandle the session handle
   * @param prepareHandle the prepare handle
   * @param newconf       the newconf
   * @return true if update is successful, false otherwise
   * @throws LensException the lens exception
   */
  boolean updateQueryConf(LensSessionHandle sessionHandle, QueryPrepareHandle prepareHandle, LensConf newconf)
    throws LensException;

  /**
   * Get queued queries count
   *
   * @return queued queries count
   */
  long getQueuedQueriesCount();

  /**
   * Get running queries count
   *
   * @return running queries count
   */
  long getRunningQueriesCount();

  /**
   * Get waiting queries count
   *
   * @return waiting queries count
   */
  long getWaitingQueriesCount();

  /**
   * Get finished queries count
   *
   * @return finished queries count
   */
  long getFinishedQueriesCount();

  /**
   * Get queries being launched count
   *
   * @return Queries being launched count
   */
  long getLaunchingQueriesCount();
}

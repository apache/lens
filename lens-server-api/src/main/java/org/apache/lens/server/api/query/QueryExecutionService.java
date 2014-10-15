package org.apache.lens.server.api.query;

/*
 * #%L
 * Grill API for server and extensions
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

import java.util.Date;
import java.util.List;

import javax.ws.rs.core.Response;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensException;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.LensPreparedQuery;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryHandleWithResultSet;
import org.apache.lens.api.query.QueryPlan;
import org.apache.lens.api.query.QueryPrepareHandle;
import org.apache.lens.api.query.QueryResult;
import org.apache.lens.api.query.QueryResultSetMetadata;


public interface QueryExecutionService {

  public static final String NAME = "query";
  /**
   * Explain the given query
   * 
   * @param query The query should be in HiveQL(SQL like)
   * @param conf The query configuration
   * 
   * @return The query plan;
   * 
   * @throws LensException
   */
  public QueryPlan explain(LensSessionHandle sessionHandle, String query, LensConf conf)
      throws LensException;

  /**
   * Prepare the query
   * 
   * @param sessionHandle
   * @param query The query should be in HiveQL(SQL like)
   * @param conf The query configuration
   *
   * @param queryName
   * @return Prepare handle
   * 
   * @throws LensException
   */
  public QueryPrepareHandle prepare(LensSessionHandle sessionHandle, String query, LensConf conf, String queryName)
      throws LensException;

  /**
   * Explain the given query and prepare it as well.
   * 
   * @param sessionHandle
   * @param query The query should be in HiveQL(SQL like)
   * @param conf The query configuration
   *
   * @param queryName
   * @return The query plan; Query plan also consists of prepare handle,
   * if it should be used to executePrepare
   * 
   * @throws LensException
   */
  public QueryPlan explainAndPrepare(LensSessionHandle sessionHandle, String query, LensConf conf, String queryName)
      throws LensException;

  /**
   * Execute already prepared query asynchronously. 
   * Query can be prepared with explain
   * 
   * @param prepareHandle The {@link QueryPrepareHandle}
   * @param conf The configuration for the query to execute
   * 
   * @return Returns the query handle
   * @throws LensException
   */
  public QueryHandle executePrepareAsync(LensSessionHandle sessionHandle, QueryPrepareHandle prepareHandle,
      LensConf conf, String queryName) throws LensException;

  /**
   * Execute already prepared query with timeout. 
   * Query can be prepared with explain
   * 
   * @param prepareHandle The {@link QueryPrepareHandle}
   * @param timeoutmillis The timeout after which it will return handle, if
   *  query did not finish before.
   * @param conf The configuration for the query to execute
   * 
   * @throws LensException
   */
  public QueryHandleWithResultSet executePrepare(LensSessionHandle sessionHandle, QueryPrepareHandle prepareHandle,
      long timeoutmillis, LensConf conf, String queryName) throws LensException;

  /**
   * Asynchronously execute the query
   * 
   * @param query The query should be in HiveQL(SQL like)
   * @param conf The query configuration
   *
   * @param queryName
   * @return a query handle, which can used to know the status.
   * 
   * @throws LensException
   */
  public QueryHandle executeAsync(LensSessionHandle sessionHandle, String query, LensConf conf, String queryName)
      throws LensException;

  /**
   * Update the query conf
   * 
   * @param queryHandle
   * @param newconf
   * 
   * @return true if update is successful 
   */
  public boolean updateQueryConf(LensSessionHandle sessionHandle, QueryHandle queryHandle, LensConf newconf)
      throws LensException;

  /**
   * Execute the query with a timeout 
   * 
   * @param query The query should be in HiveQL(SQL like)
   * @param timeoutmillis The timeout after which it will return handle, if
   *  query did not finish before.
   * @param conf The query configuration
   *
   * @param queryName
   * @return a query handle, if query did not finish within the timeout specified
   * else result will also be returned.
   * 
   * @throws LensException
   */
  public QueryHandleWithResultSet execute(LensSessionHandle sessionHandle, String query, long timeoutmillis,
                                          LensConf conf, String queryName) throws LensException;

  /**
   * Get the query, specified by the handle
   * 
   * @param queryHandle The query handle
   * 
   * @return query status
   */
  public LensQuery getQuery(LensSessionHandle sessionHandle, QueryHandle queryHandle) throws LensException;

  /**
   * Get the result set metadata - list of columns(names and types) and result size.
   * 
   * @param queryHandle
   * @return The result set metadata
   * @throws LensException
   */
  public QueryResultSetMetadata getResultSetMetadata(LensSessionHandle sessionHandle, QueryHandle queryHandle)
      throws LensException;

  /**
   * Fetch the results of the query, specified by the handle
   * 
   * @param queryHandle The query handle
   * @param startIndex The start Index from which result rows have to be fetched
   * @param fetchSize Number of rows to be fetched
   * 
   * @return returns the result set
   */
  public QueryResult fetchResultSet(LensSessionHandle sessionHandle, QueryHandle queryHandle, long startIndex,
      int fetchSize ) throws LensException;

  /**
   * Get the http end point for the result set
   *
   * @param sessionHandle The lens session handle
   * @param queryHandle The query handle
   *
   * @return returns javax.ws.rs.core.Response object
   */
  public Response getHttpResultSet(LensSessionHandle sessionHandle, QueryHandle queryHandle) throws LensException;

  /**
   * Closes result set by releasing any resources used in serving the resultset.
   * 
   * @param queryHandle
   * @throws LensException
   */
  public void closeResultSet(LensSessionHandle sessionHandle, QueryHandle queryHandle) throws LensException;

  /**
   * Cancel the execution of the query, specified by the handle
   * 
   * @param queryHandle The query handle.
   * 
   * @return true if cancel was successful, false otherwise
   */
  public boolean cancelQuery(LensSessionHandle sessionHandle, QueryHandle queryHandle) throws LensException;

  /**
   * Returns all the queries in the specified state, for the given user and matching query name.
   * 
   * @param state return queries in this state. if null, all queries will be returned
   * @param queryName return queries containing the query name. If null, all queries will be returned
   * @param user Get queries submitted by a specific user. If this set to "all", queries of all users are returned
   * @param fromDate start date of time range interval
   * @param toDate end date of the time range interval
   * @return List of query handles
   */
  public List<QueryHandle> getAllQueries(LensSessionHandle sessionHandle,
                                         String state,
                                         String user,
                                         String queryName,
                                         long fromDate,
                                         long toDate)
      throws LensException;

  /**
   * Returns all the prepared queries for the specified user. 
   * If no user is passed, queries of all users will be returned.
   *
   * @param user returns queries of the user. If set to "all", returns queries of all users. By default returns the queries
   *             of the current user.
   * @param queryName returns queries matching the query name
   * @param fromDate start time for filtering prepared queries by preparation time
   * @param toDate end time for filtering prepared queries by preparation time
   * @return List of query prepare handles
   */
  public List<QueryPrepareHandle> getAllPreparedQueries(LensSessionHandle sessionHandle, String user, String queryName,
                                                        long fromDate, long toDate)
      throws LensException;

  /**
   * Destroy a prepared query
   * 
   * @param prepared
   * @return return true if successful, false otherwise
   */
  public boolean destroyPrepared(LensSessionHandle sessionHandle, QueryPrepareHandle prepared)
      throws LensException;

  /**
   * Get prepared query
   * 
   * @param prepareHandle
   * @return PreparedQueryContext object
   * @throws LensException
   */
  public LensPreparedQuery getPreparedQuery(
      LensSessionHandle sessionHandle, QueryPrepareHandle prepareHandle)
      throws LensException;

  /**
   * Update configuration for prepared query
   * 
   * @param prepareHandle
   * @param newconf
   * @return true if update is successful, false otherwise
   * 
   * @throws LensException
   */
  public boolean updateQueryConf(LensSessionHandle sessionHandle, QueryPrepareHandle prepareHandle, LensConf newconf)
      throws LensException;
  
  /**
   * Get queued queries count
   * 
   * @return queued queries count
   */
  public long getQueuedQueriesCount();
  
  /**
   * Get running queries count
   * 
   * @return running queries count
   */
  public long getRunningQueriesCount();

  /**
   * Get finished queries count
   * 
   * @return finished queries count
   */
  public long getFinishedQueriesCount();
}

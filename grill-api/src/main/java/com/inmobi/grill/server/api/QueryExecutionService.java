package com.inmobi.grill.server.api;

import java.util.List;

import com.inmobi.grill.api.GrillResultSet;
import com.inmobi.grill.api.GrillResultSetMetadata;
import com.inmobi.grill.api.PreparedQueryContext;
import com.inmobi.grill.api.QueryContext;
import com.inmobi.grill.api.QueryPrepareHandle;
import com.inmobi.grill.api.QueryHandleWithResultSet;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryPlan;
import com.inmobi.grill.client.api.QueryConf;
import com.inmobi.grill.exception.GrillException;

public interface QueryExecutionService extends GrillService {

  /**
   * Explain the given query
   * 
   * @param query The query should be in HiveQL(SQL like)
   * @param conf The query configuration
   * 
   * @return The query plan;
   * 
   * @throws GrillException
   */
  public QueryPlan explain(String query, QueryConf conf)
      throws GrillException;

  /**
   * Prepare the query
   * 
   * @param query The query should be in HiveQL(SQL like)
   * @param conf The query configuration
   * 
   * @return Prepare handle
   * 
   * @throws GrillException
   */
  public QueryPrepareHandle prepare(String query, QueryConf conf)
      throws GrillException;

  /**
   * Explain the given query and prepare it as well.
   * 
   * @param query The query should be in HiveQL(SQL like)
   * @param conf The query configuration
   * 
   * @return The query plan; Query plan also consists of prepare handle,
   * if it should be used to executePrepare
   * 
   * @throws GrillException
   */
  public QueryPlan explainAndPrepare(String query, QueryConf conf)
      throws GrillException;

  /**
   * Execute already prepared query asynchronously. 
   * Query can be prepared with explain
   * 
   * @param handle The {@link QueryHandle}
   * @param conf The configuration for the query to execute
   * 
   * @throws GrillException
   */
  public QueryHandle executePrepareAsync(QueryPrepareHandle prepareHandle,
      QueryConf conf) throws GrillException;

  /**
   * Asynchronously execute the query
   * 
   * @param query The query should be in HiveQL(SQL like)
   * @param conf The query configuration
   * 
   * @return a query handle, which can used to know the status.
   * 
   * @throws GrillException
   */
  public QueryHandle executeAsync(String query, QueryConf conf)
      throws GrillException;

  /**
   * Update the query conf
   * 
   * @param queryHandle
   * @param newconf
   * 
   * @return true if update is successful 
   */
  public boolean updateQueryConf(QueryHandle queryHandle, QueryConf newconf)
      throws GrillException;

  /**
   * Execute the query with a timeout 
   * 
   * @param query The query should be in HiveQL(SQL like)
   * @param timeoutmillis The timeout after which it will return handle, if
   *  query did not finish before.
   * @param conf The query configuration
   * 
   * @return a query handle, if query did not finish within the timeout specified
   * else result will also be returned.
   * 
   * @throws GrillException
   */
  public QueryHandleWithResultSet execute(String query, long timeoutmillis,
      QueryConf conf) throws GrillException;

  /**
   * Get status of the query, specified by the handle
   * 
   * @param handle The query handle
   * 
   * @return query status
   */
  public QueryContext getQueryContext(QueryHandle queryHandle) throws GrillException;

  /**
   * Get the result set metadata - list of columns(names and types) and result size.
   * 
   * @param queryHandle
   * @return The result set metadata
   * @throws GrillException
   */
  public GrillResultSetMetadata getResultSetMetadata(QueryHandle queryHandle)
      throws GrillException;

  /**
   * Fetch the results of the query, specified by the handle
   * 
   * @param queryHandle The query handle
   * @param startIndex The start Index from which result rows have to be fetched
   * @param fetchSize Number of rows to be fetched
   * 
   * @return returns the result set
   */
  public GrillResultSet fetchResultSet(QueryHandle queryHandle, long startIndex,
      int fetchSize ) throws GrillException;

  /**
   * Closes result set by releasing any resources used in serving the resultset.
   * 
   * @param queryHandle
   * @throws GrillException
   */
  public void closeResultSet(QueryHandle queryHandle) throws GrillException;

  /**
   * Cancel the execution of the query, specified by the handle
   * 
   * @param queryHandle The query handle.
   * 
   * @return true if cancel was successful, false otherwise
   */
  public boolean cancelQuery(QueryHandle queryHandle) throws GrillException;

  /**
   * Returns all the queries in the specified state, for user. 
   * If no state is passed, queries in all the state will be returned. Also, if 
   * no user is passed, queries of all users will be returned.
   * 
   * @param state Any of particular state, if null all queries will be returned
   * @param user The user name, if null all user queries will be returned
   * 
   * @return List of query handles
   */
  public List<QueryHandle> getAllQueries(String state, String user)
      throws GrillException;

  /**
   * Returns all the prepared queries for the specified user. 
   * If no user is passed, queries of all users will be returned.
   * 
   * @param user The user name, if null all user queries will be returned
   * 
   * @return List of query prepare handles
   */
  public List<QueryPrepareHandle> getAllPreparedQueries(String user)
      throws GrillException;

  /**
   * Destroy a prepared query
   * 
   * @param prepared
   * @return return true if successful, false otherwise
   */
  public boolean destroyPrepared(QueryPrepareHandle prepared)
      throws GrillException;

  /**
   * Get prepared query
   * 
   * @param prepareHandle
   * @return PreparedQueryContext object
   * @throws GrillException
   */
  public PreparedQueryContext getPreparedQueryContext(QueryPrepareHandle prepareHandle)
      throws GrillException;

  /**
   * Update configuration for prepared query
   * 
   * @param prepareHandle
   * @param newconf
   * @return true if update is successful, false otherwise
   * 
   * @throws GrillException
   */
  public boolean updateQueryConf(QueryPrepareHandle prepareHandle, QueryConf newconf)
      throws GrillException;
}

package com.inmobi.grill.server.api;

import java.util.List;

import com.inmobi.grill.client.api.QueryConf;
import com.inmobi.grill.client.api.QueryHandleWithResultSet;
import com.inmobi.grill.client.api.QueryPlan;
import com.inmobi.grill.client.api.QueryResult;
import com.inmobi.grill.client.api.QueryStatus;
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
  public String prepare(String query, QueryConf conf)
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
  public void executePrepareAsync(String prepareHandle, QueryConf conf) 
      throws GrillException;

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
  public String executeAsync(String query, QueryConf conf)
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
  public QueryStatus getStatus(String queryHandle) throws GrillException;

  /**
   * Fetch the results of the query, specified by the handle
   * 
   * @param queryHandle The query handle
   * 
   * @return returns the result set
   */
  public QueryResult fetchResultSet(String queryHandle) throws GrillException;

  /**
   * Cancel the execution of the query, specified by the handle
   * 
   * @param queryHandle The query handle.
   * 
   * @return true if cancel was successful, false otherwise
   */
  public boolean cancelQuery(String queryHandle) throws GrillException;

  /**
   * Returns all the queries in the specified state, If no state is passed
   * queries in all the state will be returned.
   * 
   * @param state Any of particular state, if null all queries will be returned
   * @param user The user name, if null all user queries will be returned
   * 
   * @return List of query handle strings
   */
  public List<String> getAllQueries(String state, String user)
      throws GrillException;

  
}

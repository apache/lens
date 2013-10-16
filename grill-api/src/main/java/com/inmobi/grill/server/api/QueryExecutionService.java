package com.inmobi.grill.server.api;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.inmobi.grill.api.GrillResultSet;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryPlan;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.api.QueryStatus.Status;
import com.inmobi.grill.exception.GrillException;

public interface QueryExecutionService extends GrillService {

  /**
   * Explain the given query
   * 
   * @param query The query should be in HiveQL(SQL like)
   * @param conf The query configuration
   * 
   * @return The query plan object; Query plan also consists of query handle,
   * if it should be used to executePrepare
   * 
   * @throws GrillException
   */
  public QueryPlan explain(String query, Configuration conf)
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
  public void executePrepareAsync(QueryHandle handle, Configuration conf) 
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
  public QueryHandle executeAsync(String query, Configuration conf)
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
      Configuration conf) throws GrillException;

  /**
   * Get status of the query, specified by the handle
   * 
   * @param handle The query handle
   * 
   * @return query status
   */
  public QueryStatus getStatus(QueryHandle handle) throws GrillException;

  /**
   * Fetch the results of the query, specified by the handle
   * 
   * @param handle The query handle
   * 
   * @return returns the result set
   */
  public GrillResultSet fetchResultSet(QueryHandle handle) throws GrillException;

  /**
   * Cancel the execution of the query, specified by the handle
   * 
   * @param handle The query handle.
   * 
   * @return true if cancel was successful, false otherwise
   */
  public boolean cancelQuery(QueryHandle handle) throws GrillException;

  /**
   * Returns all the queries in the specified state, If no state is passed
   * queries in all the state will be returned.
   * 
   * @param status Any of {@link Status}. 
   * 
   * @return List of QueryHandle objects
   */
  public List<QueryHandle> getAllQueries(QueryStatus.Status status)
      throws GrillException;

  
}

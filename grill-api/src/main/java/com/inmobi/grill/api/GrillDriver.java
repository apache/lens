package com.inmobi.grill.api;


import org.apache.hadoop.conf.Configuration;

import com.inmobi.grill.exception.GrillException;

public interface GrillDriver {

  /**
   * Get driver configuration
   * 
   */
  public Configuration getConf();

  /** 
   * Configure driver with {@link Configuration} passed
   * 
   * @param conf The configuration object
   */
  public void configure(Configuration conf) throws GrillException;

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
   * Execute already prepared query. Query can be prepared with explain
   * 
   * @param handle The {@link QueryHandle}
   * @param conf The configuration for the query to execute
   * 
   * @return returns the result set
   * 
   * @throws GrillException
   */
  public GrillResultSet executePrepare(QueryHandle handle, Configuration conf) 
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
   * Blocking execute of the query
   * 
   * @param query The query should be in HiveQL(SQL like)
   * @param conf The query configuration
   * 
   * @return returns the result set
   * 
   * @throws GrillException
   */
  public GrillResultSet execute(String query, Configuration conf)
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
   * Close the query specified by the handle, by releases all the resources
   * held by the query
   * 
   * @param handle The query handle
   * 
   * @throws GrillException
   */
  public void closeQuery(QueryHandle handle) throws GrillException;

  /**
   * Close the driver, releasing all resouces used up by the driver
   * @throws GrillException
   */
  public void close() throws GrillException;
}

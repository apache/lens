package com.inmobi.grill.query.service;

import java.util.List;

import com.inmobi.grill.client.api.*;
import org.apache.hadoop.conf.Configuration;

import com.inmobi.grill.api.GrillResultSet;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryPlan;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.api.QueryStatus.Status;
import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.server.api.QueryExecutionService;

public class QueryExcecutionServiceImpl implements QueryExecutionService {

  /**
   * Explain the given query
   *
   * @param query The query should be in HiveQL(SQL like)
   * @param conf  The query configuration
   * @return The query plan;
   * @throws com.inmobi.grill.exception.GrillException
   *
   */
  @Override
  public com.inmobi.grill.client.api.QueryPlan explain(String query, QueryConf conf) throws GrillException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Prepare the query
   *
   * @param query The query should be in HiveQL(SQL like)
   * @param conf  The query configuration
   * @return Prepare handle
   * @throws com.inmobi.grill.exception.GrillException
   *
   */
  @Override
  public QueryPrepareHandle prepare(String query, QueryConf conf) throws GrillException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Explain the given query and prepare it as well.
   *
   * @param query The query should be in HiveQL(SQL like)
   * @param conf  The query configuration
   * @return The query plan; Query plan also consists of prepare handle,
   *         if it should be used to executePrepare
   * @throws com.inmobi.grill.exception.GrillException
   *
   */
  @Override
  public com.inmobi.grill.client.api.QueryPlan explainAndPrepare(String query, QueryConf conf) throws GrillException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Execute already prepared query asynchronously.
   * Query can be prepared with explain
   *
   * @param handle The {@link com.inmobi.grill.client.api.QueryHandle}
   * @param conf   The configuration for the query to execute
   * @throws com.inmobi.grill.exception.GrillException
   *
   */
  @Override
  public com.inmobi.grill.client.api.QueryHandle executePrepareAsync(String prepareHandle, QueryConf conf) throws GrillException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Asynchronously execute the query
   *
   * @param query The query should be in HiveQL(SQL like)
   * @param conf  The query configuration
   * @return a query handle, which can used to know the status.
   * @throws com.inmobi.grill.exception.GrillException
   *
   */
  @Override
  public com.inmobi.grill.client.api.QueryHandle executeAsync(String query, QueryConf conf) throws GrillException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Update the query conf
   *
   * @param queryHandle
   * @param newconf
   * @return true if update is successful
   */
  @Override
  public boolean updateQueryConf(String queryHandle, QueryConf newconf) throws GrillException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Execute the query with a timeout
   *
   * @param query         The query should be in HiveQL(SQL like)
   * @param timeoutmillis The timeout after which it will return handle, if
   *                      query did not finish before.
   * @param conf          The query configuration
   * @return a query handle, if query did not finish within the timeout specified
   *         else result will also be returned.
   * @throws com.inmobi.grill.exception.GrillException
   *
   */
  @Override
  public QueryHandleWithResultSet execute(String query, long timeoutmillis, QueryConf conf) throws GrillException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Get status of the query, specified by the handle
   *
   * @param handle The query handle
   * @return query status
   */
  @Override
  public com.inmobi.grill.client.api.QueryStatus getStatus(String queryHandle) throws GrillException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Get the result set metadata - list of columns(names and types) and result size.
   *
   * @param queryHandle
   * @return The result set metadata
   * @throws com.inmobi.grill.exception.GrillException
   *
   */
  @Override
  public QueryResultSetMetadata getResultSetMetadata(String queryHandle) throws GrillException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Fetch the results of the query, specified by the handle
   *
   * @param queryHandle The query handle
   * @param startIndex  The start Index from which result rows have to be fetched
   * @param fetchSize   Number of rows to be fetched
   * @return returns the result set
   */
  @Override
  public QueryResult fetchResultSet(String queryHandle, long startIndex, int fetchSize) throws GrillException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Closes result set by releasing any resources used in serving the resultset.
   *
   * @param queryHandle
   * @throws com.inmobi.grill.exception.GrillException
   *
   */
  @Override
  public void closeResultSet(String queryHandle) throws GrillException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Cancel the execution of the query, specified by the handle
   *
   * @param queryHandle The query handle.
   * @return true if cancel was successful, false otherwise
   */
  @Override
  public boolean cancelQuery(String queryHandle) throws GrillException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Returns all the queries in the specified state, for user.
   * If no state is passed, queries in all the state will be returned. Also, if
   * no user is passed, queries of all users will be returned.
   *
   * @param state Any of particular state, if null all queries will be returned
   * @param user  The user name, if null all user queries will be returned
   * @return List of query handle strings
   */
  @Override
  public QueryList getAllQueries(String state, String user) throws GrillException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public String getName() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void start() throws GrillException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void stop() throws GrillException {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}

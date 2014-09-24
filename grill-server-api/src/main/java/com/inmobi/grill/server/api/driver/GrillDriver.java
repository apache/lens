package com.inmobi.grill.server.api.driver;

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

import java.io.Externalizable;

import com.inmobi.grill.server.api.events.GrillEventListener;
import org.apache.hadoop.conf.Configuration;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryPrepareHandle;
import com.inmobi.grill.server.api.query.PreparedQueryContext;
import com.inmobi.grill.server.api.query.QueryContext;

public interface GrillDriver extends Externalizable {

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
   * @return The query plan object;
   * 
   * @throws GrillException
   */
  public DriverQueryPlan explain(String query, Configuration conf)
      throws GrillException;

  /**
   * Prepare the given query
   * 
   * @param pContext 
   * 
   * @throws GrillException
   */
  public void prepare(PreparedQueryContext pContext) throws GrillException;

  /**
   * Explain and prepare the given query
   * 
   * @param pContext 
   * 
   * @return The query plan object;
   * 
   * @throws GrillException
   */
  public DriverQueryPlan explainAndPrepare(PreparedQueryContext pContext)
      throws GrillException;

  /**
   * Close the prepare query specified by the prepared handle,
   * releases all the resources held by the prepared query.
   * 
   * @param handle The query handle
   * 
   * @throws GrillException
   */
  public void closePreparedQuery(QueryPrepareHandle handle) throws GrillException;

  /**
   * Blocking execute of the query
   * 
   * The driver would be closing the driver handle, once the results are fetched
   * 
   * @param context 
   * 
   * @return returns the result set, null if there is no result available
   * 
   * @throws GrillException
   */
  public GrillResultSet execute(QueryContext context)
      throws GrillException;

  /**
   * Asynchronously execute the query
   * 
   * @param context The query context
   * 
   * @throws GrillException
   */
  public void executeAsync(QueryContext context)
      throws GrillException;

  /**
   * Register for query completion notification
   * 
   * @param handle
   * @param timeoutMillis
   * @param listener
   * 
   * @throws GrillException
   */
  public void registerForCompletionNotification(QueryHandle handle,
      long timeoutMillis, QueryCompletionListener listener) throws GrillException;

  /**
   * Update driver query status in the context object.
   * 
   * @param context The query context
   */
  public void updateStatus(QueryContext context) throws GrillException;

  /**
   * Fetch the results of the query, specified by the handle
   * 
   * @param context The query context
   * 
   * @return returns the result set
   */
  public GrillResultSet fetchResultSet(QueryContext context) throws GrillException;

  /**
   * Close the resultset for the query
   * 
   * @param handle The query handle
   * 
   * @throws GrillException
   */
  public void closeResultSet(QueryHandle handle) throws GrillException;

  /**
   * Cancel the execution of the query, specified by the handle
   * 
   * @param handle The query handle.
   * 
   * @return true if cancel was successful, false otherwise
   */
  public boolean cancelQuery(QueryHandle handle) throws GrillException;

  /**
   * Close the query specified by the handle, releases all the resources
   * held by the query.
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

  /**
   * Add a listener for driver events
   */
  public void registerDriverEventListener(GrillEventListener<DriverEvent> driverEventListener);
}

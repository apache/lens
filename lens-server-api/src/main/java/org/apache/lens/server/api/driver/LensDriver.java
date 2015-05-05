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
package org.apache.lens.server.api.driver;

import java.io.Externalizable;

import org.apache.lens.api.query.QueryCost;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryPrepareHandle;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.LensEventListener;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;

import org.apache.hadoop.conf.Configuration;

/**
 * The Interface LensDriver.
 */
public interface LensDriver extends Externalizable {

  /**
   * Get driver configuration
   */
  Configuration getConf();

  /**
   * Configure driver with {@link Configuration} passed.
   *
   * @param conf The configuration object
   * @throws LensException the lens exception
   */
  void configure(Configuration conf) throws LensException;

  /**
   * Estimate the cost of execution for given query.
   *
   * This should be returned with very less latency - should return within 10s of milli seconds.
   *
   * @param qctx The query context
   *
   * @return The QueryCost object
   *
   * @throws LensException the lens exception if driver cannot estimate
   */
  QueryCost estimate(AbstractQueryContext qctx) throws LensException;

  /**
   * Explain the given query.
   *
   * @param explainCtx The explain context
   *
   * @return The query plan object
   * @throws LensException the lens exception
   */
  DriverQueryPlan explain(AbstractQueryContext explainCtx) throws LensException;

  /**
   * Prepare the given query.
   *
   * @param pContext the context
   * @throws LensException the lens exception
   */
  void prepare(PreparedQueryContext pContext) throws LensException;

  /**
   * Explain and prepare the given query.
   *
   * @param pContext the context
   * @return The query plan object;
   * @throws LensException the lens exception
   */
  DriverQueryPlan explainAndPrepare(PreparedQueryContext pContext) throws LensException;

  /**
   * Close the prepare query specified by the prepared handle, releases all the resources held by the prepared query.
   *
   * @param handle The query handle
   * @throws LensException the lens exception
   */
  void closePreparedQuery(QueryPrepareHandle handle) throws LensException;

  /**
   * Blocking execute of the query
   * <p/>
   * The driver would be closing the driver handle, once the results are fetched.
   *
   * @param context the context
   * @return returns the result set, null if there is no result available
   * @throws LensException the lens exception
   */
  LensResultSet execute(QueryContext context) throws LensException;

  /**
   * Asynchronously execute the query.
   *
   * @param context The query context
   * @throws LensException the lens exception
   */
  void executeAsync(QueryContext context) throws LensException;

  /**
   * Register for query completion notification.
   *
   * @param handle        the handle
   * @param timeoutMillis the timeout millis
   * @param listener      the listener
   * @throws LensException the lens exception
   */
  void registerForCompletionNotification(QueryHandle handle, long timeoutMillis, QueryCompletionListener listener)
    throws LensException;

  /**
   * Update driver query status in the context object.
   *
   * @param context The query context
   * @throws LensException the lens exception
   */
  void updateStatus(QueryContext context) throws LensException;

  /**
   * Fetch the results of the query, specified by the handle.
   *
   * @param context The query context
   * @return returns the result set
   * @throws LensException the lens exception
   */
  LensResultSet fetchResultSet(QueryContext context) throws LensException;

  /**
   * Close the resultset for the query.
   *
   * @param handle The query handle
   * @throws LensException the lens exception
   */
  void closeResultSet(QueryHandle handle) throws LensException;

  /**
   * Cancel the execution of the query, specified by the handle.
   *
   * @param handle The query handle.
   * @return true if cancel was successful, false otherwise
   * @throws LensException the lens exception
   */
  boolean cancelQuery(QueryHandle handle) throws LensException;

  /**
   * Close the query specified by the handle, releases all the resources held by the query.
   *
   * @param handle The query handle
   * @throws LensException the lens exception
   */
  void closeQuery(QueryHandle handle) throws LensException;

  /**
   * Close the driver, releasing all resouces used up by the driver.
   *
   * @throws LensException the lens exception
   */
  void close() throws LensException;

  /**
   * Add a listener for driver events.
   *
   * @param driverEventListener the driver event listener
   */
  void registerDriverEventListener(LensEventListener<DriverEvent> driverEventListener);
}

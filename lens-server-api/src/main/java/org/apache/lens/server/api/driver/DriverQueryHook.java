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
/*
 *
 */
package org.apache.lens.server.api.driver;

import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.QueryContext;

/**
 * Drivers should initialize a DriverQueryHook object in their  initialization and expose it
 * via {@link LensDriver#getQueryHook}. Lens Server will invoke the driver hook at relevant points during
 * query execution. By default each driver exposes a {@link NoOpDriverQueryHook} which does nothing when invoked.
 *
 * The only use case I see right now is to provide a hook just after driver has been selected for a query and
 * before query is launched on the driver. One example usage for hive driver would be to add dynamic configuration or
 * stall execution of a query by looking at the final translated query itself (based on table involved, filters
 * involved, etc in the query).
 *
 * This interface is expected to evolve for some time as more needs for hook are discovered
 *
 * Note: Note if the hook updates any configuration, same should be reflected in QueryContext
 * via {@link AbstractQueryContext#updateConf(Map)} to ensure the modified configuration is persisted and is available
 * on server restarts and other bookkeeping needs.
 */
public interface DriverQueryHook {

  /**
   * This setter method is called by the driver once hook instance is created. This driver information can be used while
   * extracting driver specific information form the QueryContext.
   * @param driver
   */
  void setDriver(LensDriver driver);

  /**
   * Called just before rewrite operation is tried on this driver
   *
   * @param ctx
   * @throws LensException
   */
  void preRewrite(AbstractQueryContext ctx) throws LensException;

  /**
   * Called just after a successful rewrite operation is tried on this driver
   *
   * @param ctx
   * @throws LensException
   */
  void postRewrite(AbstractQueryContext ctx) throws LensException;

  /**
   * Called just before estimate operation is tried on this driver
   * Note : Estimate operation will be skipped if rewrite operation fails for this driver
   *
   * @param ctx
   * @throws LensException
   */
  void preEstimate(AbstractQueryContext ctx) throws LensException;

  /**
   * Called just after a successful estimate operation is tried on this driver
   *
   * @param ctx
   * @throws LensException
   */
  void postEstimate(AbstractQueryContext ctx) throws LensException;

  /**
   * Called just after driver has been selected to execute a query.
   *
   * @param ctx
   * @throws LensException
   */
  void postDriverSelection(AbstractQueryContext ctx) throws LensException;

  /**
   * Called just before launching the query on the selected driver.
   * @param ctx
   * @throws LensException
   */
  void preLaunch(QueryContext ctx) throws LensException;

}

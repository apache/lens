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

 */
public interface DriverQueryHook {
  /**
   * Called just before launching the query on the selected driver.
   * @param ctx
   * @throws LensException
   */
  void preLaunch(QueryContext ctx) throws LensException;

  /**
   * Called just after driver has been selected to execute a query.
   *
   * Note: Note if this method updates any configuration, same should be reflected in QueryContext
   * via {@link AbstractQueryContext#updateConf(Map)} to ensure the modified configration is persisted and is available
   * on server restarts and other bookkeeping needs.
   *
   * @param ctx
   * @throws LensException
   */
  void postDriverSelection(AbstractQueryContext ctx) throws LensException;
}

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

/**
 * Drivers can choose to initialize a DriverQueryHook object in their
 * initialization and use that wherever they want.
 *
 * The only use case I see right now is to provide a hook just before query is
 * launched on the driver.
 *
 * This interface is meant to unify drivers' needs of having hooks. Each driver
 * can use the methods in their own way. Each driver can pose its own restrictions
 * or guidelines on methods for its hooks.
 * e.g. some driver may choose to not allow any hooks
 * another driver may allow hooks but for restricted usage
 * Some drivers may want their hooks to be initialized with some constructor params
 * Currently, Hivedriver and Jdbcdriver only require their hook implementations to have a default constructor
 *
 * This interface is expected to evolve for some time as more needs of hooks are discovered
 *
 */
public interface DriverQueryHook {
  /**
   * Should be Called before launch on the driver
   * @param ctx
   * @throws LensException
   */
  void preLaunch(AbstractQueryContext ctx) throws LensException;
}

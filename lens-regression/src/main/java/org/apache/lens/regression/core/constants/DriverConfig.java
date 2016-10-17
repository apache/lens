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

package org.apache.lens.regression.core.constants;

import org.apache.lens.driver.jdbc.JDBCDriverConfConstants;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.query.constraint.MaxConcurrentDriverQueriesConstraintFactory;
import org.apache.lens.server.query.constraint.TotalQueryCostCeilingConstraintFactory;

public class DriverConfig {

  private DriverConfig() {

  }

  public static final String MAX_CONCURRENT_QUERIES = MaxConcurrentDriverQueriesConstraintFactory.
      MAX_CONCURRENT_QUERIES_KEY;
  public static final String PRIORITY_MAX_CONCURRENT = MaxConcurrentDriverQueriesConstraintFactory.
      MAX_CONCURRENT_QUERIES_PER_PRIORITY_KEY;
  public static final String QUEUE_MAX_CONCURRENT = MaxConcurrentDriverQueriesConstraintFactory.
      MAX_CONCURRENT_QUERIES_PER_QUEUE_KEY;
  public static final String JDBC_POOL_SIZE = JDBCDriverConfConstants.ConnectionPoolProperties.
      JDBC_POOL_MAX_SIZE.getConfigKey();
  public static final String HIVE_CONSTRAINT_FACTORIES = LensConfConstants.QUERY_LAUNCHING_CONSTRAINT_FACTORIES_SFX;


  public static final String MAX_CONCURRENT_CONSTRAINT_FACTORY = MaxConcurrentDriverQueriesConstraintFactory
      .class.getName();
  public static final String USER_COST_CONSTRAINT_FACTORY = TotalQueryCostCeilingConstraintFactory.class.getName();
}


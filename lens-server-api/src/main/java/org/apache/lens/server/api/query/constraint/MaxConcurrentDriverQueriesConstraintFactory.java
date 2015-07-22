/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lens.server.api.query.constraint;

import static java.lang.Integer.parseInt;

import org.apache.lens.server.api.common.ConfigBasedObjectCreationFactory;

import org.apache.hadoop.conf.Configuration;

public class MaxConcurrentDriverQueriesConstraintFactory
    implements ConfigBasedObjectCreationFactory<QueryLaunchingConstraint> {

  public static final String MAX_CONCURRENT_QUERIES_KEY = "driver.max.concurrent.launched.queries";

  @Override
  public QueryLaunchingConstraint create(final Configuration conf) {

    int maxConcurrentQueries = parseInt(conf.get(MAX_CONCURRENT_QUERIES_KEY));
    return new MaxConcurrentDriverQueriesConstraint(maxConcurrentQueries);
  }
}

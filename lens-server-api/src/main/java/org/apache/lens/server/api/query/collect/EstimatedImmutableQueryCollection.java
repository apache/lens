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

package org.apache.lens.server.api.query.collect;

import java.util.Set;

import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.cost.QueryCost;

/**
 *
 * The implementations of this interface should make sure that add behaviours only accepts queries for which selected
 * driver is not null and a valid selected driver query cost is present.
 *
 * IllegalStateException shall be thrown from add behaviours when selected driver is not set or a valid selected driver
 * query cost is not present for the given query.
 *
 */
public interface EstimatedImmutableQueryCollection extends ImmutableQueryCollection {

  /**
   *
   * @param driver Driver for which queries have to be returned.
   *
   * @return A set of queries for which given driver is the selected driver. A new set is created and returned.
   * Elements in the set are not cloned or copied. Multiple iterations over returned set are guaranteed to be in same
   * order. If there are no queries, then an empty set is returned. null is never returned.
   */
  Set<QueryContext> getQueries(final LensDriver driver);

  /**
   *
   * @param driver Driver for which count of queries have to be returned.
   *
   * @return count of queries of given driver.
   */
  int getQueriesCount(final LensDriver driver);

  /**
   *
   * Get total query cost of all queries of given user
   *
   * @param user
   * @return total query cost of all queries of user
   */
  QueryCost getTotalQueryCost(final String user);
}

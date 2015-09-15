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
package org.apache.lens.server.api.query.collect;

import java.util.Set;

import org.apache.lens.server.api.query.QueryContext;

/**
 *
 * {@link ImmutableQueryCollection} interface defines immutable behaviours on queries existing in lens system.
 *
 */
public interface ImmutableQueryCollection {

  /**
   *
   * @return A new set copied from this collection of queries. Elements in the set are not cloned or copied.
   * Multiple iterations over returned set are guaranteed to be in same order. If there are no queries, then an empty
   * set is returned. null is never returned.
   */
  Set<QueryContext> getQueries();

  /**
   * @param user User for whom queries have to be returned.
   * @return A set of queries submitted by the given user. A new set is created and returned. Elements in the set
   * are not cloned or copied. Multiple iterations over returned set are guaranteed to be in same order. If there are
   * no queries, then an empty set is returned. null is never returned.
   */
  Set<QueryContext> getQueries(final String user);

  /**
   *
   * @return Count of existing queries
   */
  int getQueriesCount();

  /**
   *
   * @return Index of a query within collection
   */
  Integer getQueryIndex(final QueryContext query);
}

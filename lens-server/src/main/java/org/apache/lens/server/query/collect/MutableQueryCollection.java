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

package org.apache.lens.server.query.collect;

import java.util.Set;

import org.apache.lens.server.api.query.QueryContext;

/**
 *
 * {@link MutableQueryCollection} interface defines all mutable behaviours on queries existing in lens system.
 *
 * Responsibility of implementations of this interface is to make sure that if multiple views are created for the
 * collection of queries, then they should remain consistent with each other after mutable behaviours are executed.
 *
 */

public interface MutableQueryCollection {

  /**
   * add the given query to existing queries
   *
   * @param query
   * @return
   */
  boolean add(final QueryContext query);

  /**
   *
   * @param queries
   * @return
   */
  boolean addAll(final Set<QueryContext> queries);

  /**
   * removes given query from existing queries
   *
   * @param query
   * @return
   */
  boolean remove(final QueryContext query);

  /**
   *
   * @param queries
   */
  boolean removeAll(final Set<QueryContext> queries);
}

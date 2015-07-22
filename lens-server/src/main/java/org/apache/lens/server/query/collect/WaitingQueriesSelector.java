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
package org.apache.lens.server.query.collect;

import java.util.Set;

import org.apache.lens.server.api.query.FinishedLensQuery;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.collect.EstimatedImmutableQueryCollection;

/**
 * Selects a subset of waiting queries eligible to move out of waiting state.
 *
 */
public interface WaitingQueriesSelector {

  /**
   *
   * @param finishedQuery
   * @param waitingQueries
   * @return Set of waiting queries eligible to move out of waiting state. Empty set is returned when no query is
   *         eligible. null is never returned. Multiple iterations over the returned set are guaranteed to be in the
   *         same order.
   */
  Set<QueryContext> selectQueries(
    final FinishedLensQuery finishedQuery, final EstimatedImmutableQueryCollection waitingQueries);
}

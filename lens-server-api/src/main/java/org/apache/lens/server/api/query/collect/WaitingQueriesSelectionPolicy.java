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

import org.apache.lens.server.api.query.FinishedLensQuery;
import org.apache.lens.server.api.query.QueryContext;

public interface WaitingQueriesSelectionPolicy {

  /**
   *
   * Selects a subset of waiting queries eligible to move out of waiting state, based on logic of this selection policy.
   *
   * @param finishedQuery
   * @param waitingQueries
   * @return Waiting queries eligible to move out of waiting state. If no queries are eligible, then an empty set is
   *         returned. null is never returned.
   */
  Set<QueryContext> selectQueries(
    final FinishedLensQuery finishedQuery, final EstimatedImmutableQueryCollection waitingQueries);
}

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

import java.util.List;
import java.util.Set;

import org.apache.lens.server.api.query.FinishedLensQuery;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.collect.EstimatedImmutableQueryCollection;
import org.apache.lens.server.api.query.collect.WaitingQueriesSelectionPolicy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Selects queries eligible by all {@link WaitingQueriesSelectionPolicy} to move them out of waiting state.
 *
 */
@Slf4j
public class UnioningWaitingQueriesSelector implements WaitingQueriesSelector {

  private final ImmutableSet<WaitingQueriesSelectionPolicy> selectionPolicies;

  public UnioningWaitingQueriesSelector(
    @NonNull final ImmutableSet<WaitingQueriesSelectionPolicy> selectionPolicies) {
    this.selectionPolicies = selectionPolicies;
  }

  /**
   * Selects queries eligible by all {@link WaitingQueriesSelectionPolicy} to move them out of waiting state.
   *
   * @see WaitingQueriesSelector#selectQueries(FinishedLensQuery, EstimatedImmutableQueryCollection)
   *
   * @param finishedQuery
   * @param waitingQueries
   * @return
   */
  @Override
  public Set<QueryContext> selectQueries(final FinishedLensQuery finishedQuery,
      final EstimatedImmutableQueryCollection waitingQueries) {

    Set<WaitingQueriesSelectionPolicy> allSelectionPolicies = prepareAllSelectionPolicies(finishedQuery);

    List<Set<QueryContext>> candiateQueriesSets = getAllCandidateQueriesSets(finishedQuery, waitingQueries,
        allSelectionPolicies);

    return Sets.newHashSet(Iterables.concat(candiateQueriesSets));
  }

  @VisibleForTesting
  Set<WaitingQueriesSelectionPolicy> prepareAllSelectionPolicies(final FinishedLensQuery finishedQuery) {

    /* Get the selection policies of driver on which this query was run */
    ImmutableSet<WaitingQueriesSelectionPolicy> driverSelectionPolicies = finishedQuery.getDriverSelectionPolicies();

    return Sets.union(this.selectionPolicies, driverSelectionPolicies);
  }

  private List<Set<QueryContext>> getAllCandidateQueriesSets(
      final FinishedLensQuery finishedQuery, final EstimatedImmutableQueryCollection waitingQueries,
      final Set<WaitingQueriesSelectionPolicy> allSelectionPolicies) {

    List<Set<QueryContext>> candidateQueriesSets = Lists.newLinkedList();

    for (final WaitingQueriesSelectionPolicy selectionPolicy : allSelectionPolicies) {

      Set<QueryContext> candiateQueries = selectionPolicy.selectQueries(finishedQuery, waitingQueries);
      candidateQueriesSets.add(candiateQueries);
      log.info("Queries selected by policy: {} are: {}", selectionPolicy, candiateQueries);
    }
    return candidateQueriesSets;
  }

}

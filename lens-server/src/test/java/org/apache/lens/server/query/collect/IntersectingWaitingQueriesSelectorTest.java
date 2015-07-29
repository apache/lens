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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.lens.server.api.query.FinishedLensQuery;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.collect.EstimatedImmutableQueryCollection;
import org.apache.lens.server.api.query.collect.WaitingQueriesSelectionPolicy;

import org.testng.annotations.Test;
import org.testng.collections.Sets;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableSet;

public class IntersectingWaitingQueriesSelectorTest {

  @Test
  public void testFindCommonQueriesWhenInputIsEmptyListOfSets() {

    List<Set<QueryContext>> candidateQueriesSets = Lists.newArrayList();
    Set<QueryContext> expectedEligibleQueries = Sets.newHashSet();

    testFindCommonQueries(candidateQueriesSets, expectedEligibleQueries);
  }

  @Test
  public void testFindCommonQueriesWhenNoCommonQueryIsPresent() {

    QueryContext eligibleQuery1 = mock(QueryContext.class);
    QueryContext eligibleQuery2 = mock(QueryContext.class);

    Set<QueryContext> eligibleQueriesSetA = Sets.newHashSet(Arrays.asList(eligibleQuery1));
    Set<QueryContext> eligibleQueriesSetB = Sets.newHashSet(Arrays.asList(eligibleQuery2));

    List<Set<QueryContext>> candidateQueriesSets = Arrays.asList(eligibleQueriesSetA, eligibleQueriesSetB);
    Set<QueryContext> expectedEligibleQueries = Sets.newHashSet();

    testFindCommonQueries(candidateQueriesSets, expectedEligibleQueries);
  }

  @Test
  public void testFindCommonQueriesWhenACommonQueryIsPresent() {

    QueryContext eligibleQuery1 = mock(QueryContext.class);
    QueryContext eligibleQuery2 = mock(QueryContext.class);
    QueryContext eligibleQuery3 = mock(QueryContext.class);

    Set<QueryContext> eligibleQueriesSetA = Sets.newHashSet(Arrays.asList(eligibleQuery1, eligibleQuery2));
    Set<QueryContext> eligibleQueriesSetB = Sets.newHashSet(Arrays.asList(eligibleQuery1, eligibleQuery3));

    List<Set<QueryContext>> candidateQueriesSets = Arrays.asList(eligibleQueriesSetA, eligibleQueriesSetB);
    Set<QueryContext> expectedEligibleQueries = Sets.newHashSet(Arrays.asList(eligibleQuery1));

    testFindCommonQueries(candidateQueriesSets, expectedEligibleQueries);
  }

  private void testFindCommonQueries(final List<Set<QueryContext>> queriesSets,
      final Set<QueryContext> expectedEligibleQueries) {

    IntersectingWaitingQueriesSelector selector = new IntersectingWaitingQueriesSelector(mock(ImmutableSet.class));

    Set<QueryContext> actualCommonQueries = selector.findCommonQueries(queriesSets);
    assertEquals(actualCommonQueries, expectedEligibleQueries);
  }

  @Test
  public void testPrepareAllSelectionPolicies() {

    WaitingQueriesSelectionPolicy p1 = mock(WaitingQueriesSelectionPolicy.class);
    WaitingQueriesSelectionPolicy p2 = mock(WaitingQueriesSelectionPolicy.class);
    WaitingQueriesSelectionPolicy dp1 = mock(WaitingQueriesSelectionPolicy.class);
    WaitingQueriesSelectionPolicy dp2 = mock(WaitingQueriesSelectionPolicy.class);

    FinishedLensQuery mockFinishedQuery = mock(FinishedLensQuery.class);
    when(mockFinishedQuery.getDriverSelectionPolicies()).thenReturn(ImmutableSet.of(dp1, dp2));

    IntersectingWaitingQueriesSelector selector = new IntersectingWaitingQueriesSelector(ImmutableSet.of(p1, p2));
    assertEquals(selector.prepareAllSelectionPolicies(mockFinishedQuery), ImmutableSet.of(p1, p2, dp1, dp2));
  }

  @Test
  public void testPrepareAllSelectionPoliciesWithNoDriverSelectionPolicy() {

    WaitingQueriesSelectionPolicy p1 = mock(WaitingQueriesSelectionPolicy.class);
    WaitingQueriesSelectionPolicy p2 = mock(WaitingQueriesSelectionPolicy.class);

    final ImmutableSet<WaitingQueriesSelectionPolicy> emptySet = ImmutableSet.copyOf(
        Sets.<WaitingQueriesSelectionPolicy>newHashSet());

    FinishedLensQuery mockFinishedQuery = mock(FinishedLensQuery.class);
    when(mockFinishedQuery.getDriverSelectionPolicies()).thenReturn(emptySet);

    IntersectingWaitingQueriesSelector selector = new IntersectingWaitingQueriesSelector(ImmutableSet.of(p1, p2));

    assertEquals(selector.prepareAllSelectionPolicies(mockFinishedQuery), ImmutableSet.of(p1, p2));
  }

  @Test
  public void testSelectQueriesCommonBetweenAllSelectionPolicies(){

    QueryContext q1 = mock(QueryContext.class);
    QueryContext q2 = mock(QueryContext.class);
    QueryContext q3 = mock(QueryContext.class);

    /* eligibleQueriesSet1, eligibleQueriesSet2, eligibleQueriesSet3 have q1 in common */
    Set<QueryContext> eligibleQueriesSet1 = Sets.newHashSet(Arrays.asList(q1, q2));
    Set<QueryContext> eligibleQueriesSet2 = Sets.newHashSet(Arrays.asList(q1, q3));
    Set<QueryContext> eligibleQueriesSet3 = Sets.newHashSet(Arrays.asList(q1, q2));

    FinishedLensQuery mockFinishedQuery = mock(FinishedLensQuery.class);
    EstimatedImmutableQueryCollection mockWaitingQueries = mock(EstimatedImmutableQueryCollection.class);
    WaitingQueriesSelectionPolicy policy1 = mock(WaitingQueriesSelectionPolicy.class);
    WaitingQueriesSelectionPolicy policy2 = mock(WaitingQueriesSelectionPolicy.class);
    WaitingQueriesSelectionPolicy driverSelectionPolicy = mock(WaitingQueriesSelectionPolicy.class);

    when(mockFinishedQuery.getDriverSelectionPolicies()).thenReturn(ImmutableSet.of(driverSelectionPolicy));

    /* selection policy1 will return eligibleQueriesSet1 */
    when(policy1.selectQueries(mockFinishedQuery, mockWaitingQueries)).thenReturn(eligibleQueriesSet1);

    /* selection policy2 will return eligibleQueriesSet2 */
    when(policy2.selectQueries(mockFinishedQuery, mockWaitingQueries)).thenReturn(eligibleQueriesSet2);

    /* driver selection policy will return eligibleQueriesSet3 */
    when(driverSelectionPolicy.selectQueries(mockFinishedQuery, mockWaitingQueries)).thenReturn(eligibleQueriesSet3);

    WaitingQueriesSelector selector = new IntersectingWaitingQueriesSelector(ImmutableSet.of(policy1, policy2));

    /* selector should return only eligibleQuery1, as this is the only common eligible waiting query returned
    * by both selection policies */
    Set<QueryContext> actualEligibleQueries = selector.selectQueries(mockFinishedQuery, mockWaitingQueries);
    Set<QueryContext> expectedEligibleQueries = Sets.newHashSet(Arrays.asList(q1));

    assertEquals(actualEligibleQueries, expectedEligibleQueries);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testSelectorMustNotAcceptNullAsSelectionPolicies() {
    new IntersectingWaitingQueriesSelector(null);
  }

  @Test
  public void testSelectQueriesWithNoSelectionPolicies(){

    FinishedLensQuery mockFinishedQuery = mock(FinishedLensQuery.class);
    EstimatedImmutableQueryCollection mockWaitingQueries = mock(EstimatedImmutableQueryCollection.class);
    Set<WaitingQueriesSelectionPolicy> emptySetOfPolicies = Sets.newHashSet();

    when(mockFinishedQuery.getDriverSelectionPolicies()).thenReturn(ImmutableSet.copyOf(emptySetOfPolicies));

    WaitingQueriesSelector selector = new IntersectingWaitingQueriesSelector(ImmutableSet.copyOf(emptySetOfPolicies));

    /* selector should return an empty set as no selection policy is available */
    Set<QueryContext> actualEligibleQueries = selector.selectQueries(mockFinishedQuery, mockWaitingQueries);

    assertTrue(actualEligibleQueries.isEmpty());
  }
}

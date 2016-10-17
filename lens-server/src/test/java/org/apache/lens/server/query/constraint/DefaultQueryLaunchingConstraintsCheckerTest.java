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

package org.apache.lens.server.query.constraint;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.collect.EstimatedImmutableQueryCollection;
import org.apache.lens.server.api.query.constraint.QueryLaunchingConstraint;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class DefaultQueryLaunchingConstraintsCheckerTest {

  @Test
  public void testCanLaunchShouldReturnFalseWhenAtleastOneConstraintFails() {

    QueryContext mockCandidateQuery = mock(QueryContext.class);
    EstimatedImmutableQueryCollection mockRunningQueries = mock(EstimatedImmutableQueryCollection.class);

    final QueryLaunchingConstraint constraint1 = mock(QueryLaunchingConstraint.class);
    final QueryLaunchingConstraint constraint2 = mock(QueryLaunchingConstraint.class);
    QueryLaunchingConstraintsChecker constraintsChecker
      = new DefaultQueryLaunchingConstraintsChecker(ImmutableSet.of(constraint1, constraint2));
    QueryStatus status = QueryStatus.getQueuedStatus();
    final QueryLaunchingConstraint driverConstraint = mock(QueryLaunchingConstraint.class);
    when(mockCandidateQuery.getSelectedDriverQueryConstraints()).thenReturn(ImmutableSet.of(driverConstraint));
    when(mockCandidateQuery.getStatus()).thenReturn(status);

    /* Constraint1 stubbed to pass */
    when(constraint1.allowsLaunchOf(mockCandidateQuery, mockRunningQueries)).thenReturn(null);

    /* Constraint2 stubbed to fail */
    when(constraint2.allowsLaunchOf(mockCandidateQuery, mockRunningQueries)).thenReturn("constraint 2 failed");

    /* DriverConstraint stubbed to fail */
    when(driverConstraint.allowsLaunchOf(mockCandidateQuery, mockRunningQueries))
      .thenReturn("driver constraint failed");

    /* Execute test */
    boolean canLaunchQuery = constraintsChecker.canLaunch(mockCandidateQuery, mockRunningQueries);

    /* Verify */
    Assert.assertFalse(canLaunchQuery);
    Assert.assertEquals(mockCandidateQuery.getStatus().getProgressMessage(), "constraint 2 failed");
  }

  @Test
  public void testCanLaunchShouldReturnTrueWhenAllConstraintPass() {

    QueryContext mockCandidateQuery = mock(QueryContext.class);
    EstimatedImmutableQueryCollection mockRunningQueries = mock(EstimatedImmutableQueryCollection.class);

    final QueryLaunchingConstraint constraint1 = mock(QueryLaunchingConstraint.class);
    final QueryLaunchingConstraint constraint2 = mock(QueryLaunchingConstraint.class);

    QueryLaunchingConstraintsChecker constraintsChecker
      = new DefaultQueryLaunchingConstraintsChecker(ImmutableSet.of(constraint1, constraint2));

    final QueryLaunchingConstraint driverConstraint = mock(QueryLaunchingConstraint.class);
    when(mockCandidateQuery.getSelectedDriverQueryConstraints()).thenReturn(ImmutableSet.of(driverConstraint));

    /* all constraints stubbed to pass */
    when(constraint1.allowsLaunchOf(mockCandidateQuery, mockRunningQueries)).thenReturn(null);
    when(constraint2.allowsLaunchOf(mockCandidateQuery, mockRunningQueries)).thenReturn(null);
    when(driverConstraint.allowsLaunchOf(mockCandidateQuery, mockRunningQueries)).thenReturn(null);

    /* Execute test */
    boolean canLaunchQuery = constraintsChecker.canLaunch(mockCandidateQuery, mockRunningQueries);

    /* Verify */
    Assert.assertTrue(canLaunchQuery);
  }

  @Test
  public void testCanLaunchShouldReturnTrueWhenConstraintsSetIsEmpty() {

    QueryContext mockCandidateQuery = mock(QueryContext.class);
    EstimatedImmutableQueryCollection mockRunningQueries = mock(EstimatedImmutableQueryCollection.class);

    ImmutableSet<QueryLaunchingConstraint> emptySetOfConstraints
      = ImmutableSet.copyOf(Sets.<QueryLaunchingConstraint>newHashSet());

    when(mockCandidateQuery.getSelectedDriverQueryConstraints()).thenReturn(emptySetOfConstraints);

    QueryLaunchingConstraintsChecker constraintsChecker
      = new DefaultQueryLaunchingConstraintsChecker(ImmutableSet.copyOf(emptySetOfConstraints));

    boolean canLaunchQuery = constraintsChecker.canLaunch(mockCandidateQuery, mockRunningQueries);
    Assert.assertTrue(canLaunchQuery);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testConstraintsCheckerMustNotAcceptNullConstraintsSet() {
    new DefaultQueryLaunchingConstraintsChecker(null);
  }

  @Test
  public void testPrepareAllConstraints() {

    QueryLaunchingConstraint c1 = mock(QueryLaunchingConstraint.class);
    QueryLaunchingConstraint c2 = mock(QueryLaunchingConstraint.class);

    QueryLaunchingConstraint dc1 = mock(QueryLaunchingConstraint.class);
    QueryLaunchingConstraint dc2 = mock(QueryLaunchingConstraint.class);

    QueryContext mockCandidateQuery = mock(QueryContext.class);
    when(mockCandidateQuery.getSelectedDriverQueryConstraints()).thenReturn(ImmutableSet.of(dc1, dc2));

    DefaultQueryLaunchingConstraintsChecker constraintsChecker
      = new DefaultQueryLaunchingConstraintsChecker(ImmutableSet.of(c1, c2));
    assertEquals(constraintsChecker.prepareAllConstraints(mockCandidateQuery), ImmutableSet.of(c1, c2, dc1, dc2));
  }

  @Test
  public void testPrepareAllConstraintsWithNoDriverConstraints() {

    QueryLaunchingConstraint c1 = mock(QueryLaunchingConstraint.class);
    QueryLaunchingConstraint c2 = mock(QueryLaunchingConstraint.class);

    ImmutableSet<QueryLaunchingConstraint> emptySet = ImmutableSet.copyOf(Sets.<QueryLaunchingConstraint>newHashSet());
    QueryContext mockCandidateQuery = mock(QueryContext.class);
    when(mockCandidateQuery.getSelectedDriverQueryConstraints()).thenReturn(emptySet);

    DefaultQueryLaunchingConstraintsChecker constraintsChecker
      = new DefaultQueryLaunchingConstraintsChecker(ImmutableSet.of(c1, c2));
    assertEquals(constraintsChecker.prepareAllConstraints(mockCandidateQuery), ImmutableSet.of(c1, c2));
  }
}

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

import static org.apache.lens.api.Priority.*;
import static org.apache.lens.server.api.LensServerAPITestUtil.getConfiguration;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.HashSet;
import java.util.Set;

import org.apache.lens.api.Priority;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.collect.EstimatedImmutableQueryCollection;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import junit.framework.Assert;
import lombok.Data;

public class MaxConcurrentDriverQueriesConstraintTest {

  MaxConcurrentDriverQueriesConstraintFactory factory = new MaxConcurrentDriverQueriesConstraintFactory();
  QueryLaunchingConstraint constraint = factory.create(getConfiguration(
    "driver.max.concurrent.launched.queries", 10,
    "driver.max.concurrent.launches", 4
  ));

  QueryLaunchingConstraint perQueueConstraint = factory.create(getConfiguration(
    "driver.max.concurrent.launched.queries", 4,
    "driver.max.concurrent.launched.queries.per.queue", "*=1,q1=2,q2=3"
  ));

  QueryLaunchingConstraint perPriorityConstraint = factory.create(getConfiguration(
    "driver.max.concurrent.launched.queries", 4,
    "driver.max.concurrent.launched.queries.per.priority", "NORMAL=2,HIGH=3"
  ));

  QueryLaunchingConstraint perQueueAndPerPriorityConstraint = factory.create(getConfiguration(
    "driver.max.concurrent.launched.queries.per.queue", "q1=2,q2=3",
    "driver.max.concurrent.launched.queries.per.priority", "NORMAL=2,HIGH=3"
  ));

  @DataProvider
  public Object[][] dpTestAllowsLaunchOfQuery() {
    return new Object[][]{{2, true}, {3, true}, {4, true}, {5, true}, {10, false}, {11, false}};
  }

  @DataProvider
  public Object[][] dpTestConcurrentLaunches() {
    return new Object[][]{{2, true}, {3, true}, {4, false}, {5, false}, {10, false}, {11, false}};
  }

  @DataProvider
  public Object[][] dpTestPerQueueConstraints() {
    return new Object[][]{
      {queues("q1", "q2"), "q1", true},
      {queues("q1", "q1"), "q2", true},
      {queues("q1", "q1"), "q3", true},
      {queues("q1", "q1", "q1"), "q2", true}, // hypothetical
      {queues("q1", "q1", "q2"), "q1", false}, //q1 limit breached
      {queues("q1", "q2", "q2"), "q1", true},
      {queues("q1", "q2", "q2"), "q2", true},
      {queues("q1", "q2", "q1", "q2"), "q2", false}, // driver.max.concurrent.launched.queries breached
      {queues("q1", "q2", "q1", "q2"), "q1", false}, // driver.max.concurrent.launched.queries breached
      {queues("q1", "q2", "q1", "q2"), "q3", false}, // driver.max.concurrent.launched.queries breached
      {queues("q1", "q2", "q2"), "q3", true},
      {queues("q1", "q2", "q3"), "q3", false}, //default max concurrent queries per queue limit breached
    };
  }

  @DataProvider
  public Object[][] dpTestPerPriorityConstraints() {
    return new Object[][]{
      {priorities(NORMAL, HIGH), NORMAL, true},
      {priorities(NORMAL, NORMAL), HIGH, true},
      {priorities(NORMAL, NORMAL), LOW, true},
      {priorities(NORMAL, NORMAL, NORMAL), HIGH, true}, // hypothetical
      {priorities(NORMAL, NORMAL, HIGH), NORMAL, false},
      {priorities(NORMAL, HIGH, HIGH), NORMAL, true},
      {priorities(NORMAL, HIGH, HIGH), HIGH, true},
      {priorities(NORMAL, HIGH, NORMAL, HIGH), HIGH, false},
      {priorities(NORMAL, HIGH, NORMAL, HIGH), NORMAL, false},
      {priorities(NORMAL, HIGH, NORMAL, HIGH), LOW, false},
    };
  }

  @DataProvider
  public Object[][] dpTestPerQueuePerPriorityConstraints() {
    return new Object[][]{
      {queuePriorities("q1", NORMAL, "q2", NORMAL), "q2", NORMAL, false}, // can't launch NORMAL
      {queuePriorities("q1", NORMAL, "q1", HIGH), "q1", NORMAL, false}, // can't launch on q1
      {queuePriorities("q1", NORMAL, "q1", HIGH, "q2", HIGH), "q2", NORMAL, true}, // can launch NORMAL on q2
      {queuePriorities("q1", NORMAL, "q1", HIGH, "q2", HIGH, "q2", HIGH), "q2", NORMAL, true},
      {queuePriorities("q1", NORMAL, "q1", HIGH, "q2", HIGH, "q2", NORMAL), "q2", NORMAL, false}, // hypothetical
      {queuePriorities("q1", NORMAL, "q1", HIGH, "q2", HIGH, "q2", HIGH, "q2", NORMAL), "q3", NORMAL, false},
      {queuePriorities("q1", NORMAL, "q1", HIGH, "q2", HIGH, "q2", HIGH, "q2", NORMAL), "q3", HIGH, false},
      {queuePriorities("q1", NORMAL, "q1", HIGH, "q2", HIGH, "q2", HIGH, "q2", NORMAL), "q1", LOW, false},
      {queuePriorities("q1", NORMAL, "q1", HIGH, "q2", HIGH, "q2", HIGH, "q2", NORMAL), "q2", LOW, false},
      {queuePriorities("q1", NORMAL, "q1", HIGH, "q2", HIGH, "q2", HIGH, "q2", NORMAL), "q3", LOW, true},
    };
  }

  @Data
  public static class QueuePriority {
    private final String queue;
    private final Priority priority;
  }

  private static QueuePriority[] queuePriorities(Object... args) {
    Assert.assertEquals(args.length % 2, 0);
    QueuePriority[] queuePriorities = new QueuePriority[args.length / 2];
    for (int i = 0; i < args.length; i += 2) {
      queuePriorities[i / 2] = new QueuePriority((String) args[i], (Priority) args[i + 1]);
    }
    return queuePriorities;
  }

  private static String[] queues(Object... args) {
    String[] queues = new String[args.length];
    for (int i = 0; i < args.length; i++) {
      queues[i] = (String) args[i];
    }
    return queues;
  }

  private static Priority[] priorities(Object... args) {
    Priority[] priorities = new Priority[args.length];
    for (int i = 0; i < args.length; i++) {
      priorities[i] = (Priority) args[i];
    }
    return priorities;
  }

  @Test(dataProvider = "dpTestAllowsLaunchOfQuery")
  public void testAllowsLaunchOfQuery(final int currentDriverLaunchedQueries, final boolean expectedCanLaunch) {

    QueryContext mockCandidateQuery = mock(QueryContext.class);
    EstimatedImmutableQueryCollection mockLaunchedQueries = mock(EstimatedImmutableQueryCollection.class);
    LensDriver mockDriver = mock(LensDriver.class);

    when(mockCandidateQuery.getSelectedDriver()).thenReturn(mockDriver);
    when(mockLaunchedQueries.getQueriesCount(mockDriver)).thenReturn(currentDriverLaunchedQueries);

    String actualCanLaunch = constraint.allowsLaunchOf(mockCandidateQuery, mockLaunchedQueries);

    if (expectedCanLaunch) {
      assertNull(actualCanLaunch);
    } else {
      assertNotNull(actualCanLaunch);
    }
  }

  @Test(dataProvider = "dpTestConcurrentLaunches")
  public void testConcurrentLaunches(final int currentDriverLaunchedQueries, final boolean expectedCanLaunch) {

    QueryContext mockCandidateQuery = mock(QueryContext.class);
    EstimatedImmutableQueryCollection mockLaunchedQueries = mock(EstimatedImmutableQueryCollection.class);
    LensDriver mockDriver = mock(LensDriver.class);

    Set<QueryContext> queries = new HashSet<>(currentDriverLaunchedQueries);
    for (int i = 0; i < currentDriverLaunchedQueries; i++) {
      QueryContext mQuery = mock(QueryContext.class);
      when(mQuery.isLaunching()).thenReturn(true);
      queries.add(mQuery);
    }
    when(mockCandidateQuery.getSelectedDriver()).thenReturn(mockDriver);
    when(mockLaunchedQueries.getQueriesCount(mockDriver)).thenReturn(currentDriverLaunchedQueries);
    when(mockLaunchedQueries.getQueries(mockDriver)).thenReturn(queries);

    String actualCanLaunch = constraint.allowsLaunchOf(mockCandidateQuery, mockLaunchedQueries);

    if (expectedCanLaunch) {
      assertNull(actualCanLaunch);
    } else {
      assertNotNull(actualCanLaunch);
    }
  }

  @Test(dataProvider = "dpTestPerQueueConstraints")
  public void testPerQueueConstraints(final String[] launchedQueues, final String candidateQueue,
    final boolean expectedCanLaunch) {
    EstimatedImmutableQueryCollection mockLaunchedQueries = mock(EstimatedImmutableQueryCollection.class);
    LensDriver mockDriver = mock(LensDriver.class);
    Set<QueryContext> launchedQueries = new HashSet<>();
    for (String queue : launchedQueues) {
      QueryContext context = mock(QueryContext.class);
      when(context.getQueue()).thenReturn(queue);
      launchedQueries.add(context);
    }
    when(mockLaunchedQueries.getQueries(mockDriver)).thenReturn(launchedQueries);
    when(mockLaunchedQueries.getQueriesCount(mockDriver)).thenReturn(launchedQueries.size());

    QueryContext mockCandidateQuery = mock(QueryContext.class);
    when(mockCandidateQuery.getQueue()).thenReturn(candidateQueue);
    when(mockCandidateQuery.getSelectedDriver()).thenReturn(mockDriver);
    String actualCanLaunch = perQueueConstraint.allowsLaunchOf(mockCandidateQuery, mockLaunchedQueries);

    if (expectedCanLaunch) {
      assertNull(actualCanLaunch);
    } else {
      assertNotNull(actualCanLaunch);
    }
  }

  @Test(dataProvider = "dpTestPerPriorityConstraints")
  public void testPerPriorityConstraints(final Priority[] launchedPriorities, final Priority candidatePriority,
    final boolean expectedCanLaunch) {
    EstimatedImmutableQueryCollection mockLaunchedQueries = mock(EstimatedImmutableQueryCollection.class);
    LensDriver mockDriver = mock(LensDriver.class);
    Set<QueryContext> launchedQueries = new HashSet<>();
    for (Priority priority : launchedPriorities) {
      QueryContext context = mock(QueryContext.class);
      when(context.getPriority()).thenReturn(priority);
      launchedQueries.add(context);
    }
    when(mockLaunchedQueries.getQueries(mockDriver)).thenReturn(launchedQueries);
    when(mockLaunchedQueries.getQueriesCount(mockDriver)).thenReturn(launchedQueries.size());

    QueryContext mockCandidateQuery = mock(QueryContext.class);
    when(mockCandidateQuery.getPriority()).thenReturn(candidatePriority);
    when(mockCandidateQuery.getSelectedDriver()).thenReturn(mockDriver);
    String actualCanLaunch = perPriorityConstraint.allowsLaunchOf(mockCandidateQuery, mockLaunchedQueries);

    if (expectedCanLaunch) {
      assertNull(actualCanLaunch);
    } else {
      assertNotNull(actualCanLaunch);
    }
  }

  @Test(dataProvider = "dpTestPerQueuePerPriorityConstraints")
  public void testPerQueuePerPriorityConstraints(final QueuePriority[] launchedQueuePriorities,
    final String candidateQueue, final Priority candidatePriority, final boolean expectedCanLaunch) {
    EstimatedImmutableQueryCollection mockLaunchedQueries = mock(EstimatedImmutableQueryCollection.class);
    LensDriver mockDriver = mock(LensDriver.class);
    Set<QueryContext> launchedQueries = new HashSet<>();
    for (QueuePriority queuePriority : launchedQueuePriorities) {
      QueryContext context = mock(QueryContext.class);
      when(context.getQueue()).thenReturn(queuePriority.getQueue());
      when(context.getPriority()).thenReturn(queuePriority.getPriority());
      launchedQueries.add(context);
    }
    when(mockLaunchedQueries.getQueries(mockDriver)).thenReturn(launchedQueries);
    when(mockLaunchedQueries.getQueriesCount(mockDriver)).thenReturn(launchedQueries.size());

    QueryContext mockCandidateQuery = mock(QueryContext.class);
    when(mockCandidateQuery.getQueue()).thenReturn(candidateQueue);
    when(mockCandidateQuery.getPriority()).thenReturn(candidatePriority);
    when(mockCandidateQuery.getSelectedDriver()).thenReturn(mockDriver);
    String actualCanLaunch = perQueueAndPerPriorityConstraint.allowsLaunchOf(mockCandidateQuery, mockLaunchedQueries);

    if (expectedCanLaunch) {
      assertNull(actualCanLaunch);
    } else {
      assertNotNull(actualCanLaunch);
    }
  }
}

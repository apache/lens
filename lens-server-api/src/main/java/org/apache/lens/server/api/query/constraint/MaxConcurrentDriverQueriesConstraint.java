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

import java.util.Map;
import java.util.Set;

import org.apache.lens.api.Priority;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.collect.EstimatedImmutableQueryCollection;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class MaxConcurrentDriverQueriesConstraint implements QueryLaunchingConstraint {

  private final int maxConcurrentQueries;
  private final Map<String, Integer> maxConcurrentQueriesPerQueue;
  private final Map<Priority, Integer> maxConcurrentQueriesPerPriority;
  private final Integer defaultMaxConcurrentQueriesPerQueueLimit;
  private final int maxConcurrentLaunches;

  @Override
  public String allowsLaunchOf(
    final QueryContext candidateQuery, final EstimatedImmutableQueryCollection launchedQueries) {

    final LensDriver selectedDriver = candidateQuery.getSelectedDriver();
    final Set<QueryContext> driverLaunchedQueries = launchedQueries.getQueries(selectedDriver);

    String maxConcurrentLimitation = canLaunchWithMaxConcurrentConstraint(candidateQuery,
      launchedQueries.getQueriesCount(selectedDriver));
    if (maxConcurrentLimitation != null) {
      return maxConcurrentLimitation;
    }
    String maxLaunchingLimitation = canLaunchWithMaxLaunchingConstraint(driverLaunchedQueries);
    if (maxLaunchingLimitation != null) {
      return maxLaunchingLimitation;
    }
    String queueLimitation = canLaunchWithQueueConstraint(candidateQuery, driverLaunchedQueries);
    if (queueLimitation != null) {
      return queueLimitation;
    }
    String priorityLimitation = canLaunchWithPriorityConstraint(candidateQuery, driverLaunchedQueries);
    if (priorityLimitation != null) {
      return priorityLimitation;
    }
    return null;
  }

  private String canLaunchWithMaxLaunchingConstraint(Set<QueryContext> driverLaunchedQueries) {
    int launchingCount = getIsLaunchingCount(driverLaunchedQueries);
    if (launchingCount >= maxConcurrentLaunches) {
      return launchingCount + "/" + maxConcurrentLaunches + " launches happening";
    }
    return null;
  }

  private String canLaunchWithMaxConcurrentConstraint(QueryContext candidateQuery, int concurrentLaunched) {
    if (concurrentLaunched >= maxConcurrentQueries) {
      return concurrentLaunched + "/" + maxConcurrentQueries + " queries running on "
        + candidateQuery.getSelectedDriver().getFullyQualifiedName();
    }
    return null;
  }
  private String canLaunchWithQueueConstraint(QueryContext candidateQuery, Set<QueryContext> launchedQueries) {
    if (maxConcurrentQueriesPerQueue == null) {
      return null;
    }
    String queue = candidateQuery.getQueue();
    Integer limit = maxConcurrentQueriesPerQueue.get(queue);
    if (limit == null) {
      if (defaultMaxConcurrentQueriesPerQueueLimit != null) { //Check if any default limit is enabled for all queues
        limit = defaultMaxConcurrentQueriesPerQueueLimit;
      } else {
        return null;
      }
    }
    int launchedOnQueue = 0;
    for (QueryContext context : launchedQueries) {
      if (context.getQueue().equals(queue)) {
        launchedOnQueue++;
      }
    }
    if (launchedOnQueue >= limit) {
      return launchedOnQueue + "/" + limit + " queries running in Queue " + queue;
    }
    return null;
  }

  private String canLaunchWithPriorityConstraint(QueryContext candidateQuery, Set<QueryContext> launchedQueries) {
    if (maxConcurrentQueriesPerPriority == null) {
      return null;
    }
    Priority priority = candidateQuery.getPriority();
    Integer limit = maxConcurrentQueriesPerPriority.get(priority);
    if (limit == null) {
      return null;
    }
    int launchedOnPriority = 0;
    for (QueryContext context : launchedQueries) {
      if (context.getPriority().equals(priority)) {
        launchedOnPriority++;
      }
    }
    if (launchedOnPriority >= limit) {
      return launchedOnPriority + "/" + limit + " queries running with priority " + priority;
    }
    return null;
  }

  private int getIsLaunchingCount(final Set<QueryContext> launchedQueries) {
    int launcherCount = 0;
    for (QueryContext ctx : launchedQueries) {
      if (ctx.isLaunching()) {
        launcherCount++;
      }
    }
    return  launcherCount;
  }
}

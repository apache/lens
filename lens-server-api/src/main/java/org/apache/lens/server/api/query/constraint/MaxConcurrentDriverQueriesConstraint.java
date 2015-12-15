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

  @Override
  public boolean allowsLaunchOf(
    final QueryContext candidateQuery, final EstimatedImmutableQueryCollection launchedQueries) {

    final LensDriver selectedDriver = candidateQuery.getSelectedDriver();
    final boolean canLaunch = (launchedQueries.getQueriesCount(selectedDriver) < maxConcurrentQueries)
      && canLaunchWithQueueConstraint(candidateQuery, launchedQueries)
      && canLaunchWithPriorityConstraint(candidateQuery, launchedQueries);
    log.debug("canLaunch:{}", canLaunch);
    return canLaunch;
  }

  private boolean canLaunchWithQueueConstraint(QueryContext candidateQuery, EstimatedImmutableQueryCollection
    launchedQueries) {
    if (maxConcurrentQueriesPerQueue == null) {
      return true;
    }
    String queue = candidateQuery.getQueue();
    Integer limit = maxConcurrentQueriesPerQueue.get(queue);
    if (limit == null) {
      return true;
    }
    int launchedOnQueue = 0;
    for (QueryContext context : launchedQueries.getQueries(candidateQuery.getSelectedDriver())) {
      if (context.getQueue().equals(queue)) {
        launchedOnQueue++;
      }
    }
    return launchedOnQueue < limit;
  }

  private boolean canLaunchWithPriorityConstraint(QueryContext candidateQuery, EstimatedImmutableQueryCollection
    launchedQueries) {
    if (maxConcurrentQueriesPerPriority == null) {
      return true;
    }
    Priority priority = candidateQuery.getPriority();
    Integer limit = maxConcurrentQueriesPerPriority.get(priority);
    if (limit == null) {
      return true;
    }
    int launchedOnPriority = 0;
    for (QueryContext context : launchedQueries.getQueries(candidateQuery.getSelectedDriver())) {
      if (context.getPriority().equals(priority)) {
        launchedOnPriority++;
      }
    }
    return launchedOnPriority < limit;
  }
}

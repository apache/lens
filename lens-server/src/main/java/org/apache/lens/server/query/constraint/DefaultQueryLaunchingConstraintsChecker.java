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

import java.util.Collections;
import java.util.Set;

import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.collect.EstimatedImmutableQueryCollection;
import org.apache.lens.server.api.query.constraint.QueryLaunchingConstraint;
import org.apache.lens.server.api.retry.BackOffRetryHandler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * This {@link QueryLaunchingConstraintsChecker} enforces that a candidate query will be allowed to launch only if
 * all {@link QueryLaunchingConstraint}s of lens server and all {@link QueryLaunchingConstraint}s of driver selected
 * for query allow the query to be launched.
 *
 */
@Slf4j
public class DefaultQueryLaunchingConstraintsChecker implements QueryLaunchingConstraintsChecker {

  private final ImmutableSet<QueryLaunchingConstraint> lensQueryConstraints;

  public DefaultQueryLaunchingConstraintsChecker(
    @NonNull final ImmutableSet<QueryLaunchingConstraint> lensQueryConstraints) {
    this.lensQueryConstraints = lensQueryConstraints;
  }

  @Override
  public boolean canLaunch(final QueryContext candidateQuery, final EstimatedImmutableQueryCollection launchedQueries) {

    Set<QueryLaunchingConstraint> allConstraints = prepareAllConstraints(candidateQuery);

    for (QueryLaunchingConstraint queryConstraint : allConstraints) {
      String launchRejectionMessage = queryConstraint.allowsLaunchOf(candidateQuery, launchedQueries);
      if (launchRejectionMessage != null) {
        log.info("query {} not allowed to launch. Constraint failed: {} with message: {}",
          candidateQuery, queryConstraint, launchRejectionMessage);
        candidateQuery.getStatus().setProgressMessage(launchRejectionMessage);
        return false;
      }
    }
    return true;
  }

  @VisibleForTesting
  Set<QueryLaunchingConstraint> prepareAllConstraints(final QueryContext candidateQuery) {

    ImmutableSet<QueryLaunchingConstraint> driverConstraints = candidateQuery.getSelectedDriverQueryConstraints();
    BackOffRetryHandler<QueryContext> retryPolicy = candidateQuery.getRetryPolicy();
    Sets.SetView<QueryLaunchingConstraint> constraints = Sets.union(this.lensQueryConstraints, driverConstraints);
    if (retryPolicy == null) {
      return constraints;
    } else {
      return Sets.union(Collections.singleton(new RetryPolicyToConstraingAdapter(retryPolicy)), constraints);
    }
  }
}

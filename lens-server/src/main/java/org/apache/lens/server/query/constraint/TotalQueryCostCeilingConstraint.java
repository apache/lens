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
package org.apache.lens.server.query.constraint;

import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.collect.EstimatedImmutableQueryCollection;
import org.apache.lens.server.api.query.constraint.QueryLaunchingConstraint;
import org.apache.lens.server.api.query.cost.QueryCost;

import com.google.common.base.Optional;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@EqualsAndHashCode
public class TotalQueryCostCeilingConstraint implements QueryLaunchingConstraint {

  /**
   * Per user total query cost ceiling for launching a new query.
   */
  private final Optional<QueryCost> totalQueryCostCeilingPerUser;

  public TotalQueryCostCeilingConstraint(@NonNull final Optional<QueryCost> totalQueryCostCeilingPerUser) {
    this.totalQueryCostCeilingPerUser = totalQueryCostCeilingPerUser;
  }

  /**
   *
   * This constraint allows a query to be launched by the user,
   *
   * if total query cost of launched queries of
   * the user is less than or equal to the total query cost ceiling per user
   *
   * OR
   *
   * the total query cost ceiling per user  is not present.
   *
   * @param candidateQuery The query which is the next candidate to be launched.
   * @param launchedQueries Current launched queries
   * @return
   */
  @Override
  public String allowsLaunchOf(
    final QueryContext candidateQuery, final EstimatedImmutableQueryCollection launchedQueries) {

    if (!totalQueryCostCeilingPerUser.isPresent()) {
      return null;
    }

    final String currentUser = candidateQuery.getSubmittedUser();
    QueryCost totalQueryCostForCurrentUser = launchedQueries.getTotalQueryCost(currentUser);

    if (totalQueryCostForCurrentUser.compareTo(totalQueryCostCeilingPerUser.get()) > 0) {
      return totalQueryCostForCurrentUser + "/" + totalQueryCostCeilingPerUser + " capacity utilized by "
        + candidateQuery.getSubmittedUser();
    }
    return null;
  }
}

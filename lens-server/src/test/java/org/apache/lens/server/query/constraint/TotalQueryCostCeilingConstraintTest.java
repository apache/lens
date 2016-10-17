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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.collect.EstimatedImmutableQueryCollection;
import org.apache.lens.server.api.query.constraint.QueryLaunchingConstraint;
import org.apache.lens.server.api.query.cost.FactPartitionBasedQueryCost;
import org.apache.lens.server.api.query.cost.QueryCost;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

public class TotalQueryCostCeilingConstraintTest {

  @DataProvider
  public Object[][] dpTestAllowsLaunchOfQuery() {
    return new Object[][] { {7.0, true} , {90.0, true}, {91.0, false}};
  }

  @Test(dataProvider = "dpTestAllowsLaunchOfQuery")
  public void testAllowsLaunchOfQuery(final double totalQueryCostForCurrentUser, final boolean expectedCanLaunch) {

    final QueryCost totalQueryCostCeilingPerUser = new FactPartitionBasedQueryCost(90.0);
    final QueryLaunchingConstraint queryConstraint
      = new TotalQueryCostCeilingConstraint(Optional.of(totalQueryCostCeilingPerUser));

    final QueryContext query = mock(QueryContext.class);
    final EstimatedImmutableQueryCollection launchedQueries = mock(EstimatedImmutableQueryCollection.class);
    final String mockUser = "MockUser";

    when(query.getSubmittedUser()).thenReturn(mockUser);
    when(launchedQueries.getTotalQueryCost(mockUser))
      .thenReturn(new FactPartitionBasedQueryCost(totalQueryCostForCurrentUser));

    String actualCanLaunch = queryConstraint.allowsLaunchOf(query, launchedQueries);

    if (expectedCanLaunch) {
      assertNull(actualCanLaunch);
    } else {
      assertNotNull(actualCanLaunch);
    }
  }

  @Test
  public void testAllowsLaunchOfQueryWhenCeilingIsAbsent() {

    final QueryLaunchingConstraint queryConstraint
      = new TotalQueryCostCeilingConstraint(Optional.<QueryCost>absent());

    final QueryContext query = mock(QueryContext.class);
    final EstimatedImmutableQueryCollection launchedQueries = mock(EstimatedImmutableQueryCollection.class);

    String actualCanLaunch = queryConstraint.allowsLaunchOf(query, launchedQueries);
    assertNull(actualCanLaunch);
  }
}

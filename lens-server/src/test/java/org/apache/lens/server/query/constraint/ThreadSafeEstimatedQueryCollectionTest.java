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

import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.constraint.MaxConcurrentDriverQueriesConstraint;
import org.apache.lens.server.api.query.constraint.QueryLaunchingConstraint;
import org.apache.lens.server.api.query.cost.FactPartitionBasedQueryCost;
import org.apache.lens.server.api.query.cost.QueryCost;
import org.apache.lens.server.query.collect.DefaultEstimatedQueryCollection;
import org.apache.lens.server.query.collect.DefaultQueryCollection;
import org.apache.lens.server.query.collect.ThreadSafeEstimatedQueryCollection;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ThreadSafeEstimatedQueryCollectionTest {
  public static final QueryCost COST = new FactPartitionBasedQueryCost(10);

  @DataProvider
  public Object[][] dpTestAllowsLaunchOfQuery() {
    return new Object[][]{{2, true}, {3, false}, {11, false}};
  }

  @Test(dataProvider = "dpTestAllowsLaunchOfQuery")
  public void testAllowsLaunchOfQuery(final int currentDriverLaunchedQueries, final boolean expectedCanLaunch) {

    int maxConcurrentQueries = 3;

    LensDriver mockDriver = mock(LensDriver.class);
    LensDriver mockDriver2 = mock(LensDriver.class);

    QueryLaunchingConstraint constraint = new MaxConcurrentDriverQueriesConstraint(maxConcurrentQueries, null, null);
    ThreadSafeEstimatedQueryCollection col = new ThreadSafeEstimatedQueryCollection(new
      DefaultEstimatedQueryCollection(new DefaultQueryCollection()));

    for (int i = 0; i < currentDriverLaunchedQueries; i++) {
      QueryContext query = mock(QueryContext.class);
      when(query.getSelectedDriver()).thenReturn(mockDriver);
      when(query.getSelectedDriverQueryCost()).thenReturn(COST);
      col.add(query);
    }
    for (int i = 0; i < 2; i++) {
      QueryContext query = mock(QueryContext.class);
      when(query.getSelectedDriver()).thenReturn(mockDriver2);
      when(query.getSelectedDriverQueryCost()).thenReturn(COST);
      col.add(query);
    }

    // new candidate query
    QueryContext mockCandidateQuery = mock(QueryContext.class);
    when(mockCandidateQuery.getSelectedDriver()).thenReturn(mockDriver);
    when(mockCandidateQuery.getSelectedDriverQueryCost()).thenReturn(COST);
    boolean actualCanLaunch = constraint.allowsLaunchOf(mockCandidateQuery, col);

    assertEquals(actualCanLaunch, expectedCanLaunch);
  }
}

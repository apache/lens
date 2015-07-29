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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.collect.EstimatedImmutableQueryCollection;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class MaxConcurrentDriverQueriesConstraintTest {

  @DataProvider
  public Object[][] dpTestAllowsLaunchOfQuery() {
    return new Object[][] { {2, true} , {10, false}, {11, false}};
  }

  @Test(dataProvider = "dpTestAllowsLaunchOfQuery")
  public void testAllowsLaunchOfQuery(final int currentDriverLaunchedQueries, final boolean expectedCanLaunch) {

    int maxConcurrentQueries = 10;

    QueryContext mockCandidateQuery = mock(QueryContext.class);
    EstimatedImmutableQueryCollection mockLaunchedQueries = mock(EstimatedImmutableQueryCollection.class);
    LensDriver mockDriver = mock(LensDriver.class);

    when(mockCandidateQuery.getSelectedDriver()).thenReturn(mockDriver);
    when(mockLaunchedQueries.getQueriesCount(mockDriver)).thenReturn(currentDriverLaunchedQueries);

    QueryLaunchingConstraint constraint = new MaxConcurrentDriverQueriesConstraint(maxConcurrentQueries);
    boolean actualCanLaunch = constraint.allowsLaunchOf(mockCandidateQuery, mockLaunchedQueries);

    assertEquals(actualCanLaunch, expectedCanLaunch);
  }
}

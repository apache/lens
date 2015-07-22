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

import static org.apache.lens.server.query.collect.QueryCollectUtil.createQueriesSetWithUserStubbing;

import static org.mockito.Mockito.*;

import static org.testng.Assert.assertEquals;

import java.util.Set;

import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.cost.FactPartitionBasedQueryCost;
import org.apache.lens.server.api.query.cost.QueryCost;

import org.testng.annotations.Test;

import com.beust.jcommander.internal.Sets;
import com.google.common.collect.Iterables;

public class DefaultEstimatedQueryCollectionTest {

  private static final String MOCK_USER = "MockUserEmail";

  @Test
  public void testGetTotalQueryCostForUserWithZeroLaunchedQueries() throws LensException {

    QueryCollection mockQueries = mock(QueryCollection.class);
    when(mockQueries.getQueries(MOCK_USER)).thenReturn(Sets.<QueryContext>newLinkedHashSet());

    EstimatedQueryCollection queries = new DefaultEstimatedQueryCollection(mockQueries);
    QueryCost actualQueryCost = queries.getTotalQueryCost(MOCK_USER);
    assertEquals(actualQueryCost, new FactPartitionBasedQueryCost(0));
  }

  @Test
  public void testGetTotalQueryCostForUserWithMoreThanOneLaunchedQueries() throws LensException {

    QueryCollection mockQueries = mock(QueryCollection.class);
    Set<QueryContext> mockQueriesSet = createQueriesSetWithUserStubbing(2, MOCK_USER);
    when(mockQueries.getQueries(MOCK_USER)).thenReturn(mockQueriesSet);

    final QueryContext query0 = Iterables.get(mockQueriesSet, 0);
    final QueryContext query1 = Iterables.get(mockQueriesSet, 1);

    final QueryCost mockCost0 = mock(QueryCost.class);
    final QueryCost mockCost1 = mock(QueryCost.class);
    final QueryCost mockCost0Plus0 = mock(QueryCost.class);
    final QueryCost mockCost0Plus0Plus1 = mock(QueryCost.class);

    when(query0.getSelectedDriverQueryCost()).thenReturn(mockCost0);
    when(query1.getSelectedDriverQueryCost()).thenReturn(mockCost1);

    when(mockCost0.add(mockCost0)).thenReturn(mockCost0Plus0);
    when(mockCost0Plus0.add(mockCost1)).thenReturn(mockCost0Plus0Plus1);

    QueryCost actualQueryCost = new DefaultEstimatedQueryCollection(mockQueries).getTotalQueryCost(MOCK_USER);
    assertEquals(actualQueryCost, mockCost0Plus0Plus1);
  }

  @Test
  public void testAddAndRemoveAndGetQueriesMethod() throws LensException {

    QueryContext mockQuery = mock(QueryContext.class);
    LensDriver mockSelectedDriver = mock(LensDriver.class);
    QueryCost mockQueryCost = mock(QueryCost.class);
    when(mockQuery.getSelectedDriver()).thenReturn(mockSelectedDriver);
    when(mockQuery.getSelectedDriverQueryCost()).thenReturn(mockQueryCost);

    QueryCollection mockQueries = mock(QueryCollection.class);
    EstimatedQueryCollection queries = new DefaultEstimatedQueryCollection(mockQueries);

    queries.add(mockQuery);
    assertEquals(queries.getQueriesCount(mockSelectedDriver), 1);
    verify(mockQueries, times(1)).add(mockQuery);

    queries.remove(mockQuery);
    assertEquals(queries.getQueriesCount(mockSelectedDriver), 0);
    verify(mockQueries, times(1)).remove(mockQuery);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testCheckStateMustRecognizeIllegalStateWhenSelectedDriverIsNotSet() throws LensException {

    QueryContext mockQuery = mock(QueryContext.class);
    /* Setting selected driver cost, however since selected driver is not set. This should result in
    IllegalStateException */
    when(mockQuery.getSelectedDriverQueryCost()).thenReturn(mock(QueryCost.class));

    QueryCollection mockQueries = mock(QueryCollection.class);
    DefaultEstimatedQueryCollection queries = new DefaultEstimatedQueryCollection(mockQueries);
    queries.checkState(mockQuery);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testCheckStateMustRecognizeIllegalStateWhenQueryCostIsNotSet() {

    QueryContext mockQuery = mock(QueryContext.class);
    /* Selected Driver is set, however since selected driver query cost is not set. This should result in
    IllegalStateException. */
    when(mockQuery.getSelectedDriver()).thenReturn(mock(LensDriver.class));

    QueryCollection mockQueries = mock(QueryCollection.class);
    DefaultEstimatedQueryCollection queries = new DefaultEstimatedQueryCollection(mockQueries);
    queries.checkState(mockQuery);
  }
}

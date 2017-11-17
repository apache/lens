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

package org.apache.lens.server.query;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.Comparator;

import org.apache.lens.api.Priority;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.comparators.ChainedComparator;
import org.apache.lens.server.api.query.comparators.FIFOQueryComparator;
import org.apache.lens.server.api.query.comparators.QueryCostComparator;
import org.apache.lens.server.api.query.comparators.QueryPriorityComparator;
import org.apache.lens.server.api.query.cost.QueryCost;
import org.apache.lens.server.api.query.cost.StaticQueryCost;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class QueryContextComparatorTest {

  private final Comparator<QueryContext> priorityComparator = new QueryPriorityComparator();
  private final Comparator<QueryContext> costComparator = new QueryCostComparator();
  private final Comparator<QueryContext> fifoComparator = new FIFOQueryComparator();
  private final Comparator<QueryContext> priorityAndFifoComparator
    = new ChainedComparator<>(Lists.newArrayList(priorityComparator, fifoComparator));
  private final Comparator<QueryContext> costAndFifoComparator
    = new ChainedComparator<>(Lists.newArrayList(costComparator, fifoComparator));


  @DataProvider
  public Object[][] dpQueryCostCompare() {
    return new Object[][] {
      /* Query cost of query1 is less than query cost of query2 */
      {-1, -1},
      /* Query cost of query1 is more than query cost of query2 */
      {1, 1},
    };
  }

  @Test(dataProvider = "dpQueryCostCompare")
  public void testCompareOnQueryCost(final int resultOfQueryCostCompare, final int expectedResult) {

    QueryContext query1 = mock(QueryContext.class);
    QueryCost qcO1 = mock(QueryCost.class);
    when(query1.getSelectedDriverQueryCost()).thenReturn(qcO1);

    QueryContext query2 = mock(QueryContext.class);
    QueryCost qcO2 = mock(QueryCost.class);
    when(query2.getSelectedDriverQueryCost()).thenReturn(qcO2);

    when(qcO1.compareTo(qcO2)).thenReturn(resultOfQueryCostCompare);
    assertEquals(costAndFifoComparator.compare(query1, query2), expectedResult);
  }

  @Test
  public void testCompareOnQueryPriority() {

    QueryContext query1 = mock(QueryContext.class);
    when(query1.getPriority()).thenReturn(Priority.HIGH); // Ordinal = 1

    QueryContext query2 = mock(QueryContext.class);
    when(query2.getPriority()).thenReturn(Priority.LOW); // Ordinal = 3

    assertEquals(priorityAndFifoComparator.compare(query1, query2), -2);
  }


  @DataProvider
  public Object[][] dpSubmitTimeCompare() {
    return new Object[][] {
      /* Submission Time of query1 is less than Submission Time of query2 */
      {123, 125, -1},
      /* Submission Time of query1 is more than Submission Time of query2 */
      {125, 123, 1},
      /* Submission Time of query1 is equal to Submission Time of query2 */
      {123, 123, 0},
      /* Boundary case: Submission Time of query1 is Long.MIN_VALUE and submission time of query2 Long.MAX_VALUE */
      {Long.MIN_VALUE, Long.MAX_VALUE, -1},
      /* Boundary case: Submission Time of query1 is Long.MAX_VALUE and submission time of query2 Long.MIN_VALUE */
      {Long.MAX_VALUE, Long.MIN_VALUE, 1},
      /* Submission Time of query1 and query2 is 0 */
      {0, 0, 0},
    };
  }

  @Test(dataProvider = "dpSubmitTimeCompare")
  public void testCompareOnQuerySubmitTime(final long submitTimeQuery1, final long submitTimeQuery2,
      final int expectedResult) {

    QueryContext query1 = mock(QueryContext.class);
    when(query1.getPriority()).thenReturn(Priority.HIGH);

    QueryCost s1 = new StaticQueryCost(0.0);
    QueryCost s2 = new StaticQueryCost(0.0);

    when(query1.getSelectedDriverQueryCost()).thenReturn(s1);

    QueryContext query2 = mock(QueryContext.class);
    when(query2.getPriority()).thenReturn(Priority.HIGH);
    when(query2.getSelectedDriverQueryCost()).thenReturn(s2);

    when(query1.getSubmissionTime()).thenReturn(submitTimeQuery1);
    when(query2.getSubmissionTime()).thenReturn(submitTimeQuery2);

    // Cost and Priority both are same, hence the comparison should happen
    // on query submission time
    assertEquals(priorityAndFifoComparator.compare(query1, query2), expectedResult);
    assertEquals(costAndFifoComparator.compare(query1, query2), expectedResult);

  }
}

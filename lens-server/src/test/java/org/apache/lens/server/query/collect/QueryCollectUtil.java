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

import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isSynchronized;

import org.apache.lens.api.Priority;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.comparators.QueryCostComparator;
import org.apache.lens.server.api.query.comparators.QueryPriorityComparator;
import org.apache.lens.server.api.query.cost.FactPartitionBasedQueryCost;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.Sets;

public class QueryCollectUtil {

  protected QueryCollectUtil() {
    throw new UnsupportedOperationException();
  }

  public static Set<QueryContext> createQueriesSetWithUserStubbing(final int reqNoOfMockQueries,
      final String mockUser) {

    final Set<QueryContext> mockQueries = getMockQueriesSet(reqNoOfMockQueries);
    stubSubmittedUserInMockQueries(mockQueries, mockUser);
    return mockQueries;
  }

  public static void stubSubmittedUserInMockQueries(final Set<QueryContext> mockQueries, final String mockUser) {

    for (QueryContext mockQuery : mockQueries) {
      when(mockQuery.getSubmittedUser()).thenReturn(mockUser);
    }
  }

  public static Set<QueryContext> getMockQueriesSet(final int reqNoOfMockQueries) {

    Set<QueryContext> mockQueries = Sets.newLinkedHashSet();

    for (int i = 1; i <= reqNoOfMockQueries; ++i) {
      mockQueries.add(mock(QueryContext.class));
    }
    return mockQueries;
  }

  public static QueryCollection createQueriesInstanceWithMockedQueries(final int reqNoOfMockQueries) {

    final Set<QueryContext> mockQueries = getMockQueriesSet(reqNoOfMockQueries);
    return new DefaultQueryCollection(mockQueries);
  }

  public static QueryCollection createQueriesInstanceWithUserStubbing(final int reqNoOfMockQueries,
    final String mockUser) {

    final Set<QueryContext> mockQueries = createQueriesSetWithUserStubbing(reqNoOfMockQueries, mockUser);
    return new DefaultQueryCollection(mockQueries);
  }

  public static QueryCollection createQueriesTreeSetWithQueryHandleAndCostStubbing(final double[] queryCosts,
                                                                                   final String handlePrefix) {

    TreeSet<QueryContext> mockQueries = new TreeSet<>(new QueryCostComparator());

    for (int index = 1; index <= queryCosts.length; ++index) {
      mockQueries.add(createQueryInstanceWithQueryHandleAndCostStubbing(handlePrefix, index, queryCosts[index - 1]));
    }
    return new DefaultQueryCollection(mockQueries);
  }

  public static QueryContext createQueryInstanceWithQueryHandleAndCostStubbing(String handlePrefix, int index,
                                                                               double queryCost) {
    QueryContext mockQuery = mock(QueryContext.class);
    when(mockQuery.getQueryHandle()).thenReturn(QueryHandle.fromString(handlePrefix + index));
    when(mockQuery.getSelectedDriverQueryCost()).thenReturn(new FactPartitionBasedQueryCost(queryCost));
    return mockQuery;
  }

  public static QueryCollection createQueriesTreeSetWithQueryHandleAndPriorityStubbing(Priority[]  priorities,
                                                                                    final String handlePrefix) {
    TreeSet<QueryContext> mockQueries = new TreeSet<>(new QueryPriorityComparator());

    for (int index = 1; index <=  priorities.length; ++index) {
      mockQueries.add(createQueryInstanceWithQueryHandleAndPriorityStubbing(handlePrefix, index,
          priorities[index -1]));
    }
    return new DefaultQueryCollection(mockQueries);
  }

  public static QueryContext createQueryInstanceWithQueryHandleAndPriorityStubbing(String handlePrefix, int index,
                                                                               Priority priority) {
    QueryContext mockQuery = mock(QueryContext.class);
    when(mockQuery.getQueryHandle()).thenReturn(QueryHandle.fromString(handlePrefix + index));
    when(mockQuery.getPriority()).thenReturn(priority);
    return mockQuery;
  }

  public static QueryContext getMockedQueryFromQueries(Set<QueryContext> queries, String mockHandle, int index) {
    Iterator iterator = queries.iterator();
    while (iterator.hasNext()) {
      QueryContext queuedQuery = (QueryContext) iterator.next();
      if (queuedQuery.getQueryHandle().equals(QueryHandle.fromString(mockHandle + index))) {
        return queuedQuery;
      }
    }
    return null;
  }

  public static QueryCollection stubMockQueryAndCreateQueriesInstance(final QueryContext mockQuery,
    final String mockUser) {

    Set<QueryContext> mockQueries = Sets.newHashSet(mockQuery);
    stubSubmittedUserInMockQueries(mockQueries, mockUser);
    return new DefaultQueryCollection(mockQueries);
  }

  public static <T> void testAllMethodsHaveSynchronizedKeyword(final Class<T> clazz) {

    Method[] allMethods = clazz.getDeclaredMethods();

    for (Method method : allMethods) {

      int modifierSet = method.getModifiers();
      if (isPublic(modifierSet)) {
        /* If the method is public, then it should also have synchronized modifier on it */
        assertTrue(isSynchronized(modifierSet), "method [" + method.getName() + "] in class ["
            + clazz.getName() + "] does not have synchronized modifier on it");
      }
    }
  }
}

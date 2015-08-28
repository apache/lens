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

import static org.apache.lens.server.query.collect.QueryCollectUtil.*;

import static org.mockito.Mockito.mock;

import static org.testng.Assert.assertEquals;

import java.util.Set;

import org.apache.lens.server.api.query.QueryContext;

import org.testng.annotations.Test;

public class DefaultQueryCollectionTest {

  private static final String MOCK_USER = "MockUserEmail";

  /* Note: Since verification of addition/removal required calling get methods,
  hence methods getQueriesCount and getQueries(user) are indirectly getting tested in these tests */

  @Test
  public void testAddMethodAddsQueriesToAllViews(){

    /* Initialization */
    final int noOfQueriesUsedInTest = 2;
    QueryCollection queries = createQueriesInstanceWithUserStubbing(noOfQueriesUsedInTest, MOCK_USER);

    /* Verification 1: Verifies that queries were added to queries list by calling getQueriesCount which gets results
    from queries list */
    assertEquals(queries.getQueriesCount(), noOfQueriesUsedInTest);

    /* Verification 2: Verifies that queries were added to queries per user map by calling getQueries(user) method,
    which gets information from queries per user map */

    assertEquals(queries.getQueries(MOCK_USER).size(), noOfQueriesUsedInTest);
  }

  @Test
  public void testRemoveMethodRemovesFromAllViews() {

    /* Initialization */
    QueryContext mockQuery = mock(QueryContext.class);
    QueryCollection queries = stubMockQueryAndCreateQueriesInstance(mockQuery, MOCK_USER);

    /* Execution */
    queries.remove(mockQuery);

    /* Verification 1: Verifies that queries were removed from queries list by calling getQueriesCount which gets
    results from queries list */
    assertEquals(queries.getQueriesCount(), 0);

    /* Verification 2: Verifies that queries were removed from queries per user map by calling getQueries(user) method,
    which gets information from queries per user map */

    assertEquals(queries.getQueries(MOCK_USER).size(), 0);
  }

  @Test
  public void testGetQueriesMustReturnCopyOfUnderlyingCollection() {

    /* Initialization */
    final int noOfQueriesUsedInTest = 2;
    QueryCollection queries = createQueriesInstanceWithMockedQueries(noOfQueriesUsedInTest);

    /* Execution: Get queries and empty returned collection */
    Set<QueryContext> copiedSet = queries.getQueries();
    copiedSet.clear();

    /* System under set (queries) should still have the added queries, which can be verified by checking count */
    assertEquals(queries.getQueriesCount(), noOfQueriesUsedInTest);
  }

  @Test
  public void testGetQueriesPerUserMustReturnCopyOfUnderlyingCollection() {

    /* Initialization */
    final int noOfQueriesUsedInTest = 2;
    QueryCollection queries = createQueriesInstanceWithUserStubbing(noOfQueriesUsedInTest, MOCK_USER);

    /* Execution: Get queries for user and empty returned collection */
    Set<QueryContext> copiedSet = queries.getQueries(MOCK_USER);
    copiedSet.clear();

    /* System under set (queries) should still have the added queries, which can be verified by checking count */
    assertEquals(queries.getQueries(MOCK_USER).size(), noOfQueriesUsedInTest);
  }
}

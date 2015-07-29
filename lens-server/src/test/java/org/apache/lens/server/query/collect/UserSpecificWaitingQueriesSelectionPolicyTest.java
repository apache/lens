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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.Set;

import org.apache.lens.server.api.query.FinishedLensQuery;
import org.apache.lens.server.api.query.collect.EstimatedImmutableQueryCollection;
import org.apache.lens.server.api.query.collect.WaitingQueriesSelectionPolicy;

import org.testng.annotations.Test;

public class UserSpecificWaitingQueriesSelectionPolicyTest {

  @Test
  public void testSelectQueries() {

    FinishedLensQuery mockFinishedQuery = mock(FinishedLensQuery.class);
    EstimatedImmutableQueryCollection mockWaitingQueries = mock(EstimatedImmutableQueryCollection.class);
    String mockUser = "MockUser";
    Set expectedQueriesSet = mock(Set.class);

    when(mockFinishedQuery.getSubmitter()).thenReturn(mockUser);
    when(mockWaitingQueries.getQueries(mockUser)).thenReturn(expectedQueriesSet);

    WaitingQueriesSelectionPolicy selectionPolicy = new UserSpecificWaitingQueriesSelectionPolicy();
    Set actualEligibleQueries = selectionPolicy.selectQueries(mockFinishedQuery, mockWaitingQueries);

    assertEquals(actualEligibleQueries, expectedQueriesSet);
  }
}

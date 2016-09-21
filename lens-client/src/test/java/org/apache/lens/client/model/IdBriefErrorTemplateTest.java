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
package org.apache.lens.client.model;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class IdBriefErrorTemplateTest {

  @Test
  public void testToPrettyString() {

    /* Setup Dependencies */
    final IdBriefErrorTemplateKey testQueryIdKey = IdBriefErrorTemplateKey.QUERY_ID;
    final String testQueryIdValue = "TestQueryIdValue";
    final BriefError mockBriefError = mock(BriefError.class);

    /* Stubbing */
    final String testBriefErrorPrettyString = "TestLensBriefErrorPrettyString";
    when(mockBriefError.toPrettyString()).thenReturn(testBriefErrorPrettyString);

    /* Execution */
    IdBriefErrorTemplate template = new IdBriefErrorTemplate(testQueryIdKey, testQueryIdValue, mockBriefError);
    final String actualPrettyString = template.toPrettyString();

    /* Verfication */
    final String expectedPrettyString = "Query Id: TestQueryIdValue\nTestLensBriefErrorPrettyString";
    assertEquals(actualPrettyString, expectedPrettyString);
  }

  @DataProvider(name="dpInvalidStrings")
  public Object[][] dpInvalidStrings() {
    return new Object[][] {{null}, {""}, {"  "}};
  }

  @Test(expectedExceptions = NullPointerException.class, enabled = false)
  public void testIdBriefErrorTemplateMustNotAcceptNullIdKey() {

    final BriefError mockBriefError = mock(BriefError.class);
    new IdBriefErrorTemplate(null, "ValidIdValue", mockBriefError);
  }

  @Test(dataProvider = "dpInvalidStrings", expectedExceptions = IllegalArgumentException.class, enabled = false)
  public void testIdBriefErrorTemplateMustNotAcceptInvalidIdValue(final String invalidIdValue) {

    final BriefError mockBriefError = mock(BriefError.class);
    new IdBriefErrorTemplate(IdBriefErrorTemplateKey.QUERY_ID, invalidIdValue, mockBriefError);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testIdBriefErrorTemplateMustNotAcceptNullBriefError() {
    new IdBriefErrorTemplate(IdBriefErrorTemplateKey.QUERY_ID, "ValidIdValue", null);
  }
}

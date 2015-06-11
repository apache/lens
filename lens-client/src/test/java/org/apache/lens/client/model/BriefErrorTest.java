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

import static org.testng.Assert.assertEquals;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BriefErrorTest {

  @Test
  public void testToPrettyString() {

    final int testErrCode = 1001;
    final String testErrorMsg = "Test Error Msg, adfg-asdfk $ %, \n";
    BriefError briefError = new BriefError(testErrCode, testErrorMsg);

    final String actualPrettyString = briefError.toPrettyString();
    final String expectedPrettyString = "Error Code: 1001\nError Message: Test Error Msg, adfg-asdfk $ %, \n";

    assertEquals(actualPrettyString, expectedPrettyString);
  }

  @DataProvider(name="dpInvalidErrorCodes")
  public Object[][] dpInvalidErrorCodes() {
    return new Object[][] {{-1}, {0}};
  }

  @Test(dataProvider = "dpInvalidStrings", expectedExceptions = IllegalArgumentException.class)
  public void testBriefErrorMustNotAcceptInvalidErrorCodes(final int invalidErrCode) {
    new BriefError(invalidErrCode, "Valid Error Message");
  }

  @DataProvider(name="dpInvalidStrings")
  public Object[][] dpInvalidStrings() {
    return new Object[][] {{null}, {""}, {"  "}};
  }

  @Test(dataProvider = "dpInvalidStrings", expectedExceptions = IllegalArgumentException.class)
  public void testBriefErrorMustNotAcceptInvalidErrorMsg(final String invalidErrMsg) {
    new BriefError(1001, invalidErrMsg);
  }
}

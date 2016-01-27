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
/*
 *
 */
package org.apache.lens.server.rewrite;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CubeKeywordRemoverTest {
  CubeKeywordRemover cubeKeywordRemover = new CubeKeywordRemover();

  @DataProvider
  public Object[][] cubeQueryDataProvider() {
    return new Object[][]{
      {"cube select blah blah", "select blah blah"},
      {"cube\tselect blah blah", "select blah blah"},
      {"cube\nselect blah blah", "select blah blah"},
      {"CUBE sElEct blAh blAh", "select blAh blAh"},
    };
  }

  @Test(dataProvider = "cubeQueryDataProvider")
  public void testRewrite(String userQuery, String expected) throws Exception {
    assertEquals(cubeKeywordRemover.rewrite(userQuery, null, null), expected);
  }
}

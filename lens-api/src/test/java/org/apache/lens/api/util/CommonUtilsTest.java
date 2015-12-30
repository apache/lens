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
package org.apache.lens.api.util;

import static org.apache.lens.api.util.CommonUtils.parseMapFromString;

import static org.testng.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CommonUtilsTest {
  @DataProvider(name = "parse-data-provider")
  public Object[][] provideParseTestData() {
    return new Object[][]{
      {"", getMap()},
      {"a=b\n,\n\nc=d", getMap("a", "b", "c", "d")},
      {"a=b\\,b, c=d", getMap("a", "b,b", "c", "d")},
    };
  }

  private Object getMap(String... strings) {
    assertEquals(strings.length % 2, 0);
    Map<String, String> map = new HashMap<>();
    for (int i = 0; i < strings.length; i += 2) {
      map.put(strings[i], strings[i + 1]);
    }
    return map;
  }

  @Test(dataProvider = "parse-data-provider")
  public void testParseMapFromString(String str, Map<String, String> expected) throws Exception {
    assertEquals(parseMapFromString(str), expected);
  }
}

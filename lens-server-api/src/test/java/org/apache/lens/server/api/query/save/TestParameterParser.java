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
package org.apache.lens.server.api.query.save;

import java.util.Map;
import java.util.Set;

import org.apache.lens.server.api.query.save.param.ParameterParser;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class TestParameterParser {

  private static final ImmutableMap<String, Set<String>> QUERY_TEST_CASES;
  static {
    ImmutableMap.Builder<String, Set<String>> builder = ImmutableMap.builder();
    builder.put(
      "select col1 from table where col2 = :param",
      Sets.newHashSet("param")
    );
    builder.put(
      "select col1 from table where col2 in :param",
      Sets.newHashSet("param")
    );
    builder.put(
      "select col1 from table where col2 = 'a :param inside single quoted string literal'",
      Sets.<String>newHashSet()
    );
    builder.put(
      "select col1 from table where col2 = \"a :param inside double quoted string literal\"",
      Sets.<String>newHashSet()
    );
    builder.put(
      "select col1 from table where col1 = 'value' and col2 = :param and col3 = 'val3'",
      Sets.newHashSet("param")
    );
    builder.put(
      "select col1 from table where col1 = \"value\" and col2 = :param and col3 = \"val3\"",
      Sets.newHashSet("param")
    );


    QUERY_TEST_CASES = builder.build();
  }


  @Test
  public void testParsing() {
    for(Map.Entry<String, Set<String>> testCase : QUERY_TEST_CASES.entrySet()) {
      final String query = testCase.getKey();
      Assert.assertEquals(
        testCase.getValue(),
        Sets.newHashSet(new ParameterParser(query).extractParameterNames()),
        "Test case [[" + testCase.getKey() + "]] failed : "
      );
    }

  }
}

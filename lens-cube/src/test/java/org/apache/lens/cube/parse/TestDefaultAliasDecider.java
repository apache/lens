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

package org.apache.lens.cube.parse;

import static org.testng.Assert.assertEquals;

import org.apache.hadoop.hive.ql.parse.ASTNode;

import org.testng.annotations.Test;

public class TestDefaultAliasDecider {

  @Test
  public void testDefaultAlias() throws Exception {
    DefaultAliasDecider aliasDecider = new DefaultAliasDecider();
    ASTNode node = HQLParser.parseExpr("tbl1.col1");
    assertEquals(aliasDecider.decideAlias(node), "alias0");
    assertEquals(aliasDecider.decideAlias(node), "alias1");
    DefaultAliasDecider aliasDecider2 = new DefaultAliasDecider();
    assertEquals(aliasDecider2.decideAlias(node), "alias0");
    assertEquals(aliasDecider.decideAlias(node), "alias2");
  }

  @Test
  public void testAliasPrefix() throws Exception {
    DefaultAliasDecider aliasDecider = new DefaultAliasDecider("testAlias");
    ASTNode node = HQLParser.parseExpr("tbl1.col1");
    assertEquals(aliasDecider.decideAlias(node), "testAlias0");
    assertEquals(aliasDecider.decideAlias(node), "testAlias1");
    DefaultAliasDecider aliasDecider2 = new DefaultAliasDecider("testAlias");
    DefaultAliasDecider aliasDecider3 = new DefaultAliasDecider();
    assertEquals(aliasDecider2.decideAlias(node), "testAlias0");
    assertEquals(aliasDecider3.decideAlias(node), "alias0");
    assertEquals(aliasDecider.decideAlias(node), "testAlias2");
  }
}

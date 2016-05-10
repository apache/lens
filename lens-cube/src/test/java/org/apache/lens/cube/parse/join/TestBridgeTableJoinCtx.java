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

package org.apache.lens.cube.parse.join;

import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_INSERT;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_SELECT;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_WHERE;

import static org.testng.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.cube.parse.join.BridgeTableJoinContext.BridgeTableSelectCtx;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestBridgeTableJoinCtx {

  @DataProvider(name = "filterReplace")
  public Object[][] filterReplace() {
    return new Object[][] {
      {"t1.c1 in ('XyZ', 'abc', 'PQR', 'lKg')",
       "myfilter((t1.c1), 'XyZ') or myfilter((t1.c1), 'abc')"
         + " or myfilter((t1.c1), 'PQR') or myfilter((t1.c1), 'lKg')", },
      {"t1.c1 = ('XyZ')", "myfilter((t1.c1), 'XyZ')"},
      {"t1.c1 != ('XyZ')", "not myfilter((t1.c1), 'XyZ')"},
      {"t1.c1 != x", "not myfilter((t1.c1), x)"},
    };
  }

  @Test(dataProvider = "filterReplace")
  public void testReplaceDirectFiltersWithArrayFilter(String filter, String expected) throws LensException {
    ASTNode replaced = BridgeTableJoinContext.replaceDirectFiltersWithArrayFilter(HQLParser.parseExpr(filter),
      "myfilter");
    String replacedFilter = HQLParser.getString(replaced);
    assertEquals(replacedFilter, expected);
  }

  @Test
  public void testBridgeTableSelectCtx() throws LensException {
    String aggregator = "test_aggr";
    String arrayFilter = "test_filter";
    String bridgeTableAlias = "bt";

    // Initialization
    BridgeTableSelectCtx selectCtx = new BridgeTableSelectCtx(aggregator, arrayFilter, bridgeTableAlias);
    assertTrue(selectCtx.getSelectedBridgeExprs().isEmpty());
    assertEquals(selectCtx.getTableAlias(), bridgeTableAlias);
    assertEquals(selectCtx.getBridgeTableFieldAggr(), aggregator);
    assertEquals(selectCtx.getArrayFilter(), arrayFilter);
    assertNotNull(selectCtx.getAliasDecider());
    assertTrue(selectCtx.getExprToDotAST().isEmpty());

    String query = "select t1.c1, t2.c2, bt.c3, f1(t1.c2), f2(bt.c4), f2(bt.c3), bt.c1 + bt.c2 from t1 where t1.c1 = x"
      + " and bt.c3 = y and bt.c6 = 5 and t2.c2 = 4 and rand(bt.c7) = 6 group by t1.c1, bt.c3, bt.c8, f2(bt.c4)"
      + " order by t2.c2, bt.c3 asc, f2(bt.c3), bt.c9, bt.c4 desc";
    ASTNode queryAST = HQLParser.parseHQL(query, new HiveConf());
    ASTNode select = HQLParser.findNodeByPath(queryAST, TOK_INSERT, TOK_SELECT);
    ASTNode where = HQLParser.findNodeByPath(queryAST, TOK_INSERT, TOK_WHERE);
    ASTNode groupBy = HQLParser.findNodeByPath(queryAST, TOK_INSERT, HiveParser.TOK_GROUPBY);
    ASTNode orderBy = HQLParser.findNodeByPath(queryAST, TOK_INSERT, HiveParser.TOK_ORDERBY);

    List<String> expectedBridgeExprs = new ArrayList<>();
    expectedBridgeExprs.add(aggregator + "((bt.c3)) as balias0");
    expectedBridgeExprs.add(aggregator + "(f2((bt.c4))) as balias1");
    expectedBridgeExprs.add(aggregator + "(f2((bt.c3))) as balias2");
    expectedBridgeExprs.add(aggregator + "(((bt.c1) + (bt.c2))) as balias3");

    selectCtx.processSelectAST(select);
    String modifiedSelect = HQLParser.getString(select);
    assertEquals(modifiedSelect, "(t1.c1), (t2.c2), (bt.balias0), f1((t1.c2)), (bt.balias1),"
      + " (bt.balias2), (bt.balias3)");
    assertEquals(selectCtx.getSelectedBridgeExprs(), expectedBridgeExprs);

    selectCtx.processWhereAST(where, null, 0);
    String modifiedWhere = HQLParser.getString(where);
    assertEquals(modifiedWhere, "(((t1.c1) = x) and test_filter((bt.balias0), y) and test_filter((bt.balias4), 5)"
      + " and ((t2.c2) = 4) and test_filter((bt.balias5), 6))");
    expectedBridgeExprs.add(aggregator + "((bt.c6)) as balias4");
    expectedBridgeExprs.add(aggregator + "(rand((bt.c7))) as balias5");
    assertEquals(selectCtx.getSelectedBridgeExprs(), expectedBridgeExprs);

    selectCtx.processGroupbyAST(groupBy);
    String modifiedGroupby = HQLParser.getString(groupBy);
    assertEquals(modifiedGroupby, "(t1.c1), (bt.balias0), (bt.balias6), (bt.balias1)");
    expectedBridgeExprs.add(aggregator + "((bt.c8)) as balias6");
    assertEquals(selectCtx.getSelectedBridgeExprs(), expectedBridgeExprs);

    selectCtx.processOrderbyAST(orderBy);
    String modifiedOrderby = HQLParser.getString(orderBy);
    assertEquals(modifiedOrderby, "t2.c2 asc, bt.balias0 asc, bt.balias2 asc, bt.balias7 asc,"
      + " bt.balias8 desc");
    expectedBridgeExprs.add(aggregator + "((bt.c9)) as balias7");
    expectedBridgeExprs.add(aggregator + "((bt.c4)) as balias8");
    assertEquals(selectCtx.getSelectedBridgeExprs(), expectedBridgeExprs);
  }
}

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

import static org.apache.lens.cube.metadata.DateFactory.TWO_MONTHS_RANGE_UPTO_DAYS;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.*;
import static org.apache.lens.cube.parse.CubeTestSetup.*;
import static org.apache.lens.cube.parse.TestCubeRewriter.compareContains;

import static org.testng.Assert.*;

import org.apache.lens.server.api.LensServerAPITestUtil;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ParseException;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import jodd.util.StringUtil;

public class TestUnionAndJoinCandidates extends TestQueryRewrite {

  private Configuration conf;

  @BeforeTest
  public void setupDriver() throws Exception {
    conf = LensServerAPITestUtil.getConfigurationWithParams(getConf(),
        //Supported storage
        CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1",
        // Storage tables
        getValidStorageTablesKey("union_join_ctx_fact1"), "C1_union_join_ctx_fact1",
        getValidStorageTablesKey("union_join_ctx_fact2"), "C1_union_join_ctx_fact2",
        getValidStorageTablesKey("union_join_ctx_fact3"), "C1_union_join_ctx_fact3",
        getValidStorageTablesKey("union_join_ctx_fact4"), "C1_union_join_ctx_fact4",
        // Update periods
        getValidUpdatePeriodsKey("union_join_ctx_fact1", "C1"), "DAILY",
        getValidUpdatePeriodsKey("union_join_ctx_fact2", "C1"), "DAILY",
        getValidUpdatePeriodsKey("union_join_ctx_fact3", "C1"), "DAILY",
        getValidUpdatePeriodsKey("union_join_ctx_fact4", "C1"), "DAILY");
    conf.setBoolean(DISABLE_AUTO_JOINS, false);
    conf.setBoolean(ENABLE_SELECT_TO_GROUPBY, true);
    conf.setBoolean(ENABLE_GROUP_BY_TO_SELECT, true);
    conf.setBoolean(DISABLE_AGGREGATE_RESOLVER, false);
    conf.setBoolean(ENABLE_STORAGES_UNION, true);
  }

  @Override
  public Configuration getConf() {
    return new Configuration();
  }

  @Test
  public void testDuplicateProjectedFieldExclusion() throws ParseException, LensException {
    String colsSelected = " union_join_ctx_cityid , union_join_ctx_msr1_greater_than_100, "
        + " sum(union_join_ctx_msr1) ";
    String whereCond = " union_join_ctx_zipcode = 'a' and union_join_ctx_cityid = 'b' and "
        + "(" + TWO_MONTHS_RANGE_UPTO_DAYS + ")";
    String rewrittenQuery = rewrite("select " + colsSelected + " from basecube where " + whereCond, conf);
    assertTrue(rewrittenQuery.contains("UNION ALL"));
    assertEquals(StringUtil.count(rewrittenQuery, "sum((basecube.union_join_ctx_msr1))"), 2);
  }

  @Test
  public void testMultipleDimAttributeReferingSameJoinChain() throws ParseException, LensException {
    String colsSelected = " union_join_ctx_cityid, union_join_ctx_cityname, union_join_ctx_dup_cityname, "
        + " sum(union_join_ctx_msr1) ";
    String whereCond = " union_join_ctx_zipcode = 'a' and union_join_ctx_cityid = 'b' and "
        + "(" + TWO_MONTHS_RANGE_UPTO_DAYS + ")";
    String rewrittenQuery = rewrite("select " + colsSelected + " from basecube where " + whereCond, conf);
    assertTrue(rewrittenQuery.contains("UNION ALL"));
    // union_join_ctx_cityname and union_join_ctx_dup_cityname are refering to same join chain
    // in the final query they both will have the same alias repeated twice in each union query.
    assertEquals(StringUtil.count(rewrittenQuery, "basecube.alias1"), 4);
  }

  @Test
  public void testExpressionHavingRefcol() throws ParseException, LensException {
    String colsSelected = " union_join_ctx_cityid, union_join_ctx_cityname_msr1_expr, "
        + "union_join_ctx_cityname_msr2_expr ";
    String whereCond = "(" + TWO_MONTHS_RANGE_UPTO_DAYS + ")";
    String rewrittenQuery = rewrite("select " + colsSelected + " from basecube where " + whereCond, conf);
    assertTrue(rewrittenQuery.contains("UNION ALL"));
    String expectedInnerSelect1 = "SELECT (basecube.union_join_ctx_cityid) as `alias0`, sum(case  "
        + "when ((cubecityjoinunionctx.name) = 'blr') then (basecube.union_join_ctx_msr1) else 0 end) "
        + "as `alias1`, 0 as `alias2` FROM TestQueryRewrite.c1_union_join_ctx_fact1 basecube ";
    String expectedInnerSelect2 = "SELECT (basecube.union_join_ctx_cityid) as `alias0`, "
        + "sum(case  when ((cubecityjoinunionctx.name) = 'blr') then (basecube.union_join_ctx_msr1) else 0 end) "
        + "as `alias1`, 0 as `alias2` FROM TestQueryRewrite.c1_union_join_ctx_fact2 basecube";
    String expectedInnerSelect3 = "SELECT (basecube.union_join_ctx_cityid) as `alias0`, 0 as `alias1`, "
        + "sum(case  when ((cubecityjoinunionctx.name) = 'blr') then (basecube.union_join_ctx_msr2) else 0 end) "
        + "as `alias2` FROM TestQueryRewrite.c1_union_join_ctx_fact3 basecube";
    String outerSelect = "SELECT (basecube.alias0) as `union_join_ctx_cityid`, sum((basecube.alias1)) "
        + "as `union_join_ctx_cityname_msr1_expr`, sum((basecube.alias2)) as `union_join_ctx_cityname_msr2_expr` FROM";
    String outerGroupBy = "GROUP BY (basecube.alias0)";
    compareContains(expectedInnerSelect1, rewrittenQuery);
    compareContains(expectedInnerSelect2, rewrittenQuery);
    compareContains(expectedInnerSelect3, rewrittenQuery);
    compareContains(outerSelect, rewrittenQuery);
    compareContains(outerGroupBy, rewrittenQuery);
  }

  @Test
  public void testCustomExpressionForJoinCandidate() throws ParseException, LensException {
    // Expr : (case when union_join_ctx_msr2_expr = 0 then 0 else
    // union_join_ctx_msr4_expr * 100 / union_join_ctx_msr2_expr end) is completely answered by
    // c1_union_join_ctx_fact4 and partly answered by c1_union_join_ctx_fact3
    String colsSelected = " union_join_ctx_notnullcityid, union_join_ctx_msr22 , "
        + "case when union_join_ctx_msr2_expr = 0 then 0 else "
        + " union_join_ctx_msr4_expr * 100 / union_join_ctx_msr2_expr end";
    String whereCond =  "(" + TWO_MONTHS_RANGE_UPTO_DAYS + ")";
    String rewrittenQuery = rewrite("select " + colsSelected + " from basecube where " + whereCond, conf);
    assertTrue(rewrittenQuery.contains("UNION ALL"));
    String expectedInnerSelect1 = "SELECT case  when (basecube.union_join_ctx_cityid) is null then 0 "
        + "else (basecube.union_join_ctx_cityid) end as `alias0`, 0 as `alias1`, "
        + "sum((basecube.union_join_ctx_msr2)) as `alias2`, sum((basecube.union_join_ctx_msr4)) as `alias3` "
        + "FROM TestQueryRewrite.c1_union_join_ctx_fact4 basecube";
    String expectedInnerSelect2 = "SELECT case  when (basecube.union_join_ctx_cityid) is null then 0 "
        + "else (basecube.union_join_ctx_cityid) end as `alias0`, (basecube.union_join_ctx_msr22) as `alias1`, "
        + "0 as `alias2`, 0 as `alias3` FROM TestQueryRewrite.c1_union_join_ctx_fact3 basecube";
    String outerSelect = "SELECT (basecube.alias0) as `union_join_ctx_notnullcityid`, (basecube.alias1) "
        + "as `union_join_ctx_msr22`, case  when ((sum((basecube.alias2)) + 0) = 0) then 0 else "
        + "(((sum((basecube.alias3)) + 0) * 100) / (sum((basecube.alias2)) + 0)) end as "
        + "`case  when (union_join_ctx_msr2_expr = 0) then 0 "
        + "else ((union_join_ctx_msr4_expr * 100) / union_join_ctx_msr2_expr) end`";
    String outerGroupBy = "GROUP BY (basecube.alias0)";
    compareContains(expectedInnerSelect1, rewrittenQuery);
    compareContains(expectedInnerSelect2, rewrittenQuery);
    compareContains(outerSelect, rewrittenQuery);
    compareContains(outerGroupBy, rewrittenQuery);
  }

  @Test
  public void testDuplicateMeasureProjectionInJoinCandidate() throws ParseException, LensException {
    // union_join_ctx_msr2 is common between two storage candidates and it should be answered from one
    // and the other fact will have it replaced with 0
    String colsSelected = " union_join_ctx_notnullcityid, sum(union_join_ctx_msr22) , "
        + "sum(union_join_ctx_msr2), sum(union_join_ctx_msr4) ";
    String whereCond =  "(" + TWO_MONTHS_RANGE_UPTO_DAYS + ")";
    String rewrittenQuery = rewrite("select " + colsSelected + " from basecube where " + whereCond, conf);
    assertTrue(rewrittenQuery.contains("UNION ALL"));
    String expectedInnerSelect1 = "SELECT case  when (basecube.union_join_ctx_cityid) is null then 0 "
        + "else (basecube.union_join_ctx_cityid) end as `alias0`, 0 as `alias1`, "
        + "sum((basecube.union_join_ctx_msr2)) as `alias2`, sum((basecube.union_join_ctx_msr4)) "
        + "as `alias3` FROM TestQueryRewrite.c1_union_join_ctx_fact4 basecube";
    String expectedInnerSelect2 = "SELECT case  when (basecube.union_join_ctx_cityid) is null then 0 else "
        + "(basecube.union_join_ctx_cityid) end as `alias0`, sum((basecube.union_join_ctx_msr22)) as `alias1`, "
        + "0 as `alias2`, 0 as `alias3` FROM TestQueryRewrite.c1_union_join_ctx_fact3 basecube";
    String outerSelect = "SELECT (basecube.alias0) as `union_join_ctx_notnullcityid`, sum((basecube.alias1)) "
        + "as `sum(union_join_ctx_msr22)`, sum((basecube.alias2)) as `sum(union_join_ctx_msr2)`, "
        + "sum((basecube.alias3)) as `sum(union_join_ctx_msr4)` FROM";
    String outerGroupBy = "GROUP BY (basecube.alias0)";
    compareContains(expectedInnerSelect1, rewrittenQuery);
    compareContains(expectedInnerSelect2, rewrittenQuery);
    compareContains(outerSelect, rewrittenQuery);
    compareContains(outerGroupBy, rewrittenQuery);
  }

  @Test
  public void testFinalCandidateRewrittenQuery() throws ParseException, LensException {
    try {
      // Query with non projected measure in having clause.
      String colsSelected = "union_join_ctx_cityid, sum(union_join_ctx_msr2) ";
      String having = " having sum(union_join_ctx_msr1) > 100";
      String whereCond = " union_join_ctx_zipcode = 'a' and union_join_ctx_cityid = 'b' and "
          + "(" + TWO_MONTHS_RANGE_UPTO_DAYS + ")";
      String rewrittenQuery = rewrite("select " + colsSelected + " from basecube where " + whereCond + having, conf);
      String expectedInnerSelect1 = "SELECT (basecube.union_join_ctx_cityid) as `alias0`, 0 as `alias1`, "
          + "sum((basecube.union_join_ctx_msr1)) as `alias2` FROM TestQueryRewrite.c1_union_join_ctx_fact1 basecube ";
      String expectedInnerSelect2 = "SELECT (basecube.union_join_ctx_cityid) as `alias0`, 0 as `alias1`, "
          + "sum((basecube.union_join_ctx_msr1)) as `alias2` FROM TestQueryRewrite.c1_union_join_ctx_fact2 basecube ";
      String expectedInnerSelect3 = " SELECT (basecube.union_join_ctx_cityid) as `alias0`, "
          + "sum((basecube.union_join_ctx_msr2)) as `alias1`, 0 as `alias2` "
          + "FROM TestQueryRewrite.c1_union_join_ctx_fact3 basecube ";
      String outerHaving = "HAVING (sum((basecube.alias2)) > 100)";
      compareContains(expectedInnerSelect1, rewrittenQuery);
      compareContains(expectedInnerSelect2, rewrittenQuery);
      compareContains(expectedInnerSelect3, rewrittenQuery);
      compareContains(outerHaving, rewrittenQuery);

      // Query with measure and dim only expression
      colsSelected = " union_join_ctx_cityid , union_join_ctx_cityname , union_join_ctx_notnullcityid, "
          + "  sum(union_join_ctx_msr1), sum(union_join_ctx_msr2) ";
      whereCond = " union_join_ctx_zipcode = 'a' and union_join_ctx_cityid = 'b' and "
          + "(" + TWO_MONTHS_RANGE_UPTO_DAYS + ")";
      rewrittenQuery = rewrite("select " + colsSelected + " from basecube where " + whereCond, conf);
      String outerSelect = "SELECT (basecube.alias0) as `union_join_ctx_cityid`, "
          + "(basecube.alias1) as `union_join_ctx_cityname`, (basecube.alias2) as `union_join_ctx_notnullcityid`, "
          + "sum((basecube.alias3)) as `sum(union_join_ctx_msr1)`, "
          + "sum((basecube.alias4)) as `sum(union_join_ctx_msr2)` FROM ";
      expectedInnerSelect1 = "SELECT (basecube.union_join_ctx_cityid) as `alias0`, (cubecityjoinunionctx.name) "
          + "as `alias1`, case  when (basecube.union_join_ctx_cityid) is null then 0 else "
          + "(basecube.union_join_ctx_cityid) end as `alias2`, sum((basecube.union_join_ctx_msr1)) as `alias3`, "
          + "0 as `alias4` FROM TestQueryRewrite.c1_union_join_ctx_fact1 basecube";
      expectedInnerSelect2 = "SELECT (basecube.union_join_ctx_cityid) as `alias0`, (cubecityjoinunionctx.name) "
          + "as `alias1`, case  when (basecube.union_join_ctx_cityid) is null then 0 else "
          + "(basecube.union_join_ctx_cityid) end as `alias2`, sum((basecube.union_join_ctx_msr1)) as `alias3`, "
          + "0 as `alias4` FROM TestQueryRewrite.c1_union_join_ctx_fact2";
      expectedInnerSelect3 = "SELECT (basecube.union_join_ctx_cityid) as `alias0`, (cubecityjoinunionctx.name) "
          + "as `alias1`, case  when (basecube.union_join_ctx_cityid) is null then 0 else "
          + "(basecube.union_join_ctx_cityid) end as `alias2`, 0 as `alias3`, "
          + "sum((basecube.union_join_ctx_msr2)) as `alias4` FROM TestQueryRewrite.c1_union_join_ctx_fact3";
      String outerGroupBy = "GROUP BY (basecube.alias0), (basecube.alias1), (basecube.alias2)";
      compareContains(outerSelect, rewrittenQuery);
      compareContains(expectedInnerSelect1, rewrittenQuery);
      compareContains(expectedInnerSelect2, rewrittenQuery);
      compareContains(expectedInnerSelect3, rewrittenQuery);
      compareContains(outerGroupBy, rewrittenQuery);
      // Query with measure and measure expression eg. sum(case when....), case when sum(msr1)...
      // and measure with constant sum(msr1) + 10
      colsSelected = " union_join_ctx_cityid as `city id`, union_join_ctx_cityname, sum(union_join_ctx_msr1), "
          + "sum(union_join_ctx_msr2), union_join_ctx_non_zero_msr2_sum, union_join_ctx_msr1_greater_than_100, "
          + "sum(union_join_ctx_msr1) + 10 ";
      whereCond = " union_join_ctx_zipcode = 'a' and union_join_ctx_cityid = 'b' and "
          + "(" + TWO_MONTHS_RANGE_UPTO_DAYS + ")";
      rewrittenQuery = rewrite("select " + colsSelected + " from basecube where " + whereCond, conf);
      outerSelect = "SELECT (basecube.alias0) as `city id`, (basecube.alias1) as `union_join_ctx_cityname`, "
          + "sum((basecube.alias2)) as `sum(union_join_ctx_msr1)`, sum((basecube.alias3)) "
          + "as `sum(union_join_ctx_msr2)`, sum((basecube.alias4)) as `union_join_ctx_non_zero_msr2_sum`, "
          + "case  when (sum((basecube.alias2)) > 100) then \"high\" else \"low\" end as "
          + "`union_join_ctx_msr1_greater_than_100`, (sum((basecube.alias2)) + 10) "
          + "as `(sum(union_join_ctx_msr1) + 10)` FROM ";
      expectedInnerSelect1 = "SELECT (basecube.union_join_ctx_cityid) as `alias0`, "
          + "(cubecityjoinunionctx.name) as `alias1`, sum((basecube.union_join_ctx_msr1)) as `alias2`, "
          + "0 as `alias3`, 0 as `alias4` FROM";
      expectedInnerSelect2 = "SELECT (basecube.union_join_ctx_cityid) as `alias0`, "
          + "(cubecityjoinunionctx.name) as `alias1`, sum((basecube.union_join_ctx_msr1)) as `alias2`, "
          + "0 as `alias3`, 0 as `alias4` FROM";
      expectedInnerSelect3 = "SELECT (basecube.union_join_ctx_cityid) as `alias0`, (cubecityjoinunionctx.name) "
          + "as `alias1`, 0 as `alias2`, sum((basecube.union_join_ctx_msr2)) as `alias3`, "
          + "sum(case  when ((basecube.union_join_ctx_msr2) > 0) then (basecube.union_join_ctx_msr2) else 0 end) "
          + "as `alias4` FROM";
      String innerGroupBy = "GROUP BY (basecube.union_join_ctx_cityid), (cubecityjoinunionctx.name)";
      outerGroupBy = "GROUP BY (basecube.alias0), (basecube.alias1)";

      compareContains(outerSelect, rewrittenQuery);
      compareContains(expectedInnerSelect1, rewrittenQuery);
      compareContains(expectedInnerSelect2, rewrittenQuery);
      compareContains(expectedInnerSelect3, rewrittenQuery);
      compareContains(outerGroupBy, rewrittenQuery);
      compareContains(innerGroupBy, rewrittenQuery);

    } finally {
      getStorageToUpdatePeriodMap().clear();
    }
  }


}

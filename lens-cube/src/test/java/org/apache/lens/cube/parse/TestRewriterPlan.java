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

import static org.apache.lens.cube.parse.CubeTestSetup.TWO_DAYS_RANGE;

import java.util.Arrays;
import java.util.Collections;

import org.apache.lens.driver.cube.RewriterPlan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestRewriterPlan extends TestQueryRewrite {

  Configuration conf = new Configuration();

  TestRewriterPlan() {
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1,C2");
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY, true);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT, true);
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER, false);
  }

  @Test
  public void testPlanExtractionForSimpleQuery() throws Exception {
    // simple query
    CubeQueryContext ctx = rewriteCtx("cube select SUM(msr2) from testCube where " + TWO_DAYS_RANGE, conf);
    ctx.toHQL();
    RewriterPlan plan = new RewriterPlan(Collections.singleton(ctx));
    Assert.assertNotNull(plan);
    Assert.assertEquals(plan.getNumSels(), 1);
    Assert.assertEquals(plan.getNumGbys(), 0);
    Assert.assertEquals(plan.getNumJoins(), 0);
    Assert.assertEquals(plan.getNumHaving(), 0);
    Assert.assertEquals(plan.getNumFilters(), -1);
    Assert.assertEquals(plan.getNumOrderBys(), 0);
    Assert.assertEquals(plan.getNumAggreagateExprs(), -1);
    Assert.assertFalse(plan.getTablesQueried().isEmpty());
    Assert.assertTrue(plan.getTablesQueried().contains("TestQueryRewrite.c2_testfact"));
    Assert.assertEquals(plan.getTableWeights().get("TestQueryRewrite.c2_testfact"), 1.0);
    Assert.assertFalse(plan.getPartitions().isEmpty());
    Assert.assertFalse(plan.getPartitions().get("testfact").isEmpty());
    Assert.assertTrue(plan.getPartitions().get("testfact").size() > 1);
  }

  @Test
  public void testPlanExtractionForComplexQuery() throws Exception {
    // complex query
    CubeQueryContext ctx = rewriteCtx("cube select citydim.name, SUM(msr2) from testCube where citydim.name != \"XYZ\""
      + " and " + TWO_DAYS_RANGE + " having sum(msr2) > 1000 order by citydim.name limit 50", conf);
    ctx.toHQL();
    RewriterPlan plan = new RewriterPlan(Collections.singleton(ctx));
    Assert.assertNotNull(plan);
    Assert.assertEquals(plan.getNumSels(), 2);
    Assert.assertEquals(plan.getNumGbys(), 1);
    Assert.assertEquals(plan.getNumJoins(), 1);
    Assert.assertEquals(plan.getNumHaving(), 1);
    Assert.assertEquals(plan.getNumOrderBys(), 1);
    Assert.assertEquals(plan.getNumFilters(), -1);
    Assert.assertEquals(plan.getNumAggreagateExprs(), -1);
    Assert.assertFalse(plan.getTablesQueried().isEmpty());
    Assert.assertTrue(plan.getTablesQueried().contains("TestQueryRewrite.c2_testfact"));
    Assert.assertTrue(plan.getTablesQueried().contains("TestQueryRewrite.c1_citytable"));
    Assert.assertEquals(plan.getTableWeights().get("TestQueryRewrite.c2_testfact"), 1.0);
    Assert.assertEquals(plan.getTableWeights().get("TestQueryRewrite.c1_citytable"), 100.0);
    Assert.assertFalse(plan.getPartitions().isEmpty());
    Assert.assertFalse(plan.getPartitions().get("testfact").isEmpty());
    Assert.assertTrue(plan.getPartitions().get("testfact").size() > 1);
    Assert.assertFalse(plan.getPartitions().get("citytable").isEmpty());
    Assert.assertEquals(plan.getPartitions().get("citytable").size(), 1);
  }

  @Test
  public void testPlanExtractionForMultipleQueries() throws Exception {
    // simple query
    CubeQueryContext ctx1 = rewriteCtx("cube select SUM(msr2) from testCube where " + TWO_DAYS_RANGE, conf);
    ctx1.toHQL();
    CubeQueryContext ctx2 = rewriteCtx("cube select citydim.name, SUM(msr2) from testCube where citydim.name != \"XYZ\""
      + " and " + TWO_DAYS_RANGE + " having sum(msr2) > 1000 order by citydim.name limit 50", conf);
    ctx2.toHQL();
    RewriterPlan plan = new RewriterPlan(Arrays.asList(ctx1, ctx2));
    Assert.assertNotNull(plan);
    Assert.assertEquals(plan.getNumSels(), 3);
    Assert.assertEquals(plan.getNumGbys(), 1);
    Assert.assertEquals(plan.getNumJoins(), 1);
    Assert.assertEquals(plan.getNumHaving(), 1);
    Assert.assertEquals(plan.getNumOrderBys(), 1);
    Assert.assertEquals(plan.getNumFilters(), -1);
    Assert.assertEquals(plan.getNumAggreagateExprs(), -1);
    Assert.assertFalse(plan.getTablesQueried().isEmpty());
    Assert.assertTrue(plan.getTablesQueried().contains("TestQueryRewrite.c2_testfact"));
    Assert.assertTrue(plan.getTablesQueried().contains("TestQueryRewrite.c1_citytable"));
    Assert.assertEquals(plan.getTableWeights().get("TestQueryRewrite.c2_testfact"), 1.0);
    Assert.assertEquals(plan.getTableWeights().get("TestQueryRewrite.c1_citytable"), 100.0);
    Assert.assertFalse(plan.getPartitions().isEmpty());
    Assert.assertFalse(plan.getPartitions().get("testfact").isEmpty());
    Assert.assertTrue(plan.getPartitions().get("testfact").size() > 1);
    Assert.assertFalse(plan.getPartitions().get("citytable").isEmpty());
    Assert.assertEquals(plan.getPartitions().get("citytable").size(), 1);
  }

  @Test
  public void testUnimplemented() throws SemanticException, ParseException {
    CubeQueryContext ctx = rewriteCtx("cube select SUM(msr2) from testCube where " + TWO_DAYS_RANGE, conf);
    ctx.toHQL();
    RewriterPlan plan = new RewriterPlan(Collections.singleton(ctx));
    Assert.assertNotNull(plan);
    try {
      plan.getPlan();
      Assert.fail("getPlan is not implemented");
    } catch (UnsupportedOperationException e) {
      Assert.assertEquals(e.getMessage(), "Not implemented");
    }

    try {
      plan.getCost();
      Assert.fail("getCost is not implemented");
    } catch (UnsupportedOperationException e) {
      Assert.assertEquals(e.getMessage(), "Not implemented");
    }

  }
}

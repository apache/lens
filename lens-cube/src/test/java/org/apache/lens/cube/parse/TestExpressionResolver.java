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

import static org.apache.lens.cube.metadata.DateFactory.*;
import static org.apache.lens.cube.parse.CubeTestSetup.*;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ParseException;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestExpressionResolver extends TestQueryRewrite {

  private Configuration conf;
  private final String cubeName = CubeTestSetup.TEST_CUBE_NAME;

  @BeforeTest
  public void setupDriver() throws Exception {
    conf = new Configuration();
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY, true);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT, true);
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER, false);
  }

  @Test
  public void testColumnErrors() throws Exception {
    LensException th;
    th = getLensExceptionInRewrite("select nocolexpr, SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    Assert.assertEquals(th.getErrorCode(), LensCubeErrorCode.COLUMN_NOT_FOUND.getLensErrorInfo().getErrorCode());

    Assert.assertTrue(getLensExceptionErrorMessageInRewrite(
      "select nocolexpr, SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf).contains("nonexist"));

    Assert.assertTrue(getLensExceptionErrorMessageInRewrite(
      "select invalidexpr, SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf).contains("invalidexpr"));

    th = getLensExceptionInRewrite("select invalidexpr, " + "SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE,
        conf);
    Assert.assertEquals(th.getErrorCode(), LensCubeErrorCode.COLUMN_NOT_FOUND.getLensErrorInfo().getErrorCode());
  }

  @Test
  public void testCubeQueryExpressionSelection() throws Exception {
    // select with expression
    String hqlQuery = rewrite("select avgmsr from testCube where " + TWO_DAYS_RANGE, conf);
    String expected =
      getExpectedQuery(cubeName, "select avg(testCube.msr1 + testCube.msr2) FROM ", null, null,
        getWhereForHourly2days("C1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testCubeQueryExpressionSelectionAlongWithColumn() throws Exception {
    String hqlQuery = rewrite("select dim1, roundedmsr2 from testCube" + " where " + TWO_DAYS_RANGE, conf);
    String expected =
      getExpectedQuery(cubeName, "select testcube.dim1, round(sum(testcube.msr2)/1000) FROM ", null,
        " group by testcube.dim1", getWhereForDailyAndHourly2days(cubeName, "c1_summary1"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

  }
  @Test
  public void testExpressionInWhereAfterTimerange() throws Exception {
    // where with expression
    String hqlQuery = rewrite("select msr2 from testCube" + " where " + TWO_DAYS_RANGE + " and substrexpr != 'XYZ'",
      conf);
    String expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, " and substr(testCube.dim1, 3) != 'XYZ'",
        getWhereForDailyAndHourly2days(cubeName, "c1_summary1"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }
  @Test
  public void testExpressionInWhereBeforeTimerange() throws Exception {
    String hqlQuery = rewrite("select SUM(msr2) from testCube" + " where substrexpr != 'XYZ' and " + TWO_DAYS_RANGE,
      conf);
    String expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", "substr(testCube.dim1, 3) != 'XYZ'", null,
        getWhereForDailyAndHourly2days(cubeName, "c1_summary1"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }
  @Test
  public void testExpressionInSelectAndWhere() throws Exception {
    // expression in select and where
    String hqlQuery = rewrite("select avgmsr from testCube" + " where " + TWO_DAYS_RANGE + " and substrexpr != 'XYZ'",
      conf);
    String expected =
      getExpectedQuery(cubeName, "select avg(testCube.msr1 + testCube.msr2) FROM ", null,
        " and substr(testCube.dim1, 3) != 'XYZ'", getWhereForHourly2days("C1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }
  @Test
  public void testNestedExpressionInWhere() throws Exception {
    String hqlQuery = rewrite("select avgmsr from testCube" + " where " + TWO_DAYS_RANGE + " and indiasubstr = true",
      conf);
    String expected =
      getExpectedQuery(cubeName, "select avg(testCube.msr1 + testCube.msr2) FROM ", null,
        " and (substr(testCube.dim1, 3) = 'INDIA') = true", getWhereForHourly2days("C1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

  }
  @Test
  public void testExpressionWithTableAlias() throws Exception {
    // expression with alias
    String hqlQuery =
      rewrite("select TC.avgmsr from testCube TC" + " where " + TWO_DAYS_RANGE + " and TC.substrexpr != 'XYZ'", conf);
    String expected =
      getExpectedQuery("tc", "select avg(tc.msr1 + tc.msr2) FROM ", null, " and substr(tc.dim1, 3) != 'XYZ'",
        getWhereForHourly2days("tc", "C1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

  }
  @Test
  public void testCubeExpressionWithColumnAlias() throws Exception {
    // expression with column alias
    String hqlQuery =
      rewrite("select TC.substrexpr as subdim1, TC.avgmsr from testCube TC" + " where " + TWO_DAYS_RANGE
        + " and subdim1 != 'XYZ'", conf);
    String expected =
      getExpectedQuery("tc", "select substr(tc.dim1, 3) as `subdim1`, avg(tc.msr1 + tc.msr2) FROM ", null,
        " and subdim1 != 'XYZ' group by substr(tc.dim1, 3)", getWhereForHourly2days("tc", "C1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testCubeQueryExpressionWithAliasAsColumnName() throws Exception {
    String hqlQuery = rewrite("select dim1 as d1, roundedmsr2 as msr2 from testCube" + " where " + TWO_DAYS_RANGE,
      conf);
    String expected =
      getExpectedQuery(cubeName, "select testcube.dim1 as `d1`, round(sum(testcube.msr2)/1000) as `msr2` FROM ", null,
        " group by testcube.dim1", getWhereForDailyAndHourly2days(cubeName, "c1_summary1"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testExpressionInGroupbyToSelect() throws Exception {
    // expression with groupby
    String hqlQuery =
      rewrite("select avgmsr from testCube" + " where " + TWO_DAYS_RANGE
        + " and substrexpr != 'XYZ' group by booleancut", conf);
    String expected =
      getExpectedQuery(cubeName, "select testCube.dim1 != 'x' AND testCube.dim2 != 10 ,"
        + " avg(testCube.msr1 + testCube.msr2) FROM ", null, " and substr(testCube.dim1, 3) != 'XYZ'"
          + " group by testCube.dim1 != 'x' AND testCube.dim2 != 10", getWhereForHourly2days("C1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testExpressionInSelectToGroupby() throws Exception {
    String hqlQuery =
      rewrite("select booleancut, avgmsr from testCube" + " where " + TWO_DAYS_RANGE + " and substrexpr != 'XYZ'",
        conf);
    String expected =
      getExpectedQuery(cubeName, "select testCube.dim1 != 'x' AND testCube.dim2 != 10 ,"
        + " avg(testCube.msr1 + testCube.msr2) FROM ", null, " and substr(testCube.dim1, 3) != 'XYZ' "
          + "group by testCube.dim1 != 'x' AND testCube.dim2 != 10", getWhereForHourly2days("C1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

  }

  @Test
  public void testExpressionInSelectToGroupbyWithComplexExpression() throws Exception {
    String hqlQuery =
      rewrite("select booleancut, summsrs from testCube" + " where " + TWO_DAYS_RANGE + " and substrexpr != 'XYZ'",
        conf);
    String expected =
      getExpectedQuery(cubeName, "select testCube.dim1 != 'x' AND testCube.dim2 != 10 ,"
        + " ((1000 + sum(testCube.msr1) + sum(testCube.msr2))/100) FROM ", null,
        " and substr(testCube.dim1, 3) != 'XYZ' group by testCube.dim1 != 'x' AND testCube.dim2 != 10",
        getWhereForHourly2days("C1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testExpressionToJoin() throws Exception {
    // expression which results in join
    String hqlQuery =
      rewrite("select cityAndState, avgmsr from testCube" + " where " + TWO_DAYS_RANGE + " and substrexpr != 'XYZ'",
        conf);

    String join1 =
      " join " + getDbName() + "c1_citytable cubecity"
        + " on testcube.cityid = cubecity.id and (cubecity.dt = 'latest') ";
    String join2 = " join" + getDbName()
      + "c1_statetable cubestate on" + " testcube.stateid = cubestate.id and (cubestate.dt = 'latest')";

    String expected =
      getExpectedQuery(cubeName, "select concat(cubecity.name, \":\", cubestate.name),"
        + " avg(testcube.msr1 + testcube.msr2) FROM ", join2 + join1, null, " and substr(testcube.dim1, 3) != 'XYZ'"
          + " group by concat(cubecity.name, \":\", cubestate.name)", null, getWhereForHourly2days("C1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testExpressionToExcludeJoin() throws Exception {
    // expression which results in join
    String hqlQuery =
      rewrite("select cityAndStateNew, avgmsr from testCube" + " where " + TWO_DAYS_RANGE + " and substrexpr != 'XYZ'",
        conf);

    String expected =
      getExpectedQuery(cubeName, "select substr(testcube.concatedcitystate, 10)"
        + " avg(testcube.msr1 + testcube.msr2) FROM ", null, null, " and substr(testcube.dim1, 3) != 'XYZ'"
        + " group by substr(testcube.concatedcitystate, 10)", null, getWhereForHourly2days("C1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testExpressionInWhereWithJoinClausePassed() throws Exception {
    assertLensExceptionInRewrite("select cityAndState, avgmsr from testCube tc join citydim cd join statedim sd where "
      + TWO_DAYS_RANGE + " and substrexpr != 'XYZ'", conf, LensCubeErrorCode.NO_JOIN_CONDITION_AVAILABLE);
  }

  @Test
  public void testExpressionInJoinClause() throws Exception {
    // expression in join clause
    assertLensExceptionInRewrite("select cityAndState, avgmsr from testCube join citydim on substrexpr != 'XYZ' where "
      + TWO_DAYS_RANGE, conf, LensCubeErrorCode.NO_CANDIDATE_FACT_AVAILABLE);
  }

  @Test
  public void testExpressionInHaving() throws Exception {
    // expression with having clause
    String hqlQuery =
      rewrite("select booleancut, avgmsr from testCube" + " where " + TWO_DAYS_RANGE + " and substrexpr != 'XYZ'"
        + " having msr6 > 100.0", conf);
    String expected =
      getExpectedQuery(cubeName, "select testCube.dim1 != 'x' AND testCube.dim2 != 10 ,"
        + " avg(testCube.msr1 + testCube.msr2) FROM ", null, " and substr(testCube.dim1, 3) != 'XYZ' "
          + " group by testCube.dim1 != 'x' AND testCube.dim2 != 10"
          + " having (sum(testCube.msr2) + max(testCube.msr3))/ count(testcube.msr4) > 100.0",
          getWhereForHourly2days("C1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testExpressionInOrderby() throws Exception {
    // expression with orderby clause
    String hqlQuery =
      rewrite("select avgmsr from testCube " + " where " + TWO_DAYS_RANGE + " and substrexpr != 'XYZ'"
        + " group by booleancut having msr6 > 100.0 order by booleancut", conf);
    String expected =
      getExpectedQuery(cubeName, "select testCube.dim1 != 'x' AND testCube.dim2 != 10 ,"
        + " avg(testCube.msr1 + testCube.msr2) FROM ", null, " and substr(testCube.dim1, 3) != 'XYZ' "
          + " group by testCube.dim1 != 'x' AND testCube.dim2 != 10"
          + " having (sum(testCube.msr2) + max(testCube.msr3))/ count(testcube.msr4) > 100.0"
          + " order by testCube.dim1 != 'x' AND testCube.dim2 != 10 asc", getWhereForHourly2days("C1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }
  @Test
  public void testExpressionWithAliasInOrderby() throws Exception {
    String hqlQuery =
      rewrite("select booleancut bc, msr2 from testCube" + " where " + TWO_DAYS_RANGE + " and substrexpr != 'XYZ'"
        + " having msr6 > 100.0 order by bc", conf);
    String expected =
      getExpectedQuery(cubeName, "select testCube.dim1 != 'x' AND testCube.dim2 != 10 as `bc`,"
        + " sum(testCube.msr2) FROM ", null, " and substr(testCube.dim1, 3) != 'XYZ' "
          + " group by testCube.dim1 != 'x' AND testCube.dim2 != 10"
          + " having (sum(testCube.msr2) + max(testCube.msr3))/ count(testcube.msr4) > 100.0" + " order by bc asc",
          getWhereForDailyAndHourly2days(cubeName, "c1_summary2"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testMultipleExpressionsPickingFirstExpression() throws Exception {
    Configuration newConf = new Configuration(conf);
    newConf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    newConf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact");
    String hqlQuery = rewrite("select equalsums from testCube where " + TWO_DAYS_RANGE, newConf);
    String expected =
      getExpectedQuery(cubeName, "select max(testcube.msr3) + count(testcube.msr4) FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testMultipleExpressionsPickingSecondExpression() throws Exception {
    String hqlQuery = rewrite("select equalsums from testCube where " + TWO_DAYS_RANGE, conf);
    String expected = getExpectedQuery(cubeName, "select (max(testCube.msr3) + sum(testCube.msr2))/100 FROM ", null,
      null, getWhereForHourly2days(cubeName, "C1_testfact2"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testMaterializedExpressionPickingExpression() throws Exception {
    // select with expression
    String hqlQuery = rewrite("select msr5 from testCube where " + TWO_DAYS_RANGE, conf);
    String expected = getExpectedQuery(cubeName, "select sum(testCube.msr2) + max(testCube.msr3) FROM ", null, null,
      getWhereForHourly2days(cubeName, "C1_testfact2"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testExpressionFieldWithOtherFields() throws Exception {
    // select with expression which requires dimension tables.
    // And there is a candidate, which is removed because
    // required the dimension tables in the expression are not reachable and
    // the expression is not evaluable on the candidate.
    LensException th =
      getLensExceptionInRewrite("select cityStateName, msr2expr, msr5, msr15 from testCube where "
        + TWO_DAYS_RANGE, conf);
    Assert.assertEquals(th.getErrorCode(),
      LensCubeErrorCode.NO_CANDIDATE_FACT_AVAILABLE.getLensErrorInfo().getErrorCode());
  }
  @Test
  public void testMaterializedExpressionPickingMaterializedValue() throws Exception {
    Configuration newConf = new Configuration(conf);
    newConf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    newConf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact");
    String hqlQuery = rewrite("select msr5 from testCube where " + TWO_DAYS_RANGE, newConf);
    String expected = getExpectedQuery(cubeName, "select testcube.msr5 FROM ", null, null,
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testExprDimAttribute() throws Exception {
    // select with expression
    String hqlQuery = rewrite("select substrexpr from testCube where " + TWO_DAYS_RANGE, conf);
    String expected = getExpectedQuery(cubeName, "select distinct substr(testCube.dim1, 3) FROM ", null, null,
      getWhereForDailyAndHourly2days(cubeName, "c1_summary1"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testDerivedCube() throws ParseException, LensException, HiveException {
    LensException th =
      getLensExceptionInRewrite("select avgmsr from derivedCube" + " where " + TWO_DAYS_RANGE, conf);
    Assert.assertEquals(th.getErrorCode(), LensCubeErrorCode.COLUMN_NOT_FOUND.getLensErrorInfo().getErrorCode());
  }

  @Test
  public void testDimensionQueryWithExpression() throws Exception {
    String hqlQuery = rewrite("select citydim.name, cityaddress from" + " citydim", conf);

    String joinExpr;
    String join1 =
      " join " + getDbName() + "c1_ziptable cityzip on" + " citydim.zipcode = cityzip.code and (cityzip.dt = 'latest')";
    String join2 = " join " + getDbName() + "c1_statetable citystate on"
      + " citydim.stateid = citystate.id and (citystate.dt = 'latest')";
    String join3 = " join " + getDbName()
      + "c1_countrytable citycountry on" + " citystate.countryid = citycountry.id";
    joinExpr = join2 + join3 + join1;
    String expected =
      getExpectedQuery("citydim", "SELECT citydim.name, concat((citydim.name), \":\", (citystate.name ),"
        + " \":\",(citycountry.name),  \":\" , ( cityzip . code )) FROM ", joinExpr, null, null, "c1_citytable", true);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testExpressionPruningForInvalidDim() throws Exception {
    Configuration newConf = new Configuration(conf);
    newConf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");

    // cityaddress =
    //new ExprSpec("concat(citydim.name, \":\", statedim.name, \":\", countrydim.name, \":\", zipdim.code)", null,
    //  null), new ExprSpec("concat(citydim.name, \":\", statedim.name)", null, null)));
    // since zipdim is not available in storage C2, first expression should be have been pruned
    // And joining with statedim for second expression is not possible because of stateid missing in C2 tables
    // or citydim.name missing in c2 tables.
    CubeQueryContext ctx = rewriteCtx("select citydim.name, cityaddress from citydim", newConf);
    Assert.assertEquals(ctx.getDimPruningMsgs().get(ctx.getMetastoreClient().getDimension("citydim"))
      .get(ctx.getMetastoreClient().getDimensionTable("citytable")).size(), 1);
    CandidateTablePruneCause pruningMsg =
      ctx.getDimPruningMsgs().get(ctx.getMetastoreClient().getDimension("citydim"))
      .get(ctx.getMetastoreClient().getDimensionTable("citytable")).get(0);
    Assert.assertEquals(pruningMsg.getCause(), CandidateTablePruneCode.EXPRESSION_NOT_EVALUABLE);
    Assert.assertTrue(pruningMsg.getMissingExpressions().contains("cityaddress"));
  }

  @Test
  public void testDimensionQueryWithExpressionWithColumnAlias() throws Exception {
    String hqlQuery = rewrite("select citydim.name cname, cityaddress caddr from" + " citydim", conf);

    String joinExpr;
    String join1 =
      " join " + getDbName() + "c1_ziptable cityzip on" + " citydim.zipcode = cityzip.code and (cityzip.dt = 'latest')";
    String join2 = " join " + getDbName() + "c1_statetable citystate on"
      + " citydim.stateid = citystate.id and (citystate.dt = 'latest')";
    String join3 = " join " + getDbName()
      + "c1_countrytable citycountry on" + " citystate.countryid = citycountry.id";
    joinExpr = join2 + join3 + join1;
    String expected =
      getExpectedQuery("citydim", "SELECT citydim.name as `cname`, concat((citydim.name), \":\", (citystate.name ),"
        + " \":\",(citycountry.name),  \":\" , ( cityzip . code )) as `caddr` FROM ", joinExpr, null, null,
        "c1_citytable", true);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testDimensionQueryWithTableAlias() throws Exception {
    String hqlQuery = rewrite("select ct.name, ct.cityaddress from" + " citydim ct", conf);

    String joinExpr =
      ""
        + " join " + getDbName() + "c1_statetable citystate on ct.stateid = citystate.id and (citystate.dt = 'latest')"
        + " join " + getDbName() + "c1_countrytable citycountry on citystate.countryid = citycountry.id"
        + " join " + getDbName() + "c1_ziptable cityzip on ct.zipcode = cityzip.code and (cityzip.dt = 'latest')"
        + "";

    String expected =
      getExpectedQuery("ct", "SELECT ct.name, concat((ct.name), \":\", (citystate.name ),"
        + " \":\",(citycountry.name),  \":\" , ( cityzip . code )) FROM ", joinExpr, null, null, "c1_citytable", true);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testDimensionQueryExpressionInSelectToGroupby() throws Exception {
    String hqlQuery = rewrite("select id, AggrExpr from citydim", conf);
    String expected = getExpectedQuery("citydim", "select citydim.id, count(citydim.name) FROM ", null, null,
      " group by citydim.id", "c1_citytable", true);
    TestCubeRewriter.compareQueries(hqlQuery, expected);

  }

  @Test
  public void testDimensionQueryWithTableAliasColumnAlias() throws Exception {
    String hqlQuery = rewrite("select ct.name cname, ct.cityaddress caddr from" + " citydim ct", conf);

    String joinExpr =
      ""
        + " join " + getDbName() + "c1_statetable citystate on ct.stateid = citystate.id and (citystate.dt = 'latest')"
        + " join " + getDbName() + "c1_countrytable citycountry on citystate.countryid = citycountry.id"
        + " join " + getDbName() + "c1_ziptable cityzip on ct.zipcode = cityzip.code and (cityzip.dt = 'latest')";

    String expected =
      getExpectedQuery("ct", "SELECT ct.name as `cname`, concat((ct.name), \":\", (citystate.name ),"
        + " \":\",(citycountry.name),  \":\" , ( cityzip . code )) as `caddr` FROM ", joinExpr, null, null,
        "c1_citytable", true);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testSingleColExpression() throws Exception {
    Configuration tconf = new Configuration(conf);
    tconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    CubeQueryContext rewrittenQuery =
      rewriteCtx("select singlecolmsr2expr from testCube where " + TWO_DAYS_RANGE, tconf);
    String expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    TestCubeRewriter.compareQueries(rewrittenQuery.toHQL(), expected);
  }

  @Test
  public void testSingleDimColExpression() throws Exception {
    Configuration tconf = new Configuration(conf);
    tconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    CubeQueryContext rewrittenQuery =
      rewriteCtx("select singlecoldim1expr from testCube where " + TWO_DAYS_RANGE, tconf);
    String expected =
      getExpectedQuery(cubeName, "select distinct testcube.dim1 FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "c1_summary1"));
    TestCubeRewriter.compareQueries(rewrittenQuery.toHQL(), expected);
  }

  @Test
  public void testSingleDimColExpressionWithAlias() throws Exception {
    Configuration tconf = new Configuration(conf);
    tconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    CubeQueryContext rewrittenQuery =
      rewriteCtx("select singlecoldim1expr as x from testCube where " + TWO_DAYS_RANGE, tconf);
    String expected =
      getExpectedQuery(cubeName, "select distinct testcube.dim1 as x FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "C1_summary1"));
    TestCubeRewriter.compareQueries(rewrittenQuery.toHQL(), expected);
  }

  @Test
  public void testSingleDimColQualifiedExpression() throws Exception {
    Configuration tconf = new Configuration(conf);
    tconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    CubeQueryContext rewrittenQuery =
      rewriteCtx("select singlecoldim1qualifiedexpr from testCube where " + TWO_DAYS_RANGE, tconf);
    String expected =
      getExpectedQuery(cubeName, "select distinct testcube.dim1 FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "C1_summary1"));
    TestCubeRewriter.compareQueries(rewrittenQuery.toHQL(), expected);
  }

  @Test
  public void testSingleChainIdExpression() throws Exception {
    Configuration tconf = new Configuration(conf);
    tconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    CubeQueryContext rewrittenQuery =
      rewriteCtx("select singlecolchainid from testCube where " + TWO_DAYS_RANGE_IT, tconf);
    String expected =
      getExpectedQuery(cubeName, "select distinct dim3chain.id FROM ",
        " join " + getDbName() + "c2_testdim3tbl dim3chain on testcube.testdim3id = dim3chain.id",
        null, null, null,
        getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary1"));
    TestCubeRewriter.compareQueries(rewrittenQuery.toHQL(), expected);
  }

  @Test
  public void testSingleChainRefIdExpression() throws Exception {
    Configuration tconf = new Configuration(conf);
    tconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    CubeQueryContext rewrittenQuery =
      rewriteCtx("select singlecolchainrefexpr from testCube where " + TWO_DAYS_RANGE_IT, tconf);
    String expected =
      getExpectedQuery(cubeName, "select distinct testcube.testdim3id FROM ", null, null,
        getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary1"));
    TestCubeRewriter.compareQueries(rewrittenQuery.toHQL(), expected);
  }

  @Test
  public void testSingleChainRefColExpression() throws Exception {
    Configuration tconf = new Configuration(conf);
    tconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    CubeQueryContext rewrittenQuery =
      rewriteCtx("select singlecolchainfield from testCube where " + TWO_DAYS_RANGE, tconf);
    String expected =
      getExpectedQuery(cubeName, "select distinct cubecity.name FROM ",
        " join " + getDbName() + "c2_citytable cubecity ON testcube.cityid = cubecity.id",
        null, null, null, getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    TestCubeRewriter.compareQueries(rewrittenQuery.toHQL(), expected);
  }

  @Test
  public void testSingleChainRefColExpressionWithAlias() throws Exception {
    Configuration tconf = new Configuration(conf);
    tconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    CubeQueryContext rewrittenQuery =
      rewriteCtx("select singlecolchainfield as cityname from testCube where " + TWO_DAYS_RANGE, tconf);
    String expected =
      getExpectedQuery(cubeName, "select distinct cubecity.name as cityname FROM ",
        " join " + getDbName() + "c2_citytable cubecity ON testcube.cityid = cubecity.id",
        null, null, null, getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    TestCubeRewriter.compareQueries(rewrittenQuery.toHQL(), expected);
  }

  @Test
  public void testSingleColExpressionWithAlias() throws Exception {
    Configuration tconf = new Configuration(conf);
    tconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    CubeQueryContext rewrittenQuery =
      rewriteCtx("select singlecolmsr2expr as msr2 from testCube where " + TWO_DAYS_RANGE, tconf);
    String expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) msr2 FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    TestCubeRewriter.compareQueries(rewrittenQuery.toHQL(), expected);
  }

  @Test
  public void testSingleColQualifiedExpression() throws Exception {
    Configuration tconf = new Configuration(conf);
    tconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    CubeQueryContext rewrittenQuery =
      rewriteCtx("select singlecolmsr2qualifiedexpr from testCube where " + TWO_DAYS_RANGE, tconf);
    String expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    TestCubeRewriter.compareQueries(rewrittenQuery.toHQL(), expected);
  }

  @Test
  public void testSingleColQualifiedExpressionWithAlias() throws Exception {
    Configuration tconf = new Configuration(conf);
    tconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    CubeQueryContext rewrittenQuery =
      rewriteCtx("select singlecolmsr2qualifiedexpr from testCube tc where " + TWO_DAYS_RANGE, tconf);
    String expected =
      getExpectedQuery("tc", "select sum(tc.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days("tc", "C2_testfact"));
    TestCubeRewriter.compareQueries(rewrittenQuery.toHQL(), expected);
  }

}

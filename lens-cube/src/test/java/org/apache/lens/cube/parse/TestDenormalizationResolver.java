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
import static org.apache.lens.cube.parse.CandidateTablePruneCause.columnNotFound;
import static org.apache.lens.cube.parse.CubeTestSetup.*;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;

import java.util.*;

import org.apache.lens.cube.error.NoCandidateFactAvailableException;
import org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ParseException;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

public class TestDenormalizationResolver extends TestQueryRewrite {

  private Configuration conf;
  private final String cubeName = CubeTestSetup.TEST_CUBE_NAME;

  @BeforeTest
  public void setupDriver() throws Exception {
    conf = new Configuration();
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1,C2");
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY, true);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT, true);
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER, false);
  }

  @Test
  public void testDenormsAsDirectFields() throws ParseException, LensException, HiveException {
    // denorm fields directly available
    String hqlQuery = rewrite("select dim2big1, max(msr3)," + " msr2 from testCube" + " where " + TWO_DAYS_RANGE_IT,
      conf);
    String expecteddim2big1 =
      getExpectedQuery(cubeName, "SELECT (testcube.dim2big1) as `dim2big1`, max((testcube.msr3)) as `max(msr3)`, "
          + "sum((testcube.msr2)) as `msr2` FROM ", null,
        " group by testcube.dim2big1", getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary4"),
        null);
    TestCubeRewriter.compareQueries(hqlQuery, expecteddim2big1);
    // with another table
    hqlQuery = rewrite("select dim2big1, cubecity.name, max(msr3)," + " msr2 from testCube" + " where "
      + TWO_DAYS_RANGE_IT, conf);
    String expecteddim2big1WithAnotherTable = getExpectedQuery(cubeName,
      "SELECT (testcube.dim2big1) as `dim2big1`, (cubecity.name) as `name`, max((testcube.msr3)) as `max(msr3)`, "
          + "sum((testcube.msr2)) as `msr2` FROM ", " JOIN " + getDbName() + "c1_citytable cubecity "
            + "on testcube.cityid = cubecity.id and cubecity.dt = 'latest' ", null,
      " group by testcube.dim2big1, cubecity.name", null,
      getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary4"),
      null);
    TestCubeRewriter.compareQueries(hqlQuery, expecteddim2big1WithAnotherTable);

    hqlQuery = rewrite("select dim2big2, max(msr3)," + " msr2 from testCube" + " where " + TWO_DAYS_RANGE_IT, conf);
    String expecteddim2big2 =
      getExpectedQuery(cubeName, "SELECT (testcube.dim2big2) as `dim2big2`, max((testcube.msr3)) as `max(msr3)`, "
          + "sum((testcube.msr2)) as `msr2` FROM ", null, " group by testcube.dim2big2",
          getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary4"), null);
    TestCubeRewriter.compareQueries(hqlQuery, expecteddim2big2);

    Configuration conf2 = new Configuration(conf);
    conf2.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    hqlQuery = rewrite("select dim3chain.name, dim2big1, max(msr3), msr2 from testCube where "
      + TWO_DAYS_RANGE_IT, conf2);
    String expected =
      getExpectedQuery(cubeName,
        "SELECT (dim3chain.name) as `name`, (testcube.dim2big1) as `dim2big1`, max((testcube.msr3)) as `max(msr3)`,"
            + " sum((testcube.msr2)) as `msr2` FROM ", " JOIN "
          + getDbName() + "c2_testdim2tbl3 testdim2 " + "on testcube.dim2big1 = testdim2.bigid1" + " join "
          + getDbName() + "c2_testdim3tbl dim3chain on " + "testdim2.testdim3id = dim3chain.id", null,
        " group by dim3chain.name, (testcube.dim2big1)", null,
        getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary4"),
        null);
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select dim2big1, max(msr3), msr2 from testCube where " + TWO_DAYS_RANGE_IT, conf2);
    TestCubeRewriter.compareQueries(hqlQuery, expecteddim2big1);
    hqlQuery = rewrite("select dim2big2, max(msr3), msr2 from testCube where " + TWO_DAYS_RANGE_IT, conf2);
    TestCubeRewriter.compareQueries(hqlQuery, expecteddim2big2);
  }

  @Test
  public void testDenormsWithJoins() throws Exception {
    // all following queries use joins to get denorm fields
    Configuration tconf = new Configuration(this.conf);
    tconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    String hqlQuery = rewrite("select dim2big1, max(msr3), msr2 from testCube where " + TWO_DAYS_RANGE,
      tconf);
    String expected =
      getExpectedQuery(cubeName, "select (dim2chain.bigid1) as `dim2big1`, max((testcube.msr3)) "
          + "as `max(msr3)`, sum((testcube.msr2)) as `msr2` FROM ", " JOIN "
          + getDbName() + "c1_testdim2tbl2 dim2chain ON testcube.dim2 = "
          + " dim2chain.id and (dim2chain.dt = 'latest') ", null, "group by (dim2chain.bigid1)", null,
        getWhereForDailyAndHourly2days(cubeName, "c1_summary2"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testDenormsWithJoinsWithChainFieldSelected() throws Exception {
    Configuration tconf = new Configuration(this.conf);
    tconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    String hqlQuery = rewrite("select dim2chain.name, dim2big1, max(msr3), msr2 from testCube where " + TWO_DAYS_RANGE,
      tconf);
    String expected =
      getExpectedQuery(cubeName,
        "select (dim2chain.name) as `name`, (dim2chain.bigid1) as `dim2big1`, max((testcube.msr3)) as `max(msr3)`, "
            + "sum((testcube.msr2)) as `msr2` FROM ", " JOIN "
          + getDbName() + "c1_testdim2tbl2 dim2chain ON testcube.dim2 = "
          + " dim2chain.id and (dim2chain.dt = 'latest') ", null, "group by dim2chain.name, dim2chain.bigid1", null,
        getWhereForDailyAndHourly2days(cubeName, "c1_summary2"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testDenormsWithJoinsWithChainFieldSelectedAndJoinTypeSpecified() throws Exception {
    Configuration tconf = new Configuration(this.conf);
    tconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    tconf.set(CubeQueryConfUtil.JOIN_TYPE_KEY, "LEFTOUTER");
    String hqlQuery = rewrite("select dim2chain.name, dim2big1, max(msr3), msr2 from testCube where " + TWO_DAYS_RANGE,
      tconf);
    String expected =
      getExpectedQuery(cubeName,
        "select (dim2chain.name) as `name`, (dim2chain.bigid1) as `dim2big1`, max((testcube.msr3)) "
            + "as `max(msr3)`, sum((testcube.msr2)) as `msr2` FROM ", " LEFT OUTER JOIN "
          + getDbName() + "c1_testdim2tbl2 dim2chain ON testcube.dim2 = "
          + " dim2chain.id and (dim2chain.dt = 'latest') ", null, "group by dim2chain.name, dim2chain.bigid1", null,
        getWhereForDailyAndHourly2days(cubeName, "c1_summary2"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testDenormsWithJoinsWithExplicitJoinSpecified() throws Exception {
    Configuration tconf = new Configuration(this.conf);
    tconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    // With explicit join specified, automatic join resolver is disabled.
    // thus querying denorm variables will fail
    getLensExceptionInRewrite("select testdim2.name, dim2big1, max(msr3), msr2 from testCube left outer join testdim2"
      + " on testcube.dim2 = testdim2.id where " + TWO_DAYS_RANGE, tconf);
  }

  @Test
  public void testDenormsWithJoinsWithMergableChains() throws Exception {
    Configuration tconf = new Configuration(this.conf);
    tconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    String hqlQuery =
      rewrite("select dim3chain.name, dim2big1, max(msr3), msr2 from testCube where " + TWO_DAYS_RANGE,
        tconf);
    String expected =
      getExpectedQuery(cubeName,
        " SELECT (dim3chain.name) as `name`, (dim2chain.bigid1) as `dim2big1`, max((testcube.msr3)) "
            + "as `max(msr3)`, sum((testcube.msr2)) as `msr2` FROM ", " JOIN "
          + getDbName() + "c1_testdim2tbl3 dim2chain "
          + "on testcube.dim2 = dim2chain.id AND (dim2chain.dt = 'latest')" + " join " + getDbName()
          + "c1_testdim3tbl dim3chain on " + "dim2chain.testdim3id = dim3chain.id AND (dim3chain.dt = 'latest')",
        null, " group by dim3chain.name, (dim2chain.bigid1)", null,
        getWhereForDailyAndHourly2days(cubeName, "c1_summary2"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testDenormsWithJoinsWithNoCandidateStorages() throws Exception {
    Configuration tconf = new Configuration(this.conf);
    tconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    LensException e = getLensExceptionInRewrite(
      "select dim2big2, max(msr3)," + " msr2 from testCube" + " where " + TWO_DAYS_RANGE, tconf);
    NoCandidateFactAvailableException ne = (NoCandidateFactAvailableException) e;
    PruneCauses.BriefAndDetailedError error = ne.getJsonMessage();
    Assert.assertEquals(error.getBrief(), CandidateTablePruneCode.INVALID_DENORM_TABLE.errorFormat);

    Map<HashSet<String>, List<CandidateTablePruneCause>> enhanced = error.enhanced();
    Map<Set<String>, List<CandidateTablePruneCause>> expected = Maps.newHashMap();
    expected.put(newHashSet("c1_summary1", "c1_testfact", "c1_testfact2"),
      newArrayList(columnNotFound("dim2big2")));
    expected.put(newHashSet("c1_testfact2_raw", "c1_summary3", "c1_summary2"),
      newArrayList(new CandidateTablePruneCause(CandidateTablePruneCode.INVALID_DENORM_TABLE)));
    expected.put(newHashSet("SEG[b1cube; b2cube]"),
      newArrayList(columnNotFound("msr2", "msr3")));
    Assert.assertEquals(enhanced, expected);
  }

  @Test
  public void testCubeQueryWithExpressionHavingDenormColumnComingAsDirectColumn() throws Exception {
    String hqlQuery = rewrite("select substrdim2big1, max(msr3)," + " msr2 from testCube" + " where "
      + TWO_DAYS_RANGE_IT, conf);
    String expecteddim2big1 =
      getExpectedQuery(cubeName, "SELECT substr((testcube.dim2big1), 5) as `substrdim2big1`, max((testcube.msr3)) "
          + "as `max(msr3)`, sum((testcube.msr2)) as `msr2` FROM ",
        null, " group by substr(testcube.dim2big1, 5)",
        getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary4"),
        null);
    TestCubeRewriter.compareQueries(hqlQuery, expecteddim2big1);
  }

  @Test
  public void testCubeQueryWithExpressionHavingDenormColumnResultingJoin() throws Exception {
    Configuration tconf = new Configuration(this.conf);
    tconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    String hqlQuery = rewrite("select substrdim2big1, max(msr3)," + " msr2 from testCube" + " where " + TWO_DAYS_RANGE,
      tconf);
    String expected =
      getExpectedQuery(cubeName, "SELECT substr((dim2chain.bigid1), 5) as `substrdim2big1`, max((testcube.msr3)) "
          + "as `max(msr3)`, sum((testcube.msr2)) as `msr2` FROM ",
        " JOIN " + getDbName() + "c1_testdim2tbl2 dim2chain ON testcube.dim2 = "
          + " dim2chain.id and (dim2chain.dt = 'latest') ", null, "group by substr(dim2chain.bigid1, 5)", null,
        getWhereForDailyAndHourly2days(cubeName, "c1_summary2"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testDimensionQuery() throws Exception {
    String hqlQuery = rewrite("select citydim.name, citydim.statename from" + " citydim", conf);

    String joinExpr = " join " + getDbName() + "c1_statetable citystate on"
        + " citydim.stateid = citystate.id and (citystate.dt = 'latest')";
    String expected = getExpectedQuery("citydim", "SELECT citydim.name, citystate.name FROM ", joinExpr, null, null,
        "c1_citytable", true);
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select citydim.statename, citydim.name  from" + " citydim", conf);

    expected = getExpectedQuery("citydim", "SELECT citystate.name, citydim.name FROM ", joinExpr, null, null,
        "c1_citytable", true);
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // Query would fail because citydim.nocandidatecol does not exist in any
    // candidate
    Assert.assertEquals(getLensExceptionErrorMessageInRewrite(
        "select citydim.name, citydim.statename, citydim.nocandidatecol " + "from citydim", conf),
      "No dimension table has the queried columns " + "for citydim, columns: [name, statename, nocandidatecol]");
  }

  @Test
  public void testCubeQueryWithTwoRefCols() throws Exception {
    Configuration tConf = new Configuration(conf);
    tConf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "");
    //test_time_dim2 and dim2 are not querable together
    // not checking error codes or prune causes. Just verifying not answerable
    NoCandidateFactAvailableException e = getLensExceptionInRewrite(
      "select dim2, test_time_dim2 from testcube where " + TWO_DAYS_RANGE, tConf);
  }

  @Test
  public void testCubeQueryWithHourDimJoin() throws Exception {
    Configuration tConf = new Configuration(conf);
    tConf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1,C4");
    tConf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact2");
    tConf.set(CubeQueryConfUtil.getValidStorageTablesKey("testFact2"), "C1_testFact2");
    String hqlQuery = rewrite("select test_time_dim2, msr2 from testcube where " + TWO_DAYS_RANGE, tConf);
    String expected =
      getExpectedQuery(cubeName, "select timehourchain2.full_hour as `test_time_dim2`, sum(testcube.msr2) as `msr2` "
          + "FROM ", " join " + getDbName()
          + "c4_hourDimTbl timehourchain2 on testcube.test_time_dim_hour_id2  = timehourchain2.id", null,
        " group by timehourchain2 . full_hour ", null,
        getWhereForHourly2days("c1_testfact2"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testCubeQueryWithDayDimJoin() throws Exception {
    Configuration tConf = new Configuration(conf);
    tConf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1,C4");
    tConf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact");
    tConf.set(CubeQueryConfUtil.getValidStorageTablesKey("testFact"), "C1_testFact");
    String hqlQuery = rewrite("select test_time_dim2, msr2 from testcube where " + TWO_DAYS_RANGE, tConf);
    String expected =
      getExpectedQuery(cubeName, "select timedatechain2.full_date as `test_time_dim2`, sum(testcube.msr2)  as `msr2` "
          + "FROM ", " join " + getDbName()
          + "c4_dayDimTbl timedatechain2 on testcube.test_time_dim_day_id2  = timedatechain2.id", null,
          " group by timedatechain2 . full_date ", null,
        getWhereForDailyAndHourly2days(cubeName, "c1_testfact"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testCubeQueryWithOptionalDimsRemoved() throws Exception {
    String hqlQuery = rewrite("select cityzip.code, dim22, msr11 from basecube where " + TWO_DAYS_RANGE,
      conf);
    String joinExpr = " join " + getDbName()
      + "c1_citytable citydim on basecube.cityid = citydim.id and (citydim.dt = 'latest') "
      + " join " + getDbName() + "c1_ziptable cityzip on citydim.zipcode = cityzip.code and (cityzip.dt = 'latest')";
    String expected =
      getExpectedQuery("basecube", "SELECT (cityzip.code) as `code`, (basecube.dim22) as `dim22`, "
          + "(basecube.msr11) as `msr11` FROM ", joinExpr, null, null, null,
        getWhereForHourly2days("basecube", "C1_testfact2_raw_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testTwoFieldsFromDifferentChainButSameTable() throws Exception {
    String hqlQuery = rewrite("select cubecity1.name, cubecity2.name, msr2 from testcube where " + TWO_DAYS_RANGE,
      conf);
    String joinExpr = " join " + getDbName()
      + "c1_citytable cubecity1 on testcube.cityid1 = cubecity1.id and (cubecity1.dt = 'latest') "
      + " join " + getDbName()
      + "c1_citytable cubecity2 on testcube.cityid2 = cubecity2.id and (cubecity2.dt = 'latest')";
    String expected =
      getExpectedQuery("testcube", "SELECT (cubecity1.name) as `name`, (cubecity2.name) as `name`, "
          + "sum((testcube.msr2)) as `msr2` FROM ",
        joinExpr, null, " group by cubecity1.name, cubecity2.name", null,
        getWhereForHourly2days("testcube", "c1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }
  @Test
  public void testDimensionQueryWithTwoRefCols() throws Exception {
    Configuration tConf = new Configuration(conf);
    tConf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "");
    CubeQueryContext cubeql = rewriteCtx("select citydim.zipcode, citydim.statename from" + " citydim", tConf);
    Set<String> candidateDims = new HashSet<>();
    for (CandidateDim cdim : cubeql.getCandidateDims().get(cubeql.getMetastoreClient().getDimension("citydim"))) {
      candidateDims.add(cdim.getStorageTable());
    }
    // city_table2 contains stateid, but not zipcode - it should have been removed.
    Assert.assertFalse(candidateDims.contains("city_table2"));
  }

  @Test
  public void testDimensionQueryWithExpressionHavingDenormColumn() throws Exception {
    String hqlQuery = rewrite("select citydim.name, citydim.citystate from" + " citydim", conf);
    String joinExpr =
      " join " + getDbName() + "c1_statetable citystate on"
        + " citydim.stateid = citystate.id and (citystate.dt = 'latest')";
    String expected =
      getExpectedQuery("citydim", "SELECT citydim.name, concat(citydim.name, \":\", citystate.name) FROM ",
        joinExpr, null, null, "c1_citytable", true);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testNonExistingDimension() throws Exception {
    Assert.assertEquals(getLensExceptionErrorMessageInRewrite("select nonexist.name, msr2 from testCube where "
        + TWO_DAYS_RANGE, conf), "Neither cube nor dimensions accessed in the query");
  }

  @Test
  public void testCubeQueryMultiChainRefCol() throws Exception {
    Configuration tConf = new Configuration(conf);
    tConf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    String hqlQuery = rewrite("select cubeCountryCapital, msr12 from basecube where " + TWO_DAYS_RANGE,
      tConf);
    String joinExpr = " join " + getDbName()
      + "c1_citytable citydim on basecube.cityid = citydim.id and (citydim.dt = 'latest') "
      + " join " + getDbName() + "c1_statetable statedim on citydim.stateid = statedim.id and (statedim.dt = 'latest')"
      + " join " + getDbName() + "c1_countrytable cubecitystatecountry on statedim.countryid ="
      + " cubecitystatecountry.id";
    String expected =
      getExpectedQuery("basecube", "SELECT (cubecitystatecountry.capital) as `cubecountrycapital`, "
          + "sum((basecube.msr12)) as `msr12` FROM ",
        joinExpr, null, " group by cubecitystatecountry.capital ", null,
        getWhereForHourly2days("basecube", "C1_testfact2_raw_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }
}

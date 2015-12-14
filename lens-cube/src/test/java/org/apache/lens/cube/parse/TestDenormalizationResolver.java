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

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

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
      getExpectedQuery(cubeName, "select testcube.dim2big1," + " max(testcube.msr3), sum(testcube.msr2) FROM ", null,
        " group by testcube.dim2big1", getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary4"),
        null);
    TestCubeRewriter.compareQueries(hqlQuery, expecteddim2big1);
    // with another table
    hqlQuery = rewrite("select dim2big1, citydim.name, max(msr3)," + " msr2 from testCube" + " where "
      + TWO_DAYS_RANGE_IT, conf);
    String expecteddim2big1WithAnotherTable = getExpectedQuery(cubeName,
      "select testcube.dim2big1, citydim.name, max(testcube.msr3), sum(testcube.msr2) FROM ", " JOIN "
        + getDbName() + "c1_citytable citydim " + "on testcube.cityid = citydim.id and citydim.dt = 'latest' ", null,
      " group by testcube.dim2big1, citydim.name", null,
      getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary4"),
      null);
    TestCubeRewriter.compareQueries(hqlQuery, expecteddim2big1WithAnotherTable);

    hqlQuery = rewrite("select dim2big2, max(msr3)," + " msr2 from testCube" + " where " + TWO_DAYS_RANGE_IT, conf);
    String expecteddim2big2 =
      getExpectedQuery(cubeName, "select testcube.dim2big2, max(testcube.msr3), sum(testcube.msr2) FROM ", null,
        " group by testcube.dim2big2", getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary4"),
        null);
    TestCubeRewriter.compareQueries(hqlQuery, expecteddim2big2);

    Configuration conf2 = new Configuration(conf);
    conf2.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    hqlQuery = rewrite("select testdim3.name, dim2big1, max(msr3)," + " msr2 from testCube" + " where "
      + TWO_DAYS_RANGE_IT, conf2);
    String expected =
      getExpectedQuery(cubeName,
        "select testdim3.name, testcube.dim2big1, max(testcube.msr3), sum(testcube.msr2) FROM ", " JOIN "
          + getDbName() + "c2_testdim2tbl3 testdim2 " + "on testcube.dim2big1 = testdim2.bigid1" + " join "
          + getDbName() + "c2_testdim3tbl testdim3 on " + "testdim2.testdim3id = testdim3.id", null,
        " group by testdim3.name, (testcube.dim2big1)", null,
        getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary4"),
        null);
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select dim2big1, max(msr3)," + " msr2 from testCube" + " where " + TWO_DAYS_RANGE_IT, conf2);
    TestCubeRewriter.compareQueries(hqlQuery, expecteddim2big1);
    hqlQuery = rewrite("select dim2big2, max(msr3)," + " msr2 from testCube" + " where " + TWO_DAYS_RANGE_IT, conf2);
    TestCubeRewriter.compareQueries(hqlQuery, expecteddim2big2);
  }

  @Test
  public void testDenormsWithJoins() throws ParseException, LensException, HiveException, ClassNotFoundException {
    // all following queries use joins to get denorm fields
    Configuration tconf = new Configuration(this.conf);
    tconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    String hqlQuery = rewrite("select dim2big1, max(msr3)," + " msr2 from testCube" + " where " + TWO_DAYS_RANGE,
      tconf);
    String expected =
      getExpectedQuery(cubeName, "select testdim2.bigid1, max(testcube.msr3), sum(testcube.msr2) FROM ", " JOIN "
          + getDbName() + "c1_testdim2tbl2 testdim2 ON testcube.dim2 = "
          + " testdim2.id and (testdim2.dt = 'latest') ", null, "group by (testdim2.bigid1)", null,
        getWhereForDailyAndHourly2days(cubeName, "c1_summary2"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select testdim2.name, dim2big1, max(msr3)," + " msr2 from testCube" + " where " + TWO_DAYS_RANGE, tconf);
    expected =
      getExpectedQuery(cubeName,
        "select testdim2.name, testdim2.bigid1, max(testcube.msr3), sum(testcube.msr2) FROM ", " JOIN "
          + getDbName() + "c1_testdim2tbl2 testdim2 ON testcube.dim2 = "
          + " testdim2.id and (testdim2.dt = 'latest') ", null, "group by testdim2.name, testdim2.bigid1", null,
        getWhereForDailyAndHourly2days(cubeName, "c1_summary2"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select testdim2.name, dim2big1, max(msr3)," + " msr2 from testCube left outer join testdim2"
        + " where " + TWO_DAYS_RANGE, tconf);
    expected =
      getExpectedQuery(cubeName,
        "select testdim2.name, testdim2.bigid1, max(testcube.msr3), sum(testcube.msr2) FROM ", " left outer JOIN "
          + getDbName() + "c1_testdim2tbl2 testdim2 ON testcube.dim2 = "
          + " testdim2.id and (testdim2.dt = 'latest') ", null, "group by testdim2.name, testdim2.bigid1", null,
        getWhereForDailyAndHourly2days(cubeName, "c1_summary2"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select testdim3.name, dim2big1, max(msr3)," + " msr2 from testCube" + " where " + TWO_DAYS_RANGE, tconf);
    expected =
      getExpectedQuery(cubeName,
        "select testdim3.name, testdim2.bigid1, max(testcube.msr3), sum(testcube.msr2) FROM ", " JOIN "
          + getDbName() + "c1_testdim2tbl3 testdim2 "
          + "on testcube.dim2 = testdim2.id AND (testdim2.dt = 'latest')" + " join " + getDbName()
          + "c1_testdim3tbl testdim3 on " + "testdim2.testdim3id = testdim3.id AND (testdim3.dt = 'latest')",
        null, " group by testdim3.name, (testdim2.bigid1)", null,
        getWhereForDailyAndHourly2days(cubeName, "c1_summary2"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    LensException e = getLensExceptionInRewrite(
      "select dim2big2, max(msr3)," + " msr2 from testCube" + " where " + TWO_DAYS_RANGE, tconf);
    NoCandidateFactAvailableException ne = (NoCandidateFactAvailableException) e;
    PruneCauses.BriefAndDetailedError error = ne.getJsonMessage();
    Assert.assertEquals(error.getBrief(), CandidateTablePruneCode.NO_CANDIDATE_STORAGES.errorFormat);

    HashMap<String, List<CandidateTablePruneCause>> details = error.getDetails();

    for (Map.Entry<String, List<CandidateTablePruneCause>> entry : details.entrySet()) {
      if (entry.getValue().equals(Arrays.asList(CandidateTablePruneCause.columnNotFound("dim2big2")))) {
        Set<String> expectedKeySet =
          Sets.newTreeSet(Splitter.on(',').split("summary1,cheapfact,testfactmonthly,testfact2,testfact"));
        Assert.assertTrue(expectedKeySet.equals(Sets.newTreeSet(Splitter.on(',').split(entry.getKey()))));
      }

      if (entry.getValue().equals(
        Arrays.asList(new CandidateTablePruneCause(CandidateTablePruneCode.INVALID_DENORM_TABLE)))) {
        Set<String> expectedKeySet =
          Sets.newTreeSet(Splitter.on(',').split("summary2,testfact2_raw,summary3"));
        Assert.assertTrue(expectedKeySet.equals(Sets.newTreeSet(Splitter.on(',').split(entry.getKey()))));
      }

      if (entry.getKey().equals("testfact_continuous")) {
        Assert.assertTrue(entry.getValue().equals(
          Arrays.asList(CandidateTablePruneCause.columnNotFound("msr2", "msr3")))
          || entry.getValue().equals(Arrays.asList(CandidateTablePruneCause.columnNotFound("msr3", "msr2"))));
      }

      if (entry.getKey().equals("summary4")) {
        List<CandidateTablePruneCause> expectedPruneCauses = Arrays.asList(CandidateTablePruneCause.noCandidateStorages(
          new HashMap<String, CandidateTablePruneCause.SkipStorageCause>() {
            {
              put("C2", new CandidateTablePruneCause.SkipStorageCause(
                CandidateTablePruneCause.SkipStorageCode.UNSUPPORTED));
            }
          }));
        Assert.assertTrue(entry.getValue().equals(expectedPruneCauses));
      }
    }
  }

  @Test
  public void testCubeQueryWithExpressionHavingDenormColumnComingAsDirectColumn() throws Exception {
    String hqlQuery = rewrite("select substrdim2big1, max(msr3)," + " msr2 from testCube" + " where "
      + TWO_DAYS_RANGE_IT, conf);
    String expecteddim2big1 =
      getExpectedQuery(cubeName, "select substr(testcube.dim2big1, 5), max(testcube.msr3), sum(testcube.msr2) FROM ",
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
      getExpectedQuery(cubeName, "select substr(testdim2.bigid1, 5), max(testcube.msr3), sum(testcube.msr2) FROM ",
        " JOIN " + getDbName() + "c1_testdim2tbl2 testdim2 ON testcube.dim2 = "
          + " testdim2.id and (testdim2.dt = 'latest') ", null, "group by substr(testdim2.bigid1, 5)", null,
        getWhereForDailyAndHourly2days(cubeName, "c1_summary2"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testDimensionQuery() throws Exception {
    String hqlQuery = rewrite("select citydim.name, citydim.statename from" + " citydim", conf);

    String joinExpr = " join " + getDbName() + "c1_statetable statedim on"
        + " citydim.stateid = statedim.id and (statedim.dt = 'latest')";
    String expected = getExpectedQuery("citydim", "SELECT citydim.name, statedim.name FROM ", joinExpr, null, null,
        "c1_citytable", true);
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select citydim.statename, citydim.name  from" + " citydim", conf);

    expected = getExpectedQuery("citydim", "SELECT statedim.name, citydim.name FROM ", joinExpr, null, null,
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
    CubeQueryContext cubeql = rewriteCtx("select dim2, test_time_dim2 from testcube where " + TWO_DAYS_RANGE, tConf);
    Set<String> candidateFacts = new HashSet<String>();
    for (CandidateFact cfact : cubeql.getCandidateFacts()) {
      candidateFacts.add(cfact.getName().toLowerCase());
    }
    // testfact contains test_time_dim_day_id, but not dim2 - it should have been removed.
    Assert.assertFalse(candidateFacts.contains("testfact"));
    // summary2 contains dim2, but not test_time_dim2 - it should have been removed.
    Assert.assertFalse(candidateFacts.contains("summary2"));
  }

  @Test
  public void testCubeQueryWithHourDimJoin() throws Exception {
    Configuration tConf = new Configuration(conf);
    tConf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1,C4");
    tConf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact2");
    tConf.set(CubeQueryConfUtil.getValidStorageTablesKey("testFact2"), "C1_testFact2");
    String hqlQuery = rewrite("select test_time_dim2, msr2 from testcube where " + TWO_DAYS_RANGE, tConf);
    String expected =
      getExpectedQuery(cubeName, "select timehourchain.full_hour, sum(testcube.msr2) FROM ", " join " + getDbName()
        + "c4_hourDimTbl timehourchain on testcube.test_time_dim_hour_id2  = timehourchain.id", null,
        " group by timehourchain . full_hour ", null,
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
      getExpectedQuery(cubeName, "select timedatechain.full_date, sum(testcube.msr2) FROM ", " join " + getDbName()
        + "c4_dayDimTbl timedatechain on testcube.test_time_dim_day_id2  = timedatechain.id", null,
        " group by timedatechain . full_date ", null,
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
      getExpectedQuery("basecube", "select cityzip.code, basecube.dim22, basecube.msr11 FROM ",
        joinExpr, null, null, null,
        getWhereForHourly2days("basecube", "C1_testfact2_raw_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testDimensionQueryWithTwoRefCols() throws Exception {
    Configuration tConf = new Configuration(conf);
    tConf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "");
    CubeQueryContext cubeql = rewriteCtx("select citydim.zipcode, citydim.statename from" + " citydim", tConf);
    Set<String> candidateDims = new HashSet<String>();
    for (CandidateDim cdim : cubeql.getCandidateDims().get(cubeql.getMetastoreClient().getDimension("citydim"))) {
      candidateDims.add(cdim.getName());
    }
    // city_table2 contains stateid, but not zipcode - it should have been removed.
    Assert.assertFalse(candidateDims.contains("city_table2"));
  }

  @Test
  public void testDimensionQueryWithExpressionHavingDenormColumn() throws Exception {
    String hqlQuery = rewrite("select citydim.name, citydim.citystate from" + " citydim", conf);
    String joinExpr =
      " join " + getDbName() + "c1_statetable statedim on"
        + " citydim.stateid = statedim.id and (statedim.dt = 'latest')";
    String expected =
      getExpectedQuery("citydim", "SELECT citydim.name, concat(citydim.name, \":\", statedim.name) FROM ",
        joinExpr, null, null, "c1_citytable", true);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testNonExistingDimension() throws Exception {
    Assert.assertEquals(getLensExceptionErrorMessageInRewrite("select nonexist.name, msr2 from testCube where "
        + TWO_DAYS_RANGE, conf), "Neither cube nor dimensions accessed in the query");
  }
}

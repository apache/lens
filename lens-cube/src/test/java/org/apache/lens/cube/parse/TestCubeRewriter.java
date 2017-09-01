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

import static org.apache.lens.cube.error.LensCubeErrorCode.NO_CANDIDATE_FACT_AVAILABLE;
import static org.apache.lens.cube.error.LensCubeErrorCode.NO_FACT_HAS_COLUMN;
import static org.apache.lens.cube.metadata.DateFactory.*;
import static org.apache.lens.cube.metadata.UpdatePeriod.*;
import static org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode.*;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.*;
import static org.apache.lens.cube.parse.CubeTestSetup.*;

import static org.testng.Assert.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.error.NoCandidateDimAvailableException;
import org.apache.lens.cube.error.NoCandidateFactAvailableException;
import org.apache.lens.cube.metadata.*;
import org.apache.lens.server.api.LensServerAPITestUtil;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestCubeRewriter extends TestQueryRewrite {

  private Configuration conf;

  @BeforeTest
  public void setupDriver() throws Exception {
    conf = LensServerAPITestUtil.getConfiguration(
      DRIVER_SUPPORTED_STORAGES, "C0,C1,C2,C98",
      DISABLE_AUTO_JOINS, true,
      ENABLE_SELECT_TO_GROUPBY, true,
      ENABLE_GROUP_BY_TO_SELECT, true,
      DISABLE_AGGREGATE_RESOLVER, false);
  }

  @Override
  public Configuration getConf() {
    return new Configuration(conf);
  }

  @Test
  public void testQueryWithNow() throws Exception {
    LensException e = getLensExceptionInRewrite(
        "select SUM(msr2) from testCube where " + getTimeRangeString("NOW - 2DAYS", "NOW"), getConf());
    assertEquals(e.getErrorCode(), LensCubeErrorCode.NO_CANDIDATE_FACT_AVAILABLE.getLensErrorInfo().getErrorCode());
  }

  @Test
  public void testQueryWithContinuousUpdatePeriod() throws Exception {
    Configuration conf = getConf();
    conf.set(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "true");
    conf.setClass(CubeQueryConfUtil.TIME_RANGE_WRITER_CLASS, BetweenTimeRangeWriter.class, TimeRangeWriter.class);
    conf.setBoolean(REPLACE_TIMEDIM_WITH_PART_COL, false);
    DateFormat qFmt = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
    String timeRangeString;
    timeRangeString = getTimeRangeString(DAILY, -2, 0, qFmt);
    CubeQueryContext rewrittenQuery = rewriteCtx("select SUM(msr15) from testCube where " + timeRangeString, conf);

    String to = getDateStringWithOffset(DAILY, 0, CONTINUOUS);
    String from = getDateStringWithOffset(DAILY, -2, CONTINUOUS);

    String expected = "select SUM((testCube.msr15))  as `sum(msr15)` from "
        + "TestQueryRewrite.c0_testFact_CONTINUOUS testcube"
        + " WHERE ((( testcube . d_time ) between  '" + from + "'  and  '" + to + "' ))";
    System.out.println("rewrittenQuery.toHQL() " + rewrittenQuery.toHQL());
    System.out.println("expected " + expected);
    compareQueries(rewrittenQuery.toHQL(), expected);

    //test with msr2 on different fact
    conf.setBoolean(REPLACE_TIMEDIM_WITH_PART_COL, true);
    rewrittenQuery = rewriteCtx("select SUM(msr2) from testCube where " + timeRangeString, conf);
    expected = "select SUM((testCube.msr2)) as `sum(msr2)` from TestQueryRewrite.c2_testfact testcube"
      + " WHERE ((( testcube . dt ) between  '" + from + "'  and  '" + to + "' ))";
    System.out.println("rewrittenQuery.toHQL() " + rewrittenQuery.toHQL());
    System.out.println("expected " + expected);
    compareQueries(rewrittenQuery.toHQL(), expected);

    //from date 6 days back
    timeRangeString = getTimeRangeString(DAILY, -6, 0, qFmt);
    NoCandidateFactAvailableException th = getLensExceptionInRewrite("select SUM(msr15) from testCube where "
      + timeRangeString, getConf());
  }

  @Test
  public void testCandidateTables() throws Exception {
    LensException th = getLensExceptionInRewrite(
      "select dim12, SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, getConf());
    assertEquals(th.getErrorCode(), LensCubeErrorCode.COLUMN_NOT_FOUND.getLensErrorInfo().getErrorCode());

    // this query should through exception because invalidMsr is invalid
    th = getLensExceptionInRewrite(
      "SELECT cityid, invalidMsr from testCube " + " where " + TWO_DAYS_RANGE, getConf());
    assertEquals(th.getErrorCode(), LensCubeErrorCode.COLUMN_NOT_FOUND.getLensErrorInfo().getErrorCode());
  }

  @Test
  public void testCubeQuery() throws Exception {
    CubeQueryContext rewrittenQuery =
      rewriteCtx("select SUM(msr2) from testCube where " + TWO_DAYS_RANGE, getConfWithStorages("C2"));
    String expected =
      getExpectedQuery(TEST_CUBE_NAME, "select sum(testcube.msr2) as `sum(msr2)` FROM ", null, null,
        getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    String hql = rewrittenQuery.toHQL();
    compareQueries(hql, expected);
    System.out.println("Non existing parts:" + rewrittenQuery.getNonExistingParts());
//    assertNotNull(rewrittenQuery.getNonExistingParts());
  }

  @Test
  public void testMaxCoveringFact() throws Exception {
    Configuration conf = getConf();
    conf.setClass(CubeQueryConfUtil.TIME_RANGE_WRITER_CLASS, AbridgedTimeRangeWriter.class, TimeRangeWriter.class);
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
    conf.set(DRIVER_SUPPORTED_STORAGES, "C1,C2,C4");
    CubeQueryContext cubeQueryContext =
      rewriteCtx("select SUM(msr2) from testCube where " + THIS_YEAR_RANGE, conf);
    PruneCauses<Candidate> pruneCause = cubeQueryContext.getStoragePruningMsgs();
    long lessDataCauses = pruneCause.values().stream()
      .flatMap(Collection::stream).map(CandidateTablePruneCause::getCause).filter(LESS_DATA::equals).count();
    assertTrue(lessDataCauses > 0);
  }

  @Test
  public void testLightestFactFirst() throws Exception {
    // testFact is lighter than testFact2.
    String hqlQuery = rewrite("select SUM(msr2) from testCube where " + TWO_DAYS_RANGE, getConfWithStorages(
      "C2"));
    String expected =
      getExpectedQuery(TEST_CUBE_NAME, "select sum(testcube.msr2) as `sum(msr2)` FROM ", null, null,
        getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    Configuration conf = getConfWithStorages("C1");
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select sum(testcube.msr2) as `sum(msr2)` FROM ", null, null,
        getWhereForHourly2days("c1_testfact2"));
    compareQueries(hqlQuery, expected);

    conf.setBoolean(CubeQueryConfUtil.LIGHTEST_FACT_FIRST, true);

    NoCandidateFactAvailableException ne = getLensExceptionInRewrite(
      "select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    PruneCauses.BriefAndDetailedError pruneCauses = ne.getJsonMessage();
    int endIndex = MISSING_PARTITIONS.errorFormat.length() - 3;
    assertEquals(
      pruneCauses.getBrief().substring(0, endIndex),
      MISSING_PARTITIONS.errorFormat.substring(0, endIndex)
    );
    assertEquals(pruneCauses.getDetails().get("c1_testfact").size(), 1);
    assertEquals(pruneCauses.getDetails().get("c1_testfact").iterator().next().getCause(),
      MISSING_PARTITIONS);
  }

  @Test
  public void testDerivedCube() throws ParseException, LensException, HiveException, ClassNotFoundException {
    CubeQueryContext rewrittenQuery =
      rewriteCtx("select SUM(msr2) from derivedCube where " + TWO_DAYS_RANGE, getConfWithStorages("C2"));
    String expected =
      getExpectedQuery(DERIVED_CUBE_NAME, "select sum(derivedCube.msr2) as `sum(msr2)` FROM ", null, null,
        getWhereForDailyAndHourly2days(DERIVED_CUBE_NAME, "C2_testfact"));
    compareQueries(rewrittenQuery.toHQL(), expected);

    System.out.println("Non existing parts:" + ((StorageCandidate) rewrittenQuery.getCandidates().iterator().next())
        .getNonExistingPartitions());
    assertNotNull(((StorageCandidate) rewrittenQuery.getCandidates().iterator().next()).getNonExistingPartitions());

    LensException th = getLensExceptionInRewrite(
      "select SUM(msr4) from derivedCube where " + TWO_DAYS_RANGE, getConf());
    assertEquals(th.getErrorCode(), LensCubeErrorCode.COLUMN_NOT_FOUND.getLensErrorInfo().getErrorCode());

    // test join
    Configuration conf = getConf();
    conf.setBoolean(DISABLE_AUTO_JOINS, false);
    String hqlQuery;

    /*
    Accessing join chains from derived cubes are not supported yet.
    hqlQuery = rewrite("select dim2chain.name, SUM(msr2) from derivedCube where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(DERIVED_CUBE_NAME, "select dim2chain.name, sum(derivedCube.msr2) FROM ", " JOIN "
          + getDbName() + "c1_testdim2tbl dim2chain ON derivedCube.dim2 = "
          + " dim2chain.id and (dim2chain.dt = 'latest') ", null, "group by (dim2chain.name)", null,
        getWhereForDailyAndHourly2days(DERIVED_CUBE_NAME, "c1_summary2"));
    compareQueries(hqlQuery, expected);

    // Test that explicit join query passes with join resolver disabled
    conf.setBoolean(DISABLE_AUTO_JOINS, true);
    hqlQuery =
      rewrite("select citydim.name, SUM(msr2) from derivedCube "
        + " inner join citydim on derivedCube.cityid = citydim.id where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(DERIVED_CUBE_NAME, "select citydim.name, sum(derivedCube.msr2) FROM ",
        " inner JOIN " + getDbName()
        + "c1_citytable citydim ON derivedCube.cityid = citydim.id  and (citydim.dt = 'latest')", null,
        "group by (citydim.name)", null,
        getWhereForDailyAndHourly2days(DERIVED_CUBE_NAME, "c1_summary2"));
    compareQueries(hqlQuery, expected);*/
  }

  @Test
  public void testCubeInsert() throws Exception {
    Configuration conf = getConf();
    conf.set(DRIVER_SUPPORTED_STORAGES, "C2");
    String hqlQuery = rewrite("insert overwrite directory" + " 'target/test' select SUM(msr2) from testCube where "
      + TWO_DAYS_RANGE, conf);
    Map<String, String> wh = getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact");
    String expected = "insert overwrite directory 'target/test' "
      + getExpectedQuery(TEST_CUBE_NAME, "select sum(testcube.msr2) as `sum(msr2)` FROM ", null, null, wh);
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("insert overwrite directory" + " 'target/test' select SUM(msr2) from testCube where "
      + TWO_DAYS_RANGE, conf);
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("insert overwrite local directory" + " 'target/test' select SUM(msr2) from testCube where "
      + TWO_DAYS_RANGE, conf);
    wh = getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact");
    expected = "insert overwrite local directory 'target/test' "
      + getExpectedQuery(TEST_CUBE_NAME, "select sum(testcube.msr2) as `sum(msr2)` FROM ", null, null, wh);
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("insert overwrite local directory" + " 'target/test' select SUM(msr2) as `sum(msr2)` "
        + "from testCube where " + TWO_DAYS_RANGE, conf);
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("insert overwrite table temp" + " select SUM(msr2) from testCube where " + TWO_DAYS_RANGE,
      conf);
    wh = getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact");
    expected = "insert overwrite table temp "
      + getExpectedQuery(TEST_CUBE_NAME, "select sum(testcube.msr2) as `sum(msr2)` FROM ", null, null, wh);
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("insert overwrite table temp" + " select SUM(msr2) from testCube where " + TWO_DAYS_RANGE,
      conf);
    compareQueries(hqlQuery, expected);
  }

  static void compareQueries(String actual, String expected) {
    assertEquals(new TestQuery(actual), new TestQuery(expected));
  }

  static void compareContains(String expected, String actual) {
    if (expected == null && actual == null) {
      return;
    } else if (expected == null) {
      fail();
    } else if (actual == null) {
      fail("Rewritten query is null");
    }
    String expectedTrimmed = expected.replaceAll("\\W", "");
    String actualTrimmed = actual.replaceAll("\\W", "");

    if (!actualTrimmed.toLowerCase().contains(expectedTrimmed.toLowerCase())) {
      String method = null;
      for (StackTraceElement trace : Thread.currentThread().getStackTrace()) {
        if (trace.getMethodName().startsWith("test")) {
          method = trace.getMethodName() + ":" + trace.getLineNumber();
        }
      }

      System.err.println("__FAILED__ " + method + "\n\tExpected: " + expected + "\n\t---------\n\tActual: " + actual);
    }
    assertTrue(actualTrimmed.toLowerCase().contains(expectedTrimmed.toLowerCase()), "Expected:" + expected
      + "Actual:" + actual);
  }

  @Test
  public void testCubeWhereQuery() throws Exception {
    String hqlQuery, expected;
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, getConfWithStorages("C2"));
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select sum(testcube.msr2) as `sum(msr2)` FROM ", null, null,
        getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    // Test with partition existence
    Configuration conf = getConf();
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select sum(testcube.msr2) as `sum(msr2)`FROM ", null, null,
        getWhereForHourly2days("c1_testfact2"));
    compareQueries(hqlQuery, expected);
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);

    // Tests for valid tables
    conf.set(CubeQueryConfUtil.getValidFactTablesKey(TEST_CUBE_NAME), "testFact");
    conf.set(DRIVER_SUPPORTED_STORAGES, "C1");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select sum(testcube.msr2) as `sum(msr2)` FROM ", null, null,
        getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C1_testfact"));
    compareQueries(hqlQuery, expected);

    conf.set(DRIVER_SUPPORTED_STORAGES, "C2");
    conf.set(CubeQueryConfUtil.getValidFactTablesKey(TEST_CUBE_NAME), "testFact");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select sum(testcube.msr2) as `sum(msr2)` FROM ", null, null,
        getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    conf.set(DRIVER_SUPPORTED_STORAGES, "C1");
    conf.set(CubeQueryConfUtil.getValidFactTablesKey(TEST_CUBE_NAME), "testFact2");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select sum(testcube.msr2) as `sum(msr2)` FROM ", null, null,
        getWhereForHourly2days("c1_testfact2"));
    compareQueries(hqlQuery, expected);

    conf.set(DRIVER_SUPPORTED_STORAGES, "C1");
    conf.set(CubeQueryConfUtil.getValidFactTablesKey(TEST_CUBE_NAME), "testFact2");
    conf.set(getValidStorageTablesKey("testFact2"), "C1_testFact2");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select sum(testcube.msr2) as `sum(msr2)` FROM ", null, null,
        getWhereForHourly2days("c1_testfact2"));
    compareQueries(hqlQuery, expected);

    conf.set(CubeQueryConfUtil.getValidFactTablesKey(TEST_CUBE_NAME), "testFact");
    conf.set(getValidStorageTablesKey("testfact"), "C1_testFact");
    conf.set(getValidUpdatePeriodsKey("testfact", "C1"), "HOURLY");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    expected = getExpectedQuery(TEST_CUBE_NAME,
      "select sum(testcube.msr2)  as `sum(msr2)` FROM ", null, null, getWhereForHourly2days("c1_testfact"));
    compareQueries(hqlQuery, expected);

    conf.set(DRIVER_SUPPORTED_STORAGES, "C2");
    conf.set(getValidStorageTablesKey("testfact"), "C2_testFact");
    conf.set(getValidUpdatePeriodsKey("testfact", "C2"), "HOURLY");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    expected = getExpectedQuery(TEST_CUBE_NAME,
      "select sum(testcube.msr2)  as `sum(msr2)` FROM ", null, null, getWhereForHourly2days("c2_testfact"));
    compareQueries(hqlQuery, expected);

    // max interval test
    conf = new Configuration();
    conf.set(CubeQueryConfUtil.QUERY_MAX_INTERVAL, "HOURLY");
    conf.set(DRIVER_SUPPORTED_STORAGES, "C1");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    expected = getExpectedQuery(TEST_CUBE_NAME,
      "select sum(testcube.msr2) as `sum(msr2)` FROM ", null, null, getWhereForHourly2days("c1_testfact2"));
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testPartColAsQueryColumn() throws Exception {
    Configuration conf = getConf();
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
    conf.set(DRIVER_SUPPORTED_STORAGES, "C3");
    conf.setBoolean(DISABLE_AUTO_JOINS, false);
    String hql, expected;
    hql = rewrite(
      "select cubecountry.name, msr2 from" + " testCube" + " where cubecountry.region = 'asia' and "
        + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select cubecountry.name  as `name`, sum(testcube.msr2) as `msr2` "
          + " FROM ", " JOIN " + getDbName()
          + "c3_countrytable_partitioned cubecountry on testcube.countryid=cubecountry.id and cubecountry.dt='latest'",
        "cubecountry.region='asia'",
        " group by cubecountry.name ", null,
        getWhereForHourly2days(TEST_CUBE_NAME, "C3_testfact2_raw"));
    compareQueries(hql, expected);
    hql = rewrite(
      "select cubestate.name, cubestate.countryid, msr2 from" + " testCube" + " where cubestate.countryid = 5 and "
        + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select cubestate.name as `name`, "
          + "cubestate.countryid as `countryid`, sum(testcube.msr2) as `msr2`" + " FROM ",
        " JOIN " + getDbName()
          + "c3_statetable_partitioned cubestate ON" + " testCube.stateid = cubestate.id and cubestate.dt = 'latest'",
        "cubestate.countryid=5",
        " group by cubestate.name, cubestate.countryid", null,
        getWhereForHourly2days(TEST_CUBE_NAME, "C3_testfact2_raw"));
    compareQueries(hql, expected);
  }

  @Test
  public void testCubeJoinQuery() throws Exception {
    // q1
    Configuration conf = getConf();
    conf.set(DRIVER_SUPPORTED_STORAGES, "C2");
    String hqlQuery =
      rewrite("select SUM(msr2) from testCube" + " join citydim on testCube.cityid = citydim.id" + " where "
        + TWO_DAYS_RANGE, conf);
    String expected =
      getExpectedQuery(TEST_CUBE_NAME, "SELECT sum((testcube.msr2)) as `sum(msr2)` FROM ", " INNER JOIN " + getDbName()
          + "c2_citytable citydim ON" + " testCube.cityid = citydim.id", null, null, null,
        getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select SUM(msr2) from testCube" + " join citydim on cityid = citydim.id" + " where " + TWO_DAYS_RANGE,
        conf);
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select SUM(msr2) from testCube" + " join citydim on cityid = id" + " where " + TWO_DAYS_RANGE,
        getConfWithStorages("C2"));
    compareQueries(hqlQuery, expected);

    // q2
    hqlQuery =
      rewrite("select statedim.name, SUM(msr2) from" + " testCube" + " join citydim on testCube.cityid = citydim.id"
        + " left outer join statedim on statedim.id = citydim.stateid"
        + " right outer join zipdim on citydim.zipcode = zipdim.code" + " where " + TWO_DAYS_RANGE, getConf());
    expected =
      getExpectedQuery(TEST_CUBE_NAME,
        "select statedim.name as `name`," + " sum(testcube.msr2) as `SUM(msr2)` FROM ", "INNER JOIN " + getDbName()
          + "c1_citytable citydim ON testCube.cityid = citydim.id and citydim.dt='latest' LEFT OUTER JOIN "
          + getDbName()
          + "c1_statetable statedim" + " ON statedim.id = citydim.stateid AND "
          + "(statedim.dt = 'latest') RIGHT OUTER JOIN " + getDbName() + "c1_ziptable"
          + " zipdim ON citydim.zipcode = zipdim.code and zipdim.dt='latest'", null, " group by" + " statedim.name ",
        null,
        getWhereForHourly2days(TEST_CUBE_NAME, "C1_testfact2"));
    compareQueries(hqlQuery, expected);

    // q3
    hqlQuery =
      rewrite("select st.name, SUM(msr2) from" + " testCube TC" + " join citydim CT on TC.cityid = CT.id"
        + " left outer join statedim ST on ST.id = CT.stateid"
        + " right outer join zipdim ZT on CT.zipcode = ZT.code" + " where " + TWO_DAYS_RANGE, getConf());
    expected =
      getExpectedQuery("tc", "select st.name as `name`," + " sum(tc.msr2) as `sum(msr2)` FROM ",
          " INNER JOIN " + getDbName()
          + "c1_citytable ct ON" + " tc.cityid = ct.id and ct.dt='latest' LEFT OUTER JOIN "
          + getDbName() + "c1_statetable st"
          + " ON st.id = ct.stateid and (st.dt = 'latest') " + "RIGHT OUTER JOIN " + getDbName() + "c1_ziptable"
          + " zt ON ct.zipcode = zt.code and zt.dt='latest'", null, " group by" + " st.name ", null,
        getWhereForHourly2days("tc", "C1_testfact2"));
    compareQueries(hqlQuery, expected);

    // q4
    hqlQuery =
      rewrite("select citydim.name, SUM(msr2) from" + " testCube"
        + " left outer join citydim on testCube.cityid = citydim.id"
        + " left outer join zipdim on citydim.zipcode = zipdim.code" + " where " + TWO_DAYS_RANGE, getConf());
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select citydim.name as `name`," + " sum(testcube.msr2)  as `sum(msr2)`FROM ",
          " LEFT OUTER JOIN "
          + getDbName() + "c1_citytable citydim ON" + " testCube.cityid = citydim.id and (citydim.dt = 'latest') "
          + " LEFT OUTER JOIN " + getDbName() + "c1_ziptable" + " zipdim ON citydim.zipcode = zipdim.code AND "
          + "(zipdim.dt = 'latest')", null, " group by" + " citydim.name ", null,
        getWhereForHourly2days(TEST_CUBE_NAME, "C1_testfact2"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select SUM(msr2) from testCube" + " join countrydim on testCube.countryid = countrydim.id" + " where "
        + TWO_MONTHS_RANGE_UPTO_MONTH, getConf());
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select sum(testcube.msr2) as `sum(msr2)` FROM ", " INNER JOIN " + getDbName()
          + "c1_countrytable countrydim ON testCube.countryid = " + " countrydim.id", null, null, null,
        getWhereForMonthly2months("c2_testfactmonthly"));
    compareQueries(hqlQuery, expected);

    LensException th = getLensExceptionInRewrite(
      "select name, SUM(msr2) from testCube" + " join citydim" + " where " + TWO_DAYS_RANGE
        + " group by name", getConf());
    assertEquals(th.getErrorCode(), LensCubeErrorCode.NO_JOIN_CONDITION_AVAILABLE.getLensErrorInfo().getErrorCode());
  }

  @Test
  public void testConvertDimFilterToFactFilterForSingleFact() throws Exception {
    Configuration conf = getConf();
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
    conf.set(DRIVER_SUPPORTED_STORAGES, "C3");
    conf.setBoolean(DISABLE_AUTO_JOINS, false);
    conf.setBoolean(REWRITE_DIM_FILTER_TO_FACT_FILTER, true);

    // No filter
    String hql = rewrite("select cityid , msr2 from testCube where " + TWO_DAYS_RANGE, conf);

    String expectedQuery =  getExpectedQuery(TEST_CUBE_NAME, "select (testcube.cityid) as `cityid`, "
        + "sum((testcube.msr2)) as `msr2` from ", null, "group by testcube.cityid",
      getWhereForHourly2days(TEST_CUBE_NAME, "c3_testfact2_raw"));
    compareQueries(expectedQuery, hql);

    // filter with =
    hql = rewrite(
        "select cubecountry.name, msr2 from" + " testCube" + " where cubecountry.region = 'asia' and "
            + TWO_DAYS_RANGE, conf);
    String filterSubquery = "testcube.countryid in ( select id from TestQueryRewrite.c3_countrytable_partitioned "
        + "cubecountry where ((cubecountry.region) = 'asia') and (cubecountry.dt = 'latest') )";
    assertTrue(hql.contains(filterSubquery));

    // filter with or
    hql = rewrite(
        "select cubecountry.name, msr2 from" + " testCube" + " where (cubecountry.region = 'asia' "
            + "or cubecountry.region = 'europe') and " + TWO_DAYS_RANGE, conf);
    filterSubquery = "testcube.countryid in ( select id from TestQueryRewrite.c3_countrytable_partitioned "
        + "cubecountry where (((cubecountry.region) = 'asia') or ((cubecountry.region) = 'europe')) "
        + "and (cubecountry.dt = 'latest') )";
    assertTrue(hql.contains(filterSubquery));

    //filter with in
    hql = rewrite(
        "select cubecountry.name, msr2 from" + " testCube" + " where cubecountry.region in ('asia','europe') "
            + "and " + TWO_DAYS_RANGE, conf);
    filterSubquery = "testcube.countryid in ( select id from TestQueryRewrite.c3_countrytable_partitioned "
        + "cubecountry where (cubecountry.region) in ('asia' , 'europe') and (cubecountry.dt = 'latest') )";
    assertTrue(hql.contains(filterSubquery));

    //filter with not in
    hql = rewrite(
        "select cubecountry.name, msr2 from" + " testCube" + " where cubecountry.region not in ('asia','europe') "
            + "and " + TWO_DAYS_RANGE, conf);
    filterSubquery = "testcube.countryid in ( select id from TestQueryRewrite.c3_countrytable_partitioned "
        + "cubecountry where (cubecountry.region) not  in ('asia' , 'europe') and (cubecountry.dt = 'latest') )";
    assertTrue(hql.contains(filterSubquery));

    //filter with !=
    hql = rewrite(
        "select cubecountry.name, msr2 from" + " testCube" + " where cubecountry.region != 'asia' "
            + "and " + TWO_DAYS_RANGE, conf);
    filterSubquery = "testcube.countryid in ( select id from TestQueryRewrite.c3_countrytable_partitioned "
        + "cubecountry where ((cubecountry.region) != 'asia') and (cubecountry.dt = 'latest') )";
    assertTrue(hql.contains(filterSubquery));

    //filter with cube alias
    hql = rewrite(
        "select cubecountry.name, msr2 from" + " testCube as t" + " where cubecountry.region = 'asia' "
            + "and zipcode = 'x' and " + TWO_DAYS_RANGE, conf);
    filterSubquery = "t.countryid in ( select id from TestQueryRewrite.c3_countrytable_partitioned "
        + "cubecountry where ((cubecountry.region) = 'asia') and (cubecountry.dt = 'latest') )";
    assertTrue(hql.contains(filterSubquery));

    //filter with AbridgedTimeRangeWriter
    conf.setClass(CubeQueryConfUtil.TIME_RANGE_WRITER_CLASS, AbridgedTimeRangeWriter.class, TimeRangeWriter.class);
    hql = rewrite(
        "select cubecountry.name, msr2 from" + " testCube" + " where cubecountry.region = 'asia' and "
            + TWO_DAYS_RANGE, conf);
    filterSubquery = "testcube.countryid in ( select id from TestQueryRewrite.c3_countrytable_partitioned "
        + "cubecountry where ((cubecountry.region) = 'asia') and (cubecountry.dt = 'latest') )";
    String timeKeyIn = "(testcube.dt) in";
    assertTrue(hql.contains(timeKeyIn));
    assertTrue(hql.contains(filterSubquery));
  }

  @Test
  public void testCubeGroupbyWithConstantProjected() throws Exception {
    // check constants
    Configuration conf = getConf();
    conf.set(DRIVER_SUPPORTED_STORAGES, "C2");
    String hqlQuery1 = rewrite("select cityid, 99, \"placeHolder\", -1001, SUM(msr2) from testCube" + " where "
      + TWO_DAYS_RANGE, conf);
    String expected1 = getExpectedQuery(TEST_CUBE_NAME, "SELECT (testcube.cityid) as `cityid`, 99 as `99`, "
        + "\"placeHolder\" as `\"placeHolder\"`, (-1001) as `(-1001)`, sum((testcube.msr2)) as `sum(msr2)` FROM ",
        null, " group by testcube.cityid ",
      getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery1, expected1);

    // check constants with expression
    String hqlQuery2 = rewrite(
      "select cityid, case when stateid = 'za' then \"Not Available\" end, 99, \"placeHolder\", -1001, "
        + "SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    String expected2 = getExpectedQuery(TEST_CUBE_NAME,
      "SELECT (testcube.cityid) as `cityid`, case  when ((testcube.stateid) = 'za') then \"Not Available\" end "
          + "as `case  when (stateid = 'za') then \"Not Available\" end`, 99 as `99`, \"placeHolder\" "
          + "as `\"placeHolder\"`, (-1001) as `(-1001)`, sum((testcube.msr2)) as `sum(msr2)` FROM ", null,
      " group by testcube.cityid, case when testcube.stateid = 'za' then \"Not Available\" end ",
      getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery2, expected2);

    // check expression with boolean and numeric constants
    String hqlQuery3 = rewrite(
      "select cityid,stateid + 99, 44 + stateid, stateid - 33, 999 - stateid, TRUE, FALSE, round(123.4567,2), "
        + "case when stateid='za' then 99 else -1001 end,  "
        + "SUM(msr2), SUM(msr2 + 39), SUM(msr2) + 567 from testCube" + " where " + TWO_DAYS_RANGE, conf);
    String expected3 = getExpectedQuery(
      TEST_CUBE_NAME,
      "SELECT (testcube.cityid) as `cityid`, ((testcube.stateid) + 99) as `(stateid + 99)`, "
          + "(44 + (testcube.stateid)) as `(44 + stateid)`, ((testcube.stateid) - 33) as `(stateid - 33)`, "
          + "(999 - (testcube.stateid)) as `(999 - stateid)`,  true  as `true`,  false  "
          + "as `false`, round(123.4567, 2) as `round(123.4567, 2)`, "
          + "case  when ((testcube.stateid) = 'za') then 99 else (-1001) end "
          + "as `case  when (stateid = 'za') then 99 else (-1001) end`, "
          + "sum((testcube.msr2)) as `sum(msr2)`, sum(((testcube.msr2) + 39)) "
          + "as `sum((msr2 + 39))`, (sum((testcube.msr2)) + 567) as `(sum(msr2) + 567)` FROM ",
      null,
      " group by testcube.cityid,testcube.stateid + 99, 44 + testcube.stateid, testcube.stateid - 33, "
        + "999 - testcube.stateid, "
        + " case when testcube.stateid='za' then 99 else -1001 end ",
      getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery3, expected3);
  }

  @Test
  public void testCubeGroupbyQuery() throws Exception {
    Configuration conf = getConf();
    conf.set(DRIVER_SUPPORTED_STORAGES, "C2");

    String hqlQuery =
      rewrite("select name, SUM(msr2) from" + " testCube join citydim on testCube.cityid = citydim.id where "
        + TWO_DAYS_RANGE, conf);
    String expected =
      getExpectedQuery(TEST_CUBE_NAME, "select citydim.name as `name`, sum(testcube.msr2) as `sum(msr2)` FROM "
          , " INNER JOIN " + getDbName() + "c2_citytable citydim ON" + " testCube.cityid = citydim.id",
          null, " group by citydim.name ",
        null, getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select SUM(msr2) from testCube join citydim on testCube.cityid = citydim.id  where "
        + TWO_DAYS_RANGE + " group by name", conf);
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select cityid, sum(msr2) from testCube where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select testcube.cityid as `cityid`, sum(testcube.msr2) as `sum(msr2)` from ",
          null, " group by testcube.cityid ", getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select round(cityid), sum(msr2) from" + " testCube where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select round(testcube.cityid) as `round(cityid)`,"
          + " sum(testcube.msr2) as `sum(msr2)` FROM ", null,
        " group by round(testcube.cityid) ", getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select sum(msr2) from testCube where " + TWO_DAYS_RANGE + "group by round(zipcode)", conf);
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select round(testcube.zipcode) as `round((testcube.zipcode))`,"
          + " sum(testcube.msr2)  as `sum(msr2)` FROM ", null,
        " group by round(testcube.zipcode) ", getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select round(cityid), sum(msr2) from" + " testCube where " + TWO_DAYS_RANGE + " group by zipcode",
        conf);
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select round(testcube.cityid) as `round(cityid)`, "
          + "sum(testcube.msr2) as `sum(msr2)` FROM ", null,
        " group by testcube.zipcode", getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select round(cityid), sum(msr2) from testCube where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select round(testcube.cityid) as `round(cityid)`, "
          + "sum(testcube.msr2) as `sum(msr2)` FROM ", null,
        " group by round(testcube.cityid)", getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select cityid, sum(msr2) from testCube" + " where " + TWO_DAYS_RANGE + " group by round(zipcode)",
        conf);
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select testcube.cityid as `cityid`, sum(testcube.msr2) as `sum(msr2)` FROM ",
          null, " group by round(testcube.zipcode)", getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select sum(msr2) from testCube where " + TWO_DAYS_RANGE + " group by round(zipcode)", conf);
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select round(testcube.zipcode) as `round(testcube.zipcode)`, "
          + "sum(testcube.msr2) as `sum(msr2)` FROM ", null, " group by round(testcube.zipcode)",
          getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select cityid, msr2 from testCube where " + TWO_DAYS_RANGE + " group by round(zipcode)", conf);
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select testcube.cityid as `cityid`, sum(testcube.msr2) as `msr2` FROM ", null,
        " group by round(testcube.zipcode)", getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select round(zipcode) rzc," + " msr2 from testCube where " + TWO_DAYS_RANGE + " group by zipcode"
        + " order by rzc", conf);
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select round(testcube.zipcode) as `rzc`, sum(testcube.msr2)  as `msr2` FROM ",
          null, " group by testcube.zipcode  order by rzc asc",
          getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    //Dim attribute with aggregate function
    hqlQuery =
        rewrite("select countofdistinctcityid, zipcode from testCube where " + TWO_DAYS_RANGE, conf);
    expected =
        getExpectedQuery(TEST_CUBE_NAME, "select count(distinct(testcube.cityid)) as `countofdistinctcityid`"
            + ", (testcube.zipcode) as `zipcode` FROM ", null, " group by (testcube.zipcode)",
            getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    //Dim attribute with single row function
    hqlQuery =
        rewrite("select notnullcityid, zipcode from" + " testCube where " + TWO_DAYS_RANGE, conf);
    expected =
        getExpectedQuery(TEST_CUBE_NAME, "select distinct case  when (testcube.cityid) is null then 0 "
                + "else (testcube.cityid) end as `notnullcityid`, (testcube.zipcode) as `zipcode` FROM ", null,
            "", getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    // rewrite with expressions
    conf.setBoolean(DISABLE_AUTO_JOINS, false);
    conf.set(DRIVER_SUPPORTED_STORAGES, "C1, C2");
    hqlQuery =
      rewrite("SELECT cubecity.name AS g1," + " CASE  WHEN cubecity.name=='NULL'  THEN 'NULL' "
        + " WHEN cubecity.name=='X'  THEN 'X-NAME' " + " WHEN cubecity.name=='Y'  THEN 'Y-NAME' "
        + " ELSE 'DEFAULT'   END  AS g2, " + " cubestate.name AS g3," + " cubestate.id AS g4, "
        + " cubezip.code!=1  AND " + " ((cubezip.f1==\"xyz\"  AND  (cubezip.f2 >= \"3\"  AND "
        + "  cubezip.f2 !=\"NULL\"  AND  cubezip.f2 != \"uk\")) "
        + "  OR (cubezip.f2==\"adc\"  AND  cubezip.f1==\"js\" "
        + "  AND  ( cubecity.name == \"X\"  OR  cubecity.name == \"Y\" )) "
        + " OR ((cubezip.f1==\"api\"  OR  cubezip.f1==\"uk\"  OR  (cubezip.f1==\"adc\"  AND  cubezip.f1!=\"js\"))"
        + "  AND  cubecity.id==12) ) AS g5," + " cubezip.code==1  AND "
        + " ((cubezip.f1==\"xyz\"  AND  (cubezip.f2 >= \"3\"  AND "
        + "  cubezip.f2 !=\"NULL\"  AND  cubezip.f2 != \"uk\")) "
        + " OR (cubezip.f2==\"adc\"  AND  cubezip.f1==\"js\" "
        + " AND  ( cubecity.name == \"X\"  OR  cubecity.name == \"Y\" )) "
        + "  OR ((cubezip.f1==\"api\"  OR  cubezip.f1==\"uk\"  OR  (cubezip.f1==\"adc\"  AND  cubezip.f1!=\"js\"))"
        + "    AND  cubecity.id==12) ) AS g6, " + "  cubezip.f1 AS g7, "
        + "  format_number(SUM(msr1),\"##################.###\") AS a1,"
        + "  format_number(SUM(msr2),\"##################.###\") AS a2, "
        + "  format_number(SUM(msr3),\"##################.###\") AS a3, "
        + " format_number(SUM(msr1)+SUM(msr2), \"##################.###\") AS a4,"
        + "  format_number(SUM(msr1)+SUM(msr3),\"##################.###\") AS a5,"
        + " format_number(SUM(msr1)-(SUM(msr2)+SUM(msr3)),\"##################.###\") AS a6"
        + "  FROM testCube where " + TWO_DAYS_RANGE + " HAVING (SUM(msr1) >=1000)  AND (SUM(msr2)>=0.01)", conf);
    String actualExpr =
      ""
        + " join " + getDbName() + "c1_statetable cubestate on testcube.stateid=cubestate.id and "
        + "(cubestate.dt='latest')"
        + " join " + getDbName()
        + "c1_ziptable cubezip on testcube.zipcode = cubezip.code and (cubezip.dt = 'latest')  "
        + " join " + getDbName()
        + "c1_citytable cubecity on testcube.cityid = cubecity.id and (cubecity.dt = 'latest')"
        + "";
    expected =
      getExpectedQuery(
        TEST_CUBE_NAME,
        "SELECT ( cubecity.name ) as `g1` ,"
          + "  case  when (( cubecity.name ) ==  'NULL' ) then  'NULL'  when (( cubecity.name ) ==  'X' )"
          + " then  'X-NAME'  when (( cubecity.name ) ==  'Y' ) then  'Y-NAME'"
          + "  else  'DEFAULT'  end  as `g2` , ( cubestate.name ) as `g3` , ( cubestate.id ) as `g4` ,"
          + " ((( cubezip.code ) !=  1 ) and ((((( cubezip.f1 ) ==  \"xyz\" )"
          + " and (((( cubezip.f2 ) >=  \"3\" ) and (( cubezip.f2 ) !=  \"NULL\" ))"
          + " and (( cubezip.f2 ) !=  \"uk\" ))) or (((( cubezip.f2 ) ==  \"adc\" )"
          + " and (( cubezip.f1 ) ==  \"js\" ))"
          + " and ((( cubecity.name ) ==  \"X\" ) or (( cubecity.name ) ==  \"Y\" ))))"
          + " or ((((( cubezip.f1 ) ==  \"api\" )"
          + " or (( cubezip.f1 ) ==  \"uk\" )) or ((( cubezip.f1 ) ==  \"adc\" )"
          + " and (( cubezip.f1 ) !=  \"js\" )))"
          + " and (( cubecity.id ) ==  12 )))) as `g5` , ((( cubezip.code ) ==  1 )"
          + " and ((((( cubezip.f1 ) ==  \"xyz\" ) and (((( cubezip.f2 ) >=  \"3\" )"
          + " and (( cubezip.f2 ) !=  \"NULL\" ))"
          + " and (( cubezip.f2 ) !=  \"uk\" ))) or (((( cubezip.f2 ) ==  \"adc\" )"
          + " and (( cubezip.f1 ) ==  \"js\" ))"
          + " and ((( cubecity.name ) ==  \"X\" ) or (( cubecity.name ) ==  \"Y\" ))))"
          + " or ((((( cubezip.f1 ) ==  \"api\" )"
          + " or (( cubezip.f1 ) ==  \"uk\" )) or ((( cubezip.f1 ) ==  \"adc\" )"
          + " and (( cubezip.f1 ) !=  \"js\" )))"
          + " and (( cubecity.id ) ==  12 )))) as `g6` , ( cubezip.f1 ) as `g7` ,"
          + " format_number(sum(( testcube.msr1 )),  \"##################.###\" ) as `a1` ,"
          + " format_number(sum(( testcube.msr2 )),  \"##################.###\" ) as `a2` ,"
          + " format_number(sum(( testcube.msr3 )),  \"##################.###\" ) as `a3`, "
          + " format_number((sum(( testcube.msr1 )) + sum(( testcube.msr2 ))),  \"##################.###\" ) as `a4` ,"
          + " format_number((sum(( testcube.msr1 )) + sum(( testcube.msr3 ))),  \"##################.###\" ) as `a5` ,"
          + " format_number((sum(( testcube.msr1 )) - (sum(( testcube.msr2 )) + sum(( testcube.msr3 )))), "
          + " \"##################.###\" ) as `a6`"
          + "  FROM ",
        actualExpr,
        null,
        " GROUP BY ( cubecity.name ), case  when (( cubecity.name ) ==  'NULL' ) "
          + "then  'NULL'  when (( cubecity.name ) ==  'X' ) then  'X-NAME'  when (( cubecity.name ) ==  'Y' )"
          + " then  'Y-NAME'  else  'DEFAULT'  end, ( cubestate.name ), ( cubestate.id ),"
          + " ((( cubezip.code ) !=  1 ) and ((((( cubezip.f1 ) ==  \"xyz\" ) and (((( cubezip.f2 ) >=  \"3\" )"
          + " and (( cubezip.f2 ) !=  \"NULL\" )) and (( cubezip.f2 ) !=  \"uk\" ))) or (((( cubezip.f2 ) ==  \"adc\" )"
          + " and (( cubezip.f1 ) ==  \"js\" )) and ((( cubecity.name ) ==  \"X\" ) or (( cubecity.name ) ==  \"Y\""
          + " ))))"
          + " or ((((( cubezip.f1 ) ==  \"api\" ) or (( cubezip.f1 ) ==  \"uk\" )) or ((( cubezip.f1 ) ==  \"adc\" )"
          + " and (( cubezip.f1 ) !=  \"js\" ))) and (( cubecity.id ) ==  12 )))), ((( cubezip.code ) ==  1 ) and"
          + " ((((( cubezip.f1 ) ==  \"xyz\" ) and (((( cubezip.f2 ) >=  \"3\" ) and (( cubezip.f2 ) !=  \"NULL\" ))"
          + " and (( cubezip.f2 ) !=  \"uk\" ))) or (((( cubezip.f2 ) ==  \"adc\" ) and (( cubezip.f1 ) ==  \"js\" ))"
          + " and ((( cubecity.name ) ==  \"X\" ) or (( cubecity.name ) ==  \"Y\" )))) or ((((( cubezip.f1 )==\"api\" )"
          + " or (( cubezip.f1 ) ==  \"uk\" )) or ((( cubezip.f1 ) ==  \"adc\" ) and (( cubezip.f1 ) !=  \"js\" )))"
          + " and (( cubecity.id ) ==  12 )))), ( cubezip.f1 ) HAVING ((sum(( testcube.msr1 )) >=  1000 ) "
          + "and (sum(( testcube.msr2 )) >=  0.01 ))",
        null, getWhereForHourly2days("c1_testfact2_raw"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite(
        "SELECT cubecity.name AS g1,"
          + " CASE  WHEN cubecity.name=='NULL'  THEN 'NULL' "
          + " WHEN cubecity.name=='X'  THEN 'X-NAME' "
          + " WHEN cubecity.name=='Y'  THEN 'Y-NAME' "
          + " ELSE 'DEFAULT'   END  AS g2, "
          + " cubestate.name AS g3,"
          + " cubestate.id AS g4, "
          + " cubezip.code!=1  AND "
          + " ((cubezip.f1==\"xyz\"  AND  (cubezip.f2 >= \"3\"  AND "
          + "  cubezip.f2 !=\"NULL\"  AND  cubezip.f2 != \"uk\")) "
          + "  OR (cubezip.f2==\"adc\"  AND  cubezip.f1==\"js\" "
          + "  AND  ( cubecity.name == \"X\"  OR  cubecity.name == \"Y\" )) "
          + " OR ((cubezip.f1==\"api\"  OR  cubezip.f1==\"uk\"  OR  (cubezip.f1==\"adc\"  AND  cubezip.f1!=\"js\"))"
          + "  AND  cubecity.id==12) ) AS g5,"
          + " cubezip.code==1  AND "
          + " ((cubezip.f1==\"xyz\"  AND  (cubezip.f2 >= \"3\"  AND "
          + "  cubezip.f2 !=\"NULL\"  AND  cubezip.f2 != \"uk\")) "
          + " OR (cubezip.f2==\"adc\"  AND  cubezip.f1==\"js\" "
          + " AND  ( cubecity.name == \"X\"  OR  cubecity.name == \"Y\" )) "
          + "  OR ((cubezip.f1==\"api\"  OR  cubezip.f1==\"uk\"  OR  (cubezip.f1==\"adc\"  AND  cubezip.f1!=\"js\"))"
          + "    AND  cubecity.id==12) ) AS g6, "
          + "  cubezip.f1 AS g7, "
          + "  format_number(SUM(msr1),\"##################.###\") AS a1,"
          + "  format_number(SUM(msr2),\"##################.###\") AS a2, "
          + "  format_number(SUM(msr3),\"##################.###\") AS a3, "
          + " format_number(SUM(msr1)+SUM(msr2), \"##################.###\") AS a4,"
          + "  format_number(SUM(msr1)+SUM(msr3),\"##################.###\") AS a5,"
          + " format_number(SUM(msr1)-(SUM(msr2)+SUM(msr3)),\"##################.###\") AS a6"
          + "  FROM testCube where "
          + TWO_DAYS_RANGE
          + " group by cubecity.name, CASE WHEN cubecity.name=='NULL' THEN 'NULL'"
          + " WHEN cubecity.name=='X' THEN 'X-NAME' WHEN cubecity.name=='Y' THEN 'Y-NAME'"
          + " ELSE 'DEFAULT'   END, cubestate.name, cubestate.id,  cubezip.code!=1  AND"
          + " ((cubezip.f1==\"xyz\"  AND  (cubezip.f2 >= \"3\"  AND cubezip.f2 !=\"NULL\"  AND  cubezip.f2 != \"uk\"))"
          + " OR (cubezip.f2==\"adc\"  AND  cubezip.f1==\"js\""
          + " AND ( cubecity.name == \"X\"  OR  cubecity.name == \"Y\" ))"
          + " OR ((cubezip.f1==\"api\"  OR  cubezip.f1==\"uk\"  OR  (cubezip.f1==\"adc\"  AND  cubezip.f1!=\"js\"))"
          + " AND  cubecity.id==12) ),"
          + " cubezip.code==1  AND  ((cubezip.f1==\"xyz\" AND ( cubezip.f2 >= \"3\"  AND cubezip.f2 !=\"NULL\""
          + " AND  cubezip.f2 != \"uk\"))"
          + " OR (cubezip.f2==\"adc\"  AND  cubezip.f1==\"js\""
          + " AND  ( cubecity.name == \"X\"  OR  cubecity.name == \"Y\" ))"
          + " OR ((cubezip.f1=\"api\"  OR  cubezip.f1==\"uk\" OR (cubezip.f1==\"adc\"  AND  cubezip.f1!=\"js\")) AND"
          + " cubecity.id==12))," + " cubezip.f1 " + "HAVING (SUM(msr1) >=1000)  AND (SUM(msr2)>=0.01)", conf);
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testSelectExprPromotionToGroupByWithSpacesInDimensionAliasAndWithAsKeywordBwColAndAlias()
    throws ParseException, LensException, HiveException {

    String inputQuery = "select name as `Alias With  Spaces`, SUM(msr2) as `TestMeasure` from testCube join citydim"
      + " on testCube.cityid = citydim.id where " + LAST_HOUR_TIME_RANGE;

    String expectedRewrittenQuery = "SELECT (citydim.name) as `Alias With  Spaces`, sum((testcube.msr2)) "
      + "as `TestMeasure` FROM TestQueryRewrite.c2_testfact testcube inner JOIN TestQueryRewrite.c2_citytable citydim "
      + "ON ((testcube.cityid) = (citydim.id)) WHERE ((testcube.dt) = '"
      + getDateUptoHours(getDateWithOffset(HOURLY, -1)) + "') GROUP BY (citydim.name)";

    String actualRewrittenQuery = rewrite(inputQuery, getConfWithStorages("C2"));

    assertEquals(actualRewrittenQuery, expectedRewrittenQuery);
  }

  @Test
  public void testSelectExprPromotionToGroupByWithSpacesInDimensionAliasAndWithoutAsKeywordBwColAndAlias()
    throws ParseException, LensException, HiveException {

    String inputQuery = "select name `Alias With  Spaces`, SUM(msr2) as `TestMeasure` from testCube join citydim"
      + " on testCube.cityid = citydim.id where " + LAST_HOUR_TIME_RANGE;

    String expectedRewrittenQuery = "SELECT (citydim.name) as `Alias With  Spaces`, sum((testcube.msr2)) "
      + "as `TestMeasure` FROM TestQueryRewrite.c2_testfact testcube inner JOIN TestQueryRewrite.c2_citytable citydim "
      + "ON ((testcube.cityid) = (citydim.id)) WHERE ((testcube.dt) = '"
      + getDateUptoHours(getDateWithOffset(HOURLY, -1)) + "') GROUP BY (citydim.name)";

    String actualRewrittenQuery = rewrite(inputQuery, getConfWithStorages("C2"));

    assertEquals(actualRewrittenQuery, expectedRewrittenQuery);
  }

  @Test
  public void testCubeQueryWithAilas() throws Exception {
    String hqlQuery = rewrite("select SUM(msr2) m2 from" + " testCube where " + TWO_DAYS_RANGE, getConfWithStorages(
      "C2"));
    String expected =
      getExpectedQuery(TEST_CUBE_NAME, "select sum(testcube.msr2) as `m2` FROM ", null, null,
        getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select SUM(msr2) from testCube mycube" + " where " + TWO_DAYS_RANGE, getConfWithStorages("C2"));
    expected =
      getExpectedQuery("mycube", "select sum(mycube.msr2) as `sum(msr2)` FROM ", null, null,
        getWhereForDailyAndHourly2days("mycube", "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select SUM(testCube.msr2) from testCube" + " where " + TWO_DAYS_RANGE, getConfWithStorages("C2"));
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select sum(testcube.msr2) as `sum(testCube.msr2)` FROM ", null, null,
        getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select mycube.msr2 m2 from testCube" + " mycube where " + TWO_DAYS_RANGE, getConfWithStorages(
      "C2"));
    expected =
      getExpectedQuery("mycube", "select sum(mycube.msr2) as `m2` FROM ", null, null,
        getWhereForDailyAndHourly2days("mycube", "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select testCube.msr2 m2 from testCube" + " where " + TWO_DAYS_RANGE, getConfWithStorages("C2"));
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select sum(testcube.msr2) as `m2` FROM ", null, null,
        getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testCubeWhereQueryForMonth() throws Exception {
    String hqlQuery =
      rewrite("select SUM(msr2) from testCube" + " where " + TWO_MONTHS_RANGE_UPTO_HOURS, getConfWithStorages("C2"));
    String expected =
      getExpectedQuery(TEST_CUBE_NAME, "select sum(testcube.msr2) as `sum(msr2)` FROM ", null, null,
        getWhereForMonthlyDailyAndHourly2months(TEST_CUBE_NAME, "C2_testfact"));
    compareQueries(hqlQuery, expected);
  }

  /* The test is to check no failure on partial data when the flag FAIL_QUERY_ON_PARTIAL_DATA is not set
   */
  @Test
  public void testQueryWithMeasureWithDataCompletenessTagWithNoFailureOnPartialData() throws ParseException,
          LensException {
    //In this query a measure is used for which dataCompletenessTag is set.
    Configuration conf = getConf();
    conf.setStrings(CubeQueryConfUtil.COMPLETENESS_CHECK_PART_COL, "dt");
    String hqlQuery = rewrite("select SUM(msr1) from basecube where " + TWO_DAYS_RANGE, conf);
    String expected = getExpectedQuery("basecube", "select sum(basecube.msr1)  as `sum(msr1)` FROM ", null, null,
            getWhereForHourly2days("basecube", "c1_testfact1_raw_base"));
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testQueryWithMeasureWithDataCompletenessPresentInMultipleFacts() throws ParseException,
      LensException {
    /*In this query a measure is used which is present in two facts with different %completeness. While resolving the
    facts, the fact with the higher dataCompletenessFactor gets picked up.*/
    Configuration conf = getConf();
    conf.setStrings(CubeQueryConfUtil.COMPLETENESS_CHECK_PART_COL, "dt");
    String hqlQuery = rewrite("select SUM(msr9) from basecube where " + TWO_DAYS_RANGE, conf);
    String expected = getExpectedQuery("basecube", "select sum(basecube.msr9) as `sum(msr9)` FROM ", null, null,
      getWhereForHourly2days("basecube", "c1_testfact5_raw_base"));
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testCubeWhereQueryWithMeasureWithDataCompletenessAndFailIfPartialDataFlagSet() throws ParseException,
      LensException {
    /*In this query a measure is used for which dataCompletenessTag is set and the flag FAIL_QUERY_ON_PARTIAL_DATA is
    set. The partitions for the queried range are present but some of the them have incomplete data. So, the query
    throws NO_CANDIDATE_FACT_AVAILABLE Exception*/
    Configuration conf = getConf();
    conf.setStrings(CubeQueryConfUtil.COMPLETENESS_CHECK_PART_COL, "dt");
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);

    LensException e = getLensExceptionInRewrite("select SUM(msr9) from basecube where "
        + TWO_DAYS_RANGE, conf);
    assertEquals(e.getErrorCode(), LensCubeErrorCode.NO_CANDIDATE_FACT_AVAILABLE.getLensErrorInfo().getErrorCode());
    NoCandidateFactAvailableException ne = (NoCandidateFactAvailableException) e;
    PruneCauses.BriefAndDetailedError pruneCauses = ne.getJsonMessage();
    /*Since the Flag FAIL_QUERY_ON_PARTIAL_DATA is set, and the queried fact has incomplete data, hence, we expect the
    prune cause to be INCOMPLETE_PARTITION. The below check is to validate this.*/
    for(String part: INCOMPLETE_PARTITION.errorFormat.split("%s")) {
      assertTrue(pruneCauses.getBrief().contains(part), pruneCauses.getBrief());
    }
  }

  /*
   * The test is to check that query is rewritten successfully if there is missing entry in
   * dataavailability service for the measure's tag
   */
  @Test
  public void testQueryWithMeasureWithDataCompletenessTagWithDataAvailiability() throws ParseException,
    LensException {
    NoCandidateFactAvailableException ne;
    PruneCauses.BriefAndDetailedError pruneCauses;
    String hqlQuery;
    String expected;

    Configuration conf = getConf();
    conf.setStrings(CubeQueryConfUtil.COMPLETENESS_CHECK_PART_COL, "dt");

    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);

    // 1. data completeness check applicable
    ne = getLensExceptionInRewrite("select SUM(msr16) from basecube" + " where " + TWO_DAYS_RANGE, conf);
    pruneCauses = ne.getJsonMessage();
    assertEquals(pruneCauses.getBrief().substring(0, 10), INCOMPLETE_PARTITION.errorFormat.substring(0, 10),
      pruneCauses.getBrief());

    // 2. time outside data completeness check but missing partitions
    ne = getLensExceptionInRewrite("select SUM(msr16) from basecube where " + TWO_DAYS_RANGE_BEFORE_4_DAYS, conf);
    pruneCauses = ne.getJsonMessage();
    assertEquals(pruneCauses.getBrief().substring(0, 10), MISSING_PARTITIONS.errorFormat.substring(0, 10),
      pruneCauses.getBrief());


    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);

    // 3. query allowed on partial data although data incomplete
    hqlQuery = rewrite("select SUM(msr16) from basecube" + " where " + TWO_DAYS_RANGE, conf);
    expected = getExpectedQuery("basecube", "select sum(basecube.msr16)  as `sum(msr16)` FROM ", null, null,
      getWhereForHourly2days("basecube", "c1_testfact2_raw_base"));
    compareQueries(hqlQuery.toLowerCase(), expected.toLowerCase());

    // 4. query allowed on partial data with missing partitions but outside data availability window
    hqlQuery = rewrite("select SUM(msr16) from basecube" + " where " + TWO_DAYS_RANGE_BEFORE_4_DAYS, conf);
    expected = getExpectedQuery("basecube", "select sum(basecube.msr16)  as `sum(msr16)` FROM ", null, null,
      getWhereForUpdatePeriods("basecube", "c1_testfact2_raw_base",
        DateUtils.addHours(getDateWithOffset(UpdatePeriod.DAILY, -6), -1),
        getDateWithOffset(UpdatePeriod.DAILY, -4), Sets.newHashSet(UpdatePeriod.HOURLY)));
    compareQueries(hqlQuery.toLowerCase(), expected.toLowerCase());
  }

  @Test
  public void testCubeWhereQueryForMonthWithNoPartialData() throws Exception {
    Configuration conf = getConf();
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);

    NoCandidateFactAvailableException ne = getLensExceptionInRewrite(
      "select SUM(msr2) from testCube" + " where " + TWO_MONTHS_RANGE_UPTO_HOURS, conf);
    PruneCauses.BriefAndDetailedError pruneCauses = ne.getJsonMessage();

    assertEquals(
      pruneCauses.getBrief().substring(0, MISSING_PARTITIONS.errorFormat.length() - 3),
      MISSING_PARTITIONS.errorFormat.substring(0,
        MISSING_PARTITIONS.errorFormat.length() - 3), pruneCauses.getBrief());
    List<CandidateTablePruneCause> missingPartitionCauses = pruneCauses.enhanced().get(
      Sets.newHashSet("c1_testfact2_raw", "c1_summary3", "c1_summary2",
      "c1_summary1", "c2_testfact", "c1_testfact"));
    assertEquals(missingPartitionCauses.size(), 1);
    CandidateTablePruneCause missingPartitionCause = missingPartitionCauses.iterator().next();
    assertEquals(missingPartitionCause.getCause(), MISSING_PARTITIONS);
    assertEquals(pruneCauses.getDetails().get("c1_testfact2").iterator().next().getCause(),
      MISSING_PARTITIONS);
    /*
    assertEquals(pruneCauses.getDetails().get("c4_testfact,c3_testfact,c3_testfact2_raw,c4_testfact2," +
        "c99_cheapfact,c5_testfact").iterator().next().getCause(),
      UNSUPPORTED_STORAGE);

    CandidateTablePruneCause cheapFactPruneCauses = pruneCauses.getDetails().get("cheapfact").iterator().next();
    assertEquals(cheapFactPruneCauses.getDimStoragePruningCauses().get("c0"),
        CandidateTablePruneCause.CandidateTablePruneCode.TIME_RANGE_NOT_ANSWERABLE);
    assertEquals(cheapFactPruneCauses.getDimStoragePruningCauses().get("c99"),
        CandidateTablePruneCause.CandidateTablePruneCode.UNSUPPORTED_STORAGE);
    assertEquals(pruneCauses.getDetails().get("summary4").iterator().next().getCause(), TIMEDIM_NOT_SUPPORTED);
    assertTrue(pruneCauses.getDetails().get("summary4").iterator().next().getUnsupportedTimeDims().contains("d_time"));
    */
  }

  @Test
  public void testCubeWhereQueryForMonthUptoMonths() throws Exception {
    // this should consider only two month partitions.
    String hqlQuery = rewrite("select cityid, SUM(msr2) from testCube" + " where " + TWO_MONTHS_RANGE_UPTO_MONTH,
      getConfWithStorages("C2"));
    String expected =
      getExpectedQuery(TEST_CUBE_NAME, "select testcube.cityid as `cityid`, sum(testcube.msr2) "
          + "as `sum(msr2)` FROM ", null, "group by testcube.cityid",
          getWhereForMonthly2months("c2_testfact"));
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testNoCandidateDimAvailableExceptionCompare() throws Exception {

    //Max cause COLUMN_NOT_FOUND, Ordinal 2
    PruneCauses<CubeDimensionTable> pr1 = new PruneCauses<>();
    pr1.addPruningMsg(new CubeDimensionTable(new Table("test", "citydim")),
            CandidateTablePruneCause.columnNotFound(
              "test1", "test2", "test3"));
    NoCandidateDimAvailableException ne1 = new NoCandidateDimAvailableException(pr1);

    //Max cause EXPRESSION_NOT_EVALUABLE, Ordinal 14
    PruneCauses<CubeDimensionTable> pr2 = new PruneCauses<CubeDimensionTable>();
    pr2.addPruningMsg(new CubeDimensionTable(new Table("test", "citydim")),
            CandidateTablePruneCause.expressionNotEvaluable("testexp1", "testexp2"));
    NoCandidateDimAvailableException ne2 = new NoCandidateDimAvailableException(pr2);
    assertEquals(ne1.compareTo(ne2), -7);
  }

  @Test
  public void testDimensionQueryWithMultipleStorages() throws Exception {
    String hqlQuery = rewrite("select name, stateid from" + " citydim", getConf());
    String expected =
      getExpectedQuery("citydim", "select citydim.name," + " citydim.stateid from ", null, "c1_citytable", true);
    compareQueries(hqlQuery, expected);

    Configuration conf = getConf();
    // should pick up c2 storage when 'fail on partial data' enabled
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);
    hqlQuery = rewrite("select name, stateid from" + " citydim", conf);
    expected =
      getExpectedQuery("citydim", "select citydim.name," + " citydim.stateid from ", null, "c2_citytable", false);
    compareQueries(hqlQuery, expected);

    // state table is present on c1 with partition dumps and partitions added
    LensException e = getLensExceptionInRewrite("select name, capital from statedim ", conf);
    assertEquals(e.getErrorCode(), LensCubeErrorCode.NO_CANDIDATE_DIM_AVAILABLE.getLensErrorInfo().getErrorCode());
    NoCandidateDimAvailableException ne = (NoCandidateDimAvailableException) e;
    assertEquals(ne.getJsonMessage(), new PruneCauses.BriefAndDetailedError(
      NO_CANDIDATE_STORAGES.errorFormat,
      new HashMap<String, List<CandidateTablePruneCause>>() {
        {
          put("statetable", Arrays.asList(CandidateTablePruneCause.noCandidateStoragesForDimtable(
            new HashMap<String, CandidateTablePruneCause.CandidateTablePruneCode>() {
              {
                put("c1_statetable", CandidateTablePruneCause.CandidateTablePruneCode.NO_PARTITIONS);
                put("c6_statetable", CandidateTablePruneCause.CandidateTablePruneCode.UNSUPPORTED_STORAGE);
              }
            }))
          );
          put("statetable_partitioned", Arrays.asList(CandidateTablePruneCause.noCandidateStoragesForDimtable(
            new HashMap<String, CandidateTablePruneCause.CandidateTablePruneCode>() {
              {
                put("C3_statetable_partitioned", CandidateTablePruneCause.CandidateTablePruneCode.UNSUPPORTED_STORAGE);
              }
            }))
          );
        }
      }
    ));

    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);

    // non existing parts should be populated
    CubeQueryContext rewrittenQuery = rewriteCtx("select name, capital from statedim ", conf);
    expected =
      getExpectedQuery("statedim", "select statedim.name," + " statedim.capital from ", null, "c1_statetable", true);
    compareQueries(rewrittenQuery.toHQL(), expected);
    assertNotNull(rewrittenQuery.getNonExistingParts());

    // run a query with time range function
    hqlQuery = rewrite("select name, stateid from citydim where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery("citydim", "select citydim.name," + " citydim.stateid from ", null, TWO_DAYS_RANGE, null,
        "c1_citytable", true);
    compareQueries(hqlQuery, expected);

    // query with alias
    hqlQuery = rewrite("select name, c.stateid from citydim" + " c", conf);
    expected = getExpectedQuery("c", "select c.name, c.stateid from ", null, "c1_citytable", true);
    compareQueries(hqlQuery, expected);

    // query with where clause
    hqlQuery = rewrite("select name, c.stateid from citydim" + " c where name != 'xyz' ", conf);
    expected =
      getExpectedQuery("c", "select c.name, c.stateid from ", null, " c.name != 'xyz' ", null, "c1_citytable", true);
    compareQueries(hqlQuery, expected);

    // query with orderby
    hqlQuery = rewrite("select name, c.stateid from citydim" + " c where name != 'xyz' order by name", conf);
    expected =
      getExpectedQuery("c", "select c.name, c.stateid from ", null, " c.name != 'xyz' ", " order by c.name asc",
        "c1_citytable", true);
    compareQueries(hqlQuery, expected);

    // query with where and orderby
    hqlQuery = rewrite("select name, c.stateid from citydim" + " c where name != 'xyz' order by name", conf);
    expected =
      getExpectedQuery("c", "select c.name, c.stateid from ", null, " c.name != 'xyz' ", " order by c.name asc ",
        "c1_citytable", true);
    compareQueries(hqlQuery, expected);

    // query with orderby with order specified
    hqlQuery = rewrite("select name, c.stateid from citydim" + " c where name != 'xyz' order by name desc ", conf);
    expected =
      getExpectedQuery("c", "select c.name, c.stateid from ", null, " c.name != 'xyz' ", " order by c.name desc",
        "c1_citytable", true);
    compareQueries(hqlQuery, expected);

    conf.set(DRIVER_SUPPORTED_STORAGES, "C2");
    hqlQuery = rewrite("select name, stateid from citydim", conf);
    expected =
      getExpectedQuery("citydim", "select citydim.name," + " citydim.stateid from ", null, "c2_citytable", false);
    compareQueries(hqlQuery, expected);

    conf.set(DRIVER_SUPPORTED_STORAGES, "C1");
    hqlQuery = rewrite("select name, stateid from citydim", conf);
    expected =
      getExpectedQuery("citydim", "select citydim.name," + " citydim.stateid from ", null, "c1_citytable", true);
    compareQueries(hqlQuery, expected);

    conf.set(DRIVER_SUPPORTED_STORAGES, "");
    conf.set(CubeQueryConfUtil.VALID_STORAGE_DIM_TABLES, "C1_citytable");
    hqlQuery = rewrite("select name, stateid from citydim", conf);
    expected =
      getExpectedQuery("citydim", "select citydim.name," + " citydim.stateid from ", null, "c1_citytable", true);
    compareQueries(hqlQuery, expected);

    conf.set(DRIVER_SUPPORTED_STORAGES, "");
    conf.set(CubeQueryConfUtil.VALID_STORAGE_DIM_TABLES, "C2_citytable");
    hqlQuery = rewrite("select name, stateid from citydim", conf);
    expected =
      getExpectedQuery("citydim", "select citydim.name," + " citydim.stateid from ", null, "c2_citytable", false);
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select name n, count(1) from citydim" + " group by name order by n ", conf);
    expected =
      getExpectedQuery("citydim", "select citydim.name as `n`," + " count(1) from ",
        " group by citydim.name order by n asc", "c2_citytable", false);
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select name as `n`, count(1) from citydim" + " order by n ", conf);
    compareQueries(hqlQuery, expected);
    hqlQuery = rewrite("select count(1) from citydim" + " group by name order by name ", conf);
    expected =
      getExpectedQuery("citydim", "select citydim.name," + " count(1) from ",
        " group by citydim.name order by citydim.name asc ", "c2_citytable", false);
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testLimitQueryOnDimension() throws Exception {
    Configuration conf = getConf();
    String hqlQuery = rewrite("select name, stateid from" + " citydim limit 100", conf);
    String expected =
      getExpectedQuery("citydim", "select citydim.name," + " citydim.stateid from ", " limit 100", "c1_citytable",
        true);
    compareQueries(hqlQuery, expected);
    conf.set(DRIVER_SUPPORTED_STORAGES, "C2");
    hqlQuery = rewrite("select name, stateid from citydim " + "limit 100", conf);
    expected =
      getExpectedQuery("citydim", "select citydim.name," + "citydim.stateid from ", " limit 100", "c2_citytable",
        false);
    compareQueries(hqlQuery, expected);
    conf.set(DRIVER_SUPPORTED_STORAGES, "C1");
    hqlQuery = rewrite("select name, stateid from citydim" + " limit 100", conf);
    expected =
      getExpectedQuery("citydim", "select citydim.name," + " citydim.stateid from ", " limit 100", "c1_citytable",
        true);
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testColumnAmbiguity() throws Exception {
    String query =
      "SELECT ambigdim1, sum(testCube.msr1) FROM testCube join" + " citydim on testcube.cityid = citydim.id where "
        + TWO_DAYS_RANGE;

    LensException th = getLensExceptionInRewrite(query, getConf());
    assertEquals(th.getErrorCode(), LensCubeErrorCode.AMBIGOUS_CUBE_COLUMN.getLensErrorInfo().getErrorCode());

    String q2 =
      "SELECT ambigdim2 from citydim join" + " statedim on citydim.stateid = statedim.id join countrydim on"
        + " statedim.countryid = countrydim.id";
    th = getLensExceptionInRewrite(q2, getConf());
    assertEquals(th.getErrorCode(), LensCubeErrorCode.AMBIGOUS_DIM_COLUMN.getLensErrorInfo().getErrorCode());
  }

  @Test
  public void testAliasReplacer() throws Exception {
    String[] queries = {
      "SELECT cityid, t.msr2 FROM testCube t where " + TWO_DAYS_RANGE,
      "SELECT cityid, msr2 FROM testCube where cityid > 100 and " + TWO_DAYS_RANGE + " HAVING msr2 < 1000",
      "SELECT cityid, testCube.msr2 FROM testCube where cityid > 100 and " + TWO_DAYS_RANGE
        + " HAVING msr2 < 1000 ORDER BY cityid",
    };

    String[] expectedQueries = {
      getExpectedQuery("t", "SELECT t.cityid as `cityid`, sum(t.msr2) as `msr2` FROM ", null, " group by t.cityid",
        getWhereForDailyAndHourly2days("t", "C2_testfact")),
      getExpectedQuery(TEST_CUBE_NAME, "SELECT testCube.cityid as `cityid`, sum(testCube.msr2) as `msr2`" + " FROM ",
        " testcube.cityid > 100 ", " group by testcube.cityid having" + " sum(testCube.msr2) < 1000",
        getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact")),
      getExpectedQuery(TEST_CUBE_NAME, "SELECT testCube.cityid as `cityid`, sum(testCube.msr2) as `msr2`" + " FROM ",
        " testcube.cityid > 100 ", " group by testCube.cityid having"
          + " sum(testCube.msr2) < 1000 order by cityid asc",
        getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C2_testfact")),
    };
    Configuration conf = getConf();
    conf.set(DRIVER_SUPPORTED_STORAGES, "C2");
    for (int i = 0; i < queries.length; i++) {
      String hql = rewrite(queries[i], conf);
      compareQueries(hql, expectedQueries[i]);
    }
  }

  @Test
  public void testFactsWithInvalidColumns() throws Exception {
    String hqlQuery = rewrite("select dim1, max(msr3)," + " msr2 from testCube" + " where " + TWO_DAYS_RANGE,
      getConfWithStorages("C1"));
    String expected =
      getExpectedQuery(TEST_CUBE_NAME, "select testcube.dim1 as `dim1`, max(testcube.msr3) as `max(msr3)`"
          + ", sum(testcube.msr2) as `msr2` FROM ", null,
        " group by testcube.dim1", getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C1_summary1"));
    compareQueries(hqlQuery, expected);
    hqlQuery =
      rewrite("select dim1, dim2, COUNT(msr4)," + " SUM(msr2), msr3 from testCube" + " where " + TWO_DAYS_RANGE,
        getConfWithStorages("C1"));
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select testcube.dim1 as `dim1`, testcube,dim2 as `dim2`, "
          + "count(testcube.msr4) as `count(msr4)`,sum(testcube.msr2) as `sum(msr2)`, "
          + "max(testcube.msr3)  as `msr3`FROM ", null, " group by testcube.dim1, testcube.dim2",
        getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C1_summary2"));
    compareQueries(hqlQuery, expected);
    hqlQuery =
      rewrite("select dim1, dim2, cityid, msr4," + " SUM(msr2), msr3 from testCube" + " where " + TWO_DAYS_RANGE,
        getConfWithStorages("C1"));
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select testcube.dim1 as `dim1`, testcube,dim2 as `dim2`, "
          + "testcube.cityid as `cityid`, count(testcube.msr4) as `msr4`, "
          + "sum(testcube.msr2) as `sum(msr2)`, max(testcube.msr3) as `msr3` FROM ", null,
        " group by testcube.dim1, testcube.dim2, testcube.cityid",
        getWhereForDailyAndHourly2days(TEST_CUBE_NAME, "C1_summary3"));
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testFactsWithTimedDimension() throws Exception {

    String hqlQuery =
      rewrite("select dim1, max(msr3)," + " msr2 from testCube" + " where " + TWO_DAYS_RANGE_IT, getConf());
    String expected =
      getExpectedQuery(TEST_CUBE_NAME, "select testcube.dim1 as `dim1`, max(testcube.msr3) as `max(msr3)`, "
          + "sum(testcube.msr2)  as `msr2` FROM ", null,
        " group by testcube.dim1", getWhereForDailyAndHourly2daysWithTimeDim(TEST_CUBE_NAME, "it", "C2_summary1"),
        null);
    compareQueries(hqlQuery, expected);
    hqlQuery =
      rewrite("select dim1, dim2, COUNT(msr4)," + " SUM(msr2), msr3 from testCube" + " where " + TWO_DAYS_RANGE_IT,
        getConf());
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select testcube.dim1 as `dim1`, testcube,dim2 as `dim2`, "
          + "count(testcube.msr4) as `count(msr4)`, sum(testcube.msr2) as `sum(msr2)`, "
          + "max(testcube.msr3) as `msr3` FROM ", null, " group by testcube.dim1, testcube.dim2",
        getWhereForDailyAndHourly2daysWithTimeDim(TEST_CUBE_NAME, "it", "C2_summary2"),
        null);
    compareQueries(hqlQuery, expected);
    hqlQuery =
      rewrite("select dim1, dim2, cityid, count(msr4)," + " SUM(msr2), msr3 from testCube" + " where "
        + TWO_DAYS_RANGE_IT, getConf());
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select testcube.dim1 as `dim1`, testcube,dim2 as `dim2`, "
          + "testcube.cityid as `cityid`, count(testcube.msr4) as `count(msr4)`, sum(testcube.msr2) "
          + "as `sum(msr2)`, max(testcube.msr3)  as `msr3`FROM ", null,
        " group by testcube.dim1, testcube.dim2, testcube.cityid",
        getWhereForDailyAndHourly2daysWithTimeDim(TEST_CUBE_NAME, "it", "C2_summary3"),
        null);
    compareQueries(hqlQuery, expected);
  }

  // Disabling this as querying on part column directly is not allowed as of
  // now.
  // @Test
  public void testCubeQueryTimedDimensionFilter() throws Exception {
    String hqlQuery =
      rewrite("select dim1, max(msr3)," + " msr2 from testCube" + " where (" + TWO_DAYS_RANGE_IT
        + " OR it == 'default') AND dim1 > 1000", getConf());
    String expected = getExpectedQuery(TEST_CUBE_NAME,
      "select testcube.dim1, max(testcube.msr3), sum(testcube.msr2) FROM ",
      null, "or (( testcube.it ) == 'default')) and ((testcube.dim1) > 1000)" + " group by testcube.dim1",
      getWhereForDailyAndHourly2daysWithTimeDim(TEST_CUBE_NAME, "it", "C2_summary1"),
      null);
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE + " OR ("
      + TWO_DAYS_RANGE_BEFORE_4_DAYS + " AND dt='default')", getConf());

    String expecteddtRangeWhere1 =
      getWhereForDailyAndHourly2daysWithTimeDim(TEST_CUBE_NAME, "dt", TWODAYS_BACK, NOW)
        + " OR ("
        + getWhereForDailyAndHourly2daysWithTimeDim(TEST_CUBE_NAME, "dt", BEFORE_6_DAYS, BEFORE_4_DAYS) + ")";
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select sum(testcube.msr2) FROM ", null, " AND testcube.dt='default'",
        expecteddtRangeWhere1, "c2_testfact");
    compareQueries(hqlQuery, expected);

    String expecteddtRangeWhere2 =
      "("
        + getWhereForDailyAndHourly2daysWithTimeDim(TEST_CUBE_NAME, "dt", TWODAYS_BACK, NOW)
        + " AND testcube.dt='dt1') OR "
        + getWhereForDailyAndHourly2daysWithTimeDim(TEST_CUBE_NAME, "dt", BEFORE_6_DAYS, BEFORE_4_DAYS);
    hqlQuery =
      rewrite("select SUM(msr2) from testCube" + " where (" + TWO_DAYS_RANGE + " AND dt='dt1') OR ("
        + TWO_DAYS_RANGE_BEFORE_4_DAYS + " AND dt='default')", getConf());
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select sum(testcube.msr2) FROM ", null, " AND testcube.dt='default'",
        expecteddtRangeWhere2, "c2_testfact");
    compareQueries(hqlQuery, expected);

    String twoDaysPTRange = getTimeRangeString("pt", DAILY, -2, 0, HOURLY);
    hqlQuery =
      rewrite("select dim1, max(msr3)," + " msr2 from testCube where (" + TWO_DAYS_RANGE_IT + " OR (" + twoDaysPTRange
        + " and it == 'default')) AND dim1 > 1000", getConf());
    String expectedITPTrange =
      getWhereForDailyAndHourly2daysWithTimeDim(TEST_CUBE_NAME, "it", TWODAYS_BACK, NOW) + " OR ("
        + getWhereForDailyAndHourly2daysWithTimeDim(TEST_CUBE_NAME, "pt", TWODAYS_BACK, NOW) + ")";
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select testcube.dim1, max(testcube.msr3), sum(testcube.msr2) FROM ", null,
        "AND testcube.it == 'default' and testcube.dim1 > 1000 group by testcube.dim1", expectedITPTrange,
        "C2_summary1");
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testLookAhead() throws Exception {

    Configuration conf = getConf();
    conf.set(CubeQueryConfUtil.PROCESS_TIME_PART_COL, "pt");
    conf.setClass(CubeQueryConfUtil.TIME_RANGE_WRITER_CLASS, AbridgedTimeRangeWriter.class, TimeRangeWriter.class);
    CubeQueryContext ctx = rewriteCtx("select dim1, max(msr3)," + " msr2 from testCube" + " where " + TWO_DAYS_RANGE_IT,
      conf);
    assertEquals(ctx.getCandidates().size(), 1);
    Candidate candidate = ctx.getCandidates().iterator().next();
    Set<FactPartition> partsQueried = new TreeSet<>(((StorageCandidate)candidate).getParticipatingPartitions());
    Date ceilDay = DAILY.getCeilDate(getDateWithOffset(DAILY, -2));
    Date nextDay = DateUtils.addDays(ceilDay, 1);
    Date nextToNextDay = DateUtils.addDays(nextDay, 1);
    HashSet<String> storageTables = Sets.newHashSet();
    for (StorageCandidate sc : CandidateUtil.getStorageCandidates(candidate)) {
      storageTables.add(sc.getStorageTable());
    }
    TreeSet<FactPartition> expectedPartsQueried = Sets.newTreeSet();
    for (TimePartition p : Iterables.concat(
      TimePartition.of(HOURLY, getDateWithOffset(DAILY, -2)).rangeUpto(TimePartition.of(HOURLY, ceilDay)),
      TimePartition.of(DAILY, ceilDay).rangeUpto(TimePartition.of(DAILY, nextDay)),
      TimePartition.of(HOURLY, nextDay).rangeUpto(TimePartition.of(HOURLY, NOW)))) {
      FactPartition fp = new FactPartition("it", p, null, storageTables);
      expectedPartsQueried.add(fp);
    }
    for (TimePartition it : TimePartition.of(HOURLY, ceilDay).rangeUpto(TimePartition.of(HOURLY, nextDay))) {
      for (TimePartition pt : TimePartition.of(HOURLY, nextDay).rangeUpto(TimePartition.of(HOURLY, nextToNextDay))) {
        FactPartition ptPartition = new FactPartition("pt", pt, null, storageTables);
        FactPartition itPartition = new FactPartition("it", it, ptPartition, storageTables);
        expectedPartsQueried.add(itPartition);
      }
    }
    assertEquals(partsQueried, expectedPartsQueried);
    conf.setInt(CubeQueryConfUtil.LOOK_AHEAD_PT_PARTS_PFX, 3);
    ctx = rewriteCtx("select dim1, max(msr3)," + " msr2 from testCube" + " where " + TWO_DAYS_RANGE_IT,
      conf);
    partsQueried = new TreeSet<>(((StorageCandidate)ctx.getCandidates().iterator().next())
        .getParticipatingPartitions());
    // pt does not exist beyond 1 day. So in this test, max look ahead possible is 3
    assertEquals(partsQueried, expectedPartsQueried);
  }

  @Test
  public void testTimeRangeIn() throws Exception {
    //check whether time_range_in is resolving in cube rewrite
    Configuration conf = getConf();
    conf.set(CubeQueryConfUtil.PROCESS_TIME_PART_COL, "pt");
    conf.set(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "true");
    conf.setClass(CubeQueryConfUtil.TIME_RANGE_WRITER_CLASS, AbridgedTimeRangeWriter.class, TimeRangeWriter.class);
    CubeQueryContext ctx = rewriteCtx("select dim1, sum(msr23)" + " from testCube" + " where " + ONE_DAY_RANGE_IT,
      conf);
    String rewrittenQuery = ctx.toHQL();
    assertTrue(!rewrittenQuery.contains("time_range_in"));
  }

  @Test
  public void testCubeQueryWithMultipleRanges() throws Exception {
    String hqlQuery =
      rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE + " OR "
        + TWO_DAYS_RANGE_BEFORE_4_DAYS, getConfWithStorages("C2"));

    String expectedRangeWhere =
      getWhereForDailyAndHourly2daysWithTimeDim(TEST_CUBE_NAME, "dt", TWODAYS_BACK, NOW)
        + " OR "
        + getWhereForDailyAndHourly2daysWithTimeDim(TEST_CUBE_NAME, "dt", BEFORE_6_DAYS, BEFORE_4_DAYS);
    String expected = getExpectedQuery(TEST_CUBE_NAME, "select sum(testcube.msr2) as `sum(msr2)` FROM ",
      null, null, expectedRangeWhere, "c2_testfact");
    compareQueries(hqlQuery, expected);
    hqlQuery =
      rewrite("select dim1, max(msr3)," + " msr2 from testCube" + " where " + TWO_DAYS_RANGE + " OR "
        + TWO_DAYS_RANGE_BEFORE_4_DAYS, getConfWithStorages("C1"));
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select testcube.dim1 as `dim1`, max(testcube.msr3) as `max(msr3)`"
          + ", sum(testcube.msr2) as `msr2` FROM ", null,
        " group by testcube.dim1", expectedRangeWhere, "C1_summary1");
    compareQueries(hqlQuery, expected);
    hqlQuery =
      rewrite("select dim1, dim2, COUNT(msr4)," + " SUM(msr2), msr3 from testCube" + " where " + TWO_DAYS_RANGE
        + " OR " + TWO_DAYS_RANGE_BEFORE_4_DAYS, getConfWithStorages("C1"));
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select testcube.dim1 as `dim1`, testcube.dim2 as `dim2`, "
          + "count(testcube.msr4) as `COUNT(msr4`, sum(testcube.msr2) as `sum(msr2)`, "
          + "max(testcube.msr3) as `msr3` FROM ", null, " group by testcube.dim1, testcube.dim2",
        expectedRangeWhere, "C1_summary2");
    compareQueries(hqlQuery, expected);
    hqlQuery =
      rewrite("select dim1, dim2, cityid, count(msr4)," + " SUM(msr2), msr3 from testCube" + " where " + TWO_DAYS_RANGE
        + " OR " + TWO_DAYS_RANGE_BEFORE_4_DAYS, getConfWithStorages("C1"));
    expected =
      getExpectedQuery(TEST_CUBE_NAME, "select testcube.dim1 as `dim1`, testcube.dim2 as `dim2`, "
          + "testcube.cityid as `cityid`, count(testcube.msr4) as `count(msr4)`, "
          + "sum(testcube.msr2) as `sum(msr2)`, max(testcube.msr3)  as `msr3` FROM ", null,
        " group by testcube.dim1, testcube.dim2, testcube.cityid", expectedRangeWhere, "C1_summary3");
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testDistinctColWithoutAlias() throws Exception {
    String hqlQuery = rewrite("select DISTINCT name, stateid" + " from citydim", getConf());
    String expected =
      getExpectedQuery("citydim", "select DISTINCT" + " citydim.name, citydim.stateid from ", null, "c1_citytable",
        true);
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select id, sum(distinct id) from" + " citydim group by id", getConf());
    expected =
      getExpectedQuery("citydim", "select citydim.id," + " sum(DISTINCT citydim.id) from ", "group by citydim.id",
        "c1_citytable", true);
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select count(distinct id) from" + " citydim", getConf());
    expected = getExpectedQuery("citydim", "select count(DISTINCT" + " citydim.id) from ", null, "c1_citytable", true);
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testJoinWithMultipleAliases() throws Exception {
    String cubeQl =
      "SELECT SUM(msr2) from testCube left outer join citydim c1 on testCube.cityid = c1.id"
        + " left outer join statedim s1 on c1.stateid = s1.id"
        + " left outer join citydim c2 on s1.countryid = c2.id where " + TWO_DAYS_RANGE;
    Configuration conf = getConfWithStorages("C1");
    conf.setBoolean(DISABLE_AUTO_JOINS, true);
    String hqlQuery = rewrite(cubeQl, conf);
    String db = getDbName();
    String expectedJoin =
      " LEFT OUTER JOIN " + db + "c1_citytable c1 ON (( testcube . cityid ) = ( c1 . id )) AND (c1.dt = 'latest') "
        + " LEFT OUTER JOIN " + db
        + "c1_statetable s1 ON (( c1 . stateid ) = ( s1 . id )) AND (s1.dt = 'latest') " + " LEFT OUTER JOIN "
        + db + "c1_citytable c2 ON (( s1 . countryid ) = ( c2 . id )) AND (c2.dt = 'latest')";

    String expected =
      getExpectedQuery(TEST_CUBE_NAME, "select sum(testcube.msr2) as `sum(msr2)`"
          + " FROM ", expectedJoin, null, null, null,
        getWhereForHourly2days(TEST_CUBE_NAME, "C1_testfact2"));
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testJoinPathColumnLifeValidation() throws Exception {
    HiveConf testConf = new HiveConf(new HiveConf(getConf(), HiveConf.class));
    testConf.setBoolean(DISABLE_AUTO_JOINS, false);
    // Set column life of dim2 column in testCube
    CubeMetastoreClient client = CubeMetastoreClient.getInstance(testConf);
    Cube cube = (Cube) client.getCube(TEST_CUBE_NAME);

    BaseDimAttribute col = (BaseDimAttribute) cube.getColumnByName("cdim2");
    assertNotNull(col);

    final String query = "SELECT cdimChain.name, msr2 FROM testCube where " + TWO_DAYS_RANGE;
    try {
      CubeQueryContext context = rewriteCtx(query, testConf);
      fail("Expected query to fail because of invalid column life");
    } catch (LensException exc) {
      assertEquals(exc.getErrorCode(), LensCubeErrorCode.NO_JOIN_PATH.getLensErrorInfo().getErrorCode());
    } finally {
      // Add old column back
      cube.alterDimension(col);
      client.alterCube(TEST_CUBE_NAME, cube);
    }

    // Assert same query succeeds with valid column
    Date oneWeekBack = DateUtils.addDays(TWODAYS_BACK, -7);

    // Alter cube.dim2 with an invalid column life
    BaseDimAttribute newDim2 =
      new BaseDimAttribute(new FieldSchema(col.getName(), "string", "invalid col"), col.getDisplayString(),
        oneWeekBack, null, col.getCost(), null);
    cube.alterDimension(newDim2);
    client.alterCube(TEST_CUBE_NAME, cube);
    String hql = rewrite(query, testConf);
    assertNotNull(hql);
  }

  @Test
  public void testCubeQueryWithSpaceInAlias() throws Exception {
    String query = "SELECT sum(msr2) as `a measure` from testCube where " + TWO_DAYS_RANGE;
    try {
      String hql = rewrite(query, getConf());
      assertNotNull(hql);
      // test that quotes are preserved
      assertTrue(hql.contains("`a measure`"));
      System.out.println("@@ hql: " + hql);
    } catch (NullPointerException npe) {
      log.error("Not expecting null pointer exception", npe);
      fail("Not expecting null pointer exception");
    }
  }

  @Test
  public void testTimeDimensionAndPartCol() throws Exception {
    // Test if time dimension is replaced with partition column
    // Disabling conf should not replace the time dimension

    String query =
      "SELECT test_time_dim, msr2 FROM testCube where " + TWO_DAYS_RANGE_TTD;

    HiveConf hconf = new HiveConf(getConf(), TestCubeRewriter.class);
    hconf.setBoolean(DISABLE_AUTO_JOINS, false);
    hconf.set(DRIVER_SUPPORTED_STORAGES, "C1,C2,C3,C4");
    hconf.setBoolean(CubeQueryConfUtil.REPLACE_TIMEDIM_WITH_PART_COL, true);

    CubeQueryRewriter rewriter = new CubeQueryRewriter(hconf, hconf);
    CubeQueryContext context = rewriter.rewrite(query);
    String hql = context.toHQL();
    System.out.println("@@" + hql);
    assertTrue(hql.contains("ttd") && hql.contains("full_hour"));

    assertTrue(context.shouldReplaceTimeDimWithPart());

    String partCol = context.getPartitionColumnOfTimeDim("test_time_dim");
    assertEquals("ttd", partCol);

    String timeDimCol = context.getTimeDimOfPartitionColumn("ttd");
    assertEquals("test_time_dim".toLowerCase(), timeDimCol);

    // Rewrite with setting disabled
    hconf.setBoolean(CubeQueryConfUtil.REPLACE_TIMEDIM_WITH_PART_COL, false);
    rewriter = new CubeQueryRewriter(hconf, hconf);
    context = rewriter.rewrite(query);
    hql = context.toHQL();
    System.out.println("@@2 " + hql);
    assertTrue(!hql.contains("ttd") && hql.contains("full_hour"));
  }

  @Test
  public void testAliasNameSameAsColumnName() throws Exception {
    String query = "SELECT msr2 as msr2 from testCube WHERE " + TWO_DAYS_RANGE;
    try {
      String hql = rewrite(query, getConf());
      assertNotNull(hql);
      System.out.println("@@HQL " + hql);
    } catch (NullPointerException npe) {
      fail(npe.getMessage());
      log.error("Not expecting null pointer exception", npe);
    }
  }

  @Test
  public void testDimAttributeQueryWithFact() throws Exception {
    String query = "select count (distinct dim1) from testCube where " + TWO_DAYS_RANGE;
    String hql = rewrite(query, getConf());
    assertTrue(hql.contains("summary1"));
  }

  @Test
  public void testFactColumnStartAndEndTime() throws Exception{
    // Start time for dim attribute user_id_added_in_past is 2016-01-01
    String query1 = "select user_id_added_in_past from basecube where " + TWO_DAYS_RANGE;
    String hql1 = rewrite(query1, getConf());
    assertTrue(hql1.contains("c1_testfact4_raw_base"));
    // Start time for dim attribute user_id_added_far_future is 2099-01-01
    String query2 = "select user_id_added_far_future from basecube where " + TWO_DAYS_RANGE;
    assertLensExceptionInRewrite(query2, getConf(), NO_FACT_HAS_COLUMN);
    // End time for dim attribute user_id_deprecated is 2016-01-01
    String query3 = "select user_id_deprecated from basecube where " + TWO_DAYS_RANGE;
    assertLensExceptionInRewrite(query3, getConf(), NO_FACT_HAS_COLUMN);
    // Start time for ref column user_id_added_far_future_chain is 2099-01-01
    String query4 = "select user_id_added_far_future_chain.name from basecube where " + TWO_DAYS_RANGE;
    assertLensExceptionInRewrite(query4, getConf(), NO_CANDIDATE_FACT_AVAILABLE);
  }

  @Test
  public void testSelectDimonlyJoinOnCube() throws Exception {
    String query = "SELECT count (distinct cubecity.name) from testCube where " + TWO_DAYS_RANGE;
    Configuration conf = new Configuration(getConf());
    conf.setBoolean(DISABLE_AUTO_JOINS, false);
    String hql = rewrite(query, conf);
    System.out.println("@@ HQL = " + hql);
    assertNotNull(hql);
  }

  @Test
  public void testInTimeRangeWriterWithHQL() throws Exception {
    // For queries with large number of partitions, the where clause generated using
    // the ORTimeRangeWriter causes a stack overflow exception because the operator tree of the where clause
    // gets too deep.

    // In this test, we rewrite the query once with the InTimeRangeWriter and once with ORTimeRangeWriter
    // Explain extended for the  query rewritten with IN clauses passes, while the OR query fails with
    // stack overflow.

    // Also, we can verify by printing the explain output that partitions are indeed getting identified with
    // the IN clause


    // Test 1 - check for contained part columns
    String query = "select dim1, max(msr3)," + " msr2 from testCube" + " where " + TWO_DAYS_RANGE_IT;

    HiveConf conf = new HiveConf(getConf(), TestCubeRewriter.class);
    conf.set(CubeQueryConfUtil.PROCESS_TIME_PART_COL, "pt");
    conf.setClass(CubeQueryConfUtil.TIME_RANGE_WRITER_CLASS,
      AbridgedTimeRangeWriter.class.asSubclass(TimeRangeWriter.class), TimeRangeWriter.class);

    String hqlWithInClause = rewrite(query, conf);
    System.out.println("@@ HQL with IN and OR: " + hqlWithInClause);

    // Run explain on this command, it should pass successfully.
    CommandProcessorResponse inExplainResponse = runExplain(hqlWithInClause, conf);
    assertNotNull(inExplainResponse);
    assertTrue(hqlWithInClause.contains("in"));

    // Test 2 - check for single part column
    // Verify for large number of partitions, single column. This is just to check if we don't see
    // errors on explain of large conditions
    String largePartQuery = "SELECT msr1 from testCube WHERE " + TWO_MONTHS_RANGE_UPTO_HOURS;
    HiveConf largeConf = new HiveConf(getConf(), TestCubeRewriter.class);
    largeConf.setClass(CubeQueryConfUtil.TIME_RANGE_WRITER_CLASS,
      AbridgedTimeRangeWriter.class.asSubclass(TimeRangeWriter.class), TimeRangeWriter.class);

    String largePartRewrittenQuery = rewrite(largePartQuery, largeConf);
    CommandProcessorResponse response = runExplain(largePartRewrittenQuery, largeConf);
    assertNotNull(response);
    assertTrue(largePartRewrittenQuery.contains("in"));
  }

  private CommandProcessorResponse runExplain(String hql, HiveConf conf) throws Exception {
    Driver hiveDriver = new Driver(conf, "anonymous");
    CommandProcessorResponse response = hiveDriver.run("EXPLAIN EXTENDED " + hql);
    hiveDriver.resetFetch();
    hiveDriver.setMaxRows(Integer.MAX_VALUE);
    List<Object> explainResult = new ArrayList<Object>();
    hiveDriver.getResults(explainResult);

    for (Object explainRow : explainResult) {
      // Print the following to stdout to check partition output.
      // Not parsing the output because it will slow down the test
      assertNotNull(explainRow.toString());
    }

    return response;
  }
}

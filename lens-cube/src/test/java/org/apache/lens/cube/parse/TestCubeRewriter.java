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

import static org.apache.lens.cube.metadata.UpdatePeriod.DAILY;
import static org.apache.lens.cube.metadata.UpdatePeriod.HOURLY;
import static org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode.*;
import static org.apache.lens.cube.parse.CubeTestSetup.*;

import static org.testng.Assert.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.*;
import org.apache.lens.cube.parse.CandidateTablePruneCause.SkipStorageCause;
import org.apache.lens.cube.parse.CandidateTablePruneCause.SkipStorageCode;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.time.DateUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestCubeRewriter extends TestQueryRewrite {

  private final String cubeName = CubeTestSetup.TEST_CUBE_NAME;

  private Configuration conf;

  @BeforeTest
  public void setupDriver() throws Exception {
    conf = new Configuration();
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C0,C1,C2");
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, true);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY, true);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT, true);
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER, false);
  }

  @Override
  public Configuration getConf() {
    return new Configuration(conf);
  }

  @Test
  public void testQueryWithNow() throws Exception {
    LensException e = getLensExceptionInRewrite(
      "select SUM(msr2) from testCube where" + " time_range_in(d_time, 'NOW - 2DAYS', 'NOW')", getConf());
    assertEquals(e.getErrorCode(), LensCubeErrorCode.NO_CANDIDATE_FACT_AVAILABLE.getLensErrorInfo().getErrorCode());
  }

  @Test
  public void testQueryWithContinuousUpdatePeriod() throws Exception {
    Configuration conf = getConf();
    conf.setClass(CubeQueryConfUtil.TIME_RANGE_WRITER_CLASS, BetweenTimeRangeWriter.class, TimeRangeWriter.class);

    DateFormat qFmt = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
    Calendar qCal = Calendar.getInstance();
    Date toDate = qCal.getTime();
    String qTo = qFmt.format(toDate);
    qCal.setTime(TWODAYS_BACK);
    Date from2DaysBackDate = qCal.getTime();
    String qFrom = qFmt.format(from2DaysBackDate);

    CubeQueryContext rewrittenQuery = rewriteCtx("select SUM(msr15) from testCube where"
      + " time_range_in(d_time, '" + qFrom + "', '" + qTo + "')", conf);

    DateFormat fmt = UpdatePeriod.CONTINUOUS.format();
    String to = fmt.format(toDate);
    String from = fmt.format(from2DaysBackDate);

    String expected = "select SUM((testCube.msr15)) from TestQueryRewrite.c0_testFact_CONTINUOUS testcube"
      + " WHERE ((( testcube . dt ) between  '" + from + "'  and  '" + to + "' ))";
    System.out.println("rewrittenQuery.toHQL() " + rewrittenQuery.toHQL());
    System.out.println("expected " + expected);
    compareQueries(rewrittenQuery.toHQL(), expected);

    //test with msr2 on different fact
    rewrittenQuery = rewriteCtx("select SUM(msr2) from testCube where" + " time_range_in(d_time, '"
      + qFrom + "', '" + qTo + "')", conf);
    expected = "select SUM((testCube.msr2)) from TestQueryRewrite.c0_testFact testcube"
      + " WHERE ((( testcube . dt ) between  '" + from + "'  and  '" + to + "' ))";
    System.out.println("rewrittenQuery.toHQL() " + rewrittenQuery.toHQL());
    System.out.println("expected " + expected);
    compareQueries(rewrittenQuery.toHQL(), expected);

    //from date 4 days back
    qCal.setTime(BEFORE_4_DAYS_START);
    Date from4DaysBackDate = qCal.getTime();
    String qFrom4DaysBackDate = qFmt.format(from4DaysBackDate);
    LensException th = getLensExceptionInRewrite("select SUM(msr15) from testCube where"
      + " time_range_in(d_time, '" + qFrom4DaysBackDate + "', '" + qTo + "')", getConf());
    assertEquals(th.getErrorCode(), LensCubeErrorCode.NO_CANDIDATE_FACT_AVAILABLE.getLensErrorInfo().getErrorCode());
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
      rewriteCtx("cube select" + " SUM(msr2) from testCube where " + TWO_DAYS_RANGE, getConfWithStorages("C2"));
    String expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(rewrittenQuery.toHQL(), expected);
    System.out.println("Non existing parts:" + rewrittenQuery.getNonExistingParts());
    assertNotNull(rewrittenQuery.getNonExistingParts());
  }

  @Test
  public void testMaxCoveringFact() throws Exception {
    Configuration conf = getConf();
    conf.setClass(CubeQueryConfUtil.TIME_RANGE_WRITER_CLASS, AbridgedTimeRangeWriter.class, TimeRangeWriter.class);
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1,C2,C4");
    CubeQueryContext cubeQueryContext =
      rewriteCtx("cube select" + " SUM(msr2) from testCube where " + THIS_YEAR_RANGE, conf);
    PruneCauses<CubeFactTable> pruneCause = cubeQueryContext.getFactPruningMsgs();
    int lessDataCauses = 0;
    for (Map.Entry<CubeFactTable, List<CandidateTablePruneCause>> entry : pruneCause.entrySet()) {
      for (CandidateTablePruneCause cause : entry.getValue()) {
        if (cause.getCause().equals(LESS_DATA)) {
          lessDataCauses++;
        }
      }
    }
    assertTrue(lessDataCauses > 0);
  }

  @Test
  public void testLightestFactFirst() throws Exception {
    // testFact is lighter than testFact2.
    String hqlQuery = rewrite("cube select" + " SUM(msr2) from testCube where " + TWO_DAYS_RANGE, getConfWithStorages(
      "C2"));
    String expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    Configuration conf = getConfWithStorages("C1");
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForHourly2days("c1_testfact2"));
    compareQueries(hqlQuery, expected);

    conf.setBoolean(CubeQueryConfUtil.LIGHTEST_FACT_FIRST, true);

    LensException th = getLensExceptionInRewrite(
      "select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    assertEquals(th.getErrorCode(), LensCubeErrorCode.NO_CANDIDATE_FACT_AVAILABLE.getLensErrorInfo().getErrorCode());
    PruneCauses.BriefAndDetailedError pruneCauses = extractPruneCause(th);
    int endIndex = MISSING_PARTITIONS.errorFormat.length() - 3;
    assertEquals(
      pruneCauses.getBrief().substring(0, endIndex),
      MISSING_PARTITIONS.errorFormat.substring(0, endIndex)
    );
    assertEquals(pruneCauses.getDetails().get("testfact").size(), 1);
    assertEquals(pruneCauses.getDetails().get("testfact").iterator().next().getCause(),
      MISSING_PARTITIONS);
  }

  @Test
  public void testDerivedCube() throws ParseException, LensException, HiveException, ClassNotFoundException {
    CubeQueryContext rewrittenQuery =
      rewriteCtx("cube select" + " SUM(msr2) from derivedCube where " + TWO_DAYS_RANGE, getConfWithStorages("C2"));
    String expected =
      getExpectedQuery(CubeTestSetup.DERIVED_CUBE_NAME, "select sum(derivedCube.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days(CubeTestSetup.DERIVED_CUBE_NAME, "C2_testfact"));
    compareQueries(rewrittenQuery.toHQL(), expected);
    System.out.println("Non existing parts:" + rewrittenQuery.getNonExistingParts());
    assertNotNull(rewrittenQuery.getNonExistingParts());

    LensException th = getLensExceptionInRewrite(
      "select SUM(msr4) from derivedCube" + " where " + TWO_DAYS_RANGE, getConf());
    assertEquals(th.getErrorCode(), LensCubeErrorCode.COLUMN_NOT_FOUND.getLensErrorInfo().getErrorCode());

    // test join
    Configuration conf = getConf();
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
    String hqlQuery;

    hqlQuery = rewrite("cube select" + " testdim2.name, SUM(msr2) from derivedCube where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(CubeTestSetup.DERIVED_CUBE_NAME, "select testdim2.name, sum(derivedCube.msr2) FROM ", " JOIN "
          + getDbName() + "c1_testdim2tbl testdim2 ON derivedCube.dim2 = "
          + " testdim2.id and (testdim2.dt = 'latest') ", null, "group by (testdim2.name)", null,
        getWhereForDailyAndHourly2days(CubeTestSetup.DERIVED_CUBE_NAME, "c1_summary2"));
    compareQueries(hqlQuery, expected);

    // Test that explicit join query passes with join resolver disabled
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, true);
    List<String> joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageUtil.getWherePartClause("dt", "testdim2", StorageConstants.getPartitionsForLatest()));
    hqlQuery =
      rewrite("cube select" + " testdim2.name, SUM(msr2) from derivedCube "
        + " inner join testdim2 on derivedCube.dim2 = testdim2.id " + "where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(CubeTestSetup.DERIVED_CUBE_NAME, "select testdim2.name, sum(derivedCube.msr2) FROM ",
        " inner JOIN " + getDbName() + "c1_testdim2tbl testdim2 ON derivedCube.dim2 = " + " testdim2.id ", null,
        "group by (testdim2.name)", joinWhereConds,
        getWhereForDailyAndHourly2days(CubeTestSetup.DERIVED_CUBE_NAME, "c1_summary2"));
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testCubeInsert() throws Exception {
    Configuration conf = getConf();
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    String hqlQuery = rewrite("insert overwrite directory" + " 'target/test' select SUM(msr2) from testCube where "
      + TWO_DAYS_RANGE, conf);
    Map<String, String> wh = getWhereForDailyAndHourly2days(cubeName, "C2_testfact");
    String expected = "insert overwrite directory 'target/test' "
      + getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null, wh);
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("insert overwrite directory" + " 'target/test' cube select SUM(msr2) from testCube where "
      + TWO_DAYS_RANGE, conf);
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("insert overwrite local directory" + " 'target/test' select SUM(msr2) from testCube where "
      + TWO_DAYS_RANGE, conf);
    wh = getWhereForDailyAndHourly2days(cubeName, "C2_testfact");
    expected = "insert overwrite local directory 'target/test' "
      + getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null, wh);
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("insert overwrite local directory" + " 'target/test' cube select SUM(msr2) from testCube where "
      + TWO_DAYS_RANGE, conf);
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("insert overwrite table temp" + " select SUM(msr2) from testCube where " + TWO_DAYS_RANGE,
      conf);
    wh = getWhereForDailyAndHourly2days(cubeName, "C2_testfact");
    expected = "insert overwrite table temp "
      + getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null, wh);
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("insert overwrite table temp" + " cube select SUM(msr2) from testCube where " + TWO_DAYS_RANGE,
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
    assertTrue(actualTrimmed.toLowerCase().contains(expectedTrimmed.toLowerCase()));
  }

  @Test
  public void testCubeWhereQuery() throws Exception {
    String hqlQuery, expected;
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, getConfWithStorages("C2"));
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    // Test with partition existence
    Configuration conf = getConf();
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForHourly2days("c1_testfact2"));
    compareQueries(hqlQuery, expected);
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);

    // Tests for valid tables
    conf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact");
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    compareQueries(hqlQuery, expected);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    conf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    conf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact2");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForHourly2days("c1_testfact2"));
    compareQueries(hqlQuery, expected);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    conf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact2");
    conf.set(CubeQueryConfUtil.getValidStorageTablesKey("testFact2"), "C1_testFact2");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForHourly2days("c1_testfact2"));
    compareQueries(hqlQuery, expected);

    conf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact");
    conf.set(CubeQueryConfUtil.getValidStorageTablesKey("testfact"), "C1_testFact");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact", "C1"), "HOURLY");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null, getWhereForHourly2days("c1_testfact"));
    compareQueries(hqlQuery, expected);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    conf.set(CubeQueryConfUtil.getValidStorageTablesKey("testfact"), "C2_testFact");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact", "C2"), "HOURLY");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null, getWhereForHourly2days("c2_testfact"));
    compareQueries(hqlQuery, expected);

    // max interval test
    conf = new Configuration();
    conf.set(CubeQueryConfUtil.QUERY_MAX_INTERVAL, "HOURLY");
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1,C2");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null, getWhereForHourly2days("c1_testfact2"));
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testCubeWhereQueryDuplicatePartitionElimination() throws Exception {
    Configuration conf = getConf();
    conf.set(CubeQueryConfUtil.getValidStorageTablesKey("testfact"), "C1_testFact,C2_testFact");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact", "C1"), "DAILY,HOURLY");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact2", "C1"), "YEARLY");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact", "C2"), "MONTHLY,DAILY");

    try {
      CubeTestSetup.getStorageToUpdatePeriodMap().put("HOURLY", "c1_testfact");
      CubeTestSetup.getStorageToUpdatePeriodMap().put("DAILY", "c1_testfact");
      CubeTestSetup.getStorageToUpdatePeriodMap().put("MONTHLY", "c2_testfact");

      // Union query
      String hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_MONTHS_RANGE_UPTO_HOURS, conf);
      System.out.println("HQL: " + hqlQuery);

      String expected1 = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForMonthlyDailyAndHourly2monthsUnionQuery("c1_testfact"));
      String expected2 = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForMonthlyDailyAndHourly2monthsUnionQuery("c2_testfact"));

      System.out.println("Expected1 : " + expected1);
      System.out.println("Expected2 : " + expected2);

      TestCubeRewriter.compareContains(expected1, hqlQuery);
      TestCubeRewriter.compareContains(expected2, hqlQuery);
      TestCubeRewriter.compareContains("UNION ALL", hqlQuery);
    } finally {
      CubeTestSetup.getStorageToUpdatePeriodMap().clear();
    }

  }

  @Test
  public void testCubeWhereQueryWithMultipleTables() throws Exception {
    Configuration conf = getConf();
    conf.set(CubeQueryConfUtil.getValidStorageTablesKey("testfact"), "C1_testFact,C2_testFact");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact", "C1"), "DAILY");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact2", "C1"), "YEARLY");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact", "C2"), "HOURLY");

    CubeTestSetup.getStorageToUpdatePeriodMap().put("c1_testfact", "DAILY");
    CubeTestSetup.getStorageToUpdatePeriodMap().put("c2_testfact", "HOURLY");

    try {
      // Union query
      String hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
      System.out.println("HQL:" + hqlQuery);

      String expected1 = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "c1_testfact"));
      String expected2 = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "c2_testfact"));

      System.out.println("Expected1 : " + expected1);
      System.out.println("Expected2 : " + expected2);

      TestCubeRewriter.compareContains(expected1, hqlQuery);
      TestCubeRewriter.compareContains(expected2, hqlQuery);
      TestCubeRewriter.compareContains("UNION ALL", hqlQuery);
    } finally {
      CubeTestSetup.getStorageToUpdatePeriodMap().clear();
    }
  }

  @Test
  public void testCubeWhereQueryWithMultipleTablesForMonth() throws Exception {
    Configuration conf = getConf();
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C0,C1,C2,C3,C4,C5");
    conf.set(CubeQueryConfUtil.getValidStorageTablesKey("testfact"), "");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact", "C1"), "HOURLY");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact2", "C1"), "YEARLY");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact2_raw", "C3"), "YEARLY");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact", "C2"), "DAILY");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact", "C3"), "MONTHLY");

    CubeTestSetup.getStorageToUpdatePeriodMap().put("HOURLY", "c1_testfact");
    CubeTestSetup.getStorageToUpdatePeriodMap().put("DAILY", "c2_testfact");
    CubeTestSetup.getStorageToUpdatePeriodMap().put("MONTHLY", "c3_testfact");

    try {
      // Union query
      String hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_MONTHS_RANGE_UPTO_HOURS, conf);
      System.out.println("HQL:" + hqlQuery);

      String expected1 = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForMonthlyDailyAndHourly2monthsUnionQuery("c1_testfact"));
      String expected2 = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForMonthlyDailyAndHourly2monthsUnionQuery("c2_testfact"));
      String expected3 = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForMonthlyDailyAndHourly2monthsUnionQuery("c3_testfact"));

      System.out.println("Expected1 : " + expected1);
      System.out.println("Expected2 : " + expected2);
      System.out.println("Expected3 : " + expected3);

      TestCubeRewriter.compareContains(expected1, hqlQuery);
      TestCubeRewriter.compareContains(expected2, hqlQuery);
      TestCubeRewriter.compareContains(expected3, hqlQuery);
      TestCubeRewriter.compareContains("UNION ALL", hqlQuery);
    } finally {
      CubeTestSetup.getStorageToUpdatePeriodMap().clear();
    }
  }

  @Test
  public void testPartColAsQueryColumn() throws Exception {
    Configuration conf = getConf();
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C3");
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
    String hql, expected;
    hql = rewrite(
      "select countrydim.name, msr2 from" + " testCube" + " where countrydim.region = 'asia' and "
        + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "select countrydim.name, sum(testcube.msr2)" + " FROM ", " JOIN " + getDbName()
          + "c3_countrytable_partitioned countrydim on testcube.countryid=countrydim.id and countrydim.dt='latest'",
        "countrydim.region='asia'",
        " group by countrydim.name ", null,
        getWhereForHourly2days(cubeName, "C3_testfact2_raw"));
    compareQueries(hql, expected);
    hql = rewrite(
      "select statedim.name, statedim.countryid, msr2 from" + " testCube" + " where statedim.countryid = 5 and "
        + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "select statedim.name, statedim.countryid, sum(testcube.msr2)" + " FROM ",
        " JOIN " + getDbName()
          + "c3_statetable_partitioned statedim ON" + " testCube.stateid = statedim.id and statedim.dt = 'latest'",
        "statedim.countryid=5",
        " group by statedim.name, statedim.countryid", null,
        getWhereForHourly2days(cubeName, "C3_testfact2_raw"));
    compareQueries(hql, expected);
  }

  @Test
  public void testCubeJoinQuery() throws Exception {
    // q1
    Configuration conf = getConf();
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    String hqlQuery =
      rewrite("select SUM(msr2) from testCube" + " join citydim on testCube.cityid = citydim.id" + " where "
        + TWO_DAYS_RANGE, conf);
    List<String> joinWhereConds = new ArrayList<String>();
    //    joinWhereConds.add(StorageUtil.getWherePartClause("dt", "citydim", StorageConstants.getPartitionsForLatest
    // ()));
    String expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2)" + " FROM ", " INNER JOIN " + getDbName()
          + "c2_citytable citydim ON" + " testCube.cityid = citydim.id", null, null, joinWhereConds,
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
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
    joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageUtil.getWherePartClause("dt", "citydim", StorageConstants.getPartitionsForLatest()));
    joinWhereConds.add(StorageUtil.getWherePartClause("dt", "zipdim", StorageConstants.getPartitionsForLatest()));
    expected =
      getExpectedQuery(cubeName, "select statedim.name," + " sum(testcube.msr2) FROM ", "INNER JOIN " + getDbName()
          + "c1_citytable citydim ON" + " testCube.cityid = citydim.id LEFT OUTER JOIN " + getDbName()
          + "c1_statetable statedim" + " ON statedim.id = citydim.stateid AND "
          + "(statedim.dt = 'latest') RIGHT OUTER JOIN " + getDbName() + "c1_ziptable"
          + " zipdim ON citydim.zipcode = zipdim.code", null, " group by" + " statedim.name ", joinWhereConds,
        getWhereForHourly2days(cubeName, "C1_testfact2"));
    compareQueries(hqlQuery, expected);

    // q3
    hqlQuery =
      rewrite("select st.name, SUM(msr2) from" + " testCube TC" + " join citydim CT on TC.cityid = CT.id"
        + " left outer join statedim ST on ST.id = CT.stateid"
        + " right outer join zipdim ZT on CT.zipcode = ZT.code" + " where " + TWO_DAYS_RANGE, getConf());
    joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageUtil.getWherePartClause("dt", "ct", StorageConstants.getPartitionsForLatest()));
    joinWhereConds.add(StorageUtil.getWherePartClause("dt", "zt", StorageConstants.getPartitionsForLatest()));
    expected =
      getExpectedQuery("tc", "select st.name," + " sum(tc.msr2) FROM ", " INNER JOIN " + getDbName()
          + "c1_citytable ct ON" + " tc.cityid = ct.id LEFT OUTER JOIN " + getDbName() + "c1_statetable st"
          + " ON st.id = ct.stateid and (st.dt = 'latest') " + "RIGHT OUTER JOIN " + getDbName() + "c1_ziptable"
          + " zt ON ct.zipcode = zt.code", null, " group by" + " st.name ", joinWhereConds,
        getWhereForHourly2days("tc", "C1_testfact2"));
    compareQueries(hqlQuery, expected);

    // q4
    hqlQuery =
      rewrite("select citydim.name, SUM(msr2) from" + " testCube"
        + " left outer join citydim on testCube.cityid = citydim.id"
        + " left outer join zipdim on citydim.zipcode = zipdim.code" + " where " + TWO_DAYS_RANGE, getConf());
    expected =
      getExpectedQuery(cubeName, "select citydim.name," + " sum(testcube.msr2) FROM ", " LEFT OUTER JOIN "
          + getDbName() + "c1_citytable citydim ON" + " testCube.cityid = citydim.id and (citydim.dt = 'latest') "
          + " LEFT OUTER JOIN " + getDbName() + "c1_ziptable" + " zipdim ON citydim.zipcode = zipdim.code AND "
          + "(zipdim.dt = 'latest')", null, " group by" + " citydim.name ", null,
        getWhereForHourly2days(cubeName, "C1_testfact2"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select SUM(msr2) from testCube" + " join countrydim on testCube.countryid = countrydim.id" + " where "
        + TWO_MONTHS_RANGE_UPTO_MONTH, getConf());
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", " INNER JOIN " + getDbName()
          + "c1_countrytable countrydim ON testCube.countryid = " + " countrydim.id", null, null, null,
        getWhereForMonthly2months("c2_testfactmonthly"));
    compareQueries(hqlQuery, expected);

    LensException th = getLensExceptionInRewrite(
      "select name, SUM(msr2) from testCube" + " join citydim" + " where " + TWO_DAYS_RANGE
        + " group by name", getConf());
    assertEquals(th.getErrorCode(), LensCubeErrorCode.NO_JOIN_CONDITION_AVAIABLE.getLensErrorInfo().getErrorCode());
  }

  @Test
  public void testCubeGroupbyWithConstantProjected() throws Exception {
    // check constants
    Configuration conf = getConf();
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    String hqlQuery1 = rewrite("select cityid, 99, \"placeHolder\", -1001, SUM(msr2) from testCube" + " where "
      + TWO_DAYS_RANGE, conf);
    String expected1 = getExpectedQuery(cubeName, "select testcube.cityid, 99, \"placeHolder\", -1001,"
        + " sum(testcube.msr2) FROM ", null, " group by testcube.cityid ",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(hqlQuery1, expected1);

    // check constants with expression
    String hqlQuery2 = rewrite(
      "select cityid, case when stateid = 'za' then \"Not Available\" end, 99, \"placeHolder\", -1001, "
        + "SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    String expected2 = getExpectedQuery(cubeName,
      "select testcube.cityid, case when testcube.stateid = 'za' then \"Not Available\" end, 99, \"placeHolder\","
        + " -1001, sum(testcube.msr2) FROM ", null,
      " group by testcube.cityid, case when testcube.stateid = 'za' then \"Not Available\" end ",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(hqlQuery2, expected2);

    // check expression with boolean and numeric constants
    String hqlQuery3 = rewrite(
      "select cityid,stateid + 99, 44 + stateid, stateid - 33, 999 - stateid, TRUE, FALSE, round(123.4567,2), "
        + "case when stateid='za' then 99 else -1001 end,  "
        + "SUM(msr2), SUM(msr2 + 39), SUM(msr2) + 567 from testCube" + " where " + TWO_DAYS_RANGE, conf);
    String expected3 = getExpectedQuery(
      cubeName,
      "select testcube.cityid, testcube.stateid + 99, 44 + testcube.stateid, testcube.stateid - 33,"
        + " 999 - testcube.stateid, TRUE, FALSE, round(123.4567,2), "
        + "case when testcube.stateid='za' then 99 else -1001 end,"
        + " sum(testcube.msr2), sum(testcube.msr2 + 39), sum(testcube.msr2) + 567 FROM ",
      null,
      " group by testcube.cityid,testcube.stateid + 99, 44 + testcube.stateid, testcube.stateid - 33, "
        + "999 - testcube.stateid, "
        + " case when testcube.stateid='za' then 99 else -1001 end ",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(hqlQuery3, expected3);
  }

  @Test
  public void testCubeGroupbyQuery() throws Exception {
    Configuration conf = getConf();
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    String hqlQuery =
      rewrite("select name, SUM(msr2) from" + " testCube join citydim on testCube.cityid = citydim.id where "
        + TWO_DAYS_RANGE, conf);
    List<String> joinWhereConds = new ArrayList<String>();
    //    joinWhereConds.add(StorageUtil.getWherePartClause("dt", "citydim", StorageConstants.getPartitionsForLatest
    // ()));
    String expected =
      getExpectedQuery(cubeName, "select citydim.name," + " sum(testcube.msr2) FROM ", "INNER JOIN " + getDbName()
          + "c2_citytable citydim ON" + " testCube.cityid = citydim.id", null, " group by citydim.name ",
        joinWhereConds, getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select SUM(msr2) from testCube" + " join citydim on testCube.cityid = citydim.id" + " where "
        + TWO_DAYS_RANGE + " group by name", conf);
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite("select cityid, SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "select testcube.cityid," + " sum(testcube.msr2) FROM ", null,
        " group by testcube.cityid ", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite("select round(cityid), SUM(msr2) from" + " testCube where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "select round(testcube.cityid)," + " sum(testcube.msr2) FROM ", null,
        " group by round(testcube.cityid) ", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery =
      rewrite("select SUM(msr2) from testCube" + "  where " + TWO_DAYS_RANGE + "group by round(zipcode)", conf);
    expected =
      getExpectedQuery(cubeName, "select round(testcube.zipcode)," + " sum(testcube.msr2) FROM ", null,
        " group by round(testcube.zipcode) ", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select round(cityid), SUM(msr2) from" + " testCube where " + TWO_DAYS_RANGE + " group by zipcode",
        conf);
    expected =
      getExpectedQuery(cubeName, "select " + " round(testcube.cityid), sum(testcube.msr2) FROM ", null,
        " group by testcube.zipcode", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select round(cityid), SUM(msr2) from" + " testCube where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "select " + " round(testcube.cityid), sum(testcube.msr2) FROM ", null,
        " group by round(testcube.cityid)", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select cityid, SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE + " group by round(zipcode)",
        conf);
    expected =
      getExpectedQuery(cubeName, "select " + " testcube.cityid, sum(testcube.msr2) FROM ", null,
        " group by round(testcube.zipcode)", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE + " group by round(zipcode)", conf);
    expected =
      getExpectedQuery(cubeName, "select round(testcube.zipcode)," + " sum(testcube.msr2) FROM ", null,
        " group by round(testcube.zipcode)", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select cityid, msr2 from testCube" + " where " + TWO_DAYS_RANGE + " group by round(zipcode)", conf);
    expected =
      getExpectedQuery(cubeName, "select " + " testcube.cityid, sum(testcube.msr2) FROM ", null,
        " group by round(testcube.zipcode)", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select round(zipcode) rzc," + " msr2 from testCube where " + TWO_DAYS_RANGE + " group by zipcode"
        + " order by rzc", conf);
    expected =
      getExpectedQuery(cubeName, "select round(testcube.zipcode) as `rzc`," + " sum(testcube.msr2) FROM ", null,
        " group by testcube.zipcode  order by rzc asc", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    // rewrite with expressions
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1, C2");
    hqlQuery =
      rewrite("SELECT citydim.name AS g1," + " CASE  WHEN citydim.name=='NULL'  THEN 'NULL' "
        + " WHEN citydim.name=='X'  THEN 'X-NAME' " + " WHEN citydim.name=='Y'  THEN 'Y-NAME' "
        + " ELSE 'DEFAULT'   END  AS g2, " + " statedim.name AS g3," + " statedim.id AS g4, "
        + " zipdim.code!=1  AND " + " ((zipdim.f1==\"xyz\"  AND  (zipdim.f2 >= \"3\"  AND "
        + "  zipdim.f2 !=\"NULL\"  AND  zipdim.f2 != \"uk\")) "
        + "  OR (zipdim.f2==\"adc\"  AND  zipdim.f1==\"js\" "
        + "  AND  ( citydim.name == \"X\"  OR  citydim.name == \"Y\" )) "
        + " OR ((zipdim.f1==\"api\"  OR  zipdim.f1==\"uk\"  OR  (zipdim.f1==\"adc\"  AND  zipdim.f1!=\"js\"))"
        + "  AND  citydim.id==12) ) AS g5," + " zipdim.code==1  AND "
        + " ((zipdim.f1==\"xyz\"  AND  (zipdim.f2 >= \"3\"  AND "
        + "  zipdim.f2 !=\"NULL\"  AND  zipdim.f2 != \"uk\")) "
        + " OR (zipdim.f2==\"adc\"  AND  zipdim.f1==\"js\" "
        + " AND  ( citydim.name == \"X\"  OR  citydim.name == \"Y\" )) "
        + "  OR ((zipdim.f1==\"api\"  OR  zipdim.f1==\"uk\"  OR  (zipdim.f1==\"adc\"  AND  zipdim.f1!=\"js\"))"
        + "    AND  citydim.id==12) ) AS g6, " + "  zipdim.f1 AS g7, "
        + "  format_number(SUM(msr1),\"##################.###\") AS a1,"
        + "  format_number(SUM(msr2),\"##################.###\") AS a2, "
        + "  format_number(SUM(msr3),\"##################.###\") AS a3, "
        + " format_number(SUM(msr1)+SUM(msr2), \"##################.###\") AS a4,"
        + "  format_number(SUM(msr1)+SUM(msr3),\"##################.###\") AS a5,"
        + " format_number(SUM(msr1)-(SUM(msr2)+SUM(msr3)),\"##################.###\") AS a6"
        + "  FROM testCube where " + TWO_DAYS_RANGE + " HAVING (SUM(msr1) >=1000)  AND (SUM(msr2)>=0.01)", conf);
    String actualExpr =
      ""
        + " join " + getDbName() + "c1_statetable statedim on testcube.stateid=statedim.id and (statedim.dt='latest')"
        + " join " + getDbName() + "c1_ziptable zipdim on testcube.zipcode = zipdim.code and (zipdim.dt = 'latest')  "
        + " join " + getDbName() + "c1_citytable citydim on testcube.cityid = citydim.id and (citydim.dt = 'latest')"
        + "";
    expected =
      getExpectedQuery(
        cubeName,
        "SELECT ( citydim.name ) as `g1` ,"
          + "  case  when (( citydim.name ) ==  'NULL' ) then  'NULL'  when (( citydim.name ) ==  'X' )"
          + " then  'X-NAME'  when (( citydim.name ) ==  'Y' ) then  'Y-NAME'"
          + "  else  'DEFAULT'  end  as `g2` , ( statedim.name ) as `g3` , ( statedim.id ) as `g4` ,"
          + " ((( zipdim.code ) !=  1 ) and ((((( zipdim.f1 ) ==  \"xyz\" )"
          + " and (((( zipdim.f2 ) >=  \"3\" ) and (( zipdim.f2 ) !=  \"NULL\" ))"
          + " and (( zipdim.f2 ) !=  \"uk\" ))) or (((( zipdim.f2 ) ==  \"adc\" )"
          + " and (( zipdim.f1 ) ==  \"js\" ))"
          + " and ((( citydim.name ) ==  \"X\" ) or (( citydim.name ) ==  \"Y\" ))))"
          + " or ((((( zipdim.f1 ) ==  \"api\" )"
          + " or (( zipdim.f1 ) ==  \"uk\" )) or ((( zipdim.f1 ) ==  \"adc\" )"
          + " and (( zipdim.f1 ) !=  \"js\" )))"
          + " and (( citydim.id ) ==  12 )))) as `g5` , ((( zipdim.code ) ==  1 )"
          + " and ((((( zipdim.f1 ) ==  \"xyz\" ) and (((( zipdim.f2 ) >=  \"3\" )"
          + " and (( zipdim.f2 ) !=  \"NULL\" ))"
          + " and (( zipdim.f2 ) !=  \"uk\" ))) or (((( zipdim.f2 ) ==  \"adc\" )"
          + " and (( zipdim.f1 ) ==  \"js\" ))"
          + " and ((( citydim.name ) ==  \"X\" ) or (( citydim.name ) ==  \"Y\" ))))"
          + " or ((((( zipdim.f1 ) ==  \"api\" )"
          + " or (( zipdim.f1 ) ==  \"uk\" )) or ((( zipdim.f1 ) ==  \"adc\" )"
          + " and (( zipdim.f1 ) !=  \"js\" )))"
          + " and (( citydim.id ) ==  12 )))) as `g6` , ( zipdim.f1 ) as `g7` ,"
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
        " GROUP BY ( citydim.name ), case  when (( citydim.name ) ==  'NULL' ) "
          + "then  'NULL'  when (( citydim.name ) ==  'X' ) then  'X-NAME'  when (( citydim.name ) ==  'Y' )"
          + " then  'Y-NAME'  else  'DEFAULT'  end, ( statedim.name ), ( statedim.id ),"
          + " ((( zipdim.code ) !=  1 ) and ((((( zipdim.f1 ) ==  \"xyz\" ) and (((( zipdim.f2 ) >=  \"3\" )"
          + " and (( zipdim.f2 ) !=  \"NULL\" )) and (( zipdim.f2 ) !=  \"uk\" ))) or (((( zipdim.f2 ) ==  \"adc\" )"
          + " and (( zipdim.f1 ) ==  \"js\" )) and ((( citydim.name ) ==  \"X\" ) or (( citydim.name ) ==  \"Y\" ))))"
          + " or ((((( zipdim.f1 ) ==  \"api\" ) or (( zipdim.f1 ) ==  \"uk\" )) or ((( zipdim.f1 ) ==  \"adc\" )"
          + " and (( zipdim.f1 ) !=  \"js\" ))) and (( citydim.id ) ==  12 )))), ((( zipdim.code ) ==  1 ) and"
          + " ((((( zipdim.f1 ) ==  \"xyz\" ) and (((( zipdim.f2 ) >=  \"3\" ) and (( zipdim.f2 ) !=  \"NULL\" ))"
          + " and (( zipdim.f2 ) !=  \"uk\" ))) or (((( zipdim.f2 ) ==  \"adc\" ) and (( zipdim.f1 ) ==  \"js\" ))"
          + " and ((( citydim.name ) ==  \"X\" ) or (( citydim.name ) ==  \"Y\" )))) or ((((( zipdim.f1 ) ==  \"api\" )"
          + " or (( zipdim.f1 ) ==  \"uk\" )) or ((( zipdim.f1 ) ==  \"adc\" ) and (( zipdim.f1 ) !=  \"js\" )))"
          + " and (( citydim.id ) ==  12 )))), ( zipdim.f1 ) HAVING ((sum(( testcube.msr1 )) >=  1000 ) "
          + "and (sum(( testcube.msr2 )) >=  0.01 ))",
        null, getWhereForHourly2days("c1_testfact2_raw"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite(
        "SELECT citydim.name AS g1,"
          + " CASE  WHEN citydim.name=='NULL'  THEN 'NULL' "
          + " WHEN citydim.name=='X'  THEN 'X-NAME' "
          + " WHEN citydim.name=='Y'  THEN 'Y-NAME' "
          + " ELSE 'DEFAULT'   END  AS g2, "
          + " statedim.name AS g3,"
          + " statedim.id AS g4, "
          + " zipdim.code!=1  AND "
          + " ((zipdim.f1==\"xyz\"  AND  (zipdim.f2 >= \"3\"  AND "
          + "  zipdim.f2 !=\"NULL\"  AND  zipdim.f2 != \"uk\")) "
          + "  OR (zipdim.f2==\"adc\"  AND  zipdim.f1==\"js\" "
          + "  AND  ( citydim.name == \"X\"  OR  citydim.name == \"Y\" )) "
          + " OR ((zipdim.f1==\"api\"  OR  zipdim.f1==\"uk\"  OR  (zipdim.f1==\"adc\"  AND  zipdim.f1!=\"js\"))"
          + "  AND  citydim.id==12) ) AS g5,"
          + " zipdim.code==1  AND "
          + " ((zipdim.f1==\"xyz\"  AND  (zipdim.f2 >= \"3\"  AND "
          + "  zipdim.f2 !=\"NULL\"  AND  zipdim.f2 != \"uk\")) "
          + " OR (zipdim.f2==\"adc\"  AND  zipdim.f1==\"js\" "
          + " AND  ( citydim.name == \"X\"  OR  citydim.name == \"Y\" )) "
          + "  OR ((zipdim.f1==\"api\"  OR  zipdim.f1==\"uk\"  OR  (zipdim.f1==\"adc\"  AND  zipdim.f1!=\"js\"))"
          + "    AND  citydim.id==12) ) AS g6, "
          + "  zipdim.f1 AS g7, "
          + "  format_number(SUM(msr1),\"##################.###\") AS a1,"
          + "  format_number(SUM(msr2),\"##################.###\") AS a2, "
          + "  format_number(SUM(msr3),\"##################.###\") AS a3, "
          + " format_number(SUM(msr1)+SUM(msr2), \"##################.###\") AS a4,"
          + "  format_number(SUM(msr1)+SUM(msr3),\"##################.###\") AS a5,"
          + " format_number(SUM(msr1)-(SUM(msr2)+SUM(msr3)),\"##################.###\") AS a6"
          + "  FROM testCube where "
          + TWO_DAYS_RANGE
          + " group by citydim.name, CASE WHEN citydim.name=='NULL' THEN 'NULL'"
          + " WHEN citydim.name=='X' THEN 'X-NAME' WHEN citydim.name=='Y' THEN 'Y-NAME'"
          + " ELSE 'DEFAULT'   END, statedim.name, statedim.id,  zipdim.code!=1  AND"
          + " ((zipdim.f1==\"xyz\"  AND  (zipdim.f2 >= \"3\"  AND zipdim.f2 !=\"NULL\"  AND  zipdim.f2 != \"uk\"))"
          + " OR (zipdim.f2==\"adc\"  AND  zipdim.f1==\"js\""
          + " AND ( citydim.name == \"X\"  OR  citydim.name == \"Y\" ))"
          + " OR ((zipdim.f1==\"api\"  OR  zipdim.f1==\"uk\"  OR  (zipdim.f1==\"adc\"  AND  zipdim.f1!=\"js\"))"
          + " AND  citydim.id==12) ),"
          + " zipdim.code==1  AND  ((zipdim.f1==\"xyz\" AND ( zipdim.f2 >= \"3\"  AND zipdim.f2 !=\"NULL\""
          + " AND  zipdim.f2 != \"uk\"))"
          + " OR (zipdim.f2==\"adc\"  AND  zipdim.f1==\"js\""
          + " AND  ( citydim.name == \"X\"  OR  citydim.name == \"Y\" ))"
          + " OR ((zipdim.f1=\"api\"  OR  zipdim.f1==\"uk\" OR (zipdim.f1==\"adc\"  AND  zipdim.f1!=\"js\")) AND"
          + " citydim.id==12))," + " zipdim.f1 " + "HAVING (SUM(msr1) >=1000)  AND (SUM(msr2)>=0.01)", conf);
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testSelectExprPromotionToGroupByWithSpacesInDimensionAliasAndWithAsKeywordBwColAndAlias()
    throws ParseException, LensException, HiveException {

    String inputQuery = "cube select name as `Alias With Spaces`, SUM(msr2) as `TestMeasure` from testCube join citydim"
      + " on testCube.cityid = citydim.id where " + LAST_HOUR_TIME_RANGE;

    String expectedRewrittenQuery = "SELECT ( citydim . name ) as `Alias With Spaces` , sum(( testcube . msr2 )) "
      + "as `TestMeasure`  FROM TestQueryRewrite.c2_testfact testcube inner JOIN TestQueryRewrite.c2_citytable citydim "
      + "ON (( testcube . cityid ) = ( citydim . id )) WHERE (((( testcube . dt ) =  '"
      + CubeTestSetup.getDateUptoHours(LAST_HOUR) + "' ))) GROUP BY ( citydim . name )";

    String actualRewrittenQuery = rewrite(inputQuery, getConfWithStorages("C2"));

    assertEquals(actualRewrittenQuery, expectedRewrittenQuery);
  }

  @Test
  public void testSelectExprPromotionToGroupByWithSpacesInDimensionAliasAndWithoutAsKeywordBwColAndAlias()
    throws ParseException, LensException, HiveException {

    String inputQuery = "cube select name `Alias With Spaces`, SUM(msr2) as `TestMeasure` from testCube join citydim"
      + " on testCube.cityid = citydim.id where " + LAST_HOUR_TIME_RANGE;

    String expectedRewrittenQuery = "SELECT ( citydim . name ) as `Alias With Spaces` , sum(( testcube . msr2 )) "
      + "as `TestMeasure`  FROM TestQueryRewrite.c2_testfact testcube inner JOIN TestQueryRewrite.c2_citytable citydim "
      + "ON (( testcube . cityid ) = ( citydim . id )) WHERE (((( testcube . dt ) =  '"
      + CubeTestSetup.getDateUptoHours(LAST_HOUR) + "' ))) GROUP BY ( citydim . name )";

    String actualRewrittenQuery = rewrite(inputQuery, getConfWithStorages("C2"));

    assertEquals(actualRewrittenQuery, expectedRewrittenQuery);
  }

  @Test
  public void testCubeQueryWithAilas() throws Exception {
    String hqlQuery = rewrite("select SUM(msr2) m2 from" + " testCube where " + TWO_DAYS_RANGE, getConfWithStorages(
      "C2"));
    String expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) as `m2` FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select SUM(msr2) from testCube mycube" + " where " + TWO_DAYS_RANGE, getConfWithStorages("C2"));
    expected =
      getExpectedQuery("mycube", "select sum(mycube.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days("mycube", "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select SUM(testCube.msr2) from testCube" + " where " + TWO_DAYS_RANGE, getConfWithStorages("C2"));
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select mycube.msr2 m2 from testCube" + " mycube where " + TWO_DAYS_RANGE, getConfWithStorages(
      "C2"));
    expected =
      getExpectedQuery("mycube", "select sum(mycube.msr2) as `m2` FROM ", null, null,
        getWhereForDailyAndHourly2days("mycube", "C2_testfact"));
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select testCube.msr2 m2 from testCube" + " where " + TWO_DAYS_RANGE, getConfWithStorages("C2"));
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) as `m2` FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testCubeWhereQueryForMonth() throws Exception {
    String hqlQuery =
      rewrite("select SUM(msr2) from testCube" + " where " + TWO_MONTHS_RANGE_UPTO_HOURS, getConfWithStorages("C2"));
    String expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForMonthlyDailyAndHourly2months("C2_testfact"));
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testCubeWhereQueryForMonthWithNoPartialData() throws Exception {
    Configuration conf = getConf();
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);

    LensException e = getLensExceptionInRewrite(
      "select SUM(msr2) from testCube" + " where " + TWO_MONTHS_RANGE_UPTO_HOURS, conf);
    assertEquals(e.getErrorCode(), LensCubeErrorCode.NO_CANDIDATE_FACT_AVAILABLE.getLensErrorInfo().getErrorCode());
    PruneCauses.BriefAndDetailedError pruneCauses = extractPruneCause(e);

    assertEquals(
      pruneCauses.getBrief().substring(0, MISSING_PARTITIONS.errorFormat.length() - 3),
      MISSING_PARTITIONS.errorFormat.substring(0,
        MISSING_PARTITIONS.errorFormat.length() - 3));

    Set<String> expectedSet =
      Sets.newTreeSet(Arrays.asList("summary1", "summary2", "testfact2_raw", "summary3", "testfact"));
    boolean missingPartitionCause = false;
    for (String key : pruneCauses.getDetails().keySet()) {
      Set<String> actualKeySet = Sets.newTreeSet(Splitter.on(',').split(key));
      if (expectedSet.equals(actualKeySet)) {
        assertEquals(pruneCauses.getDetails().get(key).iterator()
          .next().getCause(), MISSING_PARTITIONS);
        missingPartitionCause = true;
      }
    }
    assertTrue(missingPartitionCause, MISSING_PARTITIONS + " error does not occur for facttables set " + expectedSet);
    assertEquals(pruneCauses.getDetails().get("testfactmonthly").iterator().next().getCause(),
      NO_FACT_UPDATE_PERIODS_FOR_GIVEN_RANGE);
    assertEquals(pruneCauses.getDetails().get("testfact2").iterator().next().getCause(),
      MISSING_PARTITIONS);
    assertEquals(pruneCauses.getDetails().get("cheapfact").iterator().next().getCause(),
      NO_CANDIDATE_STORAGES);
    CandidateTablePruneCause cheapFactPruneCauses = pruneCauses.getDetails().get("cheapfact").iterator().next();
    assertEquals(cheapFactPruneCauses.getStorageCauses().get("c0").getCause(), SkipStorageCode.RANGE_NOT_ANSWERABLE);
    assertEquals(cheapFactPruneCauses.getStorageCauses().get("c99").getCause(), SkipStorageCode.UNSUPPORTED);
    assertEquals(pruneCauses.getDetails().get("summary4").iterator().next().getCause(), TIMEDIM_NOT_SUPPORTED);
    assertTrue(pruneCauses.getDetails().get("summary4").iterator().next().getUnsupportedTimeDims().contains("d_time"));
  }

  @Test
  public void testCubeWhereQueryForMonthUptoMonths() throws Exception {
    // this should consider only two month partitions.
    String hqlQuery = rewrite("select cityid, SUM(msr2) from testCube" + " where " + TWO_MONTHS_RANGE_UPTO_MONTH,
      getConfWithStorages("C2"));
    String expected =
      getExpectedQuery(cubeName, "select testcube.cityid," + " sum(testcube.msr2) FROM ", null,
        "group by testcube.cityid", getWhereForMonthly2months("c2_testfact"));
    compareQueries(hqlQuery, expected);
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
    assertEquals(extractPruneCause(e), new PruneCauses.BriefAndDetailedError(
      NO_CANDIDATE_STORAGES.errorFormat,
      new HashMap<String, List<CandidateTablePruneCause>>() {
        {
          put("statetable", Arrays.asList(CandidateTablePruneCause.noCandidateStorages(
              new HashMap<String, SkipStorageCause>() {
                {
                  put("c1_statetable", new SkipStorageCause(SkipStorageCode.NO_PARTITIONS));
                }
              }))
          );
          put("statetable_partitioned", Arrays.asList(CandidateTablePruneCause.noCandidateStorages(
              new HashMap<String, SkipStorageCause>() {
                {
                  put("C3_statetable_partitioned", new SkipStorageCause(SkipStorageCode.UNSUPPORTED));
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

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    hqlQuery = rewrite("select name, stateid from citydim", conf);
    expected =
      getExpectedQuery("citydim", "select citydim.name," + " citydim.stateid from ", null, "c2_citytable", false);
    compareQueries(hqlQuery, expected);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    hqlQuery = rewrite("select name, stateid from citydim", conf);
    expected =
      getExpectedQuery("citydim", "select citydim.name," + " citydim.stateid from ", null, "c1_citytable", true);
    compareQueries(hqlQuery, expected);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "");
    conf.set(CubeQueryConfUtil.VALID_STORAGE_DIM_TABLES, "C1_citytable");
    hqlQuery = rewrite("select name, stateid from citydim", conf);
    expected =
      getExpectedQuery("citydim", "select citydim.name," + " citydim.stateid from ", null, "c1_citytable", true);
    compareQueries(hqlQuery, expected);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "");
    conf.set(CubeQueryConfUtil.VALID_STORAGE_DIM_TABLES, "C2_citytable");
    hqlQuery = rewrite("select name, stateid from citydim", conf);
    expected =
      getExpectedQuery("citydim", "select citydim.name," + " citydim.stateid from ", null, "c2_citytable", false);
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select name n, count(1) from citydim" + " group by name order by n ", conf);
    expected =
      getExpectedQuery("citydim", "select citydim.name as `n`," + " count(1) from ",
        "groupby citydim.name order by n asc", "c2_citytable", false);
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select name as `n`, count(1) from citydim" + " order by n ", conf);
    compareQueries(hqlQuery, expected);
    hqlQuery = rewrite("select count(1) from citydim" + " group by name order by name ", conf);
    expected =
      getExpectedQuery("citydim", "select citydim.name," + " count(1) from ",
        "groupby citydim.name order by citydim.name asc ", "c2_citytable", false);
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
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    hqlQuery = rewrite("select name, stateid from citydim " + "limit 100", conf);
    expected =
      getExpectedQuery("citydim", "select citydim.name," + "citydim.stateid from ", " limit 100", "c2_citytable",
        false);
    compareQueries(hqlQuery, expected);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
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
      getExpectedQuery("t", "SELECT t.cityid, sum(t.msr2) FROM ", null, " group by t.cityid",
        getWhereForDailyAndHourly2days("t", "C2_testfact")),
      getExpectedQuery(cubeName, "SELECT testCube.cityid, sum(testCube.msr2)" + " FROM ",
        " testcube.cityid > 100 ", " group by testcube.cityid having" + " sum(testCube.msr2 < 1000)",
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact")),
      getExpectedQuery(cubeName, "SELECT testCube.cityid, sum(testCube.msr2)" + " FROM ",
        " testcube.cityid > 100 ", " group by testcube.cityid having"
          + " sum(testCube.msr2 < 1000) orderby testCube.cityid asc",
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact")),
    };
    Configuration conf = getConf();
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
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
      getExpectedQuery(cubeName, "select testcube.dim1, max(testcube.msr3), sum(testcube.msr2) FROM ", null,
        " group by testcube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_summary1"));
    compareQueries(hqlQuery, expected);
    hqlQuery =
      rewrite("select dim1, dim2, COUNT(msr4)," + " SUM(msr2), msr3 from testCube" + " where " + TWO_DAYS_RANGE,
        getConfWithStorages("C1"));
    expected =
      getExpectedQuery(cubeName, "select testcube.dim1, testcube,dim2, count(testcube.msr4),"
          + " sum(testcube.msr2), max(testcube.msr3) FROM ", null, " group by testcube.dim1, testcube.dim2",
        getWhereForDailyAndHourly2days(cubeName, "C1_summary2"));
    compareQueries(hqlQuery, expected);
    hqlQuery =
      rewrite("select dim1, dim2, cityid, msr4," + " SUM(msr2), msr3 from testCube" + " where " + TWO_DAYS_RANGE,
        getConfWithStorages("C1"));
    expected =
      getExpectedQuery(cubeName, "select testcube.dim1, testcube,dim2, testcube.cityid,"
          + " count(testcube.msr4), sum(testcube.msr2), max(testcube.msr3) FROM ", null,
        " group by testcube.dim1, testcube.dim2, testcube.cityid",
        getWhereForDailyAndHourly2days(cubeName, "C1_summary3"));
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testFactsWithTimedDimension() throws Exception {
    String twoDaysITRange =
      "time_range_in(it, '" + CubeTestSetup.getDateUptoHours(TWODAYS_BACK) + "','"
        + CubeTestSetup.getDateUptoHours(NOW) + "')";

    String hqlQuery =
      rewrite("select dim1, max(msr3)," + " msr2 from testCube" + " where " + twoDaysITRange, getConf());
    String expected =
      getExpectedQuery(cubeName, "select testcube.dim1, max(testcube.msr3), sum(testcube.msr2) FROM ", null,
        " group by testcube.dim1", getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary1"),
        null);
    compareQueries(hqlQuery, expected);
    hqlQuery =
      rewrite("select dim1, dim2, COUNT(msr4)," + " SUM(msr2), msr3 from testCube" + " where " + twoDaysITRange,
        getConf());
    expected =
      getExpectedQuery(cubeName, "select testcube.dim1, testcube,dim2, count(testcube.msr4),"
          + " sum(testcube.msr2), max(testcube.msr3) FROM ", null, " group by testcube.dim1, testcube.dim2",
        getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary2"),
        null);
    compareQueries(hqlQuery, expected);
    hqlQuery =
      rewrite("select dim1, dim2, cityid, count(msr4)," + " SUM(msr2), msr3 from testCube" + " where "
        + twoDaysITRange, getConf());
    expected =
      getExpectedQuery(cubeName, "select testcube.dim1, testcube,dim2, testcube.cityid,"
          + " count(testcube.msr4), sum(testcube.msr2), max(testcube.msr3) FROM ", null,
        " group by testcube.dim1, testcube.dim2, testcube.cityid",
        getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary3"),
        null);
    compareQueries(hqlQuery, expected);
  }

  // Disabling this as querying on part column directly is not allowed as of
  // now.
  // @Test
  public void testCubeQueryTimedDimensionFilter() throws Exception {
    String twoDaysITRange =
      "time_range_in(it, '" + CubeTestSetup.getDateUptoHours(TWODAYS_BACK) + "','"
        + CubeTestSetup.getDateUptoHours(NOW) + "')";

    String hqlQuery =
      rewrite("select dim1, max(msr3)," + " msr2 from testCube" + " where (" + twoDaysITRange
        + " OR it == 'default') AND dim1 > 1000", getConf());
    String expected = getExpectedQuery(cubeName, "select testcube.dim1, max(testcube.msr3), sum(testcube.msr2) FROM ",
      null, "or (( testcube.it ) == 'default')) and ((testcube.dim1) > 1000)" + " group by testcube.dim1",
      getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary1"),
      null);
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE + " OR ("
      + CubeTestSetup.TWO_DAYS_RANGE_BEFORE_4_DAYS + " AND dt='default')", getConf());

    String expecteddtRangeWhere1 =
      getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "dt", TWODAYS_BACK, NOW)
        + " OR ("
        + getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "dt", BEFORE_4_DAYS_START, BEFORE_4_DAYS_END) + ")";
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, " AND testcube.dt='default'",
        expecteddtRangeWhere1, "c2_testfact");
    compareQueries(hqlQuery, expected);

    String expecteddtRangeWhere2 =
      "("
        + getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "dt", TWODAYS_BACK, NOW)
        + " AND testcube.dt='dt1') OR "
        + getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "dt", BEFORE_4_DAYS_START, BEFORE_4_DAYS_END);
    hqlQuery =
      rewrite("select SUM(msr2) from testCube" + " where (" + TWO_DAYS_RANGE + " AND dt='dt1') OR ("
        + CubeTestSetup.TWO_DAYS_RANGE_BEFORE_4_DAYS + " AND dt='default')", getConf());
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, " AND testcube.dt='default'",
        expecteddtRangeWhere2, "c2_testfact");
    compareQueries(hqlQuery, expected);

    String twoDaysPTRange =
      "time_range_in(pt, '" + CubeTestSetup.getDateUptoHours(TWODAYS_BACK) + "','"
        + CubeTestSetup.getDateUptoHours(NOW) + "')";
    hqlQuery =
      rewrite("select dim1, max(msr3)," + " msr2 from testCube where (" + twoDaysITRange + " OR (" + twoDaysPTRange
        + " and it == 'default')) AND dim1 > 1000", getConf());
    String expectedITPTrange =
      getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", TWODAYS_BACK, NOW) + " OR ("
        + getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "pt", TWODAYS_BACK, NOW) + ")";
    expected =
      getExpectedQuery(cubeName, "select testcube.dim1, max(testcube.msr3), sum(testcube.msr2) FROM ", null,
        "AND testcube.it == 'default' and testcube.dim1 > 1000 group by testcube.dim1", expectedITPTrange,
        "C2_summary1");
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testLookAhead() throws Exception {
    String twoDaysITRange =
      "time_range_in(it, '" + CubeTestSetup.getDateUptoHours(TWODAYS_BACK) + "','"
        + CubeTestSetup.getDateUptoHours(NOW) + "')";

    Configuration conf = getConf();
    conf.set(CubeQueryConfUtil.PROCESS_TIME_PART_COL, "pt");
    conf.setClass(CubeQueryConfUtil.TIME_RANGE_WRITER_CLASS, AbridgedTimeRangeWriter.class, TimeRangeWriter.class);
    CubeQueryContext ctx = rewriteCtx("select dim1, max(msr3)," + " msr2 from testCube" + " where " + twoDaysITRange,
      conf);
    assertEquals(ctx.candidateFacts.size(), 1);
    CandidateFact candidateFact = ctx.candidateFacts.iterator().next();
    Set<FactPartition> partsQueried = new TreeSet<>(candidateFact.getPartsQueried());
    Date ceilDay = DateUtil.getCeilDate(TWODAYS_BACK, DAILY);
    Date nextDay = DateUtils.addDays(ceilDay, 1);
    Date nextToNextDay = DateUtils.addDays(nextDay, 1);
    HashSet<String> storageTables = Sets.newHashSet();
    for (String storageTable : candidateFact.getStorageTables()) {
      storageTables.add(storageTable.split("\\.")[1]);
    }
    TreeSet<FactPartition> expectedPartsQueried = Sets.newTreeSet();
    for (TimePartition p : Iterables.concat(
      TimePartition.of(HOURLY, TWODAYS_BACK).rangeUpto(TimePartition.of(HOURLY, ceilDay)),
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
    ctx = rewriteCtx("select dim1, max(msr3)," + " msr2 from testCube" + " where " + twoDaysITRange,
      conf);
    partsQueried = new TreeSet<>(ctx.candidateFacts.iterator().next().getPartsQueried());
    // pt does not exist beyond 1 day. So in this test, max look ahead possible is 3
    assertEquals(partsQueried, expectedPartsQueried);
  }

  @Test
  public void testCubeQueryWithMultipleRanges() throws Exception {
    String hqlQuery =
      rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE + " OR "
        + CubeTestSetup.TWO_DAYS_RANGE_BEFORE_4_DAYS, getConfWithStorages("C2"));

    String expectedRangeWhere =
      getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "dt", TWODAYS_BACK, NOW)
        + " OR "
        + getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "dt", BEFORE_4_DAYS_START, BEFORE_4_DAYS_END);
    String expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null, expectedRangeWhere, "c2_testfact");
    compareQueries(hqlQuery, expected);
    hqlQuery =
      rewrite("select dim1, max(msr3)," + " msr2 from testCube" + " where " + TWO_DAYS_RANGE + " OR "
        + CubeTestSetup.TWO_DAYS_RANGE_BEFORE_4_DAYS, getConfWithStorages("C1"));
    expected =
      getExpectedQuery(cubeName, "select testcube.dim1, max(testcube.msr3), sum(testcube.msr2) FROM ", null,
        " group by testcube.dim1", expectedRangeWhere, "C1_summary1");
    compareQueries(hqlQuery, expected);
    hqlQuery =
      rewrite("select dim1, dim2, COUNT(msr4)," + " SUM(msr2), msr3 from testCube" + " where " + TWO_DAYS_RANGE
        + " OR " + CubeTestSetup.TWO_DAYS_RANGE_BEFORE_4_DAYS, getConfWithStorages("C1"));
    expected =
      getExpectedQuery(cubeName, "select testcube.dim1, testcube,dim2, count(testcube.msr4),"
          + " sum(testcube.msr2), max(testcube.msr3) FROM ", null, " group by testcube.dim1, testcube.dim2",
        expectedRangeWhere, "C1_summary2");
    compareQueries(hqlQuery, expected);
    hqlQuery =
      rewrite("select dim1, dim2, cityid, count(msr4)," + " SUM(msr2), msr3 from testCube" + " where " + TWO_DAYS_RANGE
        + " OR " + CubeTestSetup.TWO_DAYS_RANGE_BEFORE_4_DAYS, getConfWithStorages("C1"));
    expected =
      getExpectedQuery(cubeName, "select testcube.dim1, testcube,dim2, testcube.cityid,"
          + " count(testcube.msr4), sum(testcube.msr2), max(testcube.msr3) FROM ", null,
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
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, true);
    String hqlQuery = rewrite(cubeQl, conf);
    String db = getDbName();
    String expectedJoin =
      " LEFT OUTER JOIN " + db + ".c1_citytable c1 ON (( testcube . cityid ) = ( c1 . id )) AND (c1.dt = 'latest') "
        + " LEFT OUTER JOIN " + db
        + ".c1_statetable s1 ON (( c1 . stateid ) = ( s1 . id )) AND (s1.dt = 'latest') " + " LEFT OUTER JOIN "
        + db + ".c1_citytable c2 ON (( s1 . countryid ) = ( c2 . id )) AND (c2.dt = 'latest')";

    String expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2)" + " FROM ", expectedJoin, null, null, null,
        getWhereForHourly2days(cubeName, "C1_testfact2"));
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testJoinPathColumnLifeValidation() throws Exception {
    HiveConf testConf = new HiveConf(new HiveConf(getConf(), HiveConf.class));
    testConf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
    System.out.println("@@ Joins disabled? " + testConf.get(CubeQueryConfUtil.DISABLE_AUTO_JOINS));
    // Set column life of dim2 column in testCube
    CubeMetastoreClient client = CubeMetastoreClient.getInstance(testConf);
    Cube cube = (Cube) client.getCube(cubeName);

    ReferencedDimAtrribute col = (ReferencedDimAtrribute) cube.getColumnByName("cdim2");
    assertNotNull(col);

    final String query = "SELECT cycledim1.name, msr2 FROM testCube where " + TWO_DAYS_RANGE;
    try {
      CubeQueryContext context = rewriteCtx(query, testConf);
      System.out.println("TestJoinPathTimeRange: " + context.toHQL());
      fail("Expected query to fail because of invalid column life");
    } catch (LensException exc) {
      assertEquals(exc.getErrorCode(), LensCubeErrorCode.NO_JOIN_PATH.getLensErrorInfo().getErrorCode());
    } finally {
      // Add old column back
      cube.alterDimension(col);
      client.alterCube(cubeName, cube);
    }

    // Assert same query succeeds with valid column
    Date oneWeekBack = DateUtils.addDays(TWODAYS_BACK, -7);

    // Alter cube.dim2 with an invalid column life
    ReferencedDimAtrribute newDim2 =
      new ReferencedDimAtrribute(new FieldSchema(col.getName(), "string", "invalid col"), col.getDisplayString(),
        col.getReferences(), oneWeekBack, null,
        col.getCost());
    cube.alterDimension(newDim2);
    client.alterCube(cubeName, cube);
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
      "SELECT test_time_dim, msr2 FROM testCube where " + "time_range_in(test_time_dim, '"
        + CubeTestSetup.getDateUptoHours(TWODAYS_BACK) + "','" + CubeTestSetup.getDateUptoHours(NOW) + "')";

    HiveConf hconf = new HiveConf(getConf(), TestCubeRewriter.class);
    hconf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
    hconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1,C2,C3,C4");
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
  public void testSelectDimonlyJoinOnCube() throws Exception {
    String query = "SELECT count (distinct citydim.name) from testCube where " + TWO_DAYS_RANGE;
    Configuration conf = new Configuration(getConf());
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
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
    String twoDaysITRange =
      "time_range_in(it, '" + CubeTestSetup.getDateUptoHours(TWODAYS_BACK) + "','"
        + CubeTestSetup.getDateUptoHours(NOW) + "')";
    String query = "select dim1, max(msr3)," + " msr2 from testCube" + " where " + twoDaysITRange;

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

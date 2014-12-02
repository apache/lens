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

import static org.apache.lens.cube.parse.CubeTestSetup.getDbName;
import static org.apache.lens.cube.parse.CubeTestSetup.getExpectedQuery;
import static org.apache.lens.cube.parse.CubeTestSetup.getWhereForDailyAndHourly2days;
import static org.apache.lens.cube.parse.CubeTestSetup.getWhereForDailyAndHourly2daysWithTimeDim;
import static org.apache.lens.cube.parse.CubeTestSetup.getWhereForHourly2days;
import static org.apache.lens.cube.parse.CubeTestSetup.getWhereForMonthly2months;
import static org.apache.lens.cube.parse.CubeTestSetup.getWhereForMonthlyDailyAndHourly2months;
import static org.apache.lens.cube.parse.CubeTestSetup.now;
import static org.apache.lens.cube.parse.CubeTestSetup.twoDaysRange;
import static org.apache.lens.cube.parse.CubeTestSetup.twoMonthsRangeUptoHours;
import static org.apache.lens.cube.parse.CubeTestSetup.twoMonthsRangeUptoMonth;
import static org.apache.lens.cube.parse.CubeTestSetup.twodaysBack;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.lens.cube.metadata.*;
import org.apache.lens.cube.parse.CubeQueryConfUtil;
import org.apache.lens.cube.parse.CubeQueryContext;
import org.apache.lens.cube.parse.CubeQueryRewriter;
import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.cube.parse.StorageUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCubeRewriter extends TestQueryRewrite {

  private final String cubeName = CubeTestSetup.TEST_CUBE_NAME;

  public Configuration getConf() {
    Configuration conf = new Configuration();
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1,C2");
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, true);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY, true);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT, true);
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER, false);
    return conf;
  }

  @Test
  public void testQueryWithNow() throws Exception {
    SemanticException th = null;
    try {
      rewrite("select SUM(msr2) from testCube where" + " time_range_in(dt, 'NOW - 2DAYS', 'NOW')", getConf());
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(), ErrorMsg.NO_CANDIDATE_FACT_AVAILABLE.getErrorCode());
  }

  @Test
  public void testCandidateTables() throws Exception {
    SemanticException th = null;
    try {
      rewrite("select dim12, SUM(msr2) from testCube" + " where " + twoDaysRange, getConf());
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(), ErrorMsg.COLUMN_NOT_FOUND.getErrorCode());

    try {
      // this query should through exception because invalidMsr is invalid
      rewrite("SELECT cityid, invalidMsr from testCube " + " where " + twoDaysRange, getConf());
      Assert.assertTrue(false, "Should not reach here");
    } catch (SemanticException exc) {
      exc.printStackTrace();
      Assert.assertNotNull(exc);
      th = exc;
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(), ErrorMsg.COLUMN_NOT_FOUND.getErrorCode());
  }

  @Test
  public void testCubeQuery() throws Exception {
    CubeQueryContext rewrittenQuery =
        rewriteCtx("cube select" + " SUM(msr2) from testCube where " + twoDaysRange, getConf());
    String expected =
        getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
            getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, rewrittenQuery.toHQL());
    System.out.println("Non existing parts:" + rewrittenQuery.getNonExistingParts());
    Assert.assertNotNull(rewrittenQuery.getNonExistingParts());

    // Query with column life not in the range
    SemanticException th = null;
    try {
      rewrite("cube select SUM(newmeasure) from testCube" + " where " + twoDaysRange, getConf());
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(), ErrorMsg.NOT_AVAILABLE_IN_RANGE.getErrorCode());
  }

  @Test
  public void testLightestFactFirst() throws Exception {
    // testFact is lighter than testFact2.
    String hqlQuery = rewrite("cube select" + " SUM(msr2) from testCube where " + twoDaysRange, getConf());
    String expected =
        getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
            getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    Configuration conf = getConf();
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + twoDaysRange, conf);
    expected =
        getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
            getWhereForHourly2days("c1_testfact2"));
    compareQueries(expected, hqlQuery);

    conf.setBoolean(CubeQueryConfUtil.LIGHTEST_FACT_FIRST, true);
    conf.setBoolean(CubeQueryConfUtil.ADD_NON_EXISTING_PARTITIONS, true);
    SemanticException th = null;
    try {
      hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + twoDaysRange, conf);
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(), ErrorMsg.NO_CANDIDATE_FACT_AVAILABLE.getErrorCode());
    Assert.assertTrue(th.getMessage().contains("missingPartitions"));
  }

  @Test
  public void testDerivedCube() throws SemanticException, ParseException {
    CubeQueryContext rewrittenQuery =
        rewriteCtx("cube select" + " SUM(msr2) from derivedCube where " + twoDaysRange, getConf());
    String expected =
        getExpectedQuery(CubeTestSetup.DERIVED_CUBE_NAME, "select sum(derivedCube.msr2) FROM ", null, null,
            getWhereForDailyAndHourly2days(CubeTestSetup.DERIVED_CUBE_NAME, "C2_testfact"));
    compareQueries(expected, rewrittenQuery.toHQL());
    System.out.println("Non existing parts:" + rewrittenQuery.getNonExistingParts());
    Assert.assertNotNull(rewrittenQuery.getNonExistingParts());

    SemanticException th = null;
    try {
      rewrite("select SUM(msr4) from derivedCube" + " where " + twoDaysRange, getConf());
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(), ErrorMsg.COLUMN_NOT_FOUND.getErrorCode());

    // test join
    Configuration conf = getConf();
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
    String hqlQuery = rewrite("cube select" + " testdim2.name, SUM(msr2) from derivedCube where " + twoDaysRange, conf);
    expected =
        getExpectedQuery(CubeTestSetup.DERIVED_CUBE_NAME, "select testdim2.name, sum(derivedCube.msr2) FROM ", " JOIN "
            + getDbName() + "c1_testdim2tbl testdim2 ON derivedCube.dim2 = "
            + " testdim2.id and (testdim2.dt = 'latest') ", null, "group by (testdim2.name)", null,
            getWhereForDailyAndHourly2days(CubeTestSetup.DERIVED_CUBE_NAME, "c1_summary2"));
    compareQueries(expected, hqlQuery);

    // Test that explicit join query passes with join resolver disabled
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, true);
    List<String> joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageUtil.getWherePartClause("dt", "testdim2", StorageConstants.getPartitionsForLatest()));
    hqlQuery =
        rewrite("cube select" + " testdim2.name, SUM(msr2) from derivedCube "
            + " inner join testdim2 on derivedCube.dim2 = testdim2.id " + "where " + twoDaysRange, conf);
    expected =
        getExpectedQuery(CubeTestSetup.DERIVED_CUBE_NAME, "select testdim2.name, sum(derivedCube.msr2) FROM ",
            " inner JOIN " + getDbName() + "c1_testdim2tbl testdim2 ON derivedCube.dim2 = " + " testdim2.id ", null,
            "group by (testdim2.name)", joinWhereConds,
            getWhereForDailyAndHourly2days(CubeTestSetup.DERIVED_CUBE_NAME, "c1_summary2"));
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testCubeInsert() throws Exception {
    String hqlQuery =
        rewrite("insert overwrite directory" + " '/tmp/test' select SUM(msr2) from testCube where " + twoDaysRange,
            getConf());
    String expected =
        "insert overwrite directory '/tmp/test' "
            + getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
                getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery =
        rewrite(
            "insert overwrite directory" + " '/tmp/test' cube select SUM(msr2) from testCube where " + twoDaysRange,
            getConf());
    compareQueries(expected, hqlQuery);

    hqlQuery =
        rewrite("insert overwrite local directory" + " '/tmp/test' select SUM(msr2) from testCube where "
            + twoDaysRange, getConf());
    expected =
        "insert overwrite local directory '/tmp/test' "
            + getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
                getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery =
        rewrite("insert overwrite local directory" + " '/tmp/test' cube select SUM(msr2) from testCube where "
            + twoDaysRange, getConf());
    compareQueries(expected, hqlQuery);

    hqlQuery =
        rewrite("insert overwrite table temp" + " select SUM(msr2) from testCube where " + twoDaysRange, getConf());
    expected =
        "insert overwrite table temp "
            + getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
                getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery =
        rewrite("insert overwrite table temp" + " cube select SUM(msr2) from testCube where " + twoDaysRange, getConf());
    compareQueries(expected, hqlQuery);

  }

  static void compareQueries(String expected, String actual) {
    if (expected == null && actual == null) {
      return;
    } else if (expected == null) {
      Assert.fail();
    } else if (actual == null) {
      Assert.fail("Rewritten query is null");
    }
    String expectedTrimmed = expected.replaceAll("\\W", "");
    String actualTrimmed = actual.replaceAll("\\W", "");

    if (!expectedTrimmed.equalsIgnoreCase(actualTrimmed)) {
      String method = null;
      for (StackTraceElement trace : Thread.currentThread().getStackTrace()) {
        if (trace.getMethodName().startsWith("test")) {
          method = trace.getMethodName() + ":" + trace.getLineNumber();
        }
      }

      System.err.println("__FAILED__ " + method + "\n\tExpected: " + expected + "\n\t---------\n\tActual: " + actual);
    }
    Assert.assertTrue(expectedTrimmed.equalsIgnoreCase(actualTrimmed));
  }

  static void compareContains(String expected, String actual) {
    if (expected == null && actual == null) {
      return;
    } else if (expected == null) {
      Assert.fail();
    } else if (actual == null) {
      Assert.fail("Rewritten query is null");
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
    Assert.assertTrue(actualTrimmed.toLowerCase().contains(expectedTrimmed.toLowerCase()));
  }

  @Test
  public void testCubeWhereQuery() throws Exception {
    String hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + twoDaysRange, getConf());
    String expected =
        getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
            getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    // Test with partition existence
    Configuration conf = getConf();
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + twoDaysRange, conf);
    expected =
        getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
            getWhereForHourly2days("c1_testfact2"));
    compareQueries(expected, hqlQuery);
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);

    // Tests for valid tables
    conf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact");
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + twoDaysRange, conf);
    expected =
        getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
            getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    conf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + twoDaysRange, conf);
    expected =
        getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
            getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    conf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact2");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + twoDaysRange, conf);
    expected =
        getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
            getWhereForHourly2days("c1_testfact2"));
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    conf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact2");
    conf.set(CubeQueryConfUtil.getValidStorageTablesKey("testFact2"), "C1_testFact2");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + twoDaysRange, conf);
    expected =
        getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
            getWhereForHourly2days("c1_testfact2"));
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact");
    conf.set(CubeQueryConfUtil.getValidStorageTablesKey("testfact"), "C1_testFact");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact", "C1"), "HOURLY");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + twoDaysRange, conf);
    expected =
        getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null, getWhereForHourly2days("c1_testfact"));
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    conf.set(CubeQueryConfUtil.getValidStorageTablesKey("testfact"), "C2_testFact");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact", "C2"), "HOURLY");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + twoDaysRange, conf);
    expected =
        getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null, getWhereForHourly2days("c2_testfact"));
    compareQueries(expected, hqlQuery);

    // max interval test
    conf = new Configuration();
    conf.set(CubeQueryConfUtil.QUERY_MAX_INTERVAL, "HOURLY");
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1,C2");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + twoDaysRange, conf);
    expected =
        getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null, getWhereForHourly2days("c2_testfact"));
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testCubeWhereQueryWithMultipleTables() throws Exception {
    Configuration conf = getConf();
    conf.set(CubeQueryConfUtil.getValidStorageTablesKey("testfact"), "C1_testFact,C2_testFact");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact", "C1"), "DAILY");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact", "C2"), "HOURLY");
    String hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + twoDaysRange, conf);

    String expected = null;
    if (!CubeTestSetup.isZerothHour()) {
      expected =
          getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
              getWhereForDailyAndHourly2days(cubeName, "c1_testfact", "C2_testfact"));
    } else {
      expected =
          getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
              getWhereForDailyAndHourly2days(cubeName, "c1_testfact"));
    }
    compareQueries(expected, hqlQuery);

    // Union query
    conf.setBoolean(CubeQueryConfUtil.ENABLE_MULTI_TABLE_SELECT, false);
    try {
      // rewrite to union query
      hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + twoDaysRange, conf);
      System.out.println("Union hql query:" + hqlQuery);

      // TODO: uncomment the following once union query
      // rewriting has been done
      // expected = // write expected union query
      // compareQueries(expected, hqlQuery);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testCubeWhereQueryWithMultipleTablesForMonth() throws Exception {
    Configuration conf = getConf();
    conf.setBoolean(CubeQueryConfUtil.ENABLE_MULTI_TABLE_SELECT, true);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "");
    conf.set(CubeQueryConfUtil.getValidStorageTablesKey("testfact"), "");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact", "C1"), "HOURLY");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact", "C2"), "DAILY");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact", "C3"), "MONTHLY");

    String hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + twoMonthsRangeUptoHours, conf);
    String expected;
    if (!CubeTestSetup.isZerothHour()) {
      expected =
          getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
              getWhereForMonthlyDailyAndHourly2months("c1_testfact", "c2_testFact", "C3_testfact"));
    } else {
      expected =
          getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
              getWhereForMonthlyDailyAndHourly2months("c1_testfact", "c2_testfact", "c3_testFact"));
    }
    compareQueries(expected, hqlQuery);

    // monthly - c1,c2; daily - c1, hourly -c2
    conf.set(CubeQueryConfUtil.getValidStorageTablesKey("testfact"), "C1_testFact,C2_testFact");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact", "C1"), "MONTHLY,DAILY");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact", "C2"), "MONTHLY,HOURLY");
    try {
      hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + twoMonthsRangeUptoHours, conf);
      System.out.println("union query:" + hqlQuery);
      // TODO: uncomment the following once union query
      // rewriting has been done
      // expected = getExpectedQuery(cubeName,
      // "select sum(testcube.msr2) FROM ", null, null,
      // getWhereForMonthlyDailyAndHourly2months("C1_testfact"));
      // compareQueries(expected, hqlQuery);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testCubeJoinQuery() throws Exception {
    // q1
    String hqlQuery =
        rewrite("select SUM(msr2) from testCube" + " join citydim on testCube.cityid = citydim.id" + " where "
            + twoDaysRange, getConf());
    List<String> joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageUtil.getWherePartClause("dt", "citydim", StorageConstants.getPartitionsForLatest()));
    String expected =
        getExpectedQuery(cubeName, "select sum(testcube.msr2)" + " FROM ", " INNER JOIN " + getDbName()
            + "c1_citytable citydim ON" + " testCube.cityid = citydim.id", null, null, joinWhereConds,
            getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery =
        rewrite("select SUM(msr2) from testCube" + " join citydim on cityid = citydim.id" + " where " + twoDaysRange,
            getConf());
    compareQueries(expected, hqlQuery);

    hqlQuery =
        rewrite("select SUM(msr2) from testCube" + " join citydim on cityid = id" + " where " + twoDaysRange, getConf());
    compareQueries(expected, hqlQuery);

    // q2
    hqlQuery =
        rewrite("select statedim.name, SUM(msr2) from" + " testCube" + " join citydim on testCube.cityid = citydim.id"
            + " left outer join statedim on statedim.id = citydim.stateid"
            + " right outer join zipdim on citydim.zipcode = zipdim.code" + " where " + twoDaysRange, getConf());
    joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageUtil.getWherePartClause("dt", "citydim", StorageConstants.getPartitionsForLatest()));
    joinWhereConds.add(StorageUtil.getWherePartClause("dt", "zipdim", StorageConstants.getPartitionsForLatest()));
    expected =
        getExpectedQuery(cubeName, "select statedim.name," + " sum(testcube.msr2) FROM ", "INNER JOIN " + getDbName()
            + "c1_citytable citydim ON" + " testCube.cityid = citydim.id LEFT OUTER JOIN " + getDbName()
            + "c1_statetable statedim" + " ON statedim.id = citydim.stateid AND "
            + "(statedim.dt = 'latest') RIGHT OUTER JOIN " + getDbName() + "c1_ziptable"
            + " zipdim ON citydim.zipcode = zipdim.code", null, " group by" + " statedim.name ", joinWhereConds,
            getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    // q3
    hqlQuery =
        rewrite("select st.name, SUM(msr2) from" + " testCube TC" + " join citydim CT on TC.cityid = CT.id"
            + " left outer join statedim ST on ST.id = CT.stateid"
            + " right outer join zipdim ZT on CT.zipcode = ZT.code" + " where " + twoDaysRange, getConf());
    joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageUtil.getWherePartClause("dt", "ct", StorageConstants.getPartitionsForLatest()));
    joinWhereConds.add(StorageUtil.getWherePartClause("dt", "zt", StorageConstants.getPartitionsForLatest()));
    expected =
        getExpectedQuery("tc", "select st.name," + " sum(tc.msr2) FROM ", " INNER JOIN " + getDbName()
            + "c1_citytable ct ON" + " tc.cityid = ct.id LEFT OUTER JOIN " + getDbName() + "c1_statetable st"
            + " ON st.id = ct.stateid and (st.dt = 'latest') " + "RIGHT OUTER JOIN " + getDbName() + "c1_ziptable"
            + " zt ON ct.zipcode = zt.code", null, " group by" + " st.name ", joinWhereConds,
            getWhereForDailyAndHourly2days("tc", "C2_testfact"));
    compareQueries(expected, hqlQuery);

    // q4
    hqlQuery =
        rewrite("select citydim.name, SUM(msr2) from" + " testCube"
            + " left outer join citydim on testCube.cityid = citydim.id"
            + " left outer join zipdim on citydim.zipcode = zipdim.code" + " where " + twoDaysRange, getConf());
    expected =
        getExpectedQuery(cubeName, "select citydim.name," + " sum(testcube.msr2) FROM ", " LEFT OUTER JOIN "
            + getDbName() + "c1_citytable citydim ON" + " testCube.cityid = citydim.id and (citydim.dt = 'latest') "
            + " LEFT OUTER JOIN " + getDbName() + "c1_ziptable" + " zipdim ON citydim.zipcode = zipdim.code AND "
            + "(zipdim.dt = 'latest')", null, " group by" + " citydim.name ", null,
            getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery =
        rewrite("select SUM(msr2) from testCube" + " join countrydim on testCube.countryid = countrydim.id" + " where "
            + twoMonthsRangeUptoMonth, getConf());
    expected =
        getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", " INNER JOIN " + getDbName()
            + "c1_countrytable countrydim ON testCube.countryid = " + " countrydim.id", null, null, null,
            getWhereForMonthly2months("c2_testfactmonthly"));
    compareQueries(expected, hqlQuery);

    SemanticException th = null;
    try {
      hqlQuery =
          rewrite("select name, SUM(msr2) from testCube" + " join citydim" + " where " + twoDaysRange
              + " group by name", getConf());
      Assert.fail("Expected to throw exception");
    } catch (SemanticException e) {
      e.printStackTrace();
      th = e;
    }

    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(), ErrorMsg.NO_JOIN_CONDITION_AVAIABLE.getErrorCode());
  }

  @Test
  public void testCubeGroupbyQuery() throws Exception {
    String hqlQuery =
        rewrite("select name, SUM(msr2) from" + " testCube join citydim on testCube.cityid = citydim.id where "
            + twoDaysRange, getConf());
    List<String> joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageUtil.getWherePartClause("dt", "citydim", StorageConstants.getPartitionsForLatest()));
    String expected =
        getExpectedQuery(cubeName, "select citydim.name," + " sum(testcube.msr2) FROM ", "INNER JOIN " + getDbName()
            + "c1_citytable citydim ON" + " testCube.cityid = citydim.id", null, " group by citydim.name ",
            joinWhereConds, getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery =
        rewrite("select SUM(msr2) from testCube" + " join citydim on testCube.cityid = citydim.id" + " where "
            + twoDaysRange + " group by name", getConf());
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite("select cityid, SUM(msr2) from testCube" + " where " + twoDaysRange, getConf());
    expected =
        getExpectedQuery(cubeName, "select testcube.cityid," + " sum(testcube.msr2) FROM ", null,
            " group by testcube.cityid ", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite("select round(cityid), SUM(msr2) from" + " testCube where " + twoDaysRange, getConf());
    expected =
        getExpectedQuery(cubeName, "select round(testcube.cityid)," + " sum(testcube.msr2) FROM ", null,
            " group by round(testcube.cityid) ", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery =
        rewrite("select SUM(msr2) from testCube" + "  where " + twoDaysRange + "group by round(zipcode)", getConf());
    expected =
        getExpectedQuery(cubeName, "select round(testcube.zipcode)," + " sum(testcube.msr2) FROM ", null,
            " group by round(testcube.zipcode) ", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery =
        rewrite("select round(cityid), SUM(msr2) from" + " testCube where " + twoDaysRange + " group by zipcode",
            getConf());
    expected =
        getExpectedQuery(cubeName, "select " + " round(testcube.cityid), sum(testcube.msr2) FROM ", null,
            " group by testcube.zipcode", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite("select round(cityid), SUM(msr2) from" + " testCube where " + twoDaysRange, getConf());
    expected =
        getExpectedQuery(cubeName, "select " + " round(testcube.cityid), sum(testcube.msr2) FROM ", null,
            " group by round(testcube.cityid)", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery =
        rewrite("select cityid, SUM(msr2) from testCube" + " where " + twoDaysRange + " group by round(zipcode)",
            getConf());
    expected =
        getExpectedQuery(cubeName, "select " + " testcube.cityid, sum(testcube.msr2) FROM ", null,
            " group by round(testcube.zipcode)", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery =
        rewrite("select SUM(msr2) from testCube" + " where " + twoDaysRange + " group by round(zipcode)", getConf());
    expected =
        getExpectedQuery(cubeName, "select round(testcube.zipcode)," + " sum(testcube.msr2) FROM ", null,
            " group by round(testcube.zipcode)", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery =
        rewrite("select cityid, msr2 from testCube" + " where " + twoDaysRange + " group by round(zipcode)", getConf());
    expected =
        getExpectedQuery(cubeName, "select " + " testcube.cityid, sum(testcube.msr2) FROM ", null,
            " group by round(testcube.zipcode)", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery =
        rewrite("select round(zipcode) rzc," + " msr2 from testCube where " + twoDaysRange + " group by zipcode"
            + " order by rzc", getConf());
    expected =
        getExpectedQuery(cubeName, "select round(testcube.zipcode) rzc," + " sum(testcube.msr2) FROM ", null,
            " group by testcube.zipcode  order by rzc asc", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    // rewrite with expressions
    Configuration conf = getConf();
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
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
            + "  FROM testCube where " + twoDaysRange + " HAVING (SUM(msr1) >=1000)  AND (SUM(msr2)>=0.01)", conf);
    String actualExpr =
        " join " + getDbName() + "c1_citytable citydim on testcube.cityid = citydim.id and (citydim.dt = 'latest')"
            + " join " + getDbName()
            + "c1_ziptable zipdim on testcube.zipcode = zipdim.code and (zipdim.dt = 'latest')  " + " join "
            + getDbName() + "c1_statetable statedim on testcube.stateid = statedim.id and (statedim.dt = 'latest')";
    expected =
        getExpectedQuery(
            cubeName,
            "SELECT ( citydim  .  name ) g1 ,"
                + "  case  when (( citydim  .  name ) ==  'NULL' ) then  'NULL'  when (( citydim  .  name ) ==  'X' )"
                + " then  'X-NAME'  when (( citydim  .  name ) ==  'Y' ) then  'Y-NAME'"
                + "  else  'DEFAULT'  end  g2 , ( statedim  .  name ) g3 , ( statedim  .  id ) g4 ,"
                + " ((( zipdim  .  code ) !=  1 ) and ((((( zipdim  .  f1 ) ==  \"xyz\" )"
                + " and (((( zipdim  .  f2 ) >=  \"3\" ) and (( zipdim  .  f2 ) !=  \"NULL\" ))"
                + " and (( zipdim  .  f2 ) !=  \"uk\" ))) or (((( zipdim  .  f2 ) ==  \"adc\" ) and (( zipdim  .  f1 ) ==  \"js\" ))"
                + " and ((( citydim  .  name ) ==  \"X\" ) or (( citydim  .  name ) ==  \"Y\" )))) or ((((( zipdim  .  f1 ) ==  \"api\" )"
                + " or (( zipdim  .  f1 ) ==  \"uk\" )) or ((( zipdim  .  f1 ) ==  \"adc\" ) and (( zipdim  .  f1 ) !=  \"js\" )))"
                + " and (( citydim  .  id ) ==  12 )))) g5 , ((( zipdim  .  code ) ==  1 )"
                + " and ((((( zipdim  .  f1 ) ==  \"xyz\" ) and (((( zipdim  .  f2 ) >=  \"3\" ) and (( zipdim  .  f2 ) !=  \"NULL\" ))"
                + " and (( zipdim  .  f2 ) !=  \"uk\" ))) or (((( zipdim  .  f2 ) ==  \"adc\" ) and (( zipdim  .  f1 ) ==  \"js\" ))"
                + " and ((( citydim  .  name ) ==  \"X\" ) or (( citydim  .  name ) ==  \"Y\" )))) or ((((( zipdim  .  f1 ) ==  \"api\" )"
                + " or (( zipdim  .  f1 ) ==  \"uk\" )) or ((( zipdim  .  f1 ) ==  \"adc\" ) and (( zipdim  .  f1 ) !=  \"js\" )))"
                + " and (( citydim  .  id ) ==  12 )))) g6 , ( zipdim  .  f1 ) g7 , format_number(sum(( testcube  .  msr1 )),  \"##################.###\" ) a1 ,"
                + " format_number(sum(( testcube  .  msr2 )),  \"##################.###\" ) a2 , format_number(sum(( testcube  .  msr3 )),  \"##################.###\" ) a3, "
                + " format_number((sum(( testcube  .  msr1 )) + sum(( testcube  .  msr2 ))),  \"##################.###\" ) a4 ,"
                + " format_number((sum(( testcube  .  msr1 )) + sum(( testcube  .  msr3 ))),  \"##################.###\" ) a5 ,"
                + " format_number((sum(( testcube  .  msr1 )) - (sum(( testcube  .  msr2 )) + sum(( testcube  .  msr3 )))),  \"##################.###\" ) a6"
                + "  FROM ",
            actualExpr,
            null,
            " GROUP BY ( citydim  .  name ), case  when (( citydim  .  name ) ==  'NULL' ) "
                + "then  'NULL'  when (( citydim  .  name ) ==  'X' ) then  'X-NAME'  when (( citydim  .  name ) ==  'Y' )"
                + " then  'Y-NAME'  else  'DEFAULT'  end, ( statedim  .  name ), ( statedim  .  id ),"
                + " ((( zipdim  .  code ) !=  1 ) and ((((( zipdim  .  f1 ) ==  \"xyz\" ) and (((( zipdim  .  f2 ) >=  \"3\" )"
                + " and (( zipdim  .  f2 ) !=  \"NULL\" )) and (( zipdim  .  f2 ) !=  \"uk\" ))) or (((( zipdim  .  f2 ) ==  \"adc\" )"
                + " and (( zipdim  .  f1 ) ==  \"js\" )) and ((( citydim  .  name ) ==  \"X\" ) or (( citydim  .  name ) ==  \"Y\" ))))"
                + " or ((((( zipdim  .  f1 ) ==  \"api\" ) or (( zipdim  .  f1 ) ==  \"uk\" )) or ((( zipdim  .  f1 ) ==  \"adc\" )"
                + " and (( zipdim  .  f1 ) !=  \"js\" ))) and (( citydim  .  id ) ==  12 )))), ((( zipdim  .  code ) ==  1 ) and"
                + " ((((( zipdim  .  f1 ) ==  \"xyz\" ) and (((( zipdim  .  f2 ) >=  \"3\" ) and (( zipdim  .  f2 ) !=  \"NULL\" ))"
                + " and (( zipdim  .  f2 ) !=  \"uk\" ))) or (((( zipdim  .  f2 ) ==  \"adc\" ) and (( zipdim  .  f1 ) ==  \"js\" ))"
                + " and ((( citydim  .  name ) ==  \"X\" ) or (( citydim  .  name ) ==  \"Y\" )))) or ((((( zipdim  .  f1 ) ==  \"api\" )"
                + " or (( zipdim  .  f1 ) ==  \"uk\" )) or ((( zipdim  .  f1 ) ==  \"adc\" ) and (( zipdim  .  f1 ) !=  \"js\" )))"
                + " and (( citydim  .  id ) ==  12 )))), ( zipdim  .  f1 ) HAVING ((sum(( testcube  .  msr1 )) >=  1000 ) and (sum(( testcube  .  msr2 )) >=  0.01 ))",
            null, getWhereForHourly2days("c1_testfact2_raw"));
    compareQueries(expected, hqlQuery);

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
                + twoDaysRange
                + " group by citydim.name, CASE WHEN citydim.name=='NULL' THEN 'NULL'"
                + " WHEN citydim.name=='X' THEN 'X-NAME' WHEN citydim.name=='Y' THEN 'Y-NAME'"
                + " ELSE 'DEFAULT'   END, statedim.name, statedim.id,  zipdim.code!=1  AND"
                + "  ((zipdim.f1==\"xyz\"  AND  (zipdim.f2 >= \"3\"  AND zipdim.f2 !=\"NULL\"  AND  zipdim.f2 != \"uk\"))"
                + "   OR (zipdim.f2==\"adc\"  AND  zipdim.f1==\"js\" AND ( citydim.name == \"X\"  OR  citydim.name == \"Y\" ))"
                + "  OR ((zipdim.f1==\"api\"  OR  zipdim.f1==\"uk\"  OR  (zipdim.f1==\"adc\"  AND  zipdim.f1!=\"js\"))"
                + "  AND  citydim.id==12) ),"
                + " zipdim.code==1  AND  ((zipdim.f1==\"xyz\" AND ( zipdim.f2 >= \"3\"  AND zipdim.f2 !=\"NULL\""
                + "  AND  zipdim.f2 != \"uk\"))"
                + "  OR (zipdim.f2==\"adc\"  AND  zipdim.f1==\"js\"  AND  ( citydim.name == \"X\"  OR  citydim.name == \"Y\" ))"
                + "   OR ((zipdim.f1=\"api\"  OR  zipdim.f1==\"uk\" OR (zipdim.f1==\"adc\"  AND  zipdim.f1!=\"js\")) AND"
                + "  citydim.id==12))," + " zipdim.f1 " + "HAVING (SUM(msr1) >=1000)  AND (SUM(msr2)>=0.01)", conf);
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testCubeQueryWithAilas() throws Exception {
    String hqlQuery = rewrite("select SUM(msr2) m2 from" + " testCube where " + twoDaysRange, getConf());
    String expected =
        getExpectedQuery(cubeName, "select sum(testcube.msr2)" + " m2 FROM ", null, null,
            getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite("select SUM(msr2) from testCube mycube" + " where " + twoDaysRange, getConf());
    expected =
        getExpectedQuery("mycube", "select sum(mycube.msr2) FROM ", null, null,
            getWhereForDailyAndHourly2days("mycube", "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite("select SUM(testCube.msr2) from testCube" + " where " + twoDaysRange, getConf());
    expected =
        getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
            getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite("select mycube.msr2 m2 from testCube" + " mycube where " + twoDaysRange, getConf());
    expected =
        getExpectedQuery("mycube", "select sum(mycube.msr2) m2 FROM ", null, null,
            getWhereForDailyAndHourly2days("mycube", "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite("select testCube.msr2 m2 from testCube" + " where " + twoDaysRange, getConf());
    expected =
        getExpectedQuery(cubeName, "select sum(testcube.msr2) m2 FROM ", null, null,
            getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testCubeWhereQueryForMonth() throws Exception {
    String hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + twoMonthsRangeUptoHours, getConf());
    String expected =
        getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
            getWhereForMonthlyDailyAndHourly2months("C2_testfact"));
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testCubeWhereQueryForMonthWithNoPartialData() throws Exception {
    Configuration conf = getConf();
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);
    SemanticException th = null;
    try {
      rewrite("select SUM(msr2) from testCube" + " where " + twoMonthsRangeUptoHours, conf);
      Assert.assertTrue(false);
    } catch (SemanticException e) {
      e.printStackTrace();
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(), ErrorMsg.NO_CANDIDATE_FACT_AVAILABLE.getErrorCode());
  }

  @Test
  public void testCubeWhereQueryForMonthUptoMonths() throws Exception {
    // this should consider only two month partitions.
    String hqlQuery = rewrite("select cityid, SUM(msr2) from testCube" + " where " + twoMonthsRangeUptoMonth, getConf());
    String expected =
        getExpectedQuery(cubeName, "select testcube.cityid," + " sum(testcube.msr2) FROM ", null,
            "group by testcube.cityid", getWhereForMonthly2months("c2_testfact"));
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testDimensionQueryWithMultipleStorages() throws Exception {
    String hqlQuery = rewrite("select name, stateid from" + " citydim", getConf());
    String expected =
        getExpectedQuery("citydim", "select citydim.name," + " citydim.stateid from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    Configuration conf = getConf();
    // should pick up c2 storage when 'fail on partial data' enabled
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);
    hqlQuery = rewrite("select name, stateid from" + " citydim", conf);
    expected =
        getExpectedQuery("citydim", "select citydim.name," + " citydim.stateid from ", null, "c2_citytable", false);
    compareQueries(expected, hqlQuery);

    // state table is present on c1 with partition dumps and partitions added
    SemanticException th = null;
    try {
      hqlQuery = rewrite("select name, capital from statedim ", conf);
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(), ErrorMsg.NO_CANDIDATE_DIM_AVAILABLE.getErrorCode());
    Assert.assertTrue(th.getMessage().contains("statedim"));

    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);

    // non existing parts should be populated
    CubeQueryContext rewrittenQuery = rewriteCtx("select name, capital from statedim ", conf);
    expected =
        getExpectedQuery("statedim", "select statedim.name," + " statedim.capital from ", null, "c1_statetable", true);
    compareQueries(expected, rewrittenQuery.toHQL());
    Assert.assertNotNull(rewrittenQuery.getNonExistingParts());

    // run a query with time range function
    hqlQuery = rewrite("select name, stateid from citydim where " + twoDaysRange, conf);
    expected =
        getExpectedQuery("citydim", "select citydim.name," + " citydim.stateid from ", null, twoDaysRange, null,
            "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    // query with alias
    hqlQuery = rewrite("select name, c.stateid from citydim" + " c", conf);
    expected = getExpectedQuery("c", "select c.name, c.stateid from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    // query with where clause
    hqlQuery = rewrite("select name, c.stateid from citydim" + " c where name != 'xyz' ", conf);
    expected =
        getExpectedQuery("c", "select c.name, c.stateid from ", null, " c.name != 'xyz' ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    // query with orderby
    hqlQuery = rewrite("select name, c.stateid from citydim" + " c where name != 'xyz' order by name", conf);
    expected =
        getExpectedQuery("c", "select c.name, c.stateid from ", null, " c.name != 'xyz' ", " order by c.name asc",
            "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    // query with where and orderby
    hqlQuery = rewrite("select name, c.stateid from citydim" + " c where name != 'xyz' order by name", conf);
    expected =
        getExpectedQuery("c", "select c.name, c.stateid from ", null, " c.name != 'xyz' ", " order by c.name asc ",
            "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    // query with orderby with order specified
    hqlQuery = rewrite("select name, c.stateid from citydim" + " c where name != 'xyz' order by name desc ", conf);
    expected =
        getExpectedQuery("c", "select c.name, c.stateid from ", null, " c.name != 'xyz' ", " order by c.name desc",
            "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    hqlQuery = rewrite("select name, stateid from citydim", conf);
    expected =
        getExpectedQuery("citydim", "select citydim.name," + " citydim.stateid from ", null, "c2_citytable", false);
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    hqlQuery = rewrite("select name, stateid from citydim", conf);
    expected =
        getExpectedQuery("citydim", "select citydim.name," + " citydim.stateid from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "");
    conf.set(CubeQueryConfUtil.VALID_STORAGE_DIM_TABLES, "C1_citytable");
    hqlQuery = rewrite("select name, stateid from citydim", conf);
    expected =
        getExpectedQuery("citydim", "select citydim.name," + " citydim.stateid from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "");
    conf.set(CubeQueryConfUtil.VALID_STORAGE_DIM_TABLES, "C2_citytable");
    hqlQuery = rewrite("select name, stateid from citydim", conf);
    expected =
        getExpectedQuery("citydim", "select citydim.name," + " citydim.stateid from ", null, "c2_citytable", false);
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite("select name n, count(1) from citydim" + " group by name order by n ", conf);
    expected =
        getExpectedQuery("citydim", "select citydim.name n," + " count(1) from ",
            "groupby citydim.name order by n asc", "c2_citytable", false);
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite("select name n, count(1) from citydim" + " order by n ", conf);
    compareQueries(expected, hqlQuery);
    hqlQuery = rewrite("select count(1) from citydim" + " group by name order by name ", conf);
    expected =
        getExpectedQuery("citydim", "select citydim.name," + " count(1) from ",
            "groupby citydim.name order by citydim.name asc ", "c2_citytable", false);
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testLimitQueryOnDimension() throws Exception {
    Configuration conf = getConf();
    String hqlQuery = rewrite("select name, stateid from" + " citydim limit 100", conf);
    String expected =
        getExpectedQuery("citydim", "select citydim.name," + " citydim.stateid from ", " limit 100", "c1_citytable",
            true);
    compareQueries(expected, hqlQuery);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    hqlQuery = rewrite("select name, stateid from citydim " + "limit 100", conf);
    expected =
        getExpectedQuery("citydim", "select citydim.name," + "citydim.stateid from ", " limit 100", "c2_citytable",
            false);
    compareQueries(expected, hqlQuery);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    hqlQuery = rewrite("select name, stateid from citydim" + " limit 100", conf);
    expected =
        getExpectedQuery("citydim", "select citydim.name," + " citydim.stateid from ", " limit 100", "c1_citytable",
            true);
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testColumnAmbiguity() throws Exception {
    String query =
        "SELECT ambigdim1, sum(testCube.msr1) FROM testCube join" + " citydim on testcube.cityid = citydim.id where "
            + twoDaysRange;

    SemanticException th = null;
    try {
      String hql = rewrite(query, getConf());
      Assert.assertTrue(false, "Should not reach here:" + hql);
    } catch (SemanticException exc) {
      exc.printStackTrace();
      th = exc;
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(), ErrorMsg.AMBIGOUS_CUBE_COLUMN.getErrorCode());

    th = null;
    String q2 =
        "SELECT ambigdim2 from citydim join" + " statedim on citydim.stateid = statedim.id join countrydim on"
            + " statedim.countryid = countrydim.id";
    try {
      String hql = rewrite(q2, getConf());
      Assert.fail("Should not reach here: " + hql);
    } catch (SemanticException exc) {
      exc.printStackTrace();
      th = exc;
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(), ErrorMsg.AMBIGOUS_DIM_COLUMN.getErrorCode());
  }

  @Test
  public void testAliasReplacer() throws Exception {
    String queries[] =
        {
            "SELECT cityid, t.msr2 FROM testCube t where " + twoDaysRange,
            "SELECT cityid, msr2 FROM testCube where cityid > 100 and " + twoDaysRange + " HAVING msr2 < 1000",
            "SELECT cityid, testCube.msr2 FROM testCube where cityid > 100 and " + twoDaysRange
                + " HAVING msr2 < 1000 ORDER BY cityid" };

    String expectedQueries[] =
        {
            getExpectedQuery("t", "SELECT t.cityid, sum(t.msr2) FROM ", null, " group by t.cityid",
                getWhereForDailyAndHourly2days("t", "C2_testfact")),
            getExpectedQuery(cubeName, "SELECT testCube.cityid, sum(testCube.msr2)" + " FROM ",
                " testcube.cityid > 100 ", " group by testcube.cityid having" + " sum(testCube.msr2 < 1000)",
                getWhereForDailyAndHourly2days(cubeName, "C2_testfact")),
            getExpectedQuery(cubeName, "SELECT testCube.cityid, sum(testCube.msr2)" + " FROM ",
                " testcube.cityid > 100 ", " group by testcube.cityid having"
                    + " sum(testCube.msr2 < 1000) orderby testCube.cityid asc",
                getWhereForDailyAndHourly2days(cubeName, "C2_testfact")), };

    for (int i = 0; i < queries.length; i++) {
      String hql = rewrite(queries[i], getConf());
      compareQueries(expectedQueries[i], hql);
    }
  }

  @Test
  public void testFactsWithInvalidColumns() throws Exception {
    String hqlQuery = rewrite("select dim1, max(msr3)," + " msr2 from testCube" + " where " + twoDaysRange, getConf());
    String expected =
        getExpectedQuery(cubeName, "select testcube.dim1, max(testcube.msr3), sum(testcube.msr2) FROM ", null,
            " group by testcube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_summary1"));
    compareQueries(expected, hqlQuery);
    hqlQuery =
        rewrite("select dim1, dim2, COUNT(msr4)," + " SUM(msr2), msr3 from testCube" + " where " + twoDaysRange,
            getConf());
    expected =
        getExpectedQuery(cubeName, "select testcube.dim1, testcube,dim2, count(testcube.msr4),"
            + " sum(testcube.msr2), max(testcube.msr3) FROM ", null, " group by testcube.dim1, testcube.dim2",
            getWhereForDailyAndHourly2days(cubeName, "C1_summary2"));
    compareQueries(expected, hqlQuery);
    hqlQuery =
        rewrite("select dim1, dim2, cityid, msr4," + " SUM(msr2), msr3 from testCube" + " where " + twoDaysRange,
            getConf());
    expected =
        getExpectedQuery(cubeName, "select testcube.dim1, testcube,dim2, testcube.cityid,"
            + " count(testcube.msr4), sum(testcube.msr2), max(testcube.msr3) FROM ", null,
            " group by testcube.dim1, testcube.dim2, testcube.cityid",
            getWhereForDailyAndHourly2days(cubeName, "C1_summary3"));
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testFactsWithTimedDimension() throws Exception {
    String twoDaysITRange =
        "time_range_in(it, '" + CubeTestSetup.getDateUptoHours(twodaysBack) + "','"
            + CubeTestSetup.getDateUptoHours(now) + "')";

    String hqlQuery =
        rewrite("select dim1, max(msr3)," + " msr2 from testCube" + " where " + twoDaysITRange, getConf());
    String expected =
        getExpectedQuery(cubeName, "select testcube.dim1, max(testcube.msr3), sum(testcube.msr2) FROM ", null,
            " group by testcube.dim1", getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary1"));
    compareQueries(expected, hqlQuery);
    hqlQuery =
        rewrite("select dim1, dim2, COUNT(msr4)," + " SUM(msr2), msr3 from testCube" + " where " + twoDaysITRange,
            getConf());
    expected =
        getExpectedQuery(cubeName, "select testcube.dim1, testcube,dim2, count(testcube.msr4),"
            + " sum(testcube.msr2), max(testcube.msr3) FROM ", null, " group by testcube.dim1, testcube.dim2",
            getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary2"));
    compareQueries(expected, hqlQuery);
    hqlQuery =
        rewrite("select dim1, dim2, cityid, count(msr4)," + " SUM(msr2), msr3 from testCube" + " where "
            + twoDaysITRange, getConf());
    expected =
        getExpectedQuery(cubeName, "select testcube.dim1, testcube,dim2, testcube.cityid,"
            + " count(testcube.msr4), sum(testcube.msr2), max(testcube.msr3) FROM ", null,
            " group by testcube.dim1, testcube.dim2, testcube.cityid",
            getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary3"));
    compareQueries(expected, hqlQuery);
  }

  // Disabling this as querying on part column directly is not allowed as of
  // now.
  // @Test
  public void testCubeQueryTimedDimensionFilter() throws Exception {
    String twoDaysITRange =
        "time_range_in(it, '" + CubeTestSetup.getDateUptoHours(twodaysBack) + "','"
            + CubeTestSetup.getDateUptoHours(now) + "')";

    String hqlQuery =
        rewrite("select dim1, max(msr3)," + " msr2 from testCube" + " where (" + twoDaysITRange
            + " OR it == 'default') AND dim1 > 1000", getConf());
    String expected =
        getExpectedQuery(cubeName, "select testcube.dim1, max(testcube.msr3), sum(testcube.msr2) FROM ", null,
            "or (( testcube.it ) == 'default')) and ((testcube.dim1) > 1000)" + " group by testcube.dim1",
            getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary1"));
    compareQueries(expected, hqlQuery);

    hqlQuery =
        rewrite("select SUM(msr2) from testCube" + " where " + twoDaysRange + " OR ("
            + CubeTestSetup.twoDaysRangeBefore4days + " AND dt='default')", getConf());

    String expecteddtRangeWhere1 =
        getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "dt", twodaysBack, now)
            + " OR ("
            + getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "dt", CubeTestSetup.before4daysStart,
                CubeTestSetup.before4daysEnd) + ")";
    expected =
        getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, " AND testcube.dt='default'",
            expecteddtRangeWhere1, "c2_testfact");
    compareQueries(expected, hqlQuery);

    String expecteddtRangeWhere2 =
        "("
            + getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "dt", twodaysBack, now)
            + " AND testcube.dt='dt1') OR "
            + getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "dt", CubeTestSetup.before4daysStart,
                CubeTestSetup.before4daysEnd);
    hqlQuery =
        rewrite("select SUM(msr2) from testCube" + " where (" + twoDaysRange + " AND dt='dt1') OR ("
            + CubeTestSetup.twoDaysRangeBefore4days + " AND dt='default')", getConf());
    expected =
        getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, " AND testcube.dt='default'",
            expecteddtRangeWhere2, "c2_testfact");
    compareQueries(expected, hqlQuery);

    String twoDaysPTRange =
        "time_range_in(pt, '" + CubeTestSetup.getDateUptoHours(twodaysBack) + "','"
            + CubeTestSetup.getDateUptoHours(now) + "')";
    hqlQuery =
        rewrite("select dim1, max(msr3)," + " msr2 from testCube where (" + twoDaysITRange + " OR (" + twoDaysPTRange
            + " and it == 'default')) AND dim1 > 1000", getConf());
    String expectedITPTrange =
        getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", twodaysBack, now) + " OR ("
            + getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "pt", twodaysBack, now) + ")";
    expected =
        getExpectedQuery(cubeName, "select testcube.dim1, max(testcube.msr3), sum(testcube.msr2) FROM ", null,
            "AND testcube.it == 'default' and testcube.dim1 > 1000 group by testcube.dim1", expectedITPTrange,
            "C2_summary1");
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testFactsWithTimedDimensionWithProcessTimeCol() throws Exception {
    String twoDaysITRange =
        "time_range_in(it, '" + CubeTestSetup.getDateUptoHours(twodaysBack) + "','"
            + CubeTestSetup.getDateUptoHours(now) + "')";

    Configuration conf = getConf();
    conf.set(CubeQueryConfUtil.PROCESS_TIME_PART_COL, "pt");
    String hqlQuery = rewrite("select dim1, max(msr3)," + " msr2 from testCube" + " where " + twoDaysITRange, conf);
    System.out.println("Query With process time col:" + hqlQuery);
    // TODO compare queries
    // compareQueries(expected, hqlQuery);
    hqlQuery =
        rewrite("select dim1, dim2, COUNT(msr4)," + " SUM(msr2), msr3 from testCube" + " where " + twoDaysITRange, conf);
    System.out.println("Query With process time col:" + hqlQuery);
    // TODO compare queries
    // compareQueries(expected, hqlQuery);
    hqlQuery =
        rewrite("select dim1, dim2, cityid, count(msr4)," + " SUM(msr2), msr3 from testCube" + " where "
            + twoDaysITRange, conf);
    System.out.println("Query With process time col:" + hqlQuery);
    // TODO compare queries
    // compareQueries(expected, hqlQuery);
    conf.setInt(CubeQueryConfUtil.getLookAheadPTPartsKey(UpdatePeriod.DAILY), 3);
    hqlQuery = rewrite("select dim1, max(msr3)," + " msr2 from testCube" + " where " + twoDaysITRange, conf);
    System.out.println("Query With process time col:" + hqlQuery);
    // TODO compare queries
    // compareQueries(expected, hqlQuery);
    hqlQuery =
        rewrite("select dim1, dim2, COUNT(msr4)," + " SUM(msr2), msr3 from testCube" + " where " + twoDaysITRange, conf);
    System.out.println("Query With process time col:" + hqlQuery);
    // TODO compare queries
    // compareQueries(expected, hqlQuery);
    hqlQuery =
        rewrite("select dim1, dim2, cityid, count(msr4)," + " SUM(msr2), msr3 from testCube" + " where "
            + twoDaysITRange, conf);
    System.out.println("Query With process time col:" + hqlQuery);
    // TODO compare queries
    // compareQueries(expected, hqlQuery);
  }

  @Test
  public void testCubeQueryWithMultipleRanges() throws Exception {
    String hqlQuery =
        rewrite("select SUM(msr2) from testCube" + " where " + twoDaysRange + " OR "
            + CubeTestSetup.twoDaysRangeBefore4days, getConf());

    String expectedRangeWhere =
        getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "dt", twodaysBack, now)
            + " OR "
            + getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "dt", CubeTestSetup.before4daysStart,
                CubeTestSetup.before4daysEnd);
    String expected =
        getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null, expectedRangeWhere, "c2_testfact");
    compareQueries(expected, hqlQuery);
    hqlQuery =
        rewrite("select dim1, max(msr3)," + " msr2 from testCube" + " where " + twoDaysRange + " OR "
            + CubeTestSetup.twoDaysRangeBefore4days, getConf());
    expected =
        getExpectedQuery(cubeName, "select testcube.dim1, max(testcube.msr3), sum(testcube.msr2) FROM ", null,
            " group by testcube.dim1", expectedRangeWhere, "C1_summary1");
    compareQueries(expected, hqlQuery);
    hqlQuery =
        rewrite("select dim1, dim2, COUNT(msr4)," + " SUM(msr2), msr3 from testCube" + " where " + twoDaysRange
            + " OR " + CubeTestSetup.twoDaysRangeBefore4days, getConf());
    expected =
        getExpectedQuery(cubeName, "select testcube.dim1, testcube,dim2, count(testcube.msr4),"
            + " sum(testcube.msr2), max(testcube.msr3) FROM ", null, " group by testcube.dim1, testcube.dim2",
            expectedRangeWhere, "C1_summary2");
    compareQueries(expected, hqlQuery);
    hqlQuery =
        rewrite("select dim1, dim2, cityid, count(msr4)," + " SUM(msr2), msr3 from testCube" + " where " + twoDaysRange
            + " OR " + CubeTestSetup.twoDaysRangeBefore4days, getConf());
    expected =
        getExpectedQuery(cubeName, "select testcube.dim1, testcube,dim2, testcube.cityid,"
            + " count(testcube.msr4), sum(testcube.msr2), max(testcube.msr3) FROM ", null,
            " group by testcube.dim1, testcube.dim2, testcube.cityid", expectedRangeWhere, "C1_summary3");
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testDistinctColWithoutAlias() throws Exception {
    String hqlQuery = rewrite("select DISTINCT name, stateid" + " from citydim", getConf());
    String expected =
        getExpectedQuery("citydim", "select DISTINCT" + " citydim.name, citydim.stateid from ", null, "c1_citytable",
            true);
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite("select id, sum(distinct id) from" + " citydim group by id", getConf());
    expected =
        getExpectedQuery("citydim", "select citydim.id," + " sum(DISTINCT citydim.id) from ", "group by citydim.id",
            "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite("select count(distinct id) from" + " citydim", getConf());
    expected = getExpectedQuery("citydim", "select count(DISTINCT" + " citydim.id) from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testJoinWithMultipleAliases() throws Exception {
    String cubeQl =
        "SELECT SUM(msr2) from testCube left outer join citydim c1 on testCube.cityid = c1.id"
            + " left outer join statedim s1 on c1.stateid = s1.id"
            + " left outer join citydim c2 on s1.countryid = c2.id where " + twoDaysRange;
    Configuration conf = getConf();
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
            getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);
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
    Assert.assertNotNull(col);

    final String query = "SELECT cycledim1.name, msr2 FROM testCube where " + twoDaysRange;
    try {
      CubeQueryRewriter rewriter = new CubeQueryRewriter(testConf);
      CubeQueryContext context = rewriter.rewrite(query);
      System.out.println("TestJoinPathTimeRange: " + context.toHQL());
      Assert.fail("Expected query to fail because of invalid column life");
    } catch (SemanticException exc) {
      Assert.assertEquals(exc.getCanonicalErrorMsg(), ErrorMsg.NO_JOIN_PATH);
    } finally {
      // Add old column back
      cube.alterDimension(col);
      client.alterCube(cubeName, cube);
    }

    // Assert same query succeeds with valid column
    Date oneWeekBack = DateUtils.addDays(twodaysBack, -7);

    // Alter cube.dim2 with an invalid column life
    ReferencedDimAtrribute newDim2 =
        new ReferencedDimAtrribute(new FieldSchema(col.getName(), "string", "invalid col"), col.getDisplayString(),
            col.getReferences(), oneWeekBack, null,
            col.getCost());
    cube.alterDimension(newDim2);
    client.alterCube(cubeName, cube);
    CubeQueryRewriter rewriter = new CubeQueryRewriter(testConf);
    CubeQueryContext context = rewriter.rewrite(query);
    String hql = context.toHQL();
    Assert.assertNotNull(hql);
  }

  @Test
  public void testCubeQueryWithSpaceInAlias() throws Exception {
    String query = "SELECT sum(msr2) as `a measure` from testCube where " + twoDaysRange;
    CubeQueryRewriter rewriter = new CubeQueryRewriter(getConf());
    try {
      HQLParser.printAST(HQLParser.parseHQL(query));
      CubeQueryContext ctx = rewriter.rewrite(query);
      String hql = ctx.toHQL();
      Assert.assertNotNull(hql);
      // test that quotes are preserved
      Assert.assertTrue(hql.contains("`a measure`"));
      System.out.println("@@ hql: " + hql);
    } catch (NullPointerException npe) {
      npe.printStackTrace();
      Assert.fail("Not expecting null pointer exception");
    }
  }

  @Test
  public void testTimeDimensionAndPartCol() throws Exception {
    // Test if time dimension is replaced with partition column
    // Disabling conf should not replace the time dimension

    String query =
        "SELECT test_time_dim, msr2 FROM testCube where " + "time_range_in(test_time_dim, '"
            + CubeTestSetup.getDateUptoHours(twodaysBack) + "','" + CubeTestSetup.getDateUptoHours(now) + "')";

    HiveConf hconf = new HiveConf(getConf(), TestCubeRewriter.class);
    hconf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
    hconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1,C2,C3,C4");
    hconf.setBoolean(CubeQueryConfUtil.REPLACE_TIMEDIM_WITH_PART_COL, true);

    CubeQueryRewriter rewriter = new CubeQueryRewriter(hconf);
    CubeQueryContext context = rewriter.rewrite(query);
    String hql = context.toHQL();
    System.out.println("@@" + hql);
    Assert.assertTrue(hql.contains("ttd") && hql.contains("full_hour"));

    Assert.assertTrue(context.shouldReplaceTimeDimWithPart());

    String partCol = context.getPartitionColumnOfTimeDim("test_time_dim");
    Assert.assertEquals("ttd", partCol);

    String timeDimCol = context.getTimeDimOfPartitionColumn("ttd");
    Assert.assertEquals("test_time_dim".toLowerCase(), timeDimCol);

    // Rewrite with setting disabled
    hconf.setBoolean(CubeQueryConfUtil.REPLACE_TIMEDIM_WITH_PART_COL, false);
    rewriter = new CubeQueryRewriter(hconf);
    context = rewriter.rewrite(query);
    hql = context.toHQL();
    System.out.println("@@2 " + hql);
    Assert.assertTrue(!hql.contains("ttd") && hql.contains("full_hour"));
  }

  @Test
  public void testAliasNameSameAsColumnName() throws Exception {
    String query = "SELECT msr2 as msr2 from testCube WHERE " + twoDaysRange;
    HQLParser.printAST(HQLParser.parseHQL(query));
    HiveConf hiveConf = new HiveConf(getConf(), TestCubeRewriter.class);
    try {
      CubeQueryRewriter rewriter = new CubeQueryRewriter(hiveConf);
      CubeQueryContext ctx = rewriter.rewrite(query);
      String hql = ctx.toHQL();
      Assert.assertNotNull(hql);
      System.out.println("@@HQL " + hql);
    } catch (NullPointerException npe) {
      Assert.fail(npe.getMessage());
      npe.printStackTrace();
    }
  }
}

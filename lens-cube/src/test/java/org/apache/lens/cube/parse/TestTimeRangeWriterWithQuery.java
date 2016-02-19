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

import static org.apache.lens.cube.error.LensCubeErrorCode.CANNOT_USE_TIMERANGE_WRITER;
import static org.apache.lens.cube.metadata.DateFactory.*;
import static org.apache.lens.cube.metadata.UpdatePeriod.CONTINUOUS;
import static org.apache.lens.cube.metadata.UpdatePeriod.DAILY;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA;
import static org.apache.lens.cube.parse.CubeTestSetup.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class TestTimeRangeWriterWithQuery extends TestQueryRewrite {

  private Configuration conf;
  private final String cubeName = TEST_CUBE_NAME;

  @BeforeTest
  public void setupDriver() throws Exception {
    conf = new Configuration();
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1,C2");
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY, true);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT, true);
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER, false);
    conf.setClass(CubeQueryConfUtil.TIME_RANGE_WRITER_CLASS,
      BetweenTimeRangeWriter.class.asSubclass(TimeRangeWriter.class), TimeRangeWriter.class);
  }

  private Date getOneLess(Date in, int calField) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(in);
    cal.add(calField, -1);
    return cal.getTime();
  }

  private Date getUptoHour(Date in) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(in);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTime();
  }

  @Test
  public void testCubeQueryContinuousUpdatePeriod() throws Exception {
    LensException th = null;
    try {
      rewrite("select" + " SUM(msr2) from testCube where " + TWO_DAYS_RANGE, conf);
    } catch (LensException e) {
      th = e;
      log.error("Semantic exception while testing cube query.", e);
    }
    if (!isZerothHour()) {
      Assert.assertNotNull(th);
      Assert
      .assertEquals(th.getErrorCode(), CANNOT_USE_TIMERANGE_WRITER.getLensErrorInfo().getErrorCode());
    }
    // hourly partitions for two days
    conf.setBoolean(FAIL_QUERY_ON_PARTIAL_DATA, true);
    DateFormat qFmt = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
    String twoDaysInRangeClause = getTimeRangeString(DAILY, -2, 0, qFmt);

    String hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + twoDaysInRangeClause, conf);
    Map<String, String> whereClauses = new HashMap<String, String>();
    whereClauses.put(
      getDbName() + "c1_testfact",
      TestBetweenTimeRangeWriter.getBetweenClause(cubeName, "dt",
        getDateWithOffset(DAILY, -2), getDateWithOffset(DAILY, 0), CONTINUOUS.format()));
    String expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null, whereClauses);
    System.out.println("HQL:" + hqlQuery);
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // multiple range query
    //from date 6 days back
    String fourDaysInRangeClause = getTimeRangeString(DAILY, -6, 0, qFmt);

    hqlQuery =
      rewrite("select SUM(msr2) from testCube" + " where " + twoDaysInRangeClause + " OR "
        + fourDaysInRangeClause, conf);

    whereClauses = new HashMap<String, String>();
    whereClauses.put(
      getDbName() + "c1_testfact",
      TestBetweenTimeRangeWriter.getBetweenClause(cubeName, "dt", getDateWithOffset(DAILY, -2),
        getDateWithOffset(DAILY, 0), CONTINUOUS.format())
        + " OR"
        + TestBetweenTimeRangeWriter.getBetweenClause(cubeName, "dt", getDateWithOffset(DAILY, -6),
        getDateWithOffset(DAILY, 0), CONTINUOUS.format()));
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null, whereClauses);
    System.out.println("HQL:" + hqlQuery);
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // format option in the query
    conf.set(CubeQueryConfUtil.PART_WHERE_CLAUSE_DATE_FORMAT, "yyyy-MM-dd HH:mm:ss");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    whereClauses = new HashMap<String, String>();
    whereClauses.put(getDbName() + "c1_testfact", TestBetweenTimeRangeWriter.getBetweenClause(cubeName,
      "dt", getUptoHour(TWODAYS_BACK),
      getUptoHour(NOW), TestTimeRangeWriter.DB_FORMAT));
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null, whereClauses);
    System.out.println("HQL:" + hqlQuery);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testCubeQueryWithTimeDim() throws Exception {
    Configuration tconf = new Configuration(conf);
    // hourly partitions for two days
    tconf.setBoolean(FAIL_QUERY_ON_PARTIAL_DATA, true);
    tconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C4");
    tconf.setBoolean(CubeQueryConfUtil.REPLACE_TIMEDIM_WITH_PART_COL, false);
    tconf.set(CubeQueryConfUtil.PART_WHERE_CLAUSE_DATE_FORMAT, "yyyy-MM-dd HH:mm:ss");
    tconf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact", "C4"), "MONTHLY,DAILY,HOURLY");

    String query =
      "SELECT test_time_dim, msr2 FROM testCube where " + TWO_DAYS_RANGE_TTD;
    String hqlQuery = rewrite(query, tconf);
    Map<String, String> whereClauses = new HashMap<String, String>();
    whereClauses.put(getDbName() + "c4_testfact2", TestBetweenTimeRangeWriter.getBetweenClause("timehourchain1",
      "full_hour", getUptoHour(TWODAYS_BACK),
      getUptoHour(getOneLess(NOW, UpdatePeriod.HOURLY.calendarField())), TestTimeRangeWriter.DB_FORMAT));
    System.out.println("HQL:" + hqlQuery);
    String expected =
      getExpectedQuery(cubeName, "select timehourchain1.full_hour, sum(testcube.msr2) FROM ", " join " + getDbName()
          + "c4_hourDimTbl timehourchain1 on testcube.test_time_dim_hour_id  = timehourchain1.id", null,
        " GROUP BY timehourchain1.full_hour", null, whereClauses);
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    query =
      "SELECT msr2 FROM testCube where " + TWO_DAYS_RANGE_TTD;
    hqlQuery = rewrite(query, tconf);
    System.out.println("HQL:" + hqlQuery);
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", " join " + getDbName()
        + "c4_hourDimTbl timehourchain1 on testcube.test_time_dim_hour_id  = timehourchain1.id", null, null, null,
        whereClauses);
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    query =
      "SELECT msr2 FROM testCube where testcube.cityid > 2 and " + TWO_DAYS_RANGE_TTD + " and testcube.cityid != 5";
    hqlQuery = rewrite(query, tconf);
    System.out.println("HQL:" + hqlQuery);
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", " join " + getDbName()
          + "c4_hourDimTbl timehourchain1 on testcube.test_time_dim_hour_id  = timehourchain1.id",
        " testcube.cityid > 2 ",
        " and testcube.cityid != 5", null, whereClauses);
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // multiple range query
    hqlQuery =
      rewrite(
        "select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE_TTD
          + " OR " + TWO_DAYS_RANGE_TTD_BEFORE_4_DAYS, tconf);

    whereClauses = new HashMap<>();
    whereClauses.put(
      getDbName() + "c4_testfact2",
      TestBetweenTimeRangeWriter.getBetweenClause("timehourchain1", "full_hour", getUptoHour(TWODAYS_BACK),
        getUptoHour(getOneLess(NOW, UpdatePeriod.HOURLY.calendarField())),
        TestTimeRangeWriter.DB_FORMAT)
        + " OR "
        + TestBetweenTimeRangeWriter.getBetweenClause("timehourchain1", "full_hour", getUptoHour(BEFORE_6_DAYS),
        getUptoHour(getOneLess(BEFORE_4_DAYS, UpdatePeriod.HOURLY.calendarField())),
        TestTimeRangeWriter.DB_FORMAT));
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", " join " + getDbName()
        + "c4_hourDimTbl timehourchain1 on testcube.test_time_dim_hour_id  = timehourchain1.id", null, null, null,
        whereClauses);
    System.out.println("HQL:" + hqlQuery);
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite(
        "select to_date(test_time_dim), SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE_TTD
          + " OR " + TWO_DAYS_RANGE_TTD_BEFORE_4_DAYS, tconf);

    expected =
      getExpectedQuery(cubeName, "select to_date(timehourchain1.full_hour), sum(testcube.msr2) FROM ", " join "
          + getDbName() + "c4_hourDimTbl timehourchain1 on testcube.test_time_dim_hour_id  = timehourchain1.id", null,
        " group by to_date(timehourchain1.full_hour)", null, whereClauses);
    System.out.println("HQL:" + hqlQuery);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testCubeQueryWithTimeDimThruChain() throws Exception {
    // hourly partitions for two days
    Configuration tconf = new Configuration(conf);
    tconf.setBoolean(FAIL_QUERY_ON_PARTIAL_DATA, true);
    tconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C4");
    tconf.setBoolean(CubeQueryConfUtil.REPLACE_TIMEDIM_WITH_PART_COL, false);
    tconf.set(CubeQueryConfUtil.PART_WHERE_CLAUSE_DATE_FORMAT, "yyyy-MM-dd HH:mm:ss");
    tconf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact", "C4"), "MONTHLY,DAILY,HOURLY");

    String query =
      "SELECT test_time_dim2, msr2 FROM testCube where " + TWO_DAYS_RANGE_TTD2;
    String hqlQuery = rewrite(query, tconf);
    Map<String, String> whereClauses = new HashMap<String, String>();
    whereClauses.put(getDbName() + "c4_testfact2", TestBetweenTimeRangeWriter.getBetweenClause(
      "timehourchain2", "full_hour", getUptoHour(TWODAYS_BACK),
      getUptoHour(getOneLess(NOW, UpdatePeriod.HOURLY.calendarField())), TestTimeRangeWriter.DB_FORMAT));
    System.out.println("HQL:" + hqlQuery);
    String expected =
      getExpectedQuery(cubeName, "select timehourchain2.full_hour, sum(testcube.msr2) FROM ", " join " + getDbName()
          + "c4_hourDimTbl timehourchain2 on testcube.test_time_dim_hour_id2  = timehourchain2.id", null,
        " GROUP BY timehourchain2.full_hour", null, whereClauses);
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    query =
      "SELECT msr2 FROM testCube where " + TWO_DAYS_RANGE_TTD2;
    hqlQuery = rewrite(query, tconf);
    System.out.println("HQL:" + hqlQuery);
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", " join " + getDbName()
        + "c4_hourDimTbl timehourchain2 on testcube.test_time_dim_hour_id2  = timehourchain2.id", null, null, null,
        whereClauses);
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    query =
      "SELECT msr2 FROM testCube where testcube.cityid > 2 and " + TWO_DAYS_RANGE_TTD2 + " and testcube.cityid != 5";
    hqlQuery = rewrite(query, tconf);
    System.out.println("HQL:" + hqlQuery);
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", " join " + getDbName()
          + "c4_hourDimTbl timehourchain2 on testcube.test_time_dim_hour_id2  = timehourchain2.id",
          " testcube.cityid > 2 ", " and testcube.cityid != 5", null, whereClauses);
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // multiple range query
    hqlQuery =
      rewrite(
        "select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE_TTD2
          + " OR " + TWO_DAYS_RANGE_TTD2_BEFORE_4_DAYS, tconf);

    whereClauses = new HashMap<>();
    whereClauses.put(
      getDbName() + "c4_testfact2",
      TestBetweenTimeRangeWriter.getBetweenClause("timehourchain2", "full_hour", getUptoHour(TWODAYS_BACK),
        getUptoHour(getOneLess(NOW, UpdatePeriod.HOURLY.calendarField())),
        TestTimeRangeWriter.DB_FORMAT)
        + " OR "
        + TestBetweenTimeRangeWriter.getBetweenClause("timehourchain2", "full_hour", getUptoHour(BEFORE_6_DAYS),
        getUptoHour(getOneLess(BEFORE_4_DAYS, UpdatePeriod.HOURLY.calendarField())),
        TestTimeRangeWriter.DB_FORMAT));
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", " join " + getDbName()
        + "c4_hourDimTbl timehourchain2 on testcube.test_time_dim_hour_id2  = timehourchain2.id", null, null, null,
        whereClauses);
    System.out.println("HQL:" + hqlQuery);
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite(
        "select to_date(test_time_dim2), SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE_TTD2
          + " OR " +TWO_DAYS_RANGE_TTD2_BEFORE_4_DAYS, tconf);

    expected =
      getExpectedQuery(cubeName, "select to_date(timehourchain2.full_hour), sum(testcube.msr2) FROM ", " join "
          + getDbName() + "c4_hourDimTbl timehourchain2 on testcube.test_time_dim_hour_id2  = timehourchain2.id", null,
        " group by to_date(timehourchain2.full_hour)", null, whereClauses);
    System.out.println("HQL:" + hqlQuery);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

}

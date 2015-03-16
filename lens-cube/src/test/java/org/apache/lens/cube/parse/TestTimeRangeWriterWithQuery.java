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

import static org.apache.lens.cube.parse.CubeTestSetup.*;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.lens.cube.metadata.UpdatePeriod;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestTimeRangeWriterWithQuery extends TestQueryRewrite {

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
  public void testCubeQuery() throws Exception {
    SemanticException th = null;
    try {
      rewrite("cube select" + " SUM(msr2) from testCube where " + TWO_DAYS_RANGE, conf);
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    if (!CubeTestSetup.isZerothHour()) {
      Assert.assertNotNull(th);
      Assert
        .assertEquals(th.getCanonicalErrorMsg().getErrorCode(), ErrorMsg.CANNOT_USE_TIMERANGE_WRITER.getErrorCode());
    }
    // hourly partitions for two days
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);
    String hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    Map<String, String> whereClauses = new HashMap<String, String>();
    whereClauses.put(
      CubeTestSetup.getDbName() + "c1_testfact2",
      TestBetweenTimeRangeWriter.getBetweenClause(cubeName, "dt", CubeTestSetup.TWODAYS_BACK,
        getOneLess(CubeTestSetup.NOW, UpdatePeriod.HOURLY.calendarField()), UpdatePeriod.HOURLY.format()));
    String expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null, whereClauses);
    System.out.println("HQL:" + hqlQuery);
    TestCubeRewriter.compareQueries(expected, hqlQuery);

    // multiple range query
    hqlQuery =
      rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE + " OR "
        + CubeTestSetup.TWO_DAYS_RANGE_BEFORE_4_DAYS, conf);

    whereClauses = new HashMap<String, String>();
    whereClauses.put(
      CubeTestSetup.getDbName() + "c1_testfact2",
      TestBetweenTimeRangeWriter.getBetweenClause(cubeName, "dt", CubeTestSetup.TWODAYS_BACK,
        getOneLess(CubeTestSetup.NOW, UpdatePeriod.HOURLY.calendarField()), UpdatePeriod.HOURLY.format())
        + " OR"
        + TestBetweenTimeRangeWriter.getBetweenClause(cubeName, "dt", CubeTestSetup.BEFORE_4_DAYS_START,
        getOneLess(CubeTestSetup.BEFORE_4_DAYS_END, UpdatePeriod.HOURLY.calendarField()),
        UpdatePeriod.HOURLY.format()));
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null, whereClauses);
    System.out.println("HQL:" + hqlQuery);
    TestCubeRewriter.compareQueries(expected, hqlQuery);

    // format option in the query
    conf.set(CubeQueryConfUtil.PART_WHERE_CLAUSE_DATE_FORMAT, "yyyy-MM-dd HH:mm:ss");
    hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
    whereClauses = new HashMap<String, String>();
    whereClauses.put(CubeTestSetup.getDbName() + "c1_testfact2", TestBetweenTimeRangeWriter.getBetweenClause(cubeName,
      "dt", getUptoHour(CubeTestSetup.TWODAYS_BACK),
      getUptoHour(getOneLess(CubeTestSetup.NOW, UpdatePeriod.HOURLY.calendarField())), TestTimeRangeWriter.DB_FORMAT));
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null, whereClauses);
    System.out.println("HQL:" + hqlQuery);
    TestCubeRewriter.compareQueries(expected, hqlQuery);
  }

  @Test
  public void testCubeQueryWithTimeDim() throws Exception {
    // hourly partitions for two days
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C3,C4");
    conf.setBoolean(CubeQueryConfUtil.REPLACE_TIMEDIM_WITH_PART_COL, false);
    conf.set(CubeQueryConfUtil.PART_WHERE_CLAUSE_DATE_FORMAT, "yyyy-MM-dd HH:mm:ss");

    String query =
      "SELECT test_time_dim, msr2 FROM testCube where " + "time_range_in(test_time_dim, '"
        + CubeTestSetup.getDateUptoHours(TWODAYS_BACK) + "','" + CubeTestSetup.getDateUptoHours(NOW) + "')";
    String hqlQuery = rewrite(query, conf);
    Map<String, String> whereClauses = new HashMap<String, String>();
    whereClauses.put(CubeTestSetup.getDbName() + "c4_testfact", TestBetweenTimeRangeWriter.getBetweenClause("hourdim",
      "full_hour", getUptoHour(CubeTestSetup.TWODAYS_BACK),
      getUptoHour(getOneLess(CubeTestSetup.NOW, UpdatePeriod.HOURLY.calendarField())), TestTimeRangeWriter.DB_FORMAT));
    System.out.println("HQL:" + hqlQuery);
    String expected =
      getExpectedQuery(cubeName, "select hourdim.full_hour, sum(testcube.msr2) FROM ", " join " + getDbName()
          + "c4_hourDimTbl hourdim on testcube.test_time_dim_hour_id  = hourdim.id", null,
        " GROUP BY hourdim.full_hour", null, whereClauses);
    TestCubeRewriter.compareQueries(expected, hqlQuery);

    query =
      "SELECT msr2 FROM testCube where " + "time_range_in(test_time_dim, '"
        + CubeTestSetup.getDateUptoHours(TWODAYS_BACK) + "','" + CubeTestSetup.getDateUptoHours(NOW) + "')";
    hqlQuery = rewrite(query, conf);
    System.out.println("HQL:" + hqlQuery);
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", " join " + getDbName()
        + "c4_hourDimTbl hourdim on testcube.test_time_dim_hour_id  = hourdim.id", null, null, null, whereClauses);
    TestCubeRewriter.compareQueries(expected, hqlQuery);

    query =
      "SELECT msr2 FROM testCube where testcube.cityid > 2 and " + "time_range_in(test_time_dim, '"
        + CubeTestSetup.getDateUptoHours(TWODAYS_BACK) + "','" + CubeTestSetup.getDateUptoHours(NOW)
        + "') and testcube.cityid != 5";
    hqlQuery = rewrite(query, conf);
    System.out.println("HQL:" + hqlQuery);
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", " join " + getDbName()
          + "c4_hourDimTbl hourdim on testcube.test_time_dim_hour_id  = hourdim.id", " testcube.cityid > 2 ",
        " and testcube.cityid != 5", null, whereClauses);
    TestCubeRewriter.compareQueries(expected, hqlQuery);

    // multiple range query
    hqlQuery =
      rewrite(
        "select SUM(msr2) from testCube" + " where time_range_in(test_time_dim, '"
          + CubeTestSetup.getDateUptoHours(TWODAYS_BACK) + "','" + CubeTestSetup.getDateUptoHours(NOW) + "')"
          + " OR time_range_in(test_time_dim, '" + CubeTestSetup.getDateUptoHours(BEFORE_4_DAYS_START) + "','"
          + CubeTestSetup.getDateUptoHours(BEFORE_4_DAYS_END) + "')", conf);

    whereClauses = new HashMap<String, String>();
    whereClauses.put(
      CubeTestSetup.getDbName() + "c4_testfact",
      TestBetweenTimeRangeWriter.getBetweenClause("hourdim", "full_hour", getUptoHour(CubeTestSetup.TWODAYS_BACK),
        getUptoHour(getOneLess(CubeTestSetup.NOW, UpdatePeriod.HOURLY.calendarField())),
        TestTimeRangeWriter.DB_FORMAT)
        + " OR "
        + TestBetweenTimeRangeWriter.getBetweenClause("hourdim", "full_hour", getUptoHour(BEFORE_4_DAYS_START),
        getUptoHour(getOneLess(BEFORE_4_DAYS_END, UpdatePeriod.HOURLY.calendarField())),
        TestTimeRangeWriter.DB_FORMAT));
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", " join " + getDbName()
        + "c4_hourDimTbl hourdim on testcube.test_time_dim_hour_id  = hourdim.id", null, null, null, whereClauses);
    System.out.println("HQL:" + hqlQuery);
    TestCubeRewriter.compareQueries(expected, hqlQuery);

    hqlQuery =
      rewrite(
        "select to_date(test_time_dim), SUM(msr2) from testCube" + " where time_range_in(test_time_dim, '"
          + CubeTestSetup.getDateUptoHours(TWODAYS_BACK) + "','" + CubeTestSetup.getDateUptoHours(NOW) + "')"
          + " OR time_range_in(test_time_dim, '" + CubeTestSetup.getDateUptoHours(BEFORE_4_DAYS_START) + "','"
          + CubeTestSetup.getDateUptoHours(BEFORE_4_DAYS_END) + "')", conf);

    expected =
      getExpectedQuery(cubeName, "select to_date(hourdim.full_hour), sum(testcube.msr2) FROM ", " join "
          + getDbName() + "c4_hourDimTbl hourdim on testcube.test_time_dim_hour_id  = hourdim.id", null,
        " group by to_date(hourdim.full_hour)", null, whereClauses);
    System.out.println("HQL:" + hqlQuery);
    TestCubeRewriter.compareQueries(expected, hqlQuery);
  }

  @Test
  public void testCubeQueryWithTimeDimThruChain() throws Exception {
    // hourly partitions for two days
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C3,C4");
    conf.setBoolean(CubeQueryConfUtil.REPLACE_TIMEDIM_WITH_PART_COL, false);
    conf.set(CubeQueryConfUtil.PART_WHERE_CLAUSE_DATE_FORMAT, "yyyy-MM-dd HH:mm:ss");

    String query =
      "SELECT test_time_dim2, msr2 FROM testCube where " + "time_range_in(test_time_dim2, '"
        + CubeTestSetup.getDateUptoHours(TWODAYS_BACK) + "','" + CubeTestSetup.getDateUptoHours(NOW) + "')";
    String hqlQuery = rewrite(query, conf);
    Map<String, String> whereClauses = new HashMap<String, String>();
    whereClauses.put(CubeTestSetup.getDbName() + "c4_testfact", TestBetweenTimeRangeWriter.getBetweenClause("timechain",
      "full_hour", getUptoHour(CubeTestSetup.TWODAYS_BACK),
      getUptoHour(getOneLess(CubeTestSetup.NOW, UpdatePeriod.HOURLY.calendarField())), TestTimeRangeWriter.DB_FORMAT));
    System.out.println("HQL:" + hqlQuery);
    String expected =
      getExpectedQuery(cubeName, "select timechain.full_hour, sum(testcube.msr2) FROM ", " join " + getDbName()
          + "c4_hourDimTbl timechain on testcube.test_time_dim_hour_id2  = timechain.id", null,
        " GROUP BY timechain.full_hour", null, whereClauses);
    TestCubeRewriter.compareQueries(expected, hqlQuery);

    query =
      "SELECT msr2 FROM testCube where " + "time_range_in(test_time_dim2, '"
        + CubeTestSetup.getDateUptoHours(TWODAYS_BACK) + "','" + CubeTestSetup.getDateUptoHours(NOW) + "')";
    hqlQuery = rewrite(query, conf);
    System.out.println("HQL:" + hqlQuery);
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", " join " + getDbName()
        + "c4_hourDimTbl timechain on testcube.test_time_dim_hour_id2  = timechain.id", null, null, null, whereClauses);
    TestCubeRewriter.compareQueries(expected, hqlQuery);

    query =
      "SELECT msr2 FROM testCube where testcube.cityid > 2 and " + "time_range_in(test_time_dim2, '"
        + CubeTestSetup.getDateUptoHours(TWODAYS_BACK) + "','" + CubeTestSetup.getDateUptoHours(NOW)
        + "') and testcube.cityid != 5";
    hqlQuery = rewrite(query, conf);
    System.out.println("HQL:" + hqlQuery);
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", " join " + getDbName()
          + "c4_hourDimTbl timechain on testcube.test_time_dim_hour_id2  = timechain.id", " testcube.cityid > 2 ",
        " and testcube.cityid != 5", null, whereClauses);
    TestCubeRewriter.compareQueries(expected, hqlQuery);

    // multiple range query
    hqlQuery =
      rewrite(
        "select SUM(msr2) from testCube" + " where time_range_in(test_time_dim2, '"
          + CubeTestSetup.getDateUptoHours(TWODAYS_BACK) + "','" + CubeTestSetup.getDateUptoHours(NOW) + "')"
          + " OR time_range_in(test_time_dim2, '" + CubeTestSetup.getDateUptoHours(BEFORE_4_DAYS_START) + "','"
          + CubeTestSetup.getDateUptoHours(BEFORE_4_DAYS_END) + "')", conf);

    whereClauses = new HashMap<String, String>();
    whereClauses.put(
      CubeTestSetup.getDbName() + "c4_testfact",
      TestBetweenTimeRangeWriter.getBetweenClause("timechain", "full_hour", getUptoHour(CubeTestSetup.TWODAYS_BACK),
        getUptoHour(getOneLess(CubeTestSetup.NOW, UpdatePeriod.HOURLY.calendarField())),
        TestTimeRangeWriter.DB_FORMAT)
        + " OR "
        + TestBetweenTimeRangeWriter.getBetweenClause("timechain", "full_hour", getUptoHour(BEFORE_4_DAYS_START),
        getUptoHour(getOneLess(BEFORE_4_DAYS_END, UpdatePeriod.HOURLY.calendarField())),
        TestTimeRangeWriter.DB_FORMAT));
    expected =
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", " join " + getDbName()
        + "c4_hourDimTbl timechain on testcube.test_time_dim_hour_id2  = timechain.id", null, null, null, whereClauses);
    System.out.println("HQL:" + hqlQuery);
    TestCubeRewriter.compareQueries(expected, hqlQuery);

    hqlQuery =
      rewrite(
        "select to_date(test_time_dim2), SUM(msr2) from testCube" + " where time_range_in(test_time_dim2, '"
          + CubeTestSetup.getDateUptoHours(TWODAYS_BACK) + "','" + CubeTestSetup.getDateUptoHours(NOW) + "')"
          + " OR time_range_in(test_time_dim2, '" + CubeTestSetup.getDateUptoHours(BEFORE_4_DAYS_START) + "','"
          + CubeTestSetup.getDateUptoHours(BEFORE_4_DAYS_END) + "')", conf);

    expected =
      getExpectedQuery(cubeName, "select to_date(timechain.full_hour), sum(testcube.msr2) FROM ", " join "
          + getDbName() + "c4_hourDimTbl timechain on testcube.test_time_dim_hour_id2  = timechain.id", null,
        " group by to_date(timechain.full_hour)", null, whereClauses);
    System.out.println("HQL:" + hqlQuery);
    TestCubeRewriter.compareQueries(expected, hqlQuery);
  }

}

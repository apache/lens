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

import static java.util.Calendar.DAY_OF_MONTH;
import static java.util.Calendar.HOUR_OF_DAY;
import static java.util.Calendar.MONTH;

import static org.apache.lens.cube.metadata.UpdatePeriod.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.lens.cube.metadata.*;
import org.apache.lens.cube.metadata.ExprColumn.ExprSpec;
import org.apache.lens.cube.metadata.ReferencedDimAtrribute.ChainRefCol;
import org.apache.lens.cube.metadata.timeline.EndsAndHolesPartitionTimeline;
import org.apache.lens.cube.metadata.timeline.PartitionTimeline;
import org.apache.lens.cube.metadata.timeline.StoreAllPartitionTimeline;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.TextInputFormat;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/*
 * Here is the cube test setup
 *
 * Cube : testCube
 *
 * Fact storage and Updates:
 * testFact : {C1, C2, C3, C4} -> {Minutely, hourly, daily, monthly, quarterly, yearly}
 * testFact2 : {C1} -> {Hourly}
 * testFactMonthly : {C2} -> {Monthly}
 * summary1,summary2,summary3 - {C1, C2} -> {daily, hourly, minutely}
 * cheapFact: {C99} -> {Minutely, hourly, daily, monthly, quarterly, yearly}
 *   C2 has multiple dated partitions
 *   C99 is not to be used as supported storage in testcases
 *
 * CityTable : C1 - SNAPSHOT and C2 - NO snapshot
 *
 * Cube : Basecube
 * Derived cubes : der1, der2,der3
 *
 * Fact storage and Updates:
 * testFact1_BASE : {C1, C2, C3, C4} -> {Minutely, hourly, daily, monthly, quarterly, yearly}
 * testFact2_BASE : {C1, C2, C3, C4} -> {Minutely, hourly, daily, monthly, quarterly, yearly}
 * testFact1_RAW_BASE : {C1} -> {hourly}
 * testFact2_RAW_BASE : {C1} -> {hourly}
 */

@SuppressWarnings("deprecation")
@Slf4j
public class CubeTestSetup {

  public static final String HOUR_FMT = "yyyy-MM-dd-HH";
  public static final SimpleDateFormat HOUR_PARSER = new SimpleDateFormat(HOUR_FMT);
  public static final String MONTH_FMT = "yyyy-MM";
  public static final SimpleDateFormat MONTH_PARSER = new SimpleDateFormat(MONTH_FMT);
  private Set<CubeMeasure> cubeMeasures;
  private Set<CubeDimAttribute> cubeDimensions;
  public static final String TEST_CUBE_NAME = "testCube";
  public static final String DERIVED_CUBE_NAME = "derivedCube";
  public static final String BASE_CUBE_NAME = "baseCube";
  public static final String DERIVED_CUBE_NAME1 = "der1";
  public static final String DERIVED_CUBE_NAME2 = "der2";
  public static final String DERIVED_CUBE_NAME3 = "der3";
  public static final String DERIVED_CUBE_NAME4 = "der4";

  // Time Instances as Date Type
  public static final Date NOW;
  public static final Date LAST_HOUR;
  public static final Date TWODAYS_BACK;
  public static final Date ONE_DAY_BACK;
  public static final Date TWO_MONTHS_BACK;
  public static final Date BEFORE_4_DAYS_START;
  public static final Date BEFORE_4_DAYS_END;
  public static final Date THIS_YEAR_START;
  public static final Date THIS_YEAR_END;
  public static final Date LAST_YEAR_START;
  public static final Date LAST_YEAR_END;

  // Time Ranges
  public static final String LAST_HOUR_TIME_RANGE;
  public static final String TWO_DAYS_RANGE;
  public static final String TWO_DAYS_RANGE_TTD;
  public static final String THIS_YEAR_RANGE;
  public static final String LAST_YEAR_RANGE;
  public static final String TWO_MONTHS_RANGE_UPTO_MONTH;
  public static final String TWO_MONTHS_RANGE_UPTO_HOURS;
  public static final String TWO_DAYS_RANGE_BEFORE_4_DAYS;

  private static boolean zerothHour;
  private static String c0 = "C0";
  private static String c1 = "C1";
  private static String c2 = "C2";
  private static String c3 = "C3";
  private static String c4 = "C4";
  private static String c99 = "C99";
  private static Map<String, String> factValidityProperties = Maps.newHashMap();
  @Getter
  private static Map<String, String> storageToUpdatePeriodMap = new LinkedHashMap<String, String>();

  static {
    Calendar cal = Calendar.getInstance();
    NOW = cal.getTime();
    log.debug("Test now:{}", NOW);

    // Figure out if current hour is 0th hour
    zerothHour = (cal.get(HOUR_OF_DAY) == 0);

    // Figure out last hour
    cal.add(HOUR_OF_DAY, -1);
    LAST_HOUR = cal.getTime();
    log.debug("LastHour:{}", LAST_HOUR);

    cal.setTime(NOW);
    cal.add(DAY_OF_MONTH, -1);
    ONE_DAY_BACK = cal.getTime();
    cal.add(DAY_OF_MONTH, -1);
    TWODAYS_BACK = cal.getTime();
    System.out.println("Test TWODAYS_BACK:" + TWODAYS_BACK);

    // two months back
    cal.setTime(NOW);
    cal.add(MONTH, -2);
    TWO_MONTHS_BACK = cal.getTime();
    System.out.println("Test TWO_MONTHS_BACK:" + TWO_MONTHS_BACK);

    // Before 4days
    cal.setTime(NOW);
    cal.add(DAY_OF_MONTH, -4);
    BEFORE_4_DAYS_END = cal.getTime();
    cal.add(DAY_OF_MONTH, -2);
    BEFORE_4_DAYS_START = cal.getTime();


    THIS_YEAR_START = DateUtils.truncate(NOW, YEARLY.calendarField());
    THIS_YEAR_END = DateUtils.addYears(THIS_YEAR_START, 1);
    LAST_YEAR_START = DateUtils.addYears(THIS_YEAR_START, -1);
    LAST_YEAR_END = THIS_YEAR_START;
    TWO_DAYS_RANGE_BEFORE_4_DAYS =
      "time_range_in(d_time, '" + CubeTestSetup.getDateUptoHours(BEFORE_4_DAYS_START) + "','"
        + CubeTestSetup.getDateUptoHours(BEFORE_4_DAYS_END) + "')";


    TWO_DAYS_RANGE = "time_range_in(d_time, '" + getDateUptoHours(TWODAYS_BACK) + "','" + getDateUptoHours(NOW) + "')";
    TWO_DAYS_RANGE_TTD = "time_range_in(test_time_dim, '" + getDateUptoHours(TWODAYS_BACK) + "','"
      + getDateUptoHours(NOW) + "')";
    THIS_YEAR_RANGE =
      "time_range_in(d_time, '" + getDateUptoHours(THIS_YEAR_START) + "','" + getDateUptoHours(THIS_YEAR_END) + "')";
    LAST_YEAR_RANGE =
      "time_range_in(d_time, '" + getDateUptoHours(LAST_YEAR_START) + "','" + getDateUptoHours(LAST_YEAR_END) + "')";
    TWO_MONTHS_RANGE_UPTO_MONTH =
      "time_range_in(d_time, '" + getDateUptoMonth(TWO_MONTHS_BACK) + "','" + getDateUptoMonth(NOW) + "')";
    TWO_MONTHS_RANGE_UPTO_HOURS =
      "time_range_in(d_time, '" + getDateUptoHours(TWO_MONTHS_BACK) + "','" + getDateUptoHours(NOW) + "')";

    // calculate LAST_HOUR_TIME_RANGE
    LAST_HOUR_TIME_RANGE = getTimeRangeString(getDateUptoHours(LAST_HOUR), getDateUptoHours(NOW));
    factValidityProperties.put(MetastoreConstants.FACT_RELATIVE_START_TIME, "now.year - 90 days");
  }

  public static boolean isZerothHour() {
    return zerothHour;
  }

  public static String getDateUptoHours(Date dt) {
    return HOUR_PARSER.format(dt);
  }

  public static String getDateUptoMonth(Date dt) {
    return MONTH_PARSER.format(dt);
  }

  public static String getExpectedQuery(String cubeName, String selExpr, String whereExpr, String postWhereExpr,
    Map<String, String> storageTableToWhereClause) {
    return getExpectedQuery(cubeName, selExpr, whereExpr, postWhereExpr, storageTableToWhereClause, null);
  }

  public static String getExpectedQuery(String cubeName, String selExpr, String whereExpr, String postWhereExpr,
    Map<String, String> storageTableToWhereClause, List<String> notLatestConditions) {
    StringBuilder expected = new StringBuilder();
    for (Map.Entry<String, String> entry : storageTableToWhereClause.entrySet()) {
      String storageTable = entry.getKey();
      expected.append(selExpr);
      expected.append(storageTable);
      expected.append(" ");
      expected.append(cubeName);
      expected.append(" WHERE ");
      expected.append("(");
      if (notLatestConditions != null) {
        for (String cond : notLatestConditions) {
          expected.append(cond).append(" AND ");
        }
      }
      if (whereExpr != null) {
        expected.append(whereExpr);
        expected.append(" AND ");
      }
      expected.append(entry.getValue());
      expected.append(")");
      if (postWhereExpr != null) {
        expected.append(postWhereExpr);
      }
    }
    return expected.toString();
  }

  public static String getExpectedQuery(String cubeName, String selExpr, String whereExpr, String postWhereExpr,
    String rangeWhere, String storageTable) {
    return getExpectedQuery(cubeName, selExpr, whereExpr, postWhereExpr, rangeWhere, storageTable, null);
  }

  public static String getExpectedQuery(String cubeName, String selExpr, String whereExpr, String postWhereExpr,
    String rangeWhere, String storageTable, List<String> notLatestConditions) {
    StringBuilder expected = new StringBuilder();
    expected.append(selExpr);
    expected.append(getDbName() + storageTable);
    expected.append(" ");
    expected.append(cubeName);
    expected.append(" WHERE ");
    expected.append("(");
    if (notLatestConditions != null) {
      for (String cond : notLatestConditions) {
        expected.append(cond).append(" AND ");
      }
    }
    if (whereExpr != null) {
      expected.append(whereExpr);
      expected.append(" AND ");
    }
    expected.append(rangeWhere);
    expected.append(")");
    if (postWhereExpr != null) {
      expected.append(postWhereExpr);
    }
    return expected.toString();
  }

  public static String getExpectedQuery(String cubeName, String selExpr, String joinExpr, String whereExpr,
    String postWhereExpr, List<String> joinWhereConds, Map<String, String> storageTableToWhereClause) {
    return getExpectedQuery(cubeName, selExpr, joinExpr, whereExpr, postWhereExpr,
      joinWhereConds, storageTableToWhereClause, null);
  }

  public static String getExpectedQuery(String cubeName, String selExpr, String joinExpr, String whereExpr,
    String postWhereExpr, List<String> joinWhereConds, Map<String, String> storageTableToWhereClause,
    List<String> notLatestConditions) {
    StringBuilder expected = new StringBuilder();
    int numTabs = storageTableToWhereClause.size();
    assertEquals(1, numTabs);
    for (Map.Entry<String, String> entry : storageTableToWhereClause.entrySet()) {
      String storageTable = entry.getKey();
      expected.append(selExpr);
      expected.append(storageTable);
      expected.append(" ");
      expected.append(cubeName);
      expected.append(joinExpr);
      expected.append(" WHERE ");
      expected.append("(");
      if (notLatestConditions != null) {
        for (String cond : notLatestConditions) {
          expected.append(cond).append(" AND ");
        }
      }
      if (whereExpr != null) {
        expected.append(whereExpr);
        expected.append(" AND ");
      }
      expected.append(entry.getValue());
      if (joinWhereConds != null) {
        for (String joinEntry : joinWhereConds) {
          expected.append(" AND ");
          expected.append(joinEntry);
        }
      }
      expected.append(")");
      if (postWhereExpr != null) {
        expected.append(postWhereExpr);
      }
    }
    return expected.toString();
  }

  public static Map<String, String> getWhereForDailyAndHourly2days(String cubeName, String... storageTables) {
    return getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "dt", storageTables);
  }

  public static String getDbName() {
    String database = SessionState.get().getCurrentDatabase();
    if (!"default".equalsIgnoreCase(database) && StringUtils.isNotBlank(database)) {
      return database + ".";
    }
    return "";
  }

  public static Map<String, String> getWhereForDailyAndHourly2daysWithTimeDim(String cubeName, String timedDimension,
    String... storageTables) {
    return getWhereForDailyAndHourly2daysWithTimeDim(cubeName, timedDimension, TWODAYS_BACK, NOW, storageTables);
  }


  public static Map<String, String> getWhereForDailyAndHourly2daysWithTimeDim(String cubeName, String timedDimension,
    Date from, Date to, String... storageTables) {
    Map<String, String> storageTableToWhereClause = new LinkedHashMap<String, String>();
    if (storageToUpdatePeriodMap.isEmpty()) {
      String whereClause = getWhereForDailyAndHourly2daysWithTimeDim(cubeName, timedDimension, from, to);
      storageTableToWhereClause.put(getStorageTableString(storageTables), whereClause);
    } else {
      for (String tbl : storageTables) {
        String updatePeriod = storageToUpdatePeriodMap.get(tbl);
        String whereClause = getWhereForDailyAndHourly2daysWithTimeDimUnionQuery(cubeName, timedDimension, from, to)
            .get(updatePeriod);
        storageTableToWhereClause.put(getStorageTableString(tbl), whereClause);
      }
    }
    return storageTableToWhereClause;
  }

  private static String getStorageTableString(String... storageTables) {
    String dbName = getDbName();
    if (!StringUtils.isBlank(dbName)) {
      List<String> tbls = new ArrayList<String>();
      for (String tbl : storageTables) {
        tbls.add(dbName + tbl);
      }
      return StringUtils.join(tbls, ",");
    }
    return StringUtils.join(storageTables, ",");
  }

  public static String getWhereForDailyAndHourly2daysWithTimeDim(String cubeName, String timedDimension, Date from,
    Date to) {
    List<String> hourlyparts = new ArrayList<String>();
    List<String> dailyparts = new ArrayList<String>();
    Date dayStart;
    if (!CubeTestSetup.isZerothHour()) {
      addParts(hourlyparts, HOURLY, from, DateUtil.getCeilDate(from, DAILY));
      addParts(hourlyparts, HOURLY, DateUtil.getFloorDate(to, DAILY),
        DateUtil.getFloorDate(to, HOURLY));
      dayStart = DateUtil.getCeilDate(from, DAILY);
    } else {
      dayStart = from;
    }
    addParts(dailyparts, DAILY, dayStart, DateUtil.getFloorDate(to, DAILY));
    List<String> parts = new ArrayList<String>();
    parts.addAll(hourlyparts);
    parts.addAll(dailyparts);
    Collections.sort(parts);
    return StorageUtil.getWherePartClause(timedDimension, cubeName, parts);
  }

  public static Map<String, String> getWhereForDailyAndHourly2daysWithTimeDimUnionQuery(String cubeName,
      String timedDimension, Date from, Date to) {
    Map<String, String> updatePeriodToWhereMap = new HashMap<String, String>();
    List<String> hourlyparts = new ArrayList<String>();
    List<String> dailyparts = new ArrayList<String>();
    Date dayStart;
    if (!CubeTestSetup.isZerothHour()) {
      addParts(hourlyparts, HOURLY, from, DateUtil.getCeilDate(from, DAILY));
      addParts(hourlyparts, HOURLY, DateUtil.getFloorDate(to, DAILY),
          DateUtil.getFloorDate(to, HOURLY));
      dayStart = DateUtil.getCeilDate(from, DAILY);
    } else {
      dayStart = from;
    }
    addParts(dailyparts, DAILY, dayStart, DateUtil.getFloorDate(to, DAILY));
    updatePeriodToWhereMap.put("DAILY", StorageUtil.getWherePartClause(timedDimension, cubeName, dailyparts));
    updatePeriodToWhereMap.put("HOURLY", StorageUtil.getWherePartClause(timedDimension, cubeName, hourlyparts));
    return updatePeriodToWhereMap;
  }

  // storageTables[0] is hourly
  // storageTables[1] is daily
  // storageTables[2] is monthly
  public static Map<String, String> getWhereForMonthlyDailyAndHourly2months(String... storageTables) {
    Map<String, String> storageTableToWhereClause = new LinkedHashMap<String, String>();
    List<String> hourlyparts = new ArrayList<String>();
    List<String> dailyparts = new ArrayList<String>();
    List<String> monthlyparts = new ArrayList<String>();
    Date dayStart = TWO_MONTHS_BACK;
    Date monthStart = TWO_MONTHS_BACK;
    if (!CubeTestSetup.isZerothHour()) {
      addParts(hourlyparts, HOURLY, TWO_MONTHS_BACK,
        DateUtil.getCeilDate(TWO_MONTHS_BACK, DAILY));
      addParts(hourlyparts, HOURLY, DateUtil.getFloorDate(NOW, DAILY),
        DateUtil.getFloorDate(NOW, HOURLY));
      dayStart = DateUtil.getCeilDate(TWO_MONTHS_BACK, DAILY);
      monthStart = DateUtil.getCeilDate(TWO_MONTHS_BACK, MONTHLY);
    }
    Calendar cal = new GregorianCalendar();
    cal.setTime(dayStart);
    if (cal.get(DAY_OF_MONTH) != 1) {
      addParts(dailyparts, DAILY, dayStart, DateUtil.getCeilDate(TWO_MONTHS_BACK, MONTHLY));
      monthStart = DateUtil.getCeilDate(TWO_MONTHS_BACK, MONTHLY);
    }
    addParts(dailyparts, DAILY, DateUtil.getFloorDate(NOW, MONTHLY),
      DateUtil.getFloorDate(NOW, DAILY));
    addParts(monthlyparts, MONTHLY, monthStart, DateUtil.getFloorDate(NOW, MONTHLY));
    List<String> parts = new ArrayList<String>();
    parts.addAll(dailyparts);
    parts.addAll(hourlyparts);
    parts.addAll(monthlyparts);
    StringBuilder tables = new StringBuilder();
    if (storageTables.length > 1) {
      if (!hourlyparts.isEmpty()) {
        tables.append(getDbName());
        tables.append(storageTables[0]);
        tables.append(",");
      }
      tables.append(getDbName());
      tables.append(storageTables[2]);
      if (!dailyparts.isEmpty()) {
        tables.append(",");
        tables.append(getDbName());
        tables.append(storageTables[1]);
      }
    } else {
      tables.append(getDbName());
      tables.append(storageTables[0]);
    }
    Collections.sort(parts);
    storageTableToWhereClause.put(tables.toString(), StorageUtil.getWherePartClause("dt", TEST_CUBE_NAME, parts));
    return storageTableToWhereClause;
  }

  public static Map<String, String> getWhereForMonthlyDailyAndHourly2monthsUnionQuery(String storageTable) {
    Map<String, List<String>> updatePeriodToPart = new LinkedHashMap<String, List<String>>();
    List<String> hourlyparts = new ArrayList<String>();
    List<String> dailyparts = new ArrayList<String>();
    List<String> monthlyparts = new ArrayList<String>();

    Date dayStart = TWO_MONTHS_BACK;
    Date monthStart = TWO_MONTHS_BACK;
    if (!CubeTestSetup.isZerothHour()) {
      addParts(hourlyparts, HOURLY, TWO_MONTHS_BACK,
          DateUtil.getCeilDate(TWO_MONTHS_BACK, DAILY));
      addParts(hourlyparts, HOURLY, DateUtil.getFloorDate(NOW, DAILY),
          DateUtil.getFloorDate(NOW, HOURLY));
      dayStart = DateUtil.getCeilDate(TWO_MONTHS_BACK, DAILY);
      monthStart = DateUtil.getCeilDate(TWO_MONTHS_BACK, MONTHLY);
    }
    Calendar cal = new GregorianCalendar();
    cal.setTime(dayStart);
    if (cal.get(DAY_OF_MONTH) != 1) {
      addParts(dailyparts, DAILY, dayStart, DateUtil.getCeilDate(TWO_MONTHS_BACK, MONTHLY));
      monthStart = DateUtil.getCeilDate(TWO_MONTHS_BACK, MONTHLY);
    }
    addParts(dailyparts, DAILY, DateUtil.getFloorDate(NOW, MONTHLY),
        DateUtil.getFloorDate(NOW, DAILY));
    addParts(monthlyparts, MONTHLY, monthStart, DateUtil.getFloorDate(NOW, MONTHLY));

    updatePeriodToPart.put("HOURLY", hourlyparts);
    updatePeriodToPart.put("DAILY", dailyparts);
    updatePeriodToPart.put("MONTHLY", monthlyparts);

    List<String> unionParts = new ArrayList<String>();
    for (Map.Entry<String, String> entry : storageToUpdatePeriodMap.entrySet()) {
      String uperiod = entry.getKey();
      String table = entry.getValue();
      if (table.equals(storageTable) && updatePeriodToPart.containsKey(uperiod)) {
        unionParts.addAll(updatePeriodToPart.get(uperiod));
        Collections.sort(unionParts);
      }
    }

    HashMap<String, String> tabWhere = new LinkedHashMap<String, String>();
    tabWhere.put(getStorageTableString(storageTable), StorageUtil.getWherePartClause("dt", TEST_CUBE_NAME, unionParts));

    return tabWhere;
  }

  public static Map<String, String> getWhereForMonthly2months(String monthlyTable) {
    Map<String, String> storageTableToWhereClause = new LinkedHashMap<String, String>();
    List<String> parts = new ArrayList<String>();
    addParts(parts, MONTHLY, TWO_MONTHS_BACK, DateUtil.getFloorDate(NOW, MONTHLY));
    storageTableToWhereClause.put(getDbName() + monthlyTable,
      StorageUtil.getWherePartClause("dt", TEST_CUBE_NAME, parts));
    return storageTableToWhereClause;
  }

  public static Map<String, String> getWhereForHourly2days(String hourlyTable) {
    return getWhereForHourly2days(TEST_CUBE_NAME, hourlyTable);
  }

  public static Map<String, String> getWhereForHourly2days(String alias, String hourlyTable) {
    Map<String, String> storageTableToWhereClause = new LinkedHashMap<String, String>();
    List<String> parts = new ArrayList<String>();
    addParts(parts, HOURLY, TWODAYS_BACK, DateUtil.getFloorDate(NOW, HOURLY));
    storageTableToWhereClause.put(getDbName() + hourlyTable, StorageUtil.getWherePartClause("dt", alias, parts));
    return storageTableToWhereClause;
  }

  public static void addParts(List<String> partitions, UpdatePeriod updatePeriod, Date from, Date to) {
    DateFormat fmt = updatePeriod.format();
    Calendar cal = Calendar.getInstance();
    cal.setTime(from);
    Date dt = cal.getTime();
    while (dt.before(to)) {
      String part = fmt.format(dt);
      cal.add(updatePeriod.calendarField(), 1);
      partitions.add(part);
      dt = cal.getTime();
    }
  }

  public static String getExpectedQuery(String dimName, String selExpr, String postWhereExpr, String storageTable,
    boolean hasPart) {
    return getExpectedQuery(dimName, selExpr, null, null, postWhereExpr, storageTable, hasPart);
  }

  public static String getExpectedQuery(String dimName, String selExpr, String joinExpr, String whereExpr,
    String postWhereExpr, String storageTable, boolean hasPart) {
    StringBuilder expected = new StringBuilder();
    String partWhere = null;
    if (hasPart) {
      partWhere = StorageUtil.getWherePartClause("dt", dimName, StorageConstants.getPartitionsForLatest());
    }
    expected.append(selExpr);
    expected.append(getDbName() + storageTable);
    expected.append(" ");
    expected.append(dimName);
    if (joinExpr != null) {
      expected.append(joinExpr);
    }
    if (whereExpr != null || hasPart) {
      expected.append(" WHERE ");
      expected.append("(");
      if (whereExpr != null) {
        expected.append(whereExpr);
        if (partWhere != null) {
          expected.append(" AND ");
        }
      }
      if (partWhere != null) {
        expected.append(partWhere);
      }
      expected.append(")");
    }
    if (postWhereExpr != null) {
      expected.append(postWhereExpr);
    }
    return expected.toString();
  }

  Set<ExprColumn> exprs;

  private void createCube(CubeMetastoreClient client) throws HiveException, ParseException, LensException {
    cubeMeasures = new HashSet<CubeMeasure>();
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msr1", "int", "first measure")));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msr2", "float", "second measure"), "Measure2", null, "SUM",
      "RS"));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msr3", "double", "third measure"), "Measure3", null, "MAX",
      null));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msr4", "bigint", "fourth measure"), "Measure4", null, "COUNT",
      null));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("noAggrMsr", "bigint", "measure without a default aggregate"),
      "No aggregateMsr", null, null, null));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("newmeasure", "bigint", "measure available  from now"),
      "New measure", null, null, null, NOW, null, 100.0));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msr15", "int", "first measure"), "Measure15", null, "SUM",
        "RS"));

    cubeDimensions = new HashSet<CubeDimAttribute>();
    cubeDimensions.add(new BaseDimAttribute(new FieldSchema("d_time", "timestamp", "d time")));
    cubeDimensions.add(new BaseDimAttribute(new FieldSchema("processing_time", "timestamp", "processing time")));
    List<CubeDimAttribute> locationHierarchy = new ArrayList<CubeDimAttribute>();
    locationHierarchy.add(new ReferencedDimAtrribute(new FieldSchema("zipcode", "int", "zip"), "Zip refer",
      new TableReference("zipdim", "code")));
    locationHierarchy.add(new ReferencedDimAtrribute(new FieldSchema("cityid", "int", "city"), "City refer",
      new TableReference("citydim", "id")));
    locationHierarchy.add(new ReferencedDimAtrribute(new FieldSchema("stateid", "int", "state"), "State refer",
      new TableReference("statedim", "id")));
    locationHierarchy.add(new ReferencedDimAtrribute(new FieldSchema("countryid", "int", "country"), "Country refer",
      new TableReference("countrydim", "id")));
    List<String> regions = Arrays.asList("APAC", "EMEA", "USA");
    locationHierarchy.add(new BaseDimAttribute(new FieldSchema("regionname", "string", "region"), "regionname", null,
      null, null, null, regions));

    cubeDimensions.add(new HierarchicalDimAttribute("location", "Location hierarchy", locationHierarchy));
    cubeDimensions.add(new BaseDimAttribute(new FieldSchema("dim1", "string", "basedim")));
    // Added for ambiguity test
    cubeDimensions.add(new BaseDimAttribute(new FieldSchema("ambigdim1", "string", "used in testColumnAmbiguity")));
    cubeDimensions.add(new ReferencedDimAtrribute(new FieldSchema("dim2", "int", "ref dim"), "Dim2 refer",
      new TableReference("testdim2", "id")));
    cubeDimensions.add(new ReferencedDimAtrribute(new FieldSchema("cdim2", "int", "ref dim"), "Dim2 refer",
      new TableReference("cycledim1", "id"), NOW, null, null));
    cubeDimensions.add(new ReferencedDimAtrribute(new FieldSchema("urdimid", "int", "ref dim"), "urdim refer",
      new TableReference("unreachableDim", "id"), null, null, null, false, 10L));

    // denormalized reference
    cubeDimensions.add(new ReferencedDimAtrribute(new FieldSchema("dim2big1", "bigint", "ref dim"), "Dim2 refer",
      new TableReference("testdim2", "bigid1")));
    cubeDimensions.add(new ReferencedDimAtrribute(new FieldSchema("dim2big2", "bigint", "ref dim"), "Dim2 refer",
      new TableReference("testdim2", "bigid2")));
    cubeDimensions.add(new ReferencedDimAtrribute(new FieldSchema("dim2bignew", "bigint", "ref dim"), "Dim2 refer",
      new TableReference("testdim2", "bigidnew"), NOW, null, null));
    cubeDimensions.add(new ReferencedDimAtrribute(new FieldSchema("test_time_dim_hour_id", "int", "ref dim"),
      "Timedim reference", new TableReference("hourdim", "id"), null, null, null));
    cubeDimensions.add(new ReferencedDimAtrribute(new FieldSchema("test_time_dim_day_id", "int", "ref dim"),
      "Timedim reference", new TableReference("daydim", "id"), null, null, null));
    // not creating test_time_dim_hour_id2 ref dim attribute to avoid the reference in schema graph for other paths
    // the column is only defined in chain
    cubeDimensions.add(new BaseDimAttribute(new FieldSchema("test_time_dim_hour_id2", "int", "ref dim")));
    cubeDimensions.add(new BaseDimAttribute(new FieldSchema("test_time_dim_day_id2", "int", "ref dim")));
    cubeDimensions.add(new ReferencedDimAtrribute(new FieldSchema("testdim3id", "int", "direct id to testdim3"),
      "Timedim reference", new TableReference("testdim3", "id"), null, null, null));

    List<TableReference> references = new ArrayList<TableReference>();
    references.add(new TableReference("daydim", "full_date"));
    references.add(new TableReference("hourdim", "full_hour"));
    cubeDimensions.add(new ReferencedDimAtrribute(new FieldSchema("test_time_dim", "date", "ref dim"),
      "Timedim full date", references, null, null, null, false));
    List<ChainRefCol> chainRefs = new ArrayList<>();
    chainRefs.add(new ChainRefCol("timehourchain", "full_hour"));
    chainRefs.add(new ChainRefCol("timedatechain", "full_date"));
    cubeDimensions.add(new ReferencedDimAtrribute(new FieldSchema("test_time_dim2", "date", "chained dim"),
      "Timedim full date", chainRefs, null, null, null, null));

    Set<JoinChain> joinchains = new HashSet<JoinChain>();
    JoinChain timeHourChain = new JoinChain("timehourchain", "time chain", "time dim thru hour dim");
    List<TableReference> paths = new ArrayList<TableReference>();
    paths.add(new TableReference("testcube", "test_time_dim_hour_id2"));
    paths.add(new TableReference("hourdim", "id"));
    timeHourChain.addPath(paths);
    joinchains.add(timeHourChain);

    JoinChain timeDateChain = new JoinChain("timedatechain", "time chain", "time dim thru date dim");
    paths = new ArrayList<TableReference>();
    paths.add(new TableReference("testcube", "test_time_dim_day_id2"));
    paths.add(new TableReference("daydim", "id"));
    timeDateChain.addPath(paths);
    joinchains.add(timeDateChain);
    joinchains.add(new JoinChain("cubeState", "cube-state", "state thru cube") {
      {
        addPath(new ArrayList<TableReference>() {
          {
            add(new TableReference("basecube", "stateid"));
            add(new TableReference("statedim", "id"));
          }
        });
      }
    });

    exprs = new HashSet<ExprColumn>();
    exprs.add(new ExprColumn(new FieldSchema("avgmsr", "double", "avg measure"), "Avg Msr", "avg(msr1 + msr2)"));
    exprs.add(new ExprColumn(new FieldSchema("summsrs", "double", "sum measures"), "Sum Msrs",
      "(1000 + sum(msr1) + sum(msr2))/100"));
    exprs.add(new ExprColumn(new FieldSchema("msr5", "double", "materialized in some facts"), "Fifth Msr",
      "msr2 + msr3"));
    exprs.add(new ExprColumn(new FieldSchema("equalsums", "double", "sums are equals"), "equalsums",
      new ExprSpec("msr3 + msr4", null, null), new ExprSpec("(msr3 + msr2)/100", null, null)));
    exprs.add(new ExprColumn(new FieldSchema("roundedmsr1", "double", "rounded measure1"), "Rounded msr1",
      "round(msr1/1000)"));
    exprs.add(new ExprColumn(new FieldSchema("roundedmsr2", "double", "rounded measure2"), "Rounded msr2",
      "round(msr2/1000)"));
    exprs.add(new ExprColumn(new FieldSchema("nestedexpr", "double", "nested expr"), "Nested expr",
      new ExprSpec("avg(roundedmsr2)", null, null), new ExprSpec("avg(equalsums)", null, null),
      new ExprSpec("case when substrexpr = 'xyz' then avg(msr5) when substrexpr = 'abc' then avg(msr4)/100 end",
        null, null)));
    exprs.add(new ExprColumn(new FieldSchema("msr2expr", "double", "nested expr"), "Nested expr",
      new ExprSpec("case when cityStateName = 'xyz' then msr2 else 0 end", null, null)));
    exprs.add(new ExprColumn(new FieldSchema("nestedExprWithTimes", "double", "nested expr"), "Nested expr",
      new ExprSpec("avg(roundedmsr2)", null, null), new ExprSpec("avg(equalsums)", null, null),
      new ExprSpec("case when substrexpr = 'xyz' then avg(msr5) when substrexpr = 'abc' then avg(msr4)/100 end",
        NOW, null), new ExprSpec("avg(newmeasure)", null, null)));
    exprs.add(new ExprColumn(new FieldSchema("msr6", "bigint", "sixth measure"), "Measure6",
      "sum(msr2) + max(msr3)/ count(msr4)"));
    exprs.add(new ExprColumn(new FieldSchema("booleancut", "boolean", "a boolean expression"), "Boolean cut",
      "dim1 != 'x' AND dim2 != 10 "));
    exprs.add(new ExprColumn(new FieldSchema("substrexpr", "string", "a sub-string expression"), "Substr expr",
      new ExprSpec("substr(dim1, 3))", null, null), new ExprSpec("substr(ascii(testdim2.name), 3)", null, null)));
    exprs.add(new ExprColumn(new FieldSchema("substrexprdim2", "string", "a sub-string expression"), "Substr expr",
      new ExprSpec("substr(dim2, 3))", null, null), new ExprSpec("substr(ascii(testdim2.name), 3)", null, null)));
    exprs.add(new ExprColumn(new FieldSchema("indiasubstr", "boolean", "nested sub string expression"), "Nested expr",
      "substrexpr = 'INDIA'"));
    exprs.add(new ExprColumn(new FieldSchema("refexpr", "string", "expression which facts and dimensions"),
      "Expr with cube and dim fields", "concat(dim1, \":\", citydim.name)"));
    exprs.add(new ExprColumn(new FieldSchema("nocolexpr", "string", "expression which non existing colun"),
      "No col expr", "myfun(nonexist)"));
    exprs.add(new ExprColumn(new FieldSchema("newexpr", "string", "expression which non existing colun"),
      "new measure expr", "myfun(newmeasure)"));
    exprs.add(new ExprColumn(new FieldSchema("cityAndState", "String", "city and state together"), "City and State",
      "concat(citydim.name, \":\", statedim.name)"));
    exprs.add(new ExprColumn(new FieldSchema("cityStateName", "String", "city state"), "City State",
      "concat('CityState:', citydim.statename)"));
    exprs.add(new ExprColumn(new FieldSchema("cubeStateName", "String", "statename from cubestate"), "CubeState Name",
      "substr(cubestate.name, 5)"));
    exprs.add(new ExprColumn(new FieldSchema("substrdim2big1", "String", "substr of dim2big1"), "dim2big1 substr",
      "substr(dim2big1, 5)"));

    Map<String, String> cubeProperties = new HashMap<String, String>();
    cubeProperties.put(MetastoreUtil.getCubeTimedDimensionListKey(TEST_CUBE_NAME),
      "d_time,pt,it,et,test_time_dim,test_time_dim2");
    cubeProperties.put(MetastoreConstants.TIMEDIM_TO_PART_MAPPING_PFX + "test_time_dim", "ttd");
    cubeProperties.put(MetastoreConstants.TIMEDIM_TO_PART_MAPPING_PFX + "test_time_dim2", "ttd2");
    cubeProperties.put(MetastoreConstants.TIMEDIM_TO_PART_MAPPING_PFX + "d_time", "dt");
    cubeProperties.put(MetastoreConstants.TIMEDIM_TO_PART_MAPPING_PFX + "it", "it");
    cubeProperties.put(MetastoreConstants.TIMEDIM_TO_PART_MAPPING_PFX + "et", "et");
    cubeProperties.put(MetastoreConstants.TIMEDIM_TO_PART_MAPPING_PFX + "pt", "pt");
    cubeProperties.put(MetastoreConstants.TIMEDIM_RELATION + "d_time", "test_time_dim+[-10 days,10 days]");

    client.createCube(TEST_CUBE_NAME, cubeMeasures, cubeDimensions, exprs, joinchains, cubeProperties);

    Set<String> measures = new HashSet<String>();
    measures.add("msr1");
    measures.add("msr2");
    measures.add("msr3");
    Set<String> dimensions = new HashSet<String>();
    dimensions.add("dim1");
    dimensions.add("dim2");
    // Try creating derived cube with non existant dim/measures
    try{
      client.createDerivedCube(TEST_CUBE_NAME, DERIVED_CUBE_NAME,
        Sets.newHashSet("random_measure"), Sets.newHashSet("random_dim_attribute"),
        new HashMap<String, String>(), 5L);
    } catch(LensException e) {
      assertTrue(e.getMessage().contains("random_measure"));
      assertTrue(e.getMessage().contains("random_dim_attribute"));
      assertTrue(e.getMessage().contains("not present"));
    }
    client.createDerivedCube(TEST_CUBE_NAME, DERIVED_CUBE_NAME,
      measures, dimensions, new HashMap<String, String>(), 5L);
  }

  private void createBaseAndDerivedCubes(CubeMetastoreClient client)
    throws HiveException, ParseException, LensException {
    Set<CubeMeasure> cubeMeasures2 = new HashSet<CubeMeasure>(cubeMeasures);
    Set<CubeDimAttribute> cubeDimensions2 = new HashSet<CubeDimAttribute>(cubeDimensions);
    cubeMeasures2.add(new ColumnMeasure(new FieldSchema("msr11", "int", "first measure")));
    cubeMeasures2.add(new ColumnMeasure(new FieldSchema("msr12", "float", "second measure"), "Measure2", null, "SUM",
      "RS"));
    cubeMeasures2.add(new ColumnMeasure(new FieldSchema("msr13", "double", "third measure"), "Measure3", null, "MAX",
      null));
    cubeMeasures2.add(new ColumnMeasure(new FieldSchema("msr14", "bigint", "fourth measure"), "Measure4", null,
      "COUNT", null));

    cubeDimensions2.add(new BaseDimAttribute(new FieldSchema("dim11", "string", "basedim")));
    cubeDimensions2.add(new ReferencedDimAtrribute(new FieldSchema("dim12", "int", "ref dim"), "Dim2 refer",
      new TableReference("testdim2", "id")));
    cubeDimensions2.add(new ReferencedDimAtrribute(new FieldSchema("dim22", "int", "ref dim"), "Dim2 refer",
      "dim2chain", "id", null, null, null));
    cubeDimensions2.add(new BaseDimAttribute(new FieldSchema("userid", "int", "userid")));
    cubeDimensions2.add(new BaseDimAttribute(new FieldSchema("xuserid", "int", "userid")));
    cubeDimensions2.add(new BaseDimAttribute(new FieldSchema("yuserid", "int", "userid")));

    Map<String, String> cubeProperties = new HashMap<String, String>();
    cubeProperties.put(MetastoreUtil.getCubeTimedDimensionListKey(BASE_CUBE_NAME),
      "d_time,pt,it,et,test_time_dim,test_time_dim2");
    cubeProperties.put(MetastoreConstants.TIMEDIM_TO_PART_MAPPING_PFX + "test_time_dim", "ttd");
    cubeProperties.put(MetastoreConstants.TIMEDIM_TO_PART_MAPPING_PFX + "test_time_dim2", "ttd2");
    cubeProperties.put(MetastoreConstants.TIMEDIM_TO_PART_MAPPING_PFX + "d_time", "dt");
    cubeProperties.put(MetastoreConstants.TIMEDIM_TO_PART_MAPPING_PFX + "it", "it");
    cubeProperties.put(MetastoreConstants.TIMEDIM_TO_PART_MAPPING_PFX + "et", "et");
    cubeProperties.put(MetastoreConstants.TIMEDIM_TO_PART_MAPPING_PFX + "processing_time", "pt");
    cubeProperties.put(MetastoreConstants.TIMEDIM_RELATION + "d_time", "processing_time+[-5 days,5 days]");
    cubeProperties.put(MetastoreConstants.TIMEDIM_RELATION + "processing_time", "test_time_dim+[-5 days,5 days]");
    cubeProperties.put(MetastoreConstants.CUBE_ALL_FIELDS_QUERIABLE, "false");

    Set<JoinChain> joinchains = new HashSet<JoinChain>() {
      {
        add(new JoinChain("cityState", "city-state", "state thru city") {
          {
            addPath(new ArrayList<TableReference>() {
              {
                add(new TableReference("basecube", "cityid"));
                add(new TableReference("citydim", "id"));
                add(new TableReference("citydim", "stateid"));
                add(new TableReference("statedim", "id"));
              }
            });
            addPath(new ArrayList<TableReference>() {
              {
                add(new TableReference("basecube", "cityid"));
                add(new TableReference("citydim", "id"));
                add(new TableReference("citydim", "statename"));
                add(new TableReference("statedim", "name"));
              }
            });
          }
        });
        add(new JoinChain("cubeState", "cube-state", "state thru cube") {
          {
            addPath(new ArrayList<TableReference>() {
              {
                add(new TableReference("basecube", "stateid"));
                add(new TableReference("statedim", "id"));
              }
            });
          }
        });
        add(new JoinChain("cityZip", "city-zip", "zip thru city") {
          {
            addPath(new ArrayList<TableReference>() {
              {
                add(new TableReference("basecube", "cityid"));
                add(new TableReference("citydim", "id"));
                add(new TableReference("citydim", "zipcode"));
                add(new TableReference("zipdim", "code"));
              }
            });
          }
        });
        add(new JoinChain("cubeStateCountry", "cube-state-country", "country through state") {
          {
            addPath(new ArrayList<TableReference>() {
              {
                add(new TableReference("basecube", "stateid"));
                add(new TableReference("statedim", "id"));
                add(new TableReference("statedim", "countryid"));
                add(new TableReference("countrydim", "id"));
              }
            });
          }
        });
        add(new JoinChain("cubeCityStateCountry", "cube-city-state-country", "country through state through city") {
          {
            addPath(new ArrayList<TableReference>() {
              {
                add(new TableReference("basecube", "cityid"));
                add(new TableReference("citydim", "id"));
                add(new TableReference("citydim", "stateid"));
                add(new TableReference("statedim", "id"));
                add(new TableReference("statedim", "countryid"));
                add(new TableReference("countrydim", "id"));
              }
            });
          }
        });
        add(new JoinChain("dim2chain", "dim2chain", "dim2chain") {
          {
            addPath(new ArrayList<TableReference>() {
              {
                add(new TableReference("basecube", "dim2"));
                add(new TableReference("testdim2", "id"));
              }
            });
          }
        });
        add(new JoinChain("userSports", "user-sports", "user sports") {
          {
            addPath(new ArrayList<TableReference>() {
              {
                add(new TableReference("basecube", "userid"));
                add(new TableReference("userdim", "id"));
                add(new TableReference("userdim", "id"));
                add(new TableReference("user_interests", "user_id", true));
                add(new TableReference("user_interests", "sport_id"));
                add(new TableReference("sports", "id"));
              }
            });
          }
        });
        add(new JoinChain("userInterestIds", "user-interestsIds", "user interest ids") {
          {
            addPath(new ArrayList<TableReference>() {
              {
                add(new TableReference("basecube", "userid"));
                add(new TableReference("userdim", "id"));
                add(new TableReference("userdim", "id"));
                add(new TableReference("user_interests", "user_id", true));
              }
            });
          }
        });
        add(new JoinChain("xuserSports", "xuser-sports", "xuser sports") {
          {
            addPath(new ArrayList<TableReference>() {
              {
                add(new TableReference("basecube", "xuserid"));
                add(new TableReference("userdim", "id"));
                add(new TableReference("userdim", "id"));
                add(new TableReference("user_interests", "user_id", true));
                add(new TableReference("user_interests", "sport_id"));
                add(new TableReference("sports", "id"));
              }
            });
          }
        });
        add(new JoinChain("yuserSports", "user-sports", "user sports") {
          {
            addPath(new ArrayList<TableReference>() {
              {
                add(new TableReference("basecube", "yuserid"));
                add(new TableReference("userdim", "id"));
                add(new TableReference("userdim", "id"));
                add(new TableReference("user_interests", "user_id", true));
                add(new TableReference("user_interests", "sport_id"));
                add(new TableReference("sports", "id"));
              }
            });
          }
        });
      }
    };

    // add ref dim through chain
    cubeDimensions2.add(
      new ReferencedDimAtrribute(new FieldSchema("cityStateCapital", "string", "State's capital thru city"),
        "State's capital thru city", "cityState", "capital", null, null, null));
    client.createCube(BASE_CUBE_NAME, cubeMeasures2, cubeDimensions2, exprs, joinchains, cubeProperties);

    Map<String, String> derivedProperties = new HashMap<String, String>();
    derivedProperties.put(MetastoreConstants.CUBE_ALL_FIELDS_QUERIABLE, "true");
    Set<String> measures = new HashSet<String>();
    measures.add("msr1");
    measures.add("msr11");
    Set<String> dimensions = new HashSet<String>();
    dimensions.add("dim1");
    dimensions.add("dim11");
    dimensions.add("d_time");
    client.createDerivedCube(BASE_CUBE_NAME, DERIVED_CUBE_NAME1, measures, dimensions, derivedProperties, 5L);

    measures = new HashSet<String>();
    measures.add("msr2");
    measures.add("msr12");
    measures.add("msr13");
    measures.add("msr14");
    dimensions = new HashSet<String>();
    dimensions.add("cityid");
    dimensions.add("stateid");
    dimensions.add("userid");
    dimensions.add("xuserid");
    dimensions.add("yuserid");
    dimensions.add("dim1");
    dimensions.add("dim2");
    dimensions.add("dim11");
    dimensions.add("dim12");
    dimensions.add("dim22");
    dimensions.add("d_time");
    dimensions.add("test_time_dim");
    client.createDerivedCube(BASE_CUBE_NAME, DERIVED_CUBE_NAME2, measures, dimensions, derivedProperties, 10L);
    measures = new HashSet<String>();
    measures.add("msr3");
    measures.add("msr13");
    dimensions = new HashSet<String>();
    dimensions.add("dim1");
    dimensions.add("location");
    dimensions.add("d_time");
    dimensions.add("test_time_dim");
    client.createDerivedCube(BASE_CUBE_NAME, DERIVED_CUBE_NAME3, measures, dimensions, derivedProperties, 20L);

    // create base cube facts
    createBaseCubeFacts(client);
  }

  private void createBaseCubeFacts(CubeMetastoreClient client) throws HiveException, LensException {

    Map<String, Set<UpdatePeriod>> storageAggregatePeriods = new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates = new HashSet<UpdatePeriod>();
    updates.add(MINUTELY);
    updates.add(HOURLY);
    updates.add(DAILY);
    updates.add(MONTHLY);
    updates.add(QUARTERLY);
    updates.add(YEARLY);

    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());

    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);

    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    ArrayList<FieldSchema> s2PartCols = new ArrayList<FieldSchema>();
    s2PartCols.add(new FieldSchema("ttd", serdeConstants.STRING_TYPE_NAME, "test date partition"));
    s2PartCols.add(new FieldSchema("ttd2", serdeConstants.STRING_TYPE_NAME, "test date partition"));
    s2.setPartCols(s2PartCols);
    s2.setTimePartCols(Arrays.asList("ttd", "ttd2"));

    storageAggregatePeriods.put(c1, updates);
    storageAggregatePeriods.put(c2, updates);
    storageAggregatePeriods.put(c3, updates);
    storageAggregatePeriods.put(c4, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c4, s2);
    storageTables.put(c2, s1);
    storageTables.put(c3, s1);

    String factName = "testFact1_BASE";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add dimensions of the cube
    factColumns.add(new FieldSchema("d_time", "timestamp", "event time"));
    factColumns.add(new FieldSchema("processing_time", "timestamp", "processing time"));
    factColumns.add(new FieldSchema("zipcode", "int", "zip"));
    factColumns.add(new FieldSchema("cityid", "int", "city id"));
    factColumns.add(new FieldSchema("stateid", "int", "state id"));
    factColumns.add(new FieldSchema("userid", "int", "user id"));
    factColumns.add(new FieldSchema("xuserid", "int", "user id"));
    factColumns.add(new FieldSchema("yuserid", "int", "user id"));
    factColumns.add(new FieldSchema("dim1", "string", "base dim"));
    factColumns.add(new FieldSchema("dim11", "string", "base dim"));
    factColumns.add(new FieldSchema("test_time_dim_hour_id", "int", "time id"));

    // create cube fact
    client.createCubeFactTable(BASE_CUBE_NAME, factName, factColumns, storageAggregatePeriods, 5L,
      factValidityProperties, storageTables);

    // create fact only with extra measures
    factName = "testFact2_BASE";
    factColumns = new ArrayList<FieldSchema>();
    factColumns.add(new FieldSchema("msr12", "float", "second measure"));

    // add dimensions of the cube
    factColumns.add(new FieldSchema("d_time", "timestamp", "event time"));
    factColumns.add(new FieldSchema("processing_time", "timestamp", "processing time"));
    factColumns.add(new FieldSchema("dim1", "string", "base dim"));
    factColumns.add(new FieldSchema("dim11", "string", "base dim"));
    factColumns.add(new FieldSchema("dim2", "int", "dim2 id"));
    factColumns.add(new FieldSchema("userid", "int", "user id"));
    factColumns.add(new FieldSchema("xuserid", "int", "user id"));
    factColumns.add(new FieldSchema("yuserid", "int", "user id"));
    // create cube fact
    client.createCubeFactTable(BASE_CUBE_NAME, factName, factColumns, storageAggregatePeriods, 5L,
      factValidityProperties, storageTables);
    Map<String, String> properties = Maps.newHashMap(factValidityProperties);
    properties.put(MetastoreConstants.FACT_ABSOLUTE_END_TIME, DateUtil.relativeToAbsolute("now.day - 2 days"));
    properties.put(MetastoreConstants.FACT_ABSOLUTE_START_TIME, DateUtil.relativeToAbsolute("now.day - 3 days"));
    client.createCubeFactTable(BASE_CUBE_NAME, "testfact_deprecated", factColumns, storageAggregatePeriods, 5L,
      properties, storageTables);

    // create fact only with extra measures
    factName = "testFact3_BASE";
    factColumns = new ArrayList<FieldSchema>();
    factColumns.add(new FieldSchema("msr13", "double", "third measure"));
    factColumns.add(new FieldSchema("msr14", "bigint", "fourth measure"));

    // add dimensions of the cube
    factColumns.add(new FieldSchema("d_time", "timestamp", "event time"));
    factColumns.add(new FieldSchema("processing_time", "timestamp", "processing time"));
    factColumns.add(new FieldSchema("dim1", "string", "base dim"));
    factColumns.add(new FieldSchema("dim11", "string", "base dim"));

    // create cube fact
    client.createCubeFactTable(BASE_CUBE_NAME, factName, factColumns, storageAggregatePeriods, 5L,
      factValidityProperties, storageTables);

    // create raw fact only with extra measures
    factName = "testFact2_RAW_BASE";
    factColumns = new ArrayList<FieldSchema>();
    factColumns.add(new FieldSchema("msr11", "int", "first measure"));
    factColumns.add(new FieldSchema("msr12", "float", "second measure"));

    // add dimensions of the cube
    factColumns.add(new FieldSchema("d_time", "timestamp", "event time"));
    factColumns.add(new FieldSchema("processing_time", "timestamp", "processing time"));
    factColumns.add(new FieldSchema("dim1", "string", "base dim"));
    factColumns.add(new FieldSchema("dim11", "string", "base dim"));
    factColumns.add(new FieldSchema("dim12", "string", "base dim"));
    factColumns.add(new FieldSchema("dim22", "string", "base dim"));
    factColumns.add(new FieldSchema("cityid", "int", "city id"));

    storageAggregatePeriods = new HashMap<String, Set<UpdatePeriod>>();
    updates = new HashSet<UpdatePeriod>();
    updates.add(HOURLY);
    storageAggregatePeriods.put(c1, updates);

    storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    // create cube fact
    properties.clear();
    properties.putAll(factValidityProperties);
    properties.put(MetastoreConstants.FACT_AGGREGATED_PROPERTY, "false");

    client.createCubeFactTable(BASE_CUBE_NAME, factName, factColumns, storageAggregatePeriods, 100L, properties,
      storageTables);

    // create raw fact only with extra measures
    factName = "testFact3_RAW_BASE";
    factColumns = new ArrayList<FieldSchema>();
    factColumns.add(new FieldSchema("msr13", "double", "third measure"));
    factColumns.add(new FieldSchema("msr14", "bigint", "fourth measure"));

    // add dimensions of the cube
    factColumns.add(new FieldSchema("d_time", "timestamp", "event time"));
    factColumns.add(new FieldSchema("processing_time", "timestamp", "processing time"));
    factColumns.add(new FieldSchema("dim1", "string", "base dim"));
    factColumns.add(new FieldSchema("dim11", "string", "base dim"));
    factColumns.add(new FieldSchema("dim12", "string", "base dim"));

    storageAggregatePeriods = new HashMap<String, Set<UpdatePeriod>>();
    updates = new HashSet<UpdatePeriod>();
    updates.add(HOURLY);
    storageAggregatePeriods.put(c1, updates);

    storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    client.createCubeFactTable(BASE_CUBE_NAME, factName, factColumns, storageAggregatePeriods, 100L, properties,
      storageTables);

  }

  private void createCubeContinuousFact(CubeMetastoreClient client) throws Exception{
    // create continuous raw fact only with extra measures
    String factName = "testFact_CONTINUOUS";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>();
    factColumns.add(new FieldSchema("msr11", "double", "third measure"));
    factColumns.add(new FieldSchema("msr15", "int", "fifteenth measure"));

    // add dimensions of the cube
    factColumns.add(new FieldSchema("d_time", "timestamp", "event time"));
    factColumns.add(new FieldSchema("processing_time", "timestamp", "processing time"));
    factColumns.add(new FieldSchema("dim1", "string", "base dim"));
    factColumns.add(new FieldSchema("dim11", "string", "base dim"));
    factColumns.add(new FieldSchema("dim12", "string", "base dim"));

    Map<String, Set<UpdatePeriod>> storageAggregatePeriods = new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates = new HashSet<UpdatePeriod>();
    updates.add(CONTINUOUS);
    storageAggregatePeriods.put(c0, updates);

    StorageTableDesc s0 = new StorageTableDesc();
    s0.setInputFormat(TextInputFormat.class.getCanonicalName());
    s0.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c0, s0);
    Map<String, String> properties = Maps.newHashMap(factValidityProperties);
    properties.put(MetastoreConstants.FACT_ABSOLUTE_START_TIME, DateUtil.relativeToAbsolute("now.day - 3 days"));

    client.createCubeFactTable(TEST_CUBE_NAME, factName, factColumns, storageAggregatePeriods, 100L, properties,
      storageTables);
  }

  private void createCubeFact(CubeMetastoreClient client) throws Exception {
    String factName = "testFact";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      if (!measure.getColumn().getName().equals("msr15")) { //do not add msr15
        factColumns.add(measure.getColumn());
      }
    }
    factColumns.add(new FieldSchema("msr5", "double", "msr5"));

    // add dimensions of the cube
    factColumns.add(new FieldSchema("zipcode", "int", "zip"));
    factColumns.add(new FieldSchema("cityid", "int", "city id"));
    factColumns.add(new FieldSchema("stateid", "int", "city id"));
    factColumns.add(new FieldSchema("test_time_dim_day_id", "int", "time id"));
    factColumns.add(new FieldSchema("test_time_dim_day_id2", "int", "time id"));
    factColumns.add(new FieldSchema("ambigdim1", "string", "used in" + " testColumnAmbiguity"));

    Map<String, Set<UpdatePeriod>> storageAggregatePeriods = new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates = new HashSet<UpdatePeriod>();
    updates.add(MINUTELY);
    updates.add(HOURLY);
    updates.add(DAILY);
    updates.add(MONTHLY);
    updates.add(QUARTERLY);
    updates.add(YEARLY);

    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());

    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);

    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    ArrayList<FieldSchema> s2PartCols = new ArrayList<FieldSchema>();
    s2PartCols.add(new FieldSchema("ttd", serdeConstants.STRING_TYPE_NAME, "test date partition"));
    s2PartCols.add(new FieldSchema("ttd2", serdeConstants.STRING_TYPE_NAME, "test date partition"));
    s2.setPartCols(s2PartCols);
    s2.setTimePartCols(Arrays.asList("ttd", "ttd2"));

    storageAggregatePeriods.put(c1, updates);
    storageAggregatePeriods.put(c2, updates);
    storageAggregatePeriods.put(c3, updates);
    storageAggregatePeriods.put(c4, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c4, s2);
    storageTables.put(c2, s1);
    storageTables.put(c3, s1);

    //add storage with continuous update period
    updates.add(CONTINUOUS);
    storageAggregatePeriods.put(c0, updates);
    StorageTableDesc s0 = new StorageTableDesc();
    s0.setInputFormat(TextInputFormat.class.getCanonicalName());
    s0.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    storageTables.put(c0, s0);

    // create cube fact
    client.createCubeFactTable(TEST_CUBE_NAME, factName, factColumns, storageAggregatePeriods, 5L,
      factValidityProperties, storageTables);
    client.getTimelines(factName, c1, null, null);
    client.getTimelines(factName, c4, null, null);
    client.clearHiveTableCache();
    CubeFactTable fact = client.getFactTable(factName);
    Table table = client.getTable(MetastoreUtil.getStorageTableName(fact.getName(), Storage.getPrefix(c1)));
    assertEquals(table.getParameters().get(MetastoreUtil.getPartitionTimelineCachePresenceKey()), "true");
    for (UpdatePeriod period: Lists.newArrayList(MINUTELY, MINUTELY, DAILY, MONTHLY, YEARLY, QUARTERLY)) {
      for (String partCol: Lists.newArrayList("dt")) {
        assertTimeline(client, fact.getName(), c1, period, partCol, EndsAndHolesPartitionTimeline.class);
      }
    }

    table = client.getTable(MetastoreUtil.getStorageTableName(fact.getName(), Storage.getPrefix(c4)));
    assertEquals(table.getParameters().get(MetastoreUtil.getPartitionTimelineCachePresenceKey()), "true");
    for (UpdatePeriod period: Lists.newArrayList(MINUTELY, MINUTELY, DAILY, MONTHLY, YEARLY, QUARTERLY)) {
      for (String partCol: Lists.newArrayList("ttd", "ttd2")) {
        assertTimeline(client, fact.getName(), c4, period, partCol, EndsAndHolesPartitionTimeline.class);
      }
    }
  }

  private void assertTimeline(CubeMetastoreClient client, String factName, String storageName,
    UpdatePeriod updatePeriod, String timeDim, PartitionTimeline expectedTimeline)
    throws Exception {
    assertNotNull(factName);
    assertNotNull(storageName);
    assertNotNull(updatePeriod);
    assertNotNull(timeDim);
    String storageTableName = MetastoreUtil.getFactOrDimtableStorageTableName(factName, storageName);
    List<PartitionTimeline> timelines = client.getTimelines(factName, storageName, updatePeriod.name(), timeDim);
    assertEquals(timelines.size(), 1);
    PartitionTimeline actualTimeline = timelines.get(0);
    assertEquals(actualTimeline, expectedTimeline);
    assertEquals(client.getTable(storageTableName).getParameters()
      .get(MetastoreUtil.getPartitionTimelineStorageClassKey(updatePeriod,
        timeDim)), expectedTimeline.getClass().getCanonicalName());
    expectedTimeline.init(client.getTable(MetastoreUtil.getFactOrDimtableStorageTableName(factName, storageName)));
    assertEquals(actualTimeline, expectedTimeline);
  }

  private void assertTimeline(CubeMetastoreClient client, String factName, String storageName,
    UpdatePeriod updatePeriod, String timeDim, Class<? extends PartitionTimeline> partitionTimelineClass)
    throws Exception {
    String storageTableName = MetastoreUtil.getFactOrDimtableStorageTableName(factName, storageName);
    PartitionTimeline expectedTimeline = partitionTimelineClass.getConstructor(
      String.class, UpdatePeriod.class, String.class)
      .newInstance(storageTableName, updatePeriod, timeDim);
    assertTimeline(client, factName, storageName, updatePeriod, timeDim, expectedTimeline);
  }

  private void createCubeCheapFact(CubeMetastoreClient client) throws HiveException, LensException {
    String factName = "cheapFact";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add dimensions of the cube
    factColumns.add(new FieldSchema("zipcode", "int", "zip"));
    factColumns.add(new FieldSchema("cityid", "int", "city id"));
    factColumns.add(new FieldSchema("stateid", "int", "city id"));
    factColumns.add(new FieldSchema("test_time_dim_hour_id", "int", "time id"));
    factColumns.add(new FieldSchema("ambigdim1", "string", "used in" + " testColumnAmbiguity"));

    Map<String, Set<UpdatePeriod>> storageAggregatePeriods = new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates = new HashSet<UpdatePeriod>();
    updates.add(MINUTELY);
    updates.add(HOURLY);
    updates.add(DAILY);
    updates.add(MONTHLY);
    updates.add(QUARTERLY);
    updates.add(YEARLY);

    ArrayList<FieldSchema> partCols = new ArrayList<>();
    List<String> timePartCols = new ArrayList<>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());

    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    s1.setTblProps(new HashMap<String, String>());
    s1.getTblProps().put(MetastoreUtil.getStoragetableStartTimesKey(), "2000, now - 10 years");
    s1.getTblProps().put(MetastoreUtil.getStoragetableEndTimesKey(), "now - 5 years, 2010");

    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    ArrayList<FieldSchema> s2PartCols = new ArrayList<>();
    s2PartCols.add(new FieldSchema("ttd", serdeConstants.STRING_TYPE_NAME, "test date partition"));
    s2PartCols.add(new FieldSchema("ttd2", serdeConstants.STRING_TYPE_NAME, "test date partition"));
    s2.setPartCols(s2PartCols);
    s2.setTimePartCols(Arrays.asList("ttd", "ttd2"));

    storageAggregatePeriods.put(c99, updates);
    storageAggregatePeriods.put(c0, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<>();
    storageTables.put(c99, s2);
    storageTables.put(c0, s1);
    // create cube fact
    client.createCubeFactTable(TEST_CUBE_NAME, factName, factColumns, storageAggregatePeriods, 0L,
      factValidityProperties, storageTables);

    CubeFactTable fact = client.getFactTable(factName);
    // Add all hourly partitions for two days
    Calendar cal = Calendar.getInstance();
    cal.setTime(TWODAYS_BACK);
    Date temp = cal.getTime();
    while (!(temp.after(NOW))) {
      Map<String, Date> timeParts = new HashMap<String, Date>();
      timeParts.put("ttd", temp);
      timeParts.put("ttd2", temp);
      StoragePartitionDesc sPartSpec = new StoragePartitionDesc(fact.getName(), timeParts, null, HOURLY);
      client.addPartition(sPartSpec, c99);
      cal.add(HOUR_OF_DAY, 1);
      temp = cal.getTime();
    }

    // Add all hourly partitions for TWO_DAYS_RANGE_BEFORE_4_DAYS
    cal.setTime(BEFORE_4_DAYS_START);
    temp = cal.getTime();
    while (!(temp.after(BEFORE_4_DAYS_END))) {
      Map<String, Date> timeParts = new HashMap<String, Date>();
      timeParts.put("ttd", temp);
      timeParts.put("ttd2", temp);
      StoragePartitionDesc sPartSpec = new StoragePartitionDesc(fact.getName(), timeParts, null, HOURLY);
      client.addPartition(sPartSpec, c99);
      cal.add(HOUR_OF_DAY, 1);
      temp = cal.getTime();
    }
  }

  private void createCubeFactWeekly(CubeMetastoreClient client) throws HiveException {
    String factName = "testFactWeekly";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add dimensions of the cube
    factColumns.add(new FieldSchema("zipcode", "int", "zip"));

    Map<String, Set<UpdatePeriod>> storageAggregatePeriods = new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates = new HashSet<UpdatePeriod>();
    updates.add(WEEKLY);
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);

    storageAggregatePeriods.put(c1, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    // create cube fact
    client.createCubeFactTable(TEST_CUBE_NAME, factName, factColumns, storageAggregatePeriods, 5L,
      factValidityProperties, storageTables);
  }

  private void createCubeFactOnlyHourly(CubeMetastoreClient client) throws Exception {
    String factName = "testFact2";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      if (!measure.getName().equals("msr4")) {
        factColumns.add(measure.getColumn());
      }
    }

    // add dimensions of the cube
    factColumns.add(new FieldSchema("zipcode", "int", "zip"));
    factColumns.add(new FieldSchema("cityid", "int", "city id"));
    factColumns.add(new FieldSchema("test_time_dim_hour_id", "int", "time id"));
    factColumns.add(new FieldSchema("test_time_dim_hour_id2", "int", "time id"));
    factColumns.add(new FieldSchema("cdim2", "int", "cycledim id"));

    Map<String, Set<UpdatePeriod>> storageAggregatePeriods = new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates = new HashSet<UpdatePeriod>();
    updates.add(HOURLY);
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);

    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    ArrayList<FieldSchema> s2PartCols = new ArrayList<FieldSchema>();
    s2PartCols.add(new FieldSchema("ttd", serdeConstants.STRING_TYPE_NAME, "test date partition"));
    s2PartCols.add(new FieldSchema("ttd2", serdeConstants.STRING_TYPE_NAME, "test date partition"));
    s2.setPartCols(s2PartCols);
    s2.setTimePartCols(Arrays.asList("ttd", "ttd2"));

    storageAggregatePeriods.put(c1, updates);
    storageAggregatePeriods.put(c4, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c4, s2);

    // create cube fact
    client
      .createCubeFactTable(TEST_CUBE_NAME, factName, factColumns, storageAggregatePeriods, 10L,
        factValidityProperties, storageTables);
    CubeFactTable fact = client.getFactTable(factName);
    // Add all hourly partitions for two days
    Calendar cal = Calendar.getInstance();
    cal.setTime(TWODAYS_BACK);
    Date temp = cal.getTime();
    while (!(temp.after(NOW))) {
      Map<String, Date> timeParts = new HashMap<String, Date>();
      timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), temp);
      StoragePartitionDesc sPartSpec = new StoragePartitionDesc(fact.getName(), timeParts, null, HOURLY);
      try {
        client.addPartition(sPartSpec, c1);
      } catch (HiveException e) {
        log.error("Encountered Hive exception.", e);
      } catch (LensException e) {
        log.error("Encountered Lens exception.", e);
      }
      cal.add(HOUR_OF_DAY, 1);
      temp = cal.getTime();
    }

    // Add all hourly partitions for TWO_DAYS_RANGE_BEFORE_4_DAYS
    cal.setTime(BEFORE_4_DAYS_START);
    temp = cal.getTime();
    while (!(temp.after(BEFORE_4_DAYS_END))) {
      Map<String, Date> timeParts = new HashMap<String, Date>();
      timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), temp);
      StoragePartitionDesc sPartSpec = new StoragePartitionDesc(fact.getName(), timeParts, null, HOURLY);
      client.addPartition(sPartSpec, c1);
      cal.add(HOUR_OF_DAY, 1);
      temp = cal.getTime();
    }
    client.clearHiveTableCache();

    Table table = client.getTable(MetastoreUtil.getStorageTableName(fact.getName(),
      Storage.getPrefix(c4)));
    table.getParameters().put(MetastoreUtil.getPartitionTimelineStorageClassKey(HOURLY, "ttd"),
      StoreAllPartitionTimeline.class.getCanonicalName());
    table.getParameters().put(MetastoreUtil.getPartitionTimelineStorageClassKey(HOURLY, "ttd2"),
      StoreAllPartitionTimeline.class.getCanonicalName());
    client.pushHiveTable(table);
    // Add all hourly partitions for two days on C4
    cal = Calendar.getInstance();
    cal.setTime(TWODAYS_BACK);
    temp = cal.getTime();
    List<StoragePartitionDesc> storagePartitionDescs = Lists.newArrayList();
    List<String> partitions = Lists.newArrayList();
    StoreAllPartitionTimeline ttdStoreAll =
      new StoreAllPartitionTimeline(MetastoreUtil.getFactOrDimtableStorageTableName(fact.getName(), c4), HOURLY,
        "ttd");
    StoreAllPartitionTimeline ttd2StoreAll =
      new StoreAllPartitionTimeline(MetastoreUtil.getFactOrDimtableStorageTableName(fact.getName(), c4), HOURLY,
        "ttd2");
    while (!(temp.after(NOW))) {
      Map<String, Date> timeParts = new HashMap<String, Date>();
      timeParts.put("ttd", temp);
      timeParts.put("ttd2", temp);
      TimePartition tp = TimePartition.of(HOURLY, temp);
      ttdStoreAll.add(tp);
      ttd2StoreAll.add(tp);
      partitions.add(HOURLY.format().format(temp));
      StoragePartitionDesc sPartSpec = new StoragePartitionDesc(fact.getName(), timeParts, null, HOURLY);
      storagePartitionDescs.add(sPartSpec);
      cal.add(HOUR_OF_DAY, 1);
      temp = cal.getTime();
    }
    client.addPartitions(storagePartitionDescs, c4);
    client.clearHiveTableCache();
    table = client.getTable(MetastoreUtil.getStorageTableName(fact.getName(), Storage.getPrefix(c4)));
    assertEquals(table.getParameters().get(MetastoreUtil.getPartitionTimelineCachePresenceKey()), "true");
    assertTimeline(client, fact.getName(), c4, HOURLY, "ttd", ttdStoreAll);
    assertTimeline(client, fact.getName(), c4, HOURLY, "ttd2", ttd2StoreAll);

    // Add all hourly partitions for TWO_DAYS_RANGE_BEFORE_4_DAYS
    cal.setTime(BEFORE_4_DAYS_START);
    temp = cal.getTime();
    while (!(temp.after(BEFORE_4_DAYS_END))) {
      Map<String, Date> timeParts = new HashMap<String, Date>();
      timeParts.put("ttd", temp);
      timeParts.put("ttd2", temp);
      StoragePartitionDesc sPartSpec = new StoragePartitionDesc(fact.getName(), timeParts, null, HOURLY);
      client.addPartition(sPartSpec, c4);
      cal.add(HOUR_OF_DAY, 1);
      temp = cal.getTime();
    }
  }

  private void createCubeFactOnlyHourlyRaw(CubeMetastoreClient client) throws HiveException, LensException {
    String factName = "testFact2_raw";
    String factName2 = "testFact1_raw_BASE";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add dimensions of the cube
    factColumns.add(new FieldSchema("zipcode", "int", "zip"));
    factColumns.add(new FieldSchema("cityid", "int", "city id"));
    factColumns.add(new FieldSchema("stateid", "int", "state id"));
    factColumns.add(new FieldSchema("countryid", "int", "country id"));
    factColumns.add(new FieldSchema("dim1", "string", "dim1"));
    factColumns.add(new FieldSchema("dim2", "int", "dim2"));

    Map<String, Set<UpdatePeriod>> storageAggregatePeriods = new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates = new HashSet<UpdatePeriod>();
    updates.add(HOURLY);
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    storageAggregatePeriods.put(c1, updates);
    storageAggregatePeriods.put(c3, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c3, s1);

    // create cube fact
    Map<String, String> properties = new HashMap<String, String>();
    properties.putAll(factValidityProperties);
    properties.put(MetastoreConstants.FACT_AGGREGATED_PROPERTY, "false");

    client.createCubeFactTable(TEST_CUBE_NAME, factName, factColumns, storageAggregatePeriods, 100L, properties,
      storageTables);
    client.createCubeFactTable(BASE_CUBE_NAME, factName2, factColumns, storageAggregatePeriods, 100L, properties,
      storageTables);
    CubeFactTable fact2 = client.getFactTable(factName);
    // Add all hourly partitions for two days
    Calendar cal = Calendar.getInstance();
    cal.setTime(TWODAYS_BACK);
    Date temp = cal.getTime();
    while (!(temp.after(NOW))) {
      Map<String, Date> timeParts = new HashMap<String, Date>();
      timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), temp);
      StoragePartitionDesc sPartSpec = new StoragePartitionDesc(fact2.getName(), timeParts, null, HOURLY);
      client.addPartition(sPartSpec, c3);
      cal.add(HOUR_OF_DAY, 1);
      temp = cal.getTime();
    }
  }

  private void createCubeFactMonthly(CubeMetastoreClient client) throws HiveException {
    String factName = "testFactMonthly";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add one dimension of the cube
    factColumns.add(new FieldSchema("countryid", "int", "country id"));

    Map<String, Set<UpdatePeriod>> storageAggregatePeriods = new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates = new HashSet<UpdatePeriod>();
    updates.add(MONTHLY);
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);

    storageAggregatePeriods.put(c2, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c2, s1);

    // create cube fact
    client.createCubeFactTable(TEST_CUBE_NAME, factName, factColumns, storageAggregatePeriods, 5L,
      factValidityProperties, storageTables);
  }

  // DimWithTwoStorages
  private void createCityTable(CubeMetastoreClient client) throws HiveException, ParseException {
    Set<CubeDimAttribute> cityAttrs = new HashSet<CubeDimAttribute>();
    cityAttrs.add(new BaseDimAttribute(new FieldSchema("id", "int", "code")));
    cityAttrs.add(new BaseDimAttribute(new FieldSchema("name", "string", "city name")));
    cityAttrs.add(new BaseDimAttribute(new FieldSchema("ambigdim1", "string", "used in testColumnAmbiguity")));
    cityAttrs.add(new BaseDimAttribute(new FieldSchema("ambigdim2", "string", "used in testColumnAmbiguity")));
    cityAttrs.add(new BaseDimAttribute(new FieldSchema("nocandidatecol", "string", "used in testing no"
      + " candidate available")));
    cityAttrs.add(new ReferencedDimAtrribute(new FieldSchema("stateid", "int", "state id"), "State refer",
      new TableReference("statedim", "id")));
    cityAttrs.add(new ReferencedDimAtrribute(new FieldSchema("statename", "string", "state name"), "State name",
      new TableReference("statedim", "name")));
    cityAttrs.add(new ReferencedDimAtrribute(new FieldSchema("zipcode", "int", "zip code"), "Zip refer",
      new TableReference("zipdim", "code")));
    Map<String, String> dimProps = new HashMap<String, String>();
    dimProps.put(MetastoreUtil.getDimTimedDimensionKey("citydim"), TestCubeMetastoreClient.getDatePartitionKey());
    Set<ExprColumn> exprs = new HashSet<ExprColumn>();
    exprs.add(new ExprColumn(new FieldSchema("CityAddress", "string", "city with state and city and zip"),
      "City Address",
      new ExprSpec("concat(citydim.name, \":\", statedim.name, \":\", countrydim.name, \":\", zipdim.code)", null,
        null), new ExprSpec("concat(citydim.name, \":\", statedim.name)", null, null)));
    exprs.add(new ExprColumn(new FieldSchema("CityState", "string", "city's state"),
      "City State", new ExprSpec("concat(citydim.name, \":\", citydim.statename)", null, null)));
    exprs.add(new ExprColumn(new FieldSchema("AggrExpr", "int", "count(name)"), "city count",
      new ExprSpec("count(name)", null, null)));
    Dimension cityDim = new Dimension("citydim", cityAttrs, exprs, dimProps, 0L);
    client.createDimension(cityDim);

    String dimName = "citytable";

    List<FieldSchema> dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("stateid", "int", "state id"));
    dimColumns.add(new FieldSchema("zipcode", "int", "zip code"));
    dimColumns.add(new FieldSchema("ambigdim1", "string", "used in" + " testColumnAmbiguity"));
    dimColumns.add(new FieldSchema("ambigdim2", "string", "used in " + "testColumnAmbiguity"));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    Map<String, String> tblPros = Maps.newHashMap();
    tblPros.put(LensConfConstants.STORAGE_COST, "100");
    s1.setTblProps(tblPros);
    dumpPeriods.put(c1, HOURLY);

    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    dumpPeriods.put(c2, null);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s2);

    client.createCubeDimensionTable(cityDim.getName(), dimName, dimColumns, 0L, dumpPeriods, dimProps, storageTables);

    dimName = "citytable2";

    dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("stateid", "int", "state id"));

    dumpPeriods = new HashMap<String, UpdatePeriod>();
    storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c4, s2);
    dumpPeriods.put(c4, null);

    client.createCubeDimensionTable(cityDim.getName(), dimName, dimColumns, 0L, dumpPeriods, dimProps, storageTables);

    dimName = "citytable3";

    dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "name"));

    client.createCubeDimensionTable(cityDim.getName(), dimName, dimColumns, 0L, dumpPeriods, dimProps, storageTables);

    dimName = "citytable4";

    dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));

    client.createCubeDimensionTable(cityDim.getName(), dimName, dimColumns, 0L, dumpPeriods, dimProps, storageTables);

  }

  private void createTestDim2(CubeMetastoreClient client) throws HiveException {
    String dimName = "testDim2";
    Set<CubeDimAttribute> dimAttrs = new HashSet<CubeDimAttribute>();

    Set<JoinChain> joinchains = new HashSet<JoinChain>();
    JoinChain cityState = new JoinChain("cityState", "city-state", "state thru city");
    List<TableReference> statePaths1 = new ArrayList<TableReference>();
    statePaths1.add(new TableReference("testDim2", "cityid"));
    statePaths1.add(new TableReference("citydim", "id"));
    statePaths1.add(new TableReference("citydim", "stateid"));
    statePaths1.add(new TableReference("statedim", "id"));
    cityState.addPath(statePaths1);
    List<TableReference> statePaths2 = new ArrayList<TableReference>();
    statePaths2.add(new TableReference("testDim2", "cityid"));
    statePaths2.add(new TableReference("citydim", "id"));
    statePaths2.add(new TableReference("citydim", "statename"));
    statePaths2.add(new TableReference("statedim", "name"));
    cityState.addPath(statePaths2);
    joinchains.add(cityState);


    dimAttrs.add(new BaseDimAttribute(new FieldSchema("id", "int", "code")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("bigid1", "bigint", "big id")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("bigid2", "bigint", "big id")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("bigidnew", "bigint", "big id")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("name", "string", "name")));
    dimAttrs.add(new ReferencedDimAtrribute(new FieldSchema("testDim3id", "string", "f-key to testdim3"), "Dim3 refer",
      new TableReference("testdim3", "id")));
    dimAttrs.add(new ReferencedDimAtrribute(new FieldSchema("cityId", "string", "f-key to citydim"), "cityid",
      new TableReference("citydim", "id")));
    dimAttrs.add(new ReferencedDimAtrribute(new FieldSchema("cityname", "string", "name"), "cityid",
      new TableReference("citydim", "name"), null, null, 0.0, false));
    dimAttrs.add(new ReferencedDimAtrribute(new FieldSchema("urdimid", "int", "ref dim"), "urdim refer",
      new TableReference("unreachableDim", "id"), null, null, null, false, 10L));

    // add ref dim through chain
    dimAttrs.add(new ReferencedDimAtrribute(
      new FieldSchema("cityStateCapital", "string", "State's capital thru city"), "State's capital thru city",
      "cityState", "capital", null, null, null));

    Map<String, String> dimProps = new HashMap<String, String>();
    dimProps.put(MetastoreUtil.getDimTimedDimensionKey(dimName), TestCubeMetastoreClient.getDatePartitionKey());
    Dimension testDim2 = new Dimension(dimName, dimAttrs, null, joinchains, dimProps, 0L);
    client.createDimension(testDim2);

    String dimTblName = "testDim2Tbl";
    List<FieldSchema> dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("cityId", "string", "f-key to cityDim"));
    dimColumns.add(new FieldSchema("testDim3id", "string", "f-key to testdim3"));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, HOURLY);

    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    dumpPeriods.put(c2, null);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s2);

    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L, dumpPeriods, dimProps, storageTables);

    // create table2
    dimTblName = "testDim2Tbl2";
    dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("bigid1", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("cityId", "string", "f-key to cityDim"));
    storageTables.put(c3, s1);
    dumpPeriods.put(c3, HOURLY);

    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 10L, dumpPeriods, dimProps, storageTables);

    // create table2
    dimTblName = "testDim2Tbl3";
    dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("bigid1", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("testDim3id", "string", "f-key to testdim3"));

    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 20L, dumpPeriods, dimProps, storageTables);
  }

  private void createTimeDims(CubeMetastoreClient client) throws HiveException {
    String dimName = "dayDim";
    Set<CubeDimAttribute> dimAttrs = new HashSet<CubeDimAttribute>();
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("id", "int", "code")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("full_date", "string", "full date")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("calendar_quarter", "int", "quarter id")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("day_number_of_year", "int", "day number in year")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("is_weekend", "boolean", "is weekend?")));
    Map<String, String> dimProps = new HashMap<String, String>();
    dimProps.put(MetastoreUtil.getDimTimedDimensionKey(dimName), TestCubeMetastoreClient.getDatePartitionKey());
    Dimension testDim = new Dimension(dimName, dimAttrs, dimProps, 0L);
    client.createDimension(testDim);

    String dimTblName = "dayDimTbl";
    List<FieldSchema> dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("full_date", "string", "field1"));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c3, HOURLY);

    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    dumpPeriods.put(c4, null);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c3, s1);
    storageTables.put(c4, s2);

    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L, dumpPeriods, dimProps, storageTables);

    String dimName2 = "hourDim";
    dimAttrs = new HashSet<CubeDimAttribute>();
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("id", "int", "code")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("full_hour", "string", "full date")));
    dimProps = new HashMap<String, String>();
    dimProps.put(MetastoreUtil.getDimTimedDimensionKey(dimName2), TestCubeMetastoreClient.getDatePartitionKey());
    testDim = new Dimension(dimName2, dimAttrs, dimProps, 0L);
    client.createDimension(testDim);

    String dimTblName2 = "hourDimTbl";
    dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("full_hour", "string", "field1"));

    client.createCubeDimensionTable(dimName2, dimTblName2, dimColumns, 0L, dumpPeriods, dimProps, storageTables);

  }

  private void createTestDim3(CubeMetastoreClient client) throws HiveException {
    String dimName = "testDim3";

    Set<CubeDimAttribute> dimAttrs = new HashSet<CubeDimAttribute>();
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("id", "int", "code")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("name", "string", "name")));
    dimAttrs.add(new ReferencedDimAtrribute(new FieldSchema("testDim4id", "string", "f-key to testdim4"), "Dim4 refer",
      new TableReference("testdim4", "id")));
    Map<String, String> dimProps = new HashMap<String, String>();
    dimProps.put(MetastoreUtil.getDimTimedDimensionKey(dimName), TestCubeMetastoreClient.getDatePartitionKey());
    Dimension testDim3 = new Dimension(dimName, dimAttrs, dimProps, 0L);
    client.createDimension(testDim3);

    String dimTblName = "testDim3Tbl";
    List<FieldSchema> dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("testDim4id", "string", "f-key to testDim4"));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, HOURLY);

    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    dumpPeriods.put(c2, null);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s2);

    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L, dumpPeriods, dimProps, storageTables);
  }

  private void createTestDim4(CubeMetastoreClient client) throws HiveException {
    String dimName = "testDim4";

    Set<CubeDimAttribute> dimAttrs = new HashSet<CubeDimAttribute>();
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("id", "int", "code")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("name", "string", "name")));
    Map<String, String> dimProps = new HashMap<String, String>();
    dimProps.put(MetastoreUtil.getDimTimedDimensionKey(dimName), TestCubeMetastoreClient.getDatePartitionKey());
    Dimension testDim4 = new Dimension(dimName, dimAttrs, dimProps, 0L);
    client.createDimension(testDim4);

    String dimTblName = "testDim4Tbl";

    List<FieldSchema> dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, HOURLY);

    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    dumpPeriods.put(c2, null);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s2);

    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L, dumpPeriods, dimProps, storageTables);
  }

  private void createCyclicDim1(CubeMetastoreClient client) throws HiveException {
    String dimName = "cycleDim1";

    Set<CubeDimAttribute> dimAttrs = new HashSet<CubeDimAttribute>();
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("id", "int", "code")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("name", "string", "name")));
    dimAttrs.add(new ReferencedDimAtrribute(new FieldSchema("cyleDim2Id", "string", "link to cyclic dim 2"),
      "cycle refer2", new TableReference("cycleDim2", "id")));
    Map<String, String> dimProps = new HashMap<String, String>();
    dimProps.put(MetastoreUtil.getDimTimedDimensionKey(dimName), TestCubeMetastoreClient.getDatePartitionKey());
    Dimension cycleDim1 = new Dimension(dimName, dimAttrs, dimProps, 0L);
    client.createDimension(cycleDim1);

    String dimTblName = "cycleDim1Tbl";

    List<FieldSchema> dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("cyleDim2Id", "string", "link to cyclic dim 2"));

    Map<String, List<TableReference>> dimensionReferences = new HashMap<String, List<TableReference>>();
    dimensionReferences.put("cyleDim2Id", Arrays.asList(new TableReference("cycleDim2", "id")));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, HOURLY);

    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    dumpPeriods.put(c2, null);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s2);

    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L, dumpPeriods, dimProps, storageTables);
  }

  private void createCyclicDim2(CubeMetastoreClient client) throws HiveException {
    String dimName = "cycleDim2";

    Set<CubeDimAttribute> dimAttrs = new HashSet<CubeDimAttribute>();
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("id", "int", "code")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("name", "string", "name")));
    dimAttrs.add(new ReferencedDimAtrribute(new FieldSchema("cyleDim1Id", "string", "link to cyclic dim 1"),
      "Cycle refer1", new TableReference("cycleDim1", "id")));
    Map<String, String> dimProps = new HashMap<String, String>();
    dimProps.put(MetastoreUtil.getDimTimedDimensionKey(dimName), TestCubeMetastoreClient.getDatePartitionKey());
    Dimension cycleDim2 = new Dimension(dimName, dimAttrs, dimProps, 0L);
    client.createDimension(cycleDim2);

    String dimTblName = "cycleDim2Tbl";
    List<FieldSchema> dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("cyleDim1Id", "string", "link to cyclic dim 1"));

    Map<String, List<TableReference>> dimensionReferences = new HashMap<String, List<TableReference>>();
    dimensionReferences.put("cyleDim1Id", Arrays.asList(new TableReference("cycleDim1", "id")));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, HOURLY);

    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    dumpPeriods.put(c2, null);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s2);

    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L, dumpPeriods, dimProps, storageTables);
  }

  private void createZiptable(CubeMetastoreClient client) throws Exception {
    String dimName = "zipdim";

    Set<CubeDimAttribute> dimAttrs = new HashSet<CubeDimAttribute>();
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("code", "int", "code")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("f1", "string", "name")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("f2", "string", "name")));
    Map<String, String> dimProps = new HashMap<String, String>();
    dimProps.put(MetastoreUtil.getDimTimedDimensionKey(dimName), TestCubeMetastoreClient.getDatePartitionKey());
    Dimension zipDim = new Dimension(dimName, dimAttrs, dimProps, 0L);
    client.createDimension(zipDim);

    String dimTblName = "ziptable";
    List<FieldSchema> dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("code", "int", "code"));
    dimColumns.add(new FieldSchema("f1", "string", "field1"));
    dimColumns.add(new FieldSchema("f2", "string", "field2"));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, HOURLY);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L, dumpPeriods, dimProps, storageTables);
  }

  private void createUnReachabletable(CubeMetastoreClient client) throws Exception {
    String dimName = "unreachableDim";

    Set<CubeDimAttribute> dimAttrs = new HashSet<CubeDimAttribute>();
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("id", "int", "code")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("name", "int", "code")));
    Map<String, String> dimProps = new HashMap<String, String>();
    dimProps.put(MetastoreUtil.getDimTimedDimensionKey(dimName), TestCubeMetastoreClient.getDatePartitionKey());
    Dimension urDim = new Dimension(dimName, dimAttrs, dimProps, 0L);
    client.createDimension(urDim);

    String dimTblName = "unreachableDimTable";
    List<FieldSchema> dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, HOURLY);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L, dumpPeriods, dimProps, storageTables);
  }

  private void createCountryTable(CubeMetastoreClient client) throws Exception {
    String dimName = "countrydim";

    Set<CubeDimAttribute> dimAttrs = new HashSet<CubeDimAttribute>();
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("id", "int", "code")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("name", "string", "name")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("captial", "string", "field2")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("region", "string", "region name")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("ambigdim2", "string", "used in testColumnAmbiguity")));
    Map<String, String> dimProps = new HashMap<String, String>();
    dimProps.put(MetastoreUtil.getDimTimedDimensionKey(dimName), TestCubeMetastoreClient.getDatePartitionKey());
    Dimension countryDim = new Dimension(dimName, dimAttrs, dimProps, 0L);
    client.createDimension(countryDim);

    String dimTblName = "countrytable";
    List<FieldSchema> dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("capital", "string", "field2"));
    dimColumns.add(new FieldSchema("region", "string", "region name"));
    dimColumns.add(new FieldSchema("ambigdim2", "string", "used in" + " testColumnAmbiguity"));


    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    dumpPeriods.put(c1, null);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L, dumpPeriods, dimProps, storageTables);
    dimTblName = "countrytable_partitioned";

    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    ArrayList<FieldSchema> partCols = Lists.newArrayList();
    partCols.add(dimColumns.remove(dimColumns.size() - 2));
    s2.setPartCols(partCols);
    dumpPeriods.clear();
    dumpPeriods.put(c3, HOURLY);
    storageTables.clear();
    storageTables.put(c3, s2);
    dimProps.put(MetastoreUtil.getDimTablePartsKey(dimTblName), partCols.get(0).getName());
    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L, dumpPeriods, dimProps, storageTables);
  }

  private void createStateTable(CubeMetastoreClient client) throws Exception {
    String dimName = "statedim";

    Set<CubeDimAttribute> dimAttrs = new HashSet<CubeDimAttribute>();
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("id", "int", "code")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("name", "string", "name")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("capital", "string", "field2")));
    dimAttrs.add(new ReferencedDimAtrribute(new FieldSchema("countryid", "string", "link to country table"),
      "Country refer", new TableReference("countrydim", "id")));
    Map<String, String> dimProps = new HashMap<String, String>();
    dimProps.put(MetastoreUtil.getDimTimedDimensionKey(dimName), TestCubeMetastoreClient.getDatePartitionKey());
    Dimension countryDim = new Dimension(dimName, dimAttrs, dimProps, 0L);
    client.createDimension(countryDim);

    String dimTblName = "statetable";
    List<FieldSchema> dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("capital", "string", "field2"));
    dimColumns.add(new FieldSchema("countryid", "string", "region name"));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, HOURLY);
    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L, dumpPeriods, dimProps, storageTables);

    // In this, country id will be a partition
    dimTblName = "statetable_partitioned";

    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    partCols.add(dimColumns.remove(dimColumns.size() - 1));
    s2.setPartCols(partCols);
    s2.setTimePartCols(timePartCols);
    dumpPeriods.clear();
    dumpPeriods.put(c3, HOURLY);
    storageTables.clear();
    storageTables.put(c3, s2);
    dimProps.put(MetastoreUtil.getDimTablePartsKey(dimTblName), partCols.get(1).getName());
    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L, dumpPeriods, dimProps, storageTables);
  }

  private void createUserTable(CubeMetastoreClient client) throws Exception {
    String dimName = "userdim";

    Set<CubeDimAttribute> dimAttrs = new HashSet<CubeDimAttribute>();
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("id", "int", "id")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("name", "string", "name")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("age", "string", "age")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("gender", "string", "gender")));
    Map<String, String> dimProps = new HashMap<String, String>();
    dimProps.put(MetastoreUtil.getDimTimedDimensionKey(dimName), TestCubeMetastoreClient.getDatePartitionKey());
    Set<JoinChain> joinChains = new HashSet<JoinChain>();
    joinChains.add(new JoinChain("userSports", "user-sports", "user sports") {
      {
        addPath(new ArrayList<TableReference>() {
          {
            add(new TableReference("userdim", "id"));
            add(new TableReference("user_interests", "user_id", true));
            add(new TableReference("user_interests", "sport_id"));
            add(new TableReference("sports", "id"));
          }
        });
      }
    });
    Dimension userDim = new Dimension(dimName, dimAttrs, null, joinChains, dimProps,  0L);
    client.createDimension(userDim);

    String dimTblName = "usertable";
    List<FieldSchema> dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "id"));
    dimColumns.add(new FieldSchema("name", "string", "name"));
    dimColumns.add(new FieldSchema("age", "string", "age"));
    dimColumns.add(new FieldSchema("gender", "string", "gender"));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    dumpPeriods.put(c1, null);

    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s2.setPartCols(partCols);
    s2.setTimePartCols(timePartCols);
    dumpPeriods.put(c2, HOURLY);
    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s2);

    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L, dumpPeriods, dimProps, storageTables);
  }

  private void createUserInterests(CubeMetastoreClient client) throws Exception {
    String dimName = "user_interests";

    Set<CubeDimAttribute> dimAttrs = new HashSet<CubeDimAttribute>();
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("id", "int", "id")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("user_id", "int", "user id")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("sport_id", "int", "sport id")));
    Map<String, String> dimProps = new HashMap<String, String>();
    dimProps.put(MetastoreUtil.getDimTimedDimensionKey(dimName), TestCubeMetastoreClient.getDatePartitionKey());
    Dimension interestDim = new Dimension(dimName, dimAttrs, dimProps, 0L);
    client.createDimension(interestDim);

    String dimTblName = "user_interests_tbl";
    List<FieldSchema> dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "id"));
    dimColumns.add(new FieldSchema("user_id", "int", "user id"));
    dimColumns.add(new FieldSchema("sport_id", "int", "sport id"));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    dumpPeriods.put(c1, null);

    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s2.setPartCols(partCols);
    s2.setTimePartCols(timePartCols);
    dumpPeriods.put(c2, HOURLY);
    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s2);
    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L, dumpPeriods, dimProps, storageTables);
  }

  private void createSports(CubeMetastoreClient client) throws Exception {
    String dimName = "sports";

    Set<CubeDimAttribute> dimAttrs = new HashSet<CubeDimAttribute>();
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("id", "int", "id")));
    dimAttrs.add(new BaseDimAttribute(new FieldSchema("name", "string", "name")));
    Map<String, String> dimProps = new HashMap<String, String>();
    dimProps.put(MetastoreUtil.getDimTimedDimensionKey(dimName), TestCubeMetastoreClient.getDatePartitionKey());
    Dimension interestDim = new Dimension(dimName, dimAttrs, dimProps, 0L);
    client.createDimension(interestDim);

    String dimTblName = "sports_tbl";
    List<FieldSchema> dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "id"));
    dimColumns.add(new FieldSchema("name", "string", "name"));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    dumpPeriods.put(c1, null);

    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s2.setPartCols(partCols);
    s2.setTimePartCols(timePartCols);
    dumpPeriods.put(c2, HOURLY);
    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s2);

    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L, dumpPeriods, dimProps, storageTables);
  }
  public void createSources(HiveConf conf, String dbName) throws Exception {
    try {
      Database database = new Database();
      database.setName(dbName);
      Hive.get(conf).dropDatabase(dbName, true, true, true);
      Hive.get(conf).createDatabase(database);
      SessionState.get().setCurrentDatabase(dbName);
      CubeMetastoreClient client = CubeMetastoreClient.getInstance(conf);
      client.createStorage(new HDFSStorage(c0));
      client.createStorage(new HDFSStorage(c1));
      client.createStorage(new HDFSStorage(c2));
      client.createStorage(new HDFSStorage(c3));
      client.createStorage(new HDFSStorage(c4));
      client.createStorage(new HDFSStorage(c99));
      createCube(client);
      createBaseAndDerivedCubes(client);
      createCubeFact(client);
      createCubeContinuousFact(client);
      createCubeCheapFact(client);
      // commenting this as the week date format throws IllegalPatternException
      // createCubeFactWeekly(client);
      createCubeFactOnlyHourly(client);
      createCubeFactOnlyHourlyRaw(client);

      createCityTable(client);
      // For join resolver test
      createTestDim2(client);
      createTestDim3(client);
      createTestDim4(client);
      createTimeDims(client);

      // For join resolver cyclic links in dimension tables
      createCyclicDim1(client);
      createCyclicDim2(client);

      createCubeFactMonthly(client);
      createZiptable(client);
      createCountryTable(client);
      createStateTable(client);
      createCubeFactsWithValidColumns(client);
      createUnReachabletable(client);
      createUserTable(client);
      createSports(client);
      createUserInterests(client);
    } catch (Exception exc) {
      log.error("Exception while creating sources.", exc);
      throw exc;
    }
  }

  public void dropSources(HiveConf conf, String dbName) throws Exception {
    Hive metastore = Hive.get(conf);
    metastore.dropDatabase(dbName, true, true, true);
  }

  private void createCubeFactsWithValidColumns(CubeMetastoreClient client) throws Exception {
    String factName = "summary1";
    StringBuilder commonCols = new StringBuilder();
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
      commonCols.append(measure.getName());
      commonCols.append(",");
    }

    // add dimensions of the cube
    factColumns.add(new FieldSchema("dim1", "string", "dim1"));
    factColumns.add(new FieldSchema("dim2", "string", "dim2"));
    factColumns.add(new FieldSchema("testdim3id", "string", "dim2"));
    factColumns.add(new FieldSchema("dim2big", "string", "dim2"));
    factColumns.add(new FieldSchema("zipcode", "int", "zip"));
    factColumns.add(new FieldSchema("cityid", "int", "city id"));
    Set<UpdatePeriod> updates = new HashSet<UpdatePeriod>();
    updates.add(MINUTELY);
    updates.add(HOURLY);
    updates.add(DAILY);

    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);

    ArrayList<FieldSchema> partCols2 = new ArrayList<FieldSchema>();
    List<String> timePartCols2 = new ArrayList<String>();
    partCols2.add(new FieldSchema("pt", "string", "p time"));
    partCols2.add(new FieldSchema("it", "string", "i time"));
    partCols2.add(new FieldSchema("et", "string", "e time"));
    timePartCols2.add("pt");
    timePartCols2.add("it");
    timePartCols2.add("et");
    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s2.setPartCols(partCols2);
    s2.setTimePartCols(timePartCols2);

    Map<String, Set<UpdatePeriod>> storageUpdatePeriods = new HashMap<String, Set<UpdatePeriod>>();
    storageUpdatePeriods.put(c1, updates);
    storageUpdatePeriods.put(c2, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s2);

    // create cube fact summary1
    Map<String, String> properties = new HashMap<String, String>();
    properties.putAll(factValidityProperties);
    String validColumns = commonCols.toString() + ",dim1,testdim3id";
    properties.put(MetastoreUtil.getValidColumnsKey(factName), validColumns);
    CubeFactTable fact1 =
      new CubeFactTable(TEST_CUBE_NAME, factName, factColumns, storageUpdatePeriods, 10L, properties);
    client.createCubeTable(fact1, storageTables);
    createPIEParts(client, fact1, c2);

    // create summary2 - same schema, different valid columns
    factName = "summary2";
    validColumns = commonCols.toString() + ",dim1,dim2";
    properties.put(MetastoreUtil.getValidColumnsKey(factName), validColumns);
    CubeFactTable fact2 =
      new CubeFactTable(TEST_CUBE_NAME, factName, factColumns, storageUpdatePeriods, 20L, properties);
    client.createCubeTable(fact2, storageTables);
    createPIEParts(client, fact2, c2);

    factName = "summary3";
    validColumns = commonCols.toString() + ",dim1,dim2,cityid,stateid";
    properties.put(MetastoreUtil.getValidColumnsKey(factName), validColumns);
    CubeFactTable fact3 =
      new CubeFactTable(TEST_CUBE_NAME, factName, factColumns, storageUpdatePeriods, 30L, properties);
    client.createCubeTable(fact3, storageTables);
    createPIEParts(client, fact3, c2);

    // create summary4 only on c2
    storageUpdatePeriods = new HashMap<String, Set<UpdatePeriod>>();
    storageUpdatePeriods.put(c2, updates);

    storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c2, s2);
    factName = "summary4";
    validColumns = commonCols.toString() + ",dim1,dim2big1,dim2big2,cityid";
    properties.put(MetastoreUtil.getValidColumnsKey(factName), validColumns);
    CubeFactTable fact4 =
      new CubeFactTable(TEST_CUBE_NAME, factName, factColumns, storageUpdatePeriods, 15L, properties);
    client.createCubeTable(fact4, storageTables);
    createPIEParts(client, fact4, c2);
  }

  private void createPIEParts(CubeMetastoreClient client, CubeFactTable fact, String storageName)
    throws Exception {
    // Add partitions in PIE storage
    Calendar pcal = Calendar.getInstance();
    pcal.setTime(TWODAYS_BACK);
    pcal.set(HOUR_OF_DAY, 0);
    Calendar ical = Calendar.getInstance();
    ical.setTime(TWODAYS_BACK);
    ical.set(HOUR_OF_DAY, 0);

    Map<UpdatePeriod, TreeSet<Date>> pTimes = Maps.newHashMap();
    pTimes.put(DAILY, Sets.<Date>newTreeSet());
    pTimes.put(HOURLY, Sets.<Date>newTreeSet());
    Map<UpdatePeriod, TreeSet<Date>> iTimes = Maps.newHashMap();
    iTimes.put(DAILY, Sets.<Date>newTreeSet());
    iTimes.put(HOURLY, Sets.<Date>newTreeSet());
    Map<String, Map<UpdatePeriod, TreeSet<Date>>> times = Maps.newHashMap();
    times.put("et", iTimes);
    times.put("it", iTimes);
    times.put("pt", pTimes);
    // pt=day1 and it=day1
    // pt=day2-hour[0-3] it = day1-hour[20-23]
    // pt=day2 and it=day1
    // pt=day2-hour[4-23] it = day2-hour[0-19]
    // pt=day2 and it=day2
    // pt=day3-hour[0-3] it = day2-hour[20-23]
    // pt=day3-hour[4-23] it = day3-hour[0-19]
    for (int p = 1; p <= 3; p++) {
      Date ptime = pcal.getTime();
      Date itime = ical.getTime();
      Map<String, Date> timeParts = new HashMap<String, Date>();
      if (p == 1) { // day1
        timeParts.put("pt", ptime);
        timeParts.put("it", itime);
        timeParts.put("et", itime);
        StoragePartitionDesc sPartSpec = new StoragePartitionDesc(fact.getName(), timeParts, null, DAILY);
        pTimes.get(DAILY).add(ptime);
        iTimes.get(DAILY).add(itime);
        client.addPartition(sPartSpec, storageName);
        pcal.add(DAY_OF_MONTH, 1);
        ical.add(HOUR_OF_DAY, 20);
      } else if (p == 2) { // day2
        // pt=day2-hour[0-3] it = day1-hour[20-23]
        // pt=day2 and it=day1
        // pt=day2-hour[4-23] it = day2-hour[0-19]
        // pt=day2 and it=day2
        ptime = pcal.getTime();
        itime = ical.getTime();
        timeParts.put("pt", ptime);
        timeParts.put("it", itime);
        timeParts.put("et", itime);
        // pt=day2 and it=day1
        StoragePartitionDesc sPartSpec = new StoragePartitionDesc(fact.getName(), timeParts, null, DAILY);
        pTimes.get(DAILY).add(ptime);
        iTimes.get(DAILY).add(itime);
        client.addPartition(sPartSpec, storageName);
        // pt=day2-hour[0-3] it = day1-hour[20-23]
        // pt=day2-hour[4-23] it = day2-hour[0-19]
        for (int i = 0; i < 24; i++) {
          ptime = pcal.getTime();
          itime = ical.getTime();
          timeParts.put("pt", ptime);
          timeParts.put("it", itime);
          timeParts.put("et", itime);
          sPartSpec = new StoragePartitionDesc(fact.getName(), timeParts, null, HOURLY);
          pTimes.get(HOURLY).add(ptime);
          iTimes.get(HOURLY).add(itime);
          client.addPartition(sPartSpec, storageName);
          pcal.add(HOUR_OF_DAY, 1);
          ical.add(HOUR_OF_DAY, 1);
        }
        // pt=day2 and it=day2
        sPartSpec = new StoragePartitionDesc(fact.getName(), timeParts, null, DAILY);
        pTimes.get(DAILY).add(ptime);
        iTimes.get(DAILY).add(itime);
        client.addPartition(sPartSpec, storageName);
      } else if (p == 3) { // day3
        // pt=day3-hour[0-3] it = day2-hour[20-23]
        // pt=day3-hour[4-23] it = day3-hour[0-19]
        for (int i = 0; i < 24; i++) {
          ptime = pcal.getTime();
          itime = ical.getTime();
          timeParts.put("pt", ptime);
          timeParts.put("it", itime);
          timeParts.put("et", itime);
          StoragePartitionDesc sPartSpec =
            new StoragePartitionDesc(fact.getName(), timeParts, null, HOURLY);
          pTimes.get(HOURLY).add(ptime);
          iTimes.get(HOURLY).add(itime);
          client.addPartition(sPartSpec, storageName);
          pcal.add(HOUR_OF_DAY, 1);
          ical.add(HOUR_OF_DAY, 1);
        }
      }
    }
    String storageTableName = MetastoreUtil.getStorageTableName(fact.getName(), Storage.getPrefix(
      storageName));
    Map<String, String> params = client.getTable(storageTableName).getParameters();
    String prefix = MetastoreConstants.STORAGE_PFX + MetastoreConstants.PARTITION_TIMELINE_CACHE;
    assertEquals(params.get(prefix + "present"), "true");
    for (String p : Arrays.asList("et", "it", "pt")) {
      assertTimeline(client, fact.getName(), storageName, MINUTELY, p, EndsAndHolesPartitionTimeline.class);
      for (UpdatePeriod up : Arrays.asList(DAILY, HOURLY)) {
        EndsAndHolesPartitionTimeline timeline = new EndsAndHolesPartitionTimeline(storageTableName, up, p);
        timeline.setFirst(TimePartition.of(up, times.get(p).get(up).first()));
        timeline.setLatest(TimePartition.of(up, times.get(p).get(up).last()));
        assertTimeline(client, fact.getName(), storageName, up, p, timeline);
      }
    }
  }

  public static void printQueryAST(String query, String label) throws LensException {
    System.out.println("--" + label + "--AST--");
    System.out.println("--query- " + query);
    HQLParser.printAST(HQLParser.parseHQL(query, new HiveConf()));
  }


  private static String getTimeRangeString(final String startDate, final String endDate) {
    return "time_range_in(d_time, '" + startDate + "','" + endDate + "')";
  }
}

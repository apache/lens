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

import static org.apache.lens.cube.metadata.DateFactory.BEFORE_4_DAYS;
import static org.apache.lens.cube.metadata.DateFactory.BEFORE_6_DAYS;
import static org.apache.lens.cube.metadata.DateFactory.NOW;
import static org.apache.lens.cube.metadata.DateFactory.TWODAYS_BACK;
import static org.apache.lens.cube.metadata.DateFactory.TWO_MONTHS_BACK;
import static org.apache.lens.cube.metadata.DateFactory.isZerothHour;
import static org.apache.lens.cube.metadata.UpdatePeriod.DAILY;
import static org.apache.lens.cube.metadata.UpdatePeriod.HOURLY;
import static org.apache.lens.cube.metadata.UpdatePeriod.MINUTELY;
import static org.apache.lens.cube.metadata.UpdatePeriod.MONTHLY;
import static org.apache.lens.cube.metadata.UpdatePeriod.QUARTERLY;
import static org.apache.lens.cube.metadata.UpdatePeriod.YEARLY;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.xml.bind.JAXBException;

import org.apache.lens.api.ToXMLString;
import org.apache.lens.api.jaxb.LensJAXBContext;
import org.apache.lens.api.metastore.SchemaTraverser;
import org.apache.lens.cube.metadata.*;
import org.apache.lens.cube.metadata.timeline.EndsAndHolesPartitionTimeline;
import org.apache.lens.cube.metadata.timeline.PartitionTimeline;
import org.apache.lens.cube.metadata.timeline.StoreAllPartitionTimeline;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.text.StrLookup;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;

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

  private Set<CubeMeasure> cubeMeasures;
  private Set<CubeDimAttribute> cubeDimensions;
  public static final String TEST_CUBE_NAME = "testCube";
  public static final String DERIVED_CUBE_NAME = "derivedCube";
  public static final String BASE_CUBE_NAME = "baseCube";
  public static final String VIRTUAL_CUBE_NAME = "virtualCube";

  private static String c0 = "C0";
  private static String c1 = "C1";
  private static String c2 = "C2";
  private static String c3 = "C3";
  private static String c4 = "C4";
  private static String c5 = "C5";
  private static String c99 = "C99";
  private static Map<String, String> factValidityProperties = Maps.newHashMap();
  @Getter
  private static Map<String, List<UpdatePeriod>> storageToUpdatePeriodMap = new LinkedHashMap<>();

  static {
    factValidityProperties.put(MetastoreConstants.FACT_RELATIVE_START_TIME, "now.year - 90 days");
  }


  public static String getDateUptoHours(Date dt) {
    return HOURLY.format(dt);
  }

  interface StoragePartitionProvider {
    Map<String, String> providePartitionsForStorage(String storage);
  }

  public static String getExpectedUnionQuery(String cubeName, List<String> storages, StoragePartitionProvider provider,
    String outerSelectPart, String outerWhere, String outerPostWhere, String innerQuerySelectPart, String innerJoin,
    String innerWhere, String innerPostWhere) {
    if (!innerQuerySelectPart.trim().toLowerCase().endsWith("from")) {
      innerQuerySelectPart += " from ";
    }
    StringBuilder sb = new StringBuilder();
    sb.append(outerSelectPart);
    if (!outerSelectPart.trim().toLowerCase().endsWith("from")) {
      sb.append(" from ");
    }
    sb.append(" (");
    String sep = "";
    for (String storage : storages) {
      sb.append(sep).append(getExpectedQuery(cubeName, innerQuerySelectPart + " ", innerJoin,
        innerWhere, innerPostWhere, null, provider.providePartitionsForStorage(storage)));
      sep = " UNION ALL ";
    }
    return sb.append(") ").append(" as ").append(cubeName).append(" ").append(outerWhere == null ? "" : outerWhere)
      .append(" ").append(outerPostWhere == null ? "" : outerPostWhere).toString();
  }

  public static String getExpectedUnionQuery(String cubeName, List<String> storages, StoragePartitionProvider provider,
    String outerSelectPart, String outerWhere, String outerPostWhere, String innerQuerySelectPart,
    String innerWhere, String innerPostWhere) {
    return getExpectedUnionQuery(cubeName, storages, provider, outerSelectPart, outerWhere, outerPostWhere,
      innerQuerySelectPart, null, innerWhere, innerPostWhere);
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
      expected.append(selExpr).append(storageTable).append(" ").append(cubeName).append(" WHERE ").append("(");
      if (notLatestConditions != null) {
        for (String cond : notLatestConditions) {
          expected.append(cond).append(" AND ");
        }
      }
      if (whereExpr != null) {
        expected.append(whereExpr).append(" AND ");
      }
      expected.append(entry.getValue()).append(")");
      if (postWhereExpr != null) {
        expected.append(" ").append(postWhereExpr);
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
    StringBuilder expected = new StringBuilder()
      .append(selExpr).append(getDbName()).append(storageTable).append(" ").append(cubeName)
      .append(" WHERE ").append("(");
    if (notLatestConditions != null) {
      for (String cond : notLatestConditions) {
        expected.append(cond).append(" AND ");
      }
    }
    if (whereExpr != null) {
      expected.append(whereExpr).append(" AND ");
    }
    expected.append(rangeWhere).append(")");
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
      expected.append(selExpr).append(storageTable).append(" ").append(cubeName);
      if (joinExpr != null) {
        expected.append(joinExpr);
      }
      expected.append(" WHERE ").append("(");
      if (notLatestConditions != null) {
        for (String cond : notLatestConditions) {
          expected.append(cond).append(" AND ");
        }
      }
      if (whereExpr != null) {
        expected.append(whereExpr).append(" AND ");
      }
      expected.append(entry.getValue());
      if (joinWhereConds != null) {
        for (String joinEntry : joinWhereConds) {
          expected.append(" AND ").append(joinEntry);
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
    Map<String, String> storageTableToWhereClause = new LinkedHashMap<>();
    if (storageToUpdatePeriodMap.isEmpty()) {
      String whereClause = getWhereForDailyAndHourly2daysWithTimeDim(cubeName, timedDimension, from, to);
      storageTableToWhereClause.put(getStorageTableString(storageTables), whereClause);
    } else {
      for (String tbl : storageTables) {
        for (UpdatePeriod updatePeriod : storageToUpdatePeriodMap.get(tbl)) {
          String whereClause = getWhereForDailyAndHourly2daysWithTimeDimUnionQuery(cubeName, timedDimension, from, to)
            .get(updatePeriod.getName());
          storageTableToWhereClause.put(getStorageTableString(tbl), whereClause);
        }
      }
    }
    return storageTableToWhereClause;
  }

  private static String getStorageTableString(String... storageTables) {
    String dbName = getDbName();
    if (!StringUtils.isBlank(dbName)) {
      List<String> tbls = new ArrayList<>();
      for (String tbl : storageTables) {
        tbls.add(dbName + tbl);
      }
      return StringUtils.join(tbls, ",");
    }
    return StringUtils.join(storageTables, ",");
  }

  public static String getWhereForDailyAndHourly2daysWithTimeDim(String cubeName, String timedDimension, Date from,
    Date to) {
    Set<String> hourlyparts = new HashSet<>();
    Set<String> dailyparts = new HashSet<>();
    Date dayStart;
    if (!isZerothHour()) {
      addParts(hourlyparts, HOURLY, from, DateUtil.getCeilDate(from, DAILY));
      addParts(hourlyparts, HOURLY, DateUtil.getFloorDate(to, DAILY),
        DateUtil.getFloorDate(to, HOURLY));
      dayStart = DateUtil.getCeilDate(from, DAILY);
    } else {
      dayStart = from;
    }
    addParts(dailyparts, DAILY, dayStart, DateUtil.getFloorDate(to, DAILY));
    List<String> parts = new ArrayList<>();
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
    if (!isZerothHour()) {
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

  // storageName[0] is hourly
  // storageName[1] is daily
  // storageName[2] is monthly
  public static Map<String, String> getWhereForMonthlyDailyAndHourly2months(String cubeName, String... storageTables) {
    Map<String, String> storageTableToWhereClause = new LinkedHashMap<String, String>();
    List<String> hourlyparts = new ArrayList<String>();
    List<String> dailyparts = new ArrayList<String>();
    List<String> monthlyparts = new ArrayList<String>();
    Date dayStart = TWO_MONTHS_BACK;
    Date monthStart = TWO_MONTHS_BACK;
    if (!isZerothHour()) {
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
    storageTableToWhereClause.put(tables.toString(), StorageUtil.getWherePartClause("dt", cubeName, parts));
    return storageTableToWhereClause;
  }

  public static Map<String, String> getWhereForMonthlyDailyAndHourly2monthsUnionQuery(String storageTable) {
    Map<String, List<String>> updatePeriodToPart = new LinkedHashMap<String, List<String>>();
    List<String> hourlyparts = new ArrayList<String>();
    List<String> dailyparts = new ArrayList<String>();
    List<String> monthlyparts = new ArrayList<String>();

    Date dayStart = TWO_MONTHS_BACK;
    Date monthStart = TWO_MONTHS_BACK;
    if (!isZerothHour()) {
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
    for (Map.Entry<String, List<UpdatePeriod>> entry : storageToUpdatePeriodMap.entrySet()) {
      String table = entry.getKey();
      for (UpdatePeriod updatePeriod : entry.getValue()) {
        String uperiod = updatePeriod.getName();
        if (table.equals(storageTable) && updatePeriodToPart.containsKey(uperiod)) {
          unionParts.addAll(updatePeriodToPart.get(uperiod));
          Collections.sort(unionParts);
        }
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

  public static Map<String, String> getWhereForDays(String dailyTable, Date startDay, Date endDay) {
    Map<String, String> storageTableToWhereClause = new LinkedHashMap<>();
    List<String> parts = new ArrayList<>();
    addParts(parts, DAILY, startDay, DateUtil.getFloorDate(endDay, DAILY));
    storageTableToWhereClause.put(getDbName() + dailyTable,
      StorageUtil.getWherePartClause("dt", TEST_CUBE_NAME, parts));
    return storageTableToWhereClause;
  }

  public static Map<String, String> getWhereForUpdatePeriods(String cubeName, String table, Date start, Date end,
    Set<UpdatePeriod> updatePeriods) {
    Map<String, String> storageTableToWhereClause = new LinkedHashMap<>();
    List<String> parts = new ArrayList<>();
    addParts(parts, updatePeriods, start, end);
    storageTableToWhereClause.put(getDbName() + table,
      StorageUtil.getWherePartClause("dt", cubeName, parts));
    return storageTableToWhereClause;
  }

  public static Map<String, String> getWhereForMonthly(String monthlyTable, Date startMonth, Date endMonth) {
    Map<String, String> storageTableToWhereClause = new LinkedHashMap<String, String>();
    List<String> parts = new ArrayList<String>();
    addParts(parts, MONTHLY, startMonth, endMonth);
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

  public static void addParts(Collection<String> partitions, UpdatePeriod updatePeriod, Date from, Date to) {
    try {
      for (TimePartition timePartition : TimePartitionRange.between(from, to, updatePeriod)) {
        partitions.add(timePartition.toString());
      }
    } catch (LensException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static void addParts(Collection<String> partitions, Set<UpdatePeriod> updatePeriods, Date from, Date to) {
    if (updatePeriods.size() != 0) {
      UpdatePeriod max = CubeFactTable.maxIntervalInRange(from, to, updatePeriods);
      if (max != null) {
        updatePeriods.remove(max);
        Date ceilFromDate = DateUtil.getCeilDate(from, max);
        Date floorToDate = DateUtil.getFloorDate(to, max);
        addParts(partitions, updatePeriods, from, ceilFromDate);
        addParts(partitions, max, ceilFromDate, floorToDate);
        addParts(partitions, updatePeriods, floorToDate, to);
      }
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

  private void assertTestFactTimelineClass(CubeMetastoreClient client) throws Exception {
    String factName = "testFact";

    client.getTimelines(factName, c1, null, null);
    client.getTimelines(factName, c4, null, null);

    client.clearHiveTableCache();

    CubeFactTable fact = client.getCubeFactTable(factName);
    Table table = client.getTable(MetastoreUtil.getStorageTableName(fact.getName(), Storage.getPrefix(c1)));
    assertEquals(table.getParameters().get(MetastoreUtil.getPartitionTimelineCachePresenceKey()), "true");
    for (UpdatePeriod period : Lists.newArrayList(MINUTELY, HOURLY, DAILY, MONTHLY, YEARLY, QUARTERLY)) {
      for (String partCol : Lists.newArrayList("dt")) {
        assertTimeline(client, fact.getName(), c1, period, partCol, EndsAndHolesPartitionTimeline.class);
      }
    }

    table = client.getTable(MetastoreUtil.getStorageTableName(fact.getName(), Storage.getPrefix(c4)));
    assertEquals(table.getParameters().get(MetastoreUtil.getPartitionTimelineCachePresenceKey()), "true");
    for (UpdatePeriod period : Lists.newArrayList(MINUTELY, HOURLY, DAILY, MONTHLY, YEARLY, QUARTERLY)) {
      for (String partCol : Lists.newArrayList("ttd", "ttd2")) {
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

  private void createCubeCheapFactPartitions(CubeMetastoreClient client) throws HiveException, LensException {
    String factName = "cheapFact";
    CubeFactTable fact = client.getCubeFactTable(factName);
    // Add all hourly partitions for two days
    Calendar cal = Calendar.getInstance();
    cal.setTime(TWODAYS_BACK);
    Date temp = cal.getTime();
    while (!(temp.after(NOW))) {
      Map<String, Date> timeParts = new HashMap<String, Date>();
      timeParts.put("ttd", temp);
      timeParts.put("ttd2", temp);
      StoragePartitionDesc sPartSpec = new StoragePartitionDesc(fact.getName(), timeParts, null, HOURLY);
      client.addPartition(sPartSpec, c99, CubeTableType.FACT);
      cal.add(HOUR_OF_DAY, 1);
      temp = cal.getTime();
    }

    // Add all hourly partitions for TWO_DAYS_RANGE_BEFORE_4_DAYS
    cal.setTime(BEFORE_6_DAYS);
    temp = cal.getTime();
    while (!(temp.after(BEFORE_4_DAYS))) {
      Map<String, Date> timeParts = new HashMap<String, Date>();
      timeParts.put("ttd", temp);
      timeParts.put("ttd2", temp);
      StoragePartitionDesc sPartSpec = new StoragePartitionDesc(fact.getName(), timeParts, null, HOURLY);
      client.addPartition(sPartSpec, c99, CubeTableType.FACT);
      cal.add(HOUR_OF_DAY, 1);
      temp = cal.getTime();
    }
  }


  private void createTestFact2Partitions(CubeMetastoreClient client) throws Exception {
    String factName = "testFact2";
    CubeFactTable fact = client.getCubeFactTable(factName);
    // Add all hourly partitions for two days
    Calendar cal = Calendar.getInstance();
    cal.setTime(TWODAYS_BACK);
    Date temp = cal.getTime();
    while (!(temp.after(NOW))) {
      Map<String, Date> timeParts = new HashMap<String, Date>();
      timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), temp);
      StoragePartitionDesc sPartSpec = new StoragePartitionDesc(fact.getName(), timeParts, null, HOURLY);
      try {
        client.addPartition(sPartSpec, c1, CubeTableType.FACT);
      } catch (HiveException e) {
        log.error("Encountered Hive exception.", e);
      } catch (LensException e) {
        log.error("Encountered Lens exception.", e);
      }
      cal.add(HOUR_OF_DAY, 1);
      temp = cal.getTime();
    }

    // Add all hourly partitions for TWO_DAYS_RANGE_BEFORE_4_DAYS
    cal.setTime(BEFORE_6_DAYS);
    temp = cal.getTime();
    while (!(temp.after(BEFORE_4_DAYS))) {
      Map<String, Date> timeParts = new HashMap<String, Date>();
      timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), temp);
      StoragePartitionDesc sPartSpec = new StoragePartitionDesc(fact.getName(), timeParts, null, HOURLY);
      client.addPartition(sPartSpec, c1, CubeTableType.FACT);
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
      partitions.add(HOURLY.format(temp));
      StoragePartitionDesc sPartSpec = new StoragePartitionDesc(fact.getName(), timeParts, null, HOURLY);
      storagePartitionDescs.add(sPartSpec);
      cal.add(HOUR_OF_DAY, 1);
      temp = cal.getTime();
    }
    client.addPartitions(storagePartitionDescs, c4, CubeTableType.FACT);
    client.clearHiveTableCache();
    table = client.getTable(MetastoreUtil.getStorageTableName(fact.getName(), Storage.getPrefix(c4)));
    assertEquals(table.getParameters().get(MetastoreUtil.getPartitionTimelineCachePresenceKey()), "true");
    assertTimeline(client, fact.getName(), c4, HOURLY, "ttd", ttdStoreAll);
    assertTimeline(client, fact.getName(), c4, HOURLY, "ttd2", ttd2StoreAll);

    // Add all hourly partitions for TWO_DAYS_RANGE_BEFORE_4_DAYS
    cal.setTime(BEFORE_6_DAYS);
    temp = cal.getTime();
    while (!(temp.after(BEFORE_4_DAYS))) {
      Map<String, Date> timeParts = new HashMap<String, Date>();
      timeParts.put("ttd", temp);
      timeParts.put("ttd2", temp);
      StoragePartitionDesc sPartSpec = new StoragePartitionDesc(fact.getName(), timeParts, null, HOURLY);
      client.addPartition(sPartSpec, c4, CubeTableType.FACT);
      cal.add(HOUR_OF_DAY, 1);
      temp = cal.getTime();
    }
  }

  private void createTestFact2RawPartitions(CubeMetastoreClient client) throws HiveException, LensException {
    String factName = "testFact2_raw";
    CubeFactTable fact2 = client.getCubeFactTable(factName);
    // Add all hourly partitions for two days
    Calendar cal = Calendar.getInstance();
    cal.setTime(TWODAYS_BACK);
    Date temp = cal.getTime();
    while (!(temp.after(NOW))) {
      Map<String, Date> timeParts = new HashMap<String, Date>();
      timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), temp);
      StoragePartitionDesc sPartSpec = new StoragePartitionDesc(fact2.getName(), timeParts, null, HOURLY);
      client.addPartition(sPartSpec, c3, CubeTableType.FACT);
      cal.add(HOUR_OF_DAY, 1);
      temp = cal.getTime();
    }
  }


  public void createSources(HiveConf conf, String dbName) throws Exception {
    try {
      Database database = new Database();
      database.setName(dbName);
      Hive.get(conf).dropDatabase(dbName, true, true, true);
      Hive.get(conf).createDatabase(database);
      SessionState.get().setCurrentDatabase(dbName);
      CubeMetastoreClient client = CubeMetastoreClient.getInstance(conf);
      createFromXML(client);
      assertTestFactTimelineClass(client);
      createCubeCheapFactPartitions(client);
      // commenting this as the week date format throws IllegalPatternException
      // createCubeFactWeekly(client);
      createTestFact2Partitions(client);
      createTestFact2RawPartitions(client);
      createBaseCubeFactPartitions(client);
      createSummaryPartitions(client);
//      dump(client);
    } catch (Exception exc) {
      log.error("Exception while creating sources.", exc);
      throw exc;
    }
  }
  private static final StrSubstitutor GREGORIAN_SUBSTITUTOR = new StrSubstitutor(new StrLookup<String>() {
    @Override
    public String lookup(String s) {
      try {
        return JAXBUtils.getXMLGregorianCalendar(DateUtil.resolveDate(s, NOW)).toString();
      } catch (LensException e) {
        throw new RuntimeException(e);
      }
    }
  }, "$gregorian{", "}", '$');
  private static final StrSubstitutor ABSOLUTE_SUBSTITUTOR = new StrSubstitutor(new StrLookup<String>() {
    @Override
    public String lookup(String s) {
      try {
        return DateUtil.relativeToAbsolute(s, NOW);
      } catch (LensException e) {
        throw new RuntimeException(e);
      }
    }
  }, "$absolute{", "}", '$');
  private void createFromXML(CubeMetastoreClient client) {
    SchemaTraverser.SchemaEntityProcessor processor = (file, aClass) -> {
      Function<String, String> f = GREGORIAN_SUBSTITUTOR::replace;
      Function<String, String> g = ABSOLUTE_SUBSTITUTOR::replace;
      try {
        BufferedReader br = new BufferedReader(new FileReader(file));
        String replaced = br.lines().map(f.andThen(g)).collect(Collectors.joining("\n"));
        StringReader sr = new StringReader(replaced);
        client.createEntity(LensJAXBContext.unmarshall(sr));
      } catch (LensException | JAXBException | IOException e) {
        throw new RuntimeException(e);
      }
    };
    new SchemaTraverser(new File(getClass().getResource("/schema").getFile()), processor, null, null).run();
  }

  private void dump(CubeMetastoreClient client) throws LensException, IOException {
//    for (CubeInterface cubeInterface : client.getAllCubes()) {
//      String path = getClass().getResource("/schema/cubes/" + ((cubeInterface instanceof Cube) ? "base"
// : "derived")).getPath() + "/" + cubeInterface.getName() + ".xml";
//      try(BufferedWriter bw = new BufferedWriter(new FileWriter(path))) {
//        bw.write(ToXMLString.toString(JAXBUtils.xCubeFromHiveCube(cubeInterface)));
//      }
//    }
    for (FactTable factTable : client.getAllFacts(false)) {
      CubeFactTable cubeFactTable = (CubeFactTable) factTable;
      try(BufferedWriter bw = new BufferedWriter(new FileWriter(getClass()
          .getResource("/schema/facts").getPath()+"/"+cubeFactTable.getName()+".xml"))) {
        bw.write(ToXMLString.toString(client.getXFactTable(cubeFactTable)));
      }
    }
//    for (Dimension dim : client.getAllDimensions()) {
//      try(BufferedWriter bw = new BufferedWriter(new FileWriter(getClass()
// .getResource("/schema/dimensions").getPath()+"/"+dim.getName()+".xml"))) {
//        bw.write(ToXMLString.toString(JAXBUtils.xdimensionFromDimension(dim)));
//      }
//    }
    for (CubeDimensionTable dim : client.getAllDimensionTables()) {
      try(BufferedWriter bw = new BufferedWriter(new FileWriter(getClass()
          .getResource("/schema/dimtables").getPath()+"/"+dim.getName()+".xml"))) {
        bw.write(ToXMLString.toString(client.getXDimensionTable(dim)));
      }
    }
//    for (Storage storage : client.getAllStorages()) {
//      try(BufferedWriter bw = new BufferedWriter(new FileWriter(getClass()
// .getResource("/schema/storages").getPath()+"/"+storage.getName()+".xml"))) {
//        bw.write(ToXMLString.toString(JAXBUtils.xstorageFromStorage(storage)));
//      }
//    }
  }


  public void dropSources(HiveConf conf, String dbName) throws Exception {
    Hive metastore = Hive.get(conf);
    metastore.dropDatabase(dbName, true, true, true);
  }

  private void createSummaryPartitions(CubeMetastoreClient client) throws Exception {
    String factName = "summary1";
    CubeFactTable fact1 = client.getCubeFactTable(factName);
    createPIEParts(client, fact1, c2);

    factName = "summary2";
    CubeFactTable fact2 = client.getCubeFactTable(factName);
    createPIEParts(client, fact2, c2);

    factName = "summary3";
    CubeFactTable fact3 = client.getCubeFactTable(factName);
    createPIEParts(client, fact3, c2);


    factName = "summary4";
    CubeFactTable fact4 = client.getCubeFactTable(factName);
    createPIEParts(client, fact4, c2);
  }

  private void createBaseCubeFactPartitions(CubeMetastoreClient client) throws HiveException, LensException {
    String factName = "testFact5_RAW_BASE";
    CubeFactTable fact = client.getCubeFactTable(factName);
    // Add all hourly partitions for two days
    Calendar cal = Calendar.getInstance();
    cal.setTime(TWODAYS_BACK);
    Date temp = cal.getTime();
    while (!(temp.after(NOW))) {
      Map<String, Date> timeParts = new HashMap<String, Date>();
      timeParts.put("dt", temp);
      StoragePartitionDesc sPartSpec = new StoragePartitionDesc(fact.getName(), timeParts, null, HOURLY);
      client.addPartition(sPartSpec, c1, CubeTableType.FACT);
      cal.add(HOUR_OF_DAY, 1);
      temp = cal.getTime();
    }
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
        client.addPartition(sPartSpec, storageName, CubeTableType.FACT);
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
        client.addPartition(sPartSpec, storageName, CubeTableType.FACT);
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
          client.addPartition(sPartSpec, storageName, CubeTableType.FACT);
          pcal.add(HOUR_OF_DAY, 1);
          ical.add(HOUR_OF_DAY, 1);
        }
        // pt=day2 and it=day2
        sPartSpec = new StoragePartitionDesc(fact.getName(), timeParts, null, DAILY);
        pTimes.get(DAILY).add(ptime);
        iTimes.get(DAILY).add(itime);
        client.addPartition(sPartSpec, storageName, CubeTableType.FACT);
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
          client.addPartition(sPartSpec, storageName, CubeTableType.FACT);
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
}

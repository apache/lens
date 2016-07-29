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
import static org.apache.lens.cube.metadata.UpdatePeriod.*;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.*;
import static org.apache.lens.cube.parse.CubeTestSetup.*;
import static org.apache.lens.cube.parse.TestCubeRewriter.*;

import static org.testng.Assert.*;

import java.util.*;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.server.api.LensServerAPITestUtil;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ParseException;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestUnionQueries extends TestQueryRewrite {

  private Configuration testConf;

  @BeforeTest
  public void setupDriver() throws Exception {
    testConf = LensServerAPITestUtil.getConfiguration(
      DRIVER_SUPPORTED_STORAGES, "C0,C1,C2",
      DISABLE_AUTO_JOINS, false,
      ENABLE_SELECT_TO_GROUPBY, true,
      ENABLE_GROUP_BY_TO_SELECT, true,
      DISABLE_AGGREGATE_RESOLVER, false,
      ENABLE_STORAGES_UNION, true);
  }

  @Override
  public Configuration getConf() {
    return new Configuration(testConf);
  }

  @Test
  public void testUnionQueries() throws Exception {
    Configuration conf = getConf();
    conf.set(getValidStorageTablesKey("testfact"), "C1_testFact,C2_testFact");
    conf.set(getValidUpdatePeriodsKey("testfact", "C1"), "DAILY,HOURLY");
    conf.set(getValidUpdatePeriodsKey("testfact2", "C1"), "YEARLY");
    conf.set(getValidUpdatePeriodsKey("testfact", "C2"), "MONTHLY,DAILY");
    conf.setBoolean(CubeQueryConfUtil.ENABLE_STORAGES_UNION, false);
    ArrayList<String> storages = Lists.newArrayList("c1_testfact", "c2_testfact");
    try {
      getStorageToUpdatePeriodMap().put("c1_testfact", Lists.newArrayList(HOURLY, DAILY));
      getStorageToUpdatePeriodMap().put("c2_testfact", Lists.newArrayList(MONTHLY));

      // Union query
      String hqlQuery;
      String expected;
      StoragePartitionProvider provider = new StoragePartitionProvider() {
        @Override
        public Map<String, String> providePartitionsForStorage(String storage) {
          return getWhereForMonthlyDailyAndHourly2monthsUnionQuery(storage);
        }
      };
      try {
        rewrite("select cityid as `City ID`, msr8, msr7 as `Third measure` "
          + "from testCube where " + TWO_MONTHS_RANGE_UPTO_HOURS, conf);
        fail("Union feature is disabled, should have failed");
      } catch (LensException e) {
        assertEquals(e.getErrorCode(), LensCubeErrorCode.STORAGE_UNION_DISABLED.getLensErrorInfo().getErrorCode());
      }
      conf.setBoolean(CubeQueryConfUtil.ENABLE_STORAGES_UNION, true);

      hqlQuery = rewrite("select ascii(cityname) as `City Name`, msr8, msr7 as `Third measure` "
        + "from testCube where ascii(cityname) = 'c' and cityname = 'a' and zipcode = 'b' and "
        + TWO_MONTHS_RANGE_UPTO_HOURS, conf);
      expected = getExpectedUnionQuery(TEST_CUBE_NAME, storages, provider,
        "SELECT testcube.alias0 as `City Name`, sum(testcube.alias1) + max(testcube.alias2), "
          + "case when sum(testcube.alias1) = 0 then 0 else sum(testcube.alias3)/sum(testcube.alias1) end "
          + "as `Third Measure`",
        null, "group by testcube.alias0",
        "select ascii(cubecity.name) as `alias0`, sum(testcube.msr2) as `alias1`, "
          + "max(testcube.msr3) as `alias2`, "
          + "sum(case when testcube.cityid = 'x' then testcube.msr21 else testcube.msr22 end) as `alias3`", " join "
          + getDbName() + "c1_citytable cubecity on testcube.cityid = cubecity.id and (cubecity.dt = 'latest')",
        "ascii(cubecity.name) = 'c' and cubecity.name = 'a' and testcube.zipcode = 'b'",
        "group by ascii(cubecity.name))");
      compareQueries(hqlQuery, expected);
      hqlQuery = rewrite("select asciicity as `City Name`, msr8, msr7 as `Third measure` "
        + "from testCube where asciicity = 'c' and cityname = 'a' and zipcode = 'b' and "
        + TWO_MONTHS_RANGE_UPTO_HOURS, conf);
      compareQueries(hqlQuery, expected);

      hqlQuery = rewrite("select ascii(cityid) as `City ID`, msr8, msr7 as `Third measure` "
        + "from testCube where ascii(cityid) = 'c' and cityid = 'a' and zipcode = 'b' and "
        + TWO_MONTHS_RANGE_UPTO_HOURS, conf);

      expected = getExpectedUnionQuery(TEST_CUBE_NAME, storages, provider,
        "SELECT testcube.alias0 as `City ID`, sum(testcube.alias1) + max(testcube.alias2), "
          + "case when sum(testcube.alias1) = 0 then 0 else sum(testcube.alias3)/sum(testcube.alias1) end "
          + "as `Third Measure`",
        null, "group by testcube.alias0",
        "select ascii(testcube.cityid) as `alias0`, sum(testcube.msr2) as `alias1`, "
          + "max(testcube.msr3) as `alias2`, "
          + "sum(case when testcube.cityid = 'x' then testcube.msr21 else testcube.msr22 end) as `alias3`",
        "ascii(testcube.cityid) = 'c' and testcube.cityid = 'a' and testcube.zipcode = 'b'",
        "group by ascii(testcube.cityid)");

      compareQueries(hqlQuery, expected);

      hqlQuery = rewrite("select cityid as `City ID`, msr8, msr7 as `Third measure` "
        + "from testCube where cityid = 'a' and zipcode = 'b' and " + TWO_MONTHS_RANGE_UPTO_HOURS, conf);

      expected = getExpectedUnionQuery(TEST_CUBE_NAME, storages, provider,
        "SELECT testcube.alias0 as `City ID`, sum(testcube.alias1) + max(testcube.alias2), "
          + "case when sum(testcube.alias1) = 0 then 0 else sum(testcube.alias3)/sum(testcube.alias1) end "
          + "as `Third Measure`",
        null, "group by testcube.alias0",
        "select testcube.cityid as `alias0`, sum(testcube.msr2) as `alias1`, "
          + "max(testcube.msr3) as `alias2`, "
          + "sum(case when testcube.cityid = 'x' then testcube.msr21 else testcube.msr22 end) as `alias3`",
        "testcube.cityid = 'a' and testcube.zipcode = 'b'", "group by testcube.cityid");

      compareQueries(hqlQuery, expected);

      hqlQuery = rewrite("select cityid as `City ID`, msr3 as `Third measure` from testCube where "
        + TWO_MONTHS_RANGE_UPTO_HOURS + " having msr7 > 10", conf);

      expected = getExpectedUnionQuery(TEST_CUBE_NAME, storages, provider,
        "SELECT testcube.alias0 as `City ID`, max(testcube.alias1) as `Third measure`",
        null, "group by testcube.alias0 having "
          + "(case when sum(testcube.alias2)=0 then 0 else sum(testcube.alias3)/sum(testcube.alias2) end > 10 )",
        "SELECT testcube.cityid as `alias0`, max(testcube.msr3) as `alias1`, "
          + "sum(testcube.msr2) as `alias2`, "
          + "sum(case when testcube.cityid='x' then testcube.msr21 else testcube.msr22 end) as `alias3`",
        null, "group by testcube.cityid");
      compareQueries(hqlQuery, expected);

      hqlQuery = rewrite("select cityid as `City ID`, msr3 as `Third measure` from testCube where "
        + TWO_MONTHS_RANGE_UPTO_HOURS + " having msr8 > 10", conf);

      expected = getExpectedUnionQuery(TEST_CUBE_NAME, storages, provider,
        "SELECT testcube.alias0 as `City ID`, max(testcube.alias1) as `Third measure`",
        null, "GROUP BY testcube.alias0 "
          + "HAVING (sum(testcube.alias2) + max(testcube.alias1)) > 10 ",
        "SELECT testcube.cityid as `alias0`, max(testcube.msr3) as `alias1`, "
          + "sum(testcube.msr2)as `alias2`", null, "group by testcube.cityid");
      compareQueries(hqlQuery, expected);

      hqlQuery = rewrite("select msr3 as `Measure 3` from testCube where "
        + TWO_MONTHS_RANGE_UPTO_HOURS + " having msr2 > 10 and msr2 < 100", conf);

      expected = getExpectedUnionQuery(TEST_CUBE_NAME, storages, provider,
        "SELECT max(testcube.alias0) as `Measure 3` ",
        null, " HAVING sum(testcube.alias1) > 10 and sum(testcube.alias1) < 100",
        "SELECT max(testcube.msr3) as `alias0`, sum(testcube.msr2) as `alias1`", null, null);
      compareQueries(hqlQuery, expected);

      hqlQuery = rewrite("select zipcode, cityid as `City ID`, msr3 as `Measure 3`, msr4, "
        + "SUM(msr2) as `Measure 2` from testCube where "
        + TWO_MONTHS_RANGE_UPTO_HOURS + " having msr4 > 10 order by cityid desc limit 5", conf);

      expected = getExpectedUnionQuery(TEST_CUBE_NAME, storages, provider,
        "SELECT testcube.alias0, testcube.alias1 as `City ID`, max(testcube.alias2) as `Measure 3`, "
          + "count(testcube.alias3), sum(testcube.alias4) as `Measure 2`",
        null, "group by testcube.alias0, testcube.alias1 "
          + " having count(testcube.alias3) > 10 order by testcube.alias1 desc limit 5",
        "select testcube.zipcode as `alias0`, testcube.cityid as `alias1`, "
          + "max(testcube.msr3) as `alias2`,count(testcube.msr4) as `alias3`, sum(testcube.msr2) as `alias4`",
        null, "group by testcube.zipcode, testcube.cityid ");
      compareQueries(hqlQuery, expected);

      conf.setBoolean(CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT, false);
      conf.setBoolean(ENABLE_SELECT_TO_GROUPBY, false);
      hqlQuery = rewrite("select cityid as `City ID`, msr3 as `Measure 3`, "
        + "SUM(msr2) as `Measure 2` from testCube" + " where "
        + TWO_MONTHS_RANGE_UPTO_HOURS + " group by zipcode having msr4 > 10 order by cityid desc limit 5", conf);

      expected = getExpectedUnionQuery(TEST_CUBE_NAME, storages, provider,
        "SELECT testcube.alias0 as `City ID`,max(testcube.alias1) as `Measure 3`,sum(testcube.alias2) as `Measure 2` ",
        null, "group by testcube.alias3 having count(testcube.alias4) > 10 order by testcube.alias0 desc limit 5",
        "SELECT testcube.cityid as `alias0`, max(testcube.msr3) as `alias1`, "
          + "sum(testcube.msr2) as `alias2`, testcube.zipcode as `alias3`, count(testcube .msr4) as `alias4` FROM ",
        null, "GROUP BY testcube.zipcode");
      compareQueries(hqlQuery, expected);
    } finally {
      getStorageToUpdatePeriodMap().clear();
    }
  }

  @Test
  public void testDimAttrExpressionQuery() throws Exception {
    Configuration conf = getConf();
    conf.set(getValidStorageTablesKey("testfact"), "C1_testFact,C2_testFact");
    conf.set(getValidUpdatePeriodsKey("testfact", "C1"), "DAILY,HOURLY");
    conf.set(getValidUpdatePeriodsKey("testfact2", "C1"), "YEARLY");
    conf.set(getValidUpdatePeriodsKey("testfact", "C2"), "MONTHLY,DAILY");

    String hqlQuery = rewrite("select asciicity as `City Name`, cityAndState as citystate, isIndia as isIndia,"
      + " msr8, msr7 as `Third measure` "
      + "from testCube where asciicity = 'c' and cityname = 'a' and zipcode = 'b' and "
      + TWO_MONTHS_RANGE_UPTO_HOURS, conf);
    String joinExpr1 =  " join "
      + getDbName() + "c1_statetable cubestate on testcube.stateid = cubestate.id and (cubestate.dt = 'latest') join"
      + getDbName() + "c1_citytable cubecity on testcube.cityid = cubecity.id and (cubecity.dt = 'latest')";
    String joinExpr2 =  " join "
      + getDbName() + "c1_citytable cubecity on testcube.cityid = cubecity.id and (cubecity.dt = 'latest') join"
      + getDbName() + "c1_statetable cubestate on testcube.stateid = cubestate.id and (cubestate.dt = 'latest')";

    String expected1 = getExpectedQueryForDimAttrExpressionQuery(joinExpr1);
    String expected2 = getExpectedQueryForDimAttrExpressionQuery(joinExpr2);
    assertTrue(new TestQuery(hqlQuery).equals(new TestQuery(expected1))
      || new TestQuery(hqlQuery).equals(new TestQuery(expected2)),
      "Actual :" + hqlQuery + " Expected1:" + expected1 + " Expected2 : "+ expected2);
  }

  private String getExpectedQueryForDimAttrExpressionQuery(String joinExpr) {
    try {
      ArrayList<String> storages = Lists.newArrayList("c1_testfact", "c2_testfact");
      getStorageToUpdatePeriodMap().put("c1_testfact", Lists.newArrayList(HOURLY, DAILY));
      getStorageToUpdatePeriodMap().put("c2_testfact", Lists.newArrayList(MONTHLY));
      StoragePartitionProvider provider = new StoragePartitionProvider() {
        @Override
        public Map<String, String> providePartitionsForStorage(String storage) {
          return getWhereForMonthlyDailyAndHourly2monthsUnionQuery(storage);
        }
      };
      return getExpectedUnionQuery(TEST_CUBE_NAME, storages, provider,
        "SELECT testcube.alias0 as `City Name`, testcube.alias1 as citystate, testcube.alias2 as isIndia, "
          + "sum(testcube.alias3) + max(testcube.alias4), "
          + "case when sum(testcube.alias3) = 0 then 0 else sum(testcube.alias5)/sum(testcube.alias3) end "
          + "as `Third Measure`",
        null, " group by testcube.alias0, testcube.alias1, testcube.alias2",
        "select ascii(cubecity.name) as `alias0`, concat(cubecity.name, \":\", cubestate.name) as alias1,"
          + "cubecity.name == 'DELHI' OR cubestate.name == 'KARNATAKA' OR cubestate.name == 'MAHARASHTRA' as alias2,"
          + "sum(testcube.msr2) as `alias3`, max(testcube.msr3) as `alias4`, "
          + "sum(case when testcube.cityid = 'x' then testcube.msr21 else testcube.msr22 end) as `alias5`", joinExpr,
        "ascii(cubecity.name) = 'c' and cubecity.name = 'a' and testcube.zipcode = 'b'",
        " group by ascii(cubecity.name)), concat(cubecity.name, \":\", cubestate.name),"
          + "cubecity.name == 'DELHI' OR cubestate.name == 'KARNATAKA' OR cubestate.name == 'MAHARASHTRA'");
    } finally {
      getStorageToUpdatePeriodMap().clear();
    }
  }
  @Test
  public void testNonAggregateOverAggregateFunction() throws Exception {
    try {
      Configuration conf = getConf();
      conf.set(getValidStorageTablesKey("testfact"), "C1_testFact,C2_testFact");
      conf.set(getValidUpdatePeriodsKey("testfact", "C1"), "DAILY,HOURLY");
      conf.set(getValidUpdatePeriodsKey("testfact2", "C1"), "YEARLY");
      conf.set(getValidUpdatePeriodsKey("testfact", "C2"), "MONTHLY,DAILY");
      ArrayList<String> storages = Lists.newArrayList("c1_testfact", "c2_testfact");
      getStorageToUpdatePeriodMap().put("c1_testfact", Lists.newArrayList(HOURLY, DAILY));
      getStorageToUpdatePeriodMap().put("c2_testfact", Lists.newArrayList(MONTHLY));
      StoragePartitionProvider provider = new StoragePartitionProvider() {
        @Override
        public Map<String, String> providePartitionsForStorage(String storage) {
          return getWhereForMonthlyDailyAndHourly2monthsUnionQuery(storage);
        }
      };
      String hqlQuery = rewrite("select cityid as `City ID`, msr3 as `Measure 3`, "
        + "round(SUM(msr2)) as `Measure 2` from testCube" + " where "
        + TWO_MONTHS_RANGE_UPTO_HOURS + " group by zipcode having msr4 > 10 order by cityid desc, stateid asc, zipcode "
        + "asc limit 5",
        conf);
      String expected = getExpectedUnionQuery(TEST_CUBE_NAME, storages, provider,
        "SELECT testcube.alias0 as `City ID`,max(testcube.alias1) as `Measure 3`,round(sum(testcube.alias2)) as "
          + "`Measure 2` ", null, "group by testcube.alias3 having count(testcube.alias4) > 10 "
          + "order by testcube.alias0 desc, testcube.alias5 asc, testcube.alias3 asc limit 5",
        "SELECT testcube.cityid as `alias0`, max(testcube.msr3) as `alias1`, sum(testcube.msr2) as `alias2`, "
          + "testcube.zipcode as `alias3`, count(testcube .msr4) as `alias4`, (testcube.stateid) as `alias5` FROM ",
        null, "GROUP BY testcube.zipcode");
      compareQueries(hqlQuery, expected);
    } finally {
      getStorageToUpdatePeriodMap().clear();
    }
  }

  @Test
  public void testMultiFactMultiStorage() throws ParseException, LensException {
    try {
      Configuration conf = LensServerAPITestUtil.getConfigurationWithParams(getConf(),
        CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1,C2",
        getValidStorageTablesKey("testfact"), "C1_testFact,C2_testFact",
        getValidUpdatePeriodsKey("testfact", "C1"), "HOURLY",
        getValidUpdatePeriodsKey("testfact", "C2"), "DAILY",
        getValidUpdatePeriodsKey("testfact2_raw", "C1"), "YEARLY",
        getValidUpdatePeriodsKey("testfact2_raw", "C2"), "YEARLY");
      getStorageToUpdatePeriodMap().put("c1_testfact", Lists.newArrayList(HOURLY));
      getStorageToUpdatePeriodMap().put("c2_testfact", Lists.newArrayList(DAILY));
      String whereCond = "zipcode = 'a' and cityid = 'b' and (" + TWO_DAYS_RANGE_SPLIT_OVER_UPDATE_PERIODS + ")";
      String hqlQuery = rewrite("select zipcode, count(msr4), sum(msr15) from testCube where " + whereCond, conf);
      System.out.println(hqlQuery);
      String possibleStart1 = "SELECT COALESCE(mq1.zipcode, mq2.zipcode) zipcode, mq1.expr2 `count(msr4)`,"
        + " mq2.expr3 `sum(msr15)` FROM ";
      String possibleStart2 = "SELECT COALESCE(mq1.zipcode, mq2.zipcode) zipcode, mq2.expr2 `count(msr4)`,"
        + " mq1.expr3 `sum(msr15)` FROM ";

      assertTrue(hqlQuery.startsWith(possibleStart1) || hqlQuery.startsWith(possibleStart2));
      compareContains(rewrite("select zipcode as `zipcode`, sum(msr15) as `expr3` from testcube where " + whereCond,
        conf), hqlQuery);
      compareContains(rewrite("select zipcode as `zipcode`, count(msr4) as `expr2` from testcube where " + whereCond,
        conf), hqlQuery);
      assertTrue(hqlQuery.endsWith("on mq1.zipcode <=> mq2.zipcode"));
      // No time_range_in should be remaining
      assertFalse(hqlQuery.contains("time_range_in"));
      //TODO: handle having after LENS-813, also handle for order by and limit
    } finally {
      getStorageToUpdatePeriodMap().clear();
    }
  }

  @Test
  public void testCubeWhereQueryWithMultipleTables() throws Exception {
    Configuration conf = getConf();
    conf.setBoolean(CubeQueryConfUtil.ENABLE_STORAGES_UNION, true);
    conf.set(getValidStorageTablesKey("testfact"), "C1_testFact,C2_testFact");
    conf.set(getValidUpdatePeriodsKey("testfact", "C1"), "DAILY");
    conf.set(getValidUpdatePeriodsKey("testfact2", "C1"), "YEARLY");
    conf.set(getValidUpdatePeriodsKey("testfact", "C2"), "HOURLY");

    getStorageToUpdatePeriodMap().put("c1_testfact", Lists.newArrayList(DAILY));
    getStorageToUpdatePeriodMap().put("c2_testfact", Lists.newArrayList(HOURLY));
    StoragePartitionProvider provider = new StoragePartitionProvider() {
      @Override
      public Map<String, String> providePartitionsForStorage(String storage) {
        return getWhereForDailyAndHourly2days(TEST_CUBE_NAME, storage);
      }
    };
    try {
      // Union query
      String hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_DAYS_RANGE, conf);
      System.out.println("HQL:" + hqlQuery);

      String expected = getExpectedUnionQuery(TEST_CUBE_NAME,
        Lists.newArrayList("c1_testfact", "c2_testfact"), provider,
        "select sum(testcube.alias0) ", null, null,
        "select sum(testcube.msr2) as `alias0` from ", null, null
      );
      compareQueries(hqlQuery, expected);
    } finally {
      getStorageToUpdatePeriodMap().clear();
    }
  }

  @Test
  public void testCubeWhereQueryWithMultipleTablesForMonth() throws Exception {
    Configuration conf = getConf();
    conf.set(DRIVER_SUPPORTED_STORAGES, "C1,C2,C3");
    conf.set(getValidStorageTablesKey("testfact"), "");
    conf.set(getValidUpdatePeriodsKey("testfact", "C1"), "HOURLY");
    conf.set(getValidUpdatePeriodsKey("testfact2", "C1"), "YEARLY");
    conf.set(getValidUpdatePeriodsKey("testfact2_raw", "C3"), "YEARLY");
    conf.set(getValidUpdatePeriodsKey("testfact", "C2"), "DAILY");
    conf.set(getValidUpdatePeriodsKey("testfact", "C3"), "MONTHLY");

    getStorageToUpdatePeriodMap().put("c1_testfact", Lists.newArrayList(HOURLY));
    getStorageToUpdatePeriodMap().put("c2_testfact", Lists.newArrayList(DAILY));
    getStorageToUpdatePeriodMap().put("c3_testfact", Lists.newArrayList(MONTHLY));
    StoragePartitionProvider provider = new StoragePartitionProvider() {
      @Override
      public Map<String, String> providePartitionsForStorage(String storage) {
        return getWhereForMonthlyDailyAndHourly2monthsUnionQuery(storage);
      }
    };
    try {
      // Union query
      String hqlQuery = rewrite("select SUM(msr2) from testCube" + " where " + TWO_MONTHS_RANGE_UPTO_HOURS, conf);
      System.out.println("HQL:" + hqlQuery);
      ArrayList<String> storages = Lists.newArrayList("c1_testfact", "c2_testfact", "c3_testfact");
      String expected = getExpectedUnionQuery(TEST_CUBE_NAME, storages, provider,
        "select sum(testcube.alias0)", null, null,
        "select sum(testcube.msr2) as `alias0` from ", null, null
      );
      compareQueries(hqlQuery, expected);
    } finally {
      getStorageToUpdatePeriodMap().clear();
    }
  }

  @Test
  public void testConvertDimFilterToFactFilterForMultiFact() throws Exception {
    Configuration conf = getConf();
    conf.set(getValidStorageTablesKey("testfact"), "C1_testFact,C2_testFact");
    conf.set(getValidUpdatePeriodsKey("testfact", "C1"), "DAILY,HOURLY");
    conf.set(getValidUpdatePeriodsKey("testfact2", "C1"), "YEARLY");
    conf.set(getValidUpdatePeriodsKey("testfact", "C2"), "MONTHLY,DAILY");
    conf.setBoolean(REWRITE_DIM_FILTER_TO_FACT_FILTER, true);
    try {
      getStorageToUpdatePeriodMap().put("c1_testfact", Lists.newArrayList(HOURLY, DAILY));
      getStorageToUpdatePeriodMap().put("c2_testfact", Lists.newArrayList(MONTHLY));

      String hqlQuery = rewrite("select asciicity as `City Name`, msr8, msr7 as `Third measure` "
          + "from testCube where asciicity = 'c' and cityname = 'a' and zipcode = 'b' and "
          + TWO_MONTHS_RANGE_UPTO_HOURS, conf);

      String filter1 = "testcube.cityid in ( select id from TestQueryRewrite.c1_citytable cubecity "
          + "where (ascii((cubecity.name)) = 'c') and (cubecity.dt = 'latest') )";
      String filter2 = "testcube.cityid in ( select id from TestQueryRewrite.c1_citytable cubecity "
          + "where ((cubecity.name) = 'a') and (cubecity.dt = 'latest') )";

      assertTrue(hqlQuery.contains(filter1));
      assertTrue(hqlQuery.contains(filter2));

    } finally {
      getStorageToUpdatePeriodMap().clear();
    }
  }

  @Test
  public void testSingleFactMultiStorage() throws Exception {
    Configuration conf = LensServerAPITestUtil.getConfigurationWithParams(getConf(),
      CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C3,C5",
      getValidFactTablesKey("testcube"), "testfact",
      getValidUpdatePeriodsKey("testfact", "C3"), "DAILY",
      getValidUpdatePeriodsKey("testfact", "C5"), "DAILY",
      FAIL_QUERY_ON_PARTIAL_DATA, false);

    String hqlQuery = rewrite("select count(msr4) from testCube where " + TWO_MONTHS_RANGE_UPTO_DAYS, conf);
    System.out.println(hqlQuery);

    // No time_range_in should be remaining
    assertFalse(hqlQuery.contains("time_range_in"));
    ArrayList<String> storages = Lists.newArrayList("c3_testfact", "c5_testfact");
    StoragePartitionProvider provider = new StoragePartitionProvider() {
      @Override
      public Map<String, String> providePartitionsForStorage(String storage) {
        if (storage.contains("c3")) {
          return getWhereForDays(storage, TWO_MONTHS_BACK, getDateWithOffset(DAILY, -10));
        } else if (storage.contains("c5")) {
          return getWhereForDays(storage, getDateWithOffset(DAILY, -10), NOW);
        }
        return null;
      }
    };
    String expected = getExpectedUnionQuery(TEST_CUBE_NAME, storages, provider,
      "select count(testcube.alias0)", null, null,
      "select count(testcube.msr4) as `alias0` from ", null, null
    );
    compareQueries(hqlQuery, expected);
  }
}

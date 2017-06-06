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
import java.util.stream.Collectors;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.error.NoCandidateFactAvailableException;
import org.apache.lens.server.api.LensServerAPITestUtil;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;

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
    Configuration conf = LensServerAPITestUtil.getConfigurationWithParams(getConf(),
        CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C6",
        getValidFactTablesKey("basecube"), "testfact",
        FAIL_QUERY_ON_PARTIAL_DATA, false);
    ArrayList<String> storages = Lists.newArrayList("daily_c6_testfact", "monthly_c6_testfact");
    try {
      // Union query
      String hqlQuery;
      String expected;
      StoragePartitionProvider provider = new StoragePartitionProvider() {
        @Override
        public Map<String, String> providePartitionsForStorage(String storage) {
          if (storage.contains("daily_c6_testfact")) {
            return getWhereForDays(storage, ONE_MONTH_BACK_TRUNCATED, getTruncatedDateWithOffset(MONTHLY, 0));
          } else if (storage.contains("monthly_c6_testfact")) {
            return getWhereForMonthly(storage, THREE_MONTHS_BACK_TRUNCATED, ONE_MONTH_BACK_TRUNCATED);
          }
          return null;
        }
      };
      hqlQuery = rewrite("select cityname1 as `City Name`, msr8, msr7 as `Third measure` "
          + "from testCube where cityname1 = 'a' and zipcode = 'b' and "
          + THREE_MONTHS_RANGE_UPTO_MONTH, conf);
      expected = getExpectedUnionQuery(TEST_CUBE_NAME, storages, provider,
          "SELECT (testcube.alias0) as `City Name`, (sum((testcube.alias1)) + max((testcube.alias2))) "
              + "as `msr8`, case  when (sum((testcube.alias1)) = 0) then 0 else (sum((testcube.alias4)) / "
              + "sum((testcube.alias1))) end as `Third measure` ",
          null, "group by testcube.alias0",
          "SELECT (cubecity1.name) as `alias0`, sum((testcube.msr2)) as `alias1`, max((testcube.msr3)) "
              + "as `alias2`, sum(case  when ((testcube.cityid) = 'x') then (testcube.msr21) "
              + "else (testcube.msr22) end) as `alias4` ", " join "
              + getDbName() + "c6_citytable cubecity1 on testcube.cityid1 = cubecity1.id "
              + "and (cubecity1.dt = 'latest') ",
          "((cubecity1.name) = 'a') and ((testcube.zipcode) = 'b')",
          "group by (cubecity1.name)");
      compareQueries(hqlQuery, expected);

      hqlQuery = rewrite("select ascii(cityid) as `City ID`, msr8, msr7 as `Third measure` "
          + "from testCube where ascii(cityid) = 'c' and cityid = 'a' and zipcode = 'b' and "
          + THREE_MONTHS_RANGE_UPTO_MONTH, conf);

      expected = getExpectedUnionQuery(TEST_CUBE_NAME, storages, provider,
          "SELECT (testcube.alias0) as `City ID`, (sum((testcube.alias1)) + max((testcube.alias2))) as `msr8`, "
              + "case  when (sum((testcube.alias1)) = 0) then 0 else (sum((testcube.alias4)) / sum((testcube.alias1))) "
              + "end as `Third measure`",
          null, "group by testcube.alias0",
          "SELECT ascii((testcube.cityid)) as `alias0`, sum((testcube.msr2)) as `alias1`, max((testcube.msr3)) "
              + "as `alias2`, sum(case  when ((testcube.cityid) = 'x') then (testcube.msr21) "
              + "else (testcube.msr22) end) as `alias4`",
          "ascii(testcube.cityid) = 'c' and testcube.cityid = 'a' and testcube.zipcode = 'b'",
          "group by ascii(testcube.cityid)");

      compareQueries(hqlQuery, expected);

      hqlQuery = rewrite("select cityid as `City ID`, msr8, msr7 as `Third measure` "
          + "from testCube where cityid = 'a' and zipcode = 'b' and " + THREE_MONTHS_RANGE_UPTO_MONTH, conf);

      expected = getExpectedUnionQuery(TEST_CUBE_NAME, storages, provider,
          "SELECT (testcube.alias0) as `City ID`, (sum((testcube.alias1)) + max((testcube.alias2))) as `msr8`, "
              + "case  when (sum((testcube.alias1)) = 0) then 0 else (sum((testcube.alias4)) / sum((testcube.alias1)))"
              + " end as `Third measure`",
          null, "group by testcube.alias0",
          "SELECT (testcube.cityid) as `alias0`, sum((testcube.msr2)) as `alias1`, max((testcube.msr3)) as `alias2`, "
              + "sum(case  when ((testcube.cityid) = 'x') then (testcube.msr21) else (testcube.msr22) end) as `alias4`",
          "testcube.cityid = 'a' and testcube.zipcode = 'b'", "group by testcube.cityid");

      compareQueries(hqlQuery, expected);

      hqlQuery = rewrite("select cityid as `City ID`, msr3 as `Third measure` from testCube where "
          + THREE_MONTHS_RANGE_UPTO_MONTH + " having msr7 > 10", conf);

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
          + THREE_MONTHS_RANGE_UPTO_MONTH + " having msr8 > 10", conf);

      expected = getExpectedUnionQuery(TEST_CUBE_NAME, storages, provider,
          "SELECT testcube.alias0 as `City ID`, max(testcube.alias1) as `Third measure`",
          null, "GROUP BY testcube.alias0 "
              + "HAVING (sum(testcube.alias2) + max(testcube.alias1)) > 10 ",
          "SELECT testcube.cityid as `alias0`, max(testcube.msr3) as `alias1`, "
              + "sum(testcube.msr2)as `alias2`", null, "group by testcube.cityid");
      compareQueries(hqlQuery, expected);

      hqlQuery = rewrite("select msr3 as `Measure 3` from testCube where "
          + THREE_MONTHS_RANGE_UPTO_MONTH + " having msr2 > 10 and msr2 < 100", conf);

      expected = getExpectedUnionQuery(TEST_CUBE_NAME, storages, provider,
          "SELECT max(testcube.alias0) as `Measure 3` ",
          null, " HAVING sum(testcube.alias1) > 10 and sum(testcube.alias1) < 100",
          "SELECT max(testcube.msr3) as `alias0`, sum(testcube.msr2) as `alias1`", null, null);
      compareQueries(hqlQuery, expected);

      hqlQuery = rewrite("select zipcode, cityid as `CityID`, msr3 as `Measure 3`, msr4, "
          + "SUM(msr2) as `Measure 2` from testCube where "
          + THREE_MONTHS_RANGE_UPTO_MONTH + " having msr4 > 10 order by cityid desc limit 5", conf);

      expected = getExpectedUnionQuery(TEST_CUBE_NAME, storages, provider,
          "SELECT (testcube.alias0) as `zipcode`, (testcube.alias1) as `City ID`, max((testcube.alias2)) "
              + "as `Measure 3`, count((testcube.alias3)) as `msr4`, sum((testcube.alias4)) as `Measure 2`",
          null, "group by testcube.alias0, testcube.alias1 "
              + " having count(testcube.alias3) > 10 order by cityid desc limit 5",
          "SELECT (testcube.zipcode) as `alias0`, (testcube.cityid) as `alias1`, max((testcube.msr3)) as `alias2`, "
              + "count((testcube.msr4)) as `alias3`, sum((testcube.msr2)) as `alias4`",
          null, "group by testcube.zipcode, testcube.cityid ");
      compareQueries(hqlQuery, expected);

      // Order by column with whitespace in alias should fail
      try {
        hqlQuery = rewrite("select zipcode, cityid as `City ID`, msr3 as `Measure 3`, msr4, "
            + "SUM(msr2) as `Measure 2` from testCube where "
            + THREE_MONTHS_RANGE_UPTO_MONTH + " having msr4 > 10 order by cityid desc limit 5", conf);
      } catch (LensException e) {
        assertEquals(e.getErrorCode(),
            LensCubeErrorCode.ORDERBY_ALIAS_CONTAINING_WHITESPACE.getLensErrorInfo().getErrorCode());
      }

    } finally {
      getStorageToUpdatePeriodMap().clear();
    }
  }
  @Test
  public void testDimAttrExpressionQuery() throws Exception {
    Configuration conf = LensServerAPITestUtil.getConfigurationWithParams(getConf(),
        CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C6",
        getValidFactTablesKey("testcube"), "testfact",
        FAIL_QUERY_ON_PARTIAL_DATA, false);
    ArrayList<String> storages = Lists.newArrayList("daily_c6_testfact", "monthly_c6_testfact");

    StoragePartitionProvider provider = new StoragePartitionProvider() {
      @Override
      public Map<String, String> providePartitionsForStorage(String storage) {
        if (storage.contains("daily_c6_testfact")) {
          return getWhereForDays(storage, ONE_MONTH_BACK_TRUNCATED, getTruncatedDateWithOffset(MONTHLY, 0));
        } else if (storage.contains("monthly_c6_testfact")) {
          return getWhereForMonthly(storage, THREE_MONTHS_BACK_TRUNCATED, ONE_MONTH_BACK_TRUNCATED);
        }
        return null;
      }
    };
    // exception in following line
    String hqlQuery = rewrite("select asciicity as `City Name`, cityAndState as citystate, isIndia as isIndia,"
        + " msr8, msr7 as `Third measure` "
        + "from testCube where asciicity = 'c' and cityname = 'a' and zipcode = 'b' and "
        + THREE_MONTHS_RANGE_UPTO_MONTH, conf);
    String joinExpr1 =  " join "
        + getDbName() + "c6_statetable cubestate on testcube.stateid = cubestate.id and (cubestate.dt = 'latest') join"
        + getDbName() + "c6_citytable cubecity on testcube.cityid = cubecity.id and (cubecity.dt = 'latest')";
    String joinExpr2 =  " join "
        + getDbName() + "c6_citytable cubecity on testcube.cityid = cubecity.id and (cubecity.dt = 'latest') join"
        + getDbName() + "c6_statetable cubestate on testcube.stateid = cubestate.id and (cubestate.dt = 'latest')";

    String expected1 = getExpectedQueryForDimAttrExpressionQuery(joinExpr1);
    String expected2 = getExpectedQueryForDimAttrExpressionQuery(joinExpr2);
    assertTrue(new TestQuery(hqlQuery).equals(new TestQuery(expected1))
            || new TestQuery(hqlQuery).equals(new TestQuery(expected2)),
        "Actual :" + hqlQuery + " Expected1:" + expected1 + " Expected2 : "+ expected2);
  }

  private String getExpectedQueryForDimAttrExpressionQuery(String joinExpr) {
    try {
      ArrayList<String> storages = Lists.newArrayList("daily_c6_testfact", "monthly_c6_testfact");

      StoragePartitionProvider provider = new StoragePartitionProvider() {
        @Override
        public Map<String, String> providePartitionsForStorage(String storage) {
          if (storage.contains("daily_c6_testfact")) {
            return getWhereForDays(storage, ONE_MONTH_BACK_TRUNCATED, getTruncatedDateWithOffset(MONTHLY, 0));
          } else if (storage.contains("monthly_c6_testfact")) {
            return getWhereForMonthly(storage, THREE_MONTHS_BACK_TRUNCATED, ONE_MONTH_BACK_TRUNCATED);
          }
          return null;
        }
      };
      return getExpectedUnionQuery(TEST_CUBE_NAME, storages, provider,
          "SELECT (testcube.alias0) as `City Name`, (testcube.alias1) as `citystate`, (testcube.alias2) as `isIndia`, "
              + "(sum((testcube.alias3)) + max((testcube.alias4))) as `msr8`, case  when (sum((testcube.alias3)) = 0) "
              + "then 0 else (sum((testcube.alias6)) / sum((testcube.alias3))) end as `Third measure` ",
          null, " group by testcube.alias0, testcube.alias1, testcube.alias2",
          "SELECT ascii((cubecity.name)) as `alias0`, concat((cubecity.name), \":\", (cubestate.name)) as `alias1`, "
              + "(((cubecity.name) == 'DELHI') or ((cubestate.name) == 'KARNATAKA') or ((cubestate.name) "
              + "== 'MAHARASHTRA')) as `alias2`, sum((testcube.msr2)) as `alias3`, max((testcube.msr3)) as `alias4`, "
              + "sum(case  when ((testcube.cityid) = 'x') then (testcube.msr21) else (testcube.msr22) end) "
              + "as `alias6` ", joinExpr,
          "ascii(cubecity.name) = 'c' and cubecity.name = 'a' and testcube.zipcode = 'b'",
          " GROUP BY ascii((cubecity.name)), concat((cubecity.name), \":\", (cubestate.name)), "
              + "(((cubecity.name) == 'DELHI') or ((cubestate.name) == 'KARNATAKA') "
              + "or ((cubestate.name) == 'MAHARASHTRA'))");
    } finally {
      getStorageToUpdatePeriodMap().clear();
    }
  }
  @Test
  public void testNonAggregateOverAggregateFunction() throws Exception {
    try {
      Configuration conf = LensServerAPITestUtil.getConfigurationWithParams(getConf(),
          CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C6",
          getValidFactTablesKey("testcube"), "testfact",
          FAIL_QUERY_ON_PARTIAL_DATA, false);
      ArrayList<String> storages = Lists.newArrayList("daily_c6_testfact", "monthly_c6_testfact");

      StoragePartitionProvider provider = new StoragePartitionProvider() {
        @Override
        public Map<String, String> providePartitionsForStorage(String storage) {
          if (storage.contains("daily_c6_testfact")) {
            return getWhereForDays(storage, ONE_MONTH_BACK_TRUNCATED, getTruncatedDateWithOffset(MONTHLY, 0));
          } else if (storage.contains("monthly_c6_testfact")) {
            return getWhereForMonthly(storage, THREE_MONTHS_BACK_TRUNCATED, ONE_MONTH_BACK_TRUNCATED);
          }
          return null;
        }
      };

      String hqlQuery = rewrite("select cityid as `CityID`, msr3 as `Measure 3`, "
          + "round(SUM(msr2)) as `Measure 2` from testCube" + " where "
          + THREE_MONTHS_RANGE_UPTO_MONTH + " group by cityid having msr3 > 10 order by cityid desc limit 5", conf);
      String expected = getExpectedUnionQuery(TEST_CUBE_NAME, storages, provider,
          "SELECT (testcube.alias0) as `City ID`, max((testcube.alias1)) as `Measure 3`, round(sum((testcube.alias2))) "
              + "as `Measure 2` ", null, "GROUP BY (testcube.alias0) HAVING (max((testcube.alias1)) > 10) "
              + "ORDER BY cityid desc LIMIT 5",
          "SELECT (testcube.cityid) as `alias0`, max((testcube.msr3)) as `alias1`, "
              + "sum((testcube.msr2)) as `alias2` FROM ",
          null, "GROUP BY testcube.cityid");
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
        CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C6",
        getValidFactTablesKey("testcube"), "testfact",
        FAIL_QUERY_ON_PARTIAL_DATA, false);

    String hqlQuery = rewrite("select count(msr4) from testCube where " + THREE_MONTHS_RANGE_UPTO_MONTH, conf);
    System.out.println(hqlQuery);

    // No time_range_in should be remaining
    assertFalse(hqlQuery.contains("time_range_in"));
    ArrayList<String> storages = Lists.newArrayList("daily_c6_testfact", "monthly_c6_testfact");
    StoragePartitionProvider provider = new StoragePartitionProvider() {
      @Override
      public Map<String, String> providePartitionsForStorage(String storage) {
        if (storage.contains("daily_c6_testfact")) {
          return getWhereForDays(storage, ONE_MONTH_BACK_TRUNCATED, getTruncatedDateWithOffset(MONTHLY, 0));
        } else if (storage.contains("monthly_c6_testfact")) {
          return getWhereForMonthly(storage, THREE_MONTHS_BACK_TRUNCATED, ONE_MONTH_BACK_TRUNCATED);
        }
        return null;
      }
    };
    String expected = getExpectedUnionQuery(TEST_CUBE_NAME, storages, provider,
        "SELECT count((testcube.alias0)) as `count(msr4)`", null, null,
        "select count(testcube.msr4) as `alias0` from ", null, null
    );
    compareQueries(hqlQuery, expected);
  }


  @Test
  public void testSingleFactSingleStorageWithMultipleTableDescriptions() throws Exception {
    Configuration conf = LensServerAPITestUtil.getConfigurationWithParams(getConf(),
        CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C6",
        getValidFactTablesKey("testcube"), "testfact",
        FAIL_QUERY_ON_PARTIAL_DATA, false);

    //If not beginning of month. Expecting this to pass at beginning of every month (example April 01 00:00)
    if (Calendar.getInstance().get(Calendar.DAY_OF_MONTH) != 1) {
      NoCandidateFactAvailableException e = getLensExceptionInRewrite("select count(msr4) from testCube where "
          + THREE_MONTHS_RANGE_UPTO_DAYS, conf);
      Set<Map.Entry<Candidate, List<CandidateTablePruneCause>>> causes =
        e.getBriefAndDetailedError().entrySet().stream().filter(x ->
          x.getKey() instanceof StorageCandidate
            && ((StorageCandidate)x.getKey()).getStorageTable().equalsIgnoreCase("c6_testfact"))
          .collect(Collectors.toSet());
      assertEquals(causes.size(), 1);
      List<CandidateTablePruneCause> pruneCauses = causes.iterator().next().getValue();
      assertEquals(pruneCauses.size(), 1);
      assertEquals(pruneCauses.get(0).getCause(), CandidateTablePruneCause.
          CandidateTablePruneCode.STORAGE_NOT_AVAILABLE_IN_RANGE);
    }

    String hqlQuery2 = rewrite("select count(msr4) from testCube where " + THREE_MONTHS_RANGE_UPTO_MONTH, conf);
    System.out.println(hqlQuery2);

    ArrayList<String> storages = Lists.newArrayList("daily_c6_testfact", "monthly_c6_testfact");
    StoragePartitionProvider provider = new StoragePartitionProvider() {
      @Override
      public Map<String, String> providePartitionsForStorage(String storage) {
        if (storage.contains("daily_c6_testfact")) {
          return getWhereForDays(storage, ONE_MONTH_BACK_TRUNCATED, getTruncatedDateWithOffset(MONTHLY, 0));
        } else if (storage.contains("monthly_c6_testfact")) {
          return getWhereForMonthly(storage, THREE_MONTHS_BACK_TRUNCATED, ONE_MONTH_BACK_TRUNCATED);
        }
        return null;
      }
    };
    String expected = getExpectedUnionQuery(TEST_CUBE_NAME, storages, provider,
        "select count(testcube.alias0) AS `count(msr4)`", null, null,
        "select count((testcube.msr4)) AS `alias0` from ", null, null
    );
    compareQueries(hqlQuery2, expected);
  }
}

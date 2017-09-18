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

import static org.apache.lens.cube.metadata.DateFactory.TWO_DAYS_RANGE;
import static org.apache.lens.cube.parse.CubeTestSetup.*;

import static org.testng.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestBridgeTableQueries extends TestQueryRewrite {

  private static HiveConf hConf = new HiveConf(TestBridgeTableQueries.class);

  @BeforeTest
  public void setupInstance() throws Exception {
    hConf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    hConf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
    hConf.setBoolean(CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT, true);
    hConf.setBoolean(CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY, true);
    hConf.setBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER, false);
    hConf.setBoolean(CubeQueryConfUtil.ENABLE_FLATTENING_FOR_BRIDGETABLES, true);
  }

  @Test
  public void testBridgeTablesWithoutDimtablePartitioning() throws Exception {
    String query = "select usersports.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, hConf);
    String expected = getExpectedQuery("basecube", "SELECT (usersports.balias0) as `name`, sum((basecube.msr2)) "
        + "as `sum(msr2)` FROM ",
      " join " + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as balias0"
        + " from " + getDbName() + "c1_user_interests_tbl user_interests"
        + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports"
        + " on userdim.id = usersports.user_id ",
      null, "group by usersports.balias0", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sports, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    expected = getExpectedQuery("basecube", "SELECT (usersports.balias0) as `sports`, sum((basecube.msr2)) "
            + "as `sum(msr2)` FROM ",
        " join " + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
            + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as balias0"
            + " from " + getDbName() + "c1_user_interests_tbl user_interests"
            + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
            + " group by user_interests.user_id) usersports"
            + " on userdim.id = usersports.user_id ",
        null, "group by usersports.balias0", null,
        getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    hqlQuery = rewrite(query, hConf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testBridgeTablesForExprFieldWithoutDimtablePartitioning() throws Exception {
    String query = "select substr(usersports.name, 10), sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, hConf);
    String expected = getExpectedQuery("basecube", "SELECT (usersports.balias0) as `substr((usersports.name), 10)`, "
        + "sum((basecube.msr2)) as `sum(msr2)` FROM", " join "
        + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(substr(usersports.name, 10)) as balias0"
        + " from " + getDbName() + "c1_user_interests_tbl user_interests"
        + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports"
        + " on userdim.id = usersports.user_id ",
      null, "group by usersports.balias0", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select substrsprorts, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hConf);
    expected = getExpectedQuery("basecube", "SELECT (usersports.balias0) as `substrsprorts`, "
            + "sum((basecube.msr2)) as `sum(msr2)` FROM", " join "
            + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
            + " join (select user_interests.user_id as user_id,collect_set(substr(usersports.name, 10)) as balias0"
            + " from " + getDbName() + "c1_user_interests_tbl user_interests"
            + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
            + " group by user_interests.user_id) usersports"
            + " on userdim.id = usersports.user_id ",
        null, "group by usersports.balias0", null,
        getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testFlattenBridgeTablesOFF() throws Exception {
    Configuration conf = new Configuration(hConf);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_FLATTENING_FOR_BRIDGETABLES, false);
    String query = "select usersports.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, conf);
    String expected = getExpectedQuery("basecube", "SELECT (usersports.name) as `name`, sum((basecube.msr2)) "
        + "as `sum(msr2)` FROM ", " join " + getDbName()
        + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join " + getDbName() + "c1_user_interests_tbl user_interests on userdim.id = user_interests.user_id"
        + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id",
      null, "group by usersports.name", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sports, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    expected = getExpectedQuery("basecube", "SELECT (usersports.name) as `sports`, sum((basecube.msr2)) "
            + "as `sum(msr2)` FROM ", " join " + getDbName()
            + "c1_usertable userdim ON basecube.userid = userdim.id "
            + " join " + getDbName() + "c1_user_interests_tbl user_interests on userdim.id = user_interests.user_id"
            + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id",
        null, "group by usersports.name", null,
        getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    hqlQuery = rewrite(query, conf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testFlattenBridgeTablesWithCustomAggregate() throws Exception {
    Configuration conf = new Configuration(hConf);
    conf.set(CubeQueryConfUtil.BRIDGE_TABLE_FIELD_AGGREGATOR, "custom_aggr");
    String query = "select usersports.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, conf);
    String expected = getExpectedQuery("basecube", "SELECT (usersports.balias0) as `name`, sum((basecube.msr2)) "
        + "as `sum(msr2)` FROM ", " join "
        + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,custom_aggr(usersports.name) as balias0"
        + " from " + getDbName() + "c1_user_interests_tbl user_interests"
        + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports"
        + " on userdim.id = usersports.user_id ",
      null, "group by usersports.balias0", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sports, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    expected = getExpectedQuery("basecube", "SELECT (usersports.balias0) as `sports`, sum((basecube.msr2)) "
            + "as `sum(msr2)` FROM ", " join "
            + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
            + " join (select user_interests.user_id as user_id,custom_aggr(usersports.name) as balias0"
            + " from " + getDbName() + "c1_user_interests_tbl user_interests"
            + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
            + " group by user_interests.user_id) usersports"
            + " on userdim.id = usersports.user_id ",
        null, "group by usersports.balias0", null,
        getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    hqlQuery = rewrite(query, conf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testBridgeTablesWithMegringChains() throws Exception {
    String query = "select userInterestIds.sport_id, usersports.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, hConf);
    String expected = getExpectedQuery("basecube", "SELECT (userinterestids.balias0) as `sport_id`, "
        + "(usersports.balias0) as `name`, sum((basecube.msr2)) as `sum(msr2)` FROM ",
      " join " + getDbName() + "c1_usertable userdim on basecube.userid = userdim.id join (select userinterestids"
        + ".user_id as user_id,collect_set(userinterestids.sport_id) as balias0 from " + getDbName()
        + "c1_user_interests_tbl userinterestids group by userinterestids.user_id) userinterestids on userdim.id = "
        + "userinterestids.user_id "
        + "join (select userinterestids.user_id as user_id,collect_set(usersports.name) as balias0 from "
        + getDbName() + "c1_user_interests_tbl userinterestids join "
        + getDbName() + "c1_sports_tbl usersports on userinterestids.sport_id = usersports.id"
        + " group by userinterestids.user_id) usersports on userdim.id = usersports.user_id",
       null, "group by userinterestids.balias0, usersports.balias0", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sportids, sports, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    expected = getExpectedQuery("basecube", "SELECT (userinterestids.balias0) as `sportids`, "
            + "(usersports.balias0) as `sports`, sum((basecube.msr2)) as `sum(msr2)` FROM ",
        " join " + getDbName() + "c1_usertable userdim on basecube.userid = userdim.id join (select userinterestids"
            + ".user_id as user_id,collect_set(userinterestids.sport_id) as balias0 from " + getDbName()
            + "c1_user_interests_tbl userinterestids group by userinterestids.user_id) userinterestids on userdim.id = "
            + "userinterestids.user_id "
            + "join (select userinterestids.user_id as user_id,collect_set(usersports.name) as balias0 from "
            + getDbName() + "c1_user_interests_tbl userinterestids join "
            + getDbName() + "c1_sports_tbl usersports on userinterestids.sport_id = usersports.id"
            + " group by userinterestids.user_id) usersports on userdim.id = usersports.user_id",
        null, "group by userinterestids.balias0, usersports.balias0", null,
        getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    hqlQuery = rewrite(query, hConf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testBridgeTablesWithMultipleFacts() throws Exception {
    String query = "select usersports.name, msr2, msr12 from basecube where " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, hConf);
    String expected1 = getExpectedQuery("basecube",
        "SELECT (usersports.balias0) as `alias0`, sum((basecube.msr2)) as `alias1`, 0 as `alias2` FROM ",
        " join " + getDbName()
            + "c1_usertable userdim ON basecube.userid = userdim.id "
            + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as balias0" + " from "
            + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
            + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
            + " group by user_interests.user_id) usersports" + " on userdim.id = usersports.user_id ", null,
        "group by usersports.balias0", null, getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    String expected2 = getExpectedQuery("basecube",
        "SELECT (usersports.balias0) as `alias0`, 0 as `alias1`, sum((basecube.msr12)) as `alias2` FROM ",
        " join " + getDbName()
            + "c1_usertable userdim ON basecube.userid = userdim.id "
            + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as balias0" + " from "
            + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
            + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
            + " group by user_interests.user_id) usersports" + " on userdim.id = usersports.user_id ", null,
        "group by usersports.balias0", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact2_base"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    String lower = hqlQuery.toLowerCase();
    assertTrue(
      lower.startsWith("select (basecube.alias0) as `name`, sum((basecube.alias1)) as `msr2`, "
          + "sum((basecube.alias2)) as `msr12` from"), hqlQuery);

    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("GROUP BY (basecube.alias0)"),
      hqlQuery);

    // run with chain ref column
    query = "select sports, msr2, msr12 from basecube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hConf);
    expected1 = getExpectedQuery("basecube",
      "SELECT (usersports.balias0) as `alias0`, sum((basecube.msr2)) as `alias1`, 0 as `alias2` FROM ",
        " join " + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as balias0" + " from "
        + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
        + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports" + " on userdim.id = usersports.user_id ", null,
      "group by usersports.balias0", null, getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    expected2 = getExpectedQuery("basecube",
      "SELECT (usersports.balias0) as `alias0`, 0 as `alias1`, sum((basecube.msr12)) "
          + "as `alias2` FROM ", " join " + getDbName()
        + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as balias0" + " from "
        + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
        + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports" + " on userdim.id = usersports.user_id ", null,
      "group by usersports.balias0", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact2_base"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    lower = hqlQuery.toLowerCase();
    assertTrue(
      lower.startsWith("select (basecube.alias0) as `sports`, sum((basecube.alias1)) as `msr2`, "
          + "sum((basecube.alias2)) as `msr12` from"), hqlQuery);

    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("GROUP BY (basecube.alias0)"),
      hqlQuery);
  }

  @Test
  public void testBridgeTablesWithMultipleChains() throws Exception {
    String query = "select usersports.name, xusersports.name, yusersports.name, sum(msr2) from basecube where "
      + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, hConf);
    String expected = getExpectedQuery("basecube", "SELECT (usersports.balias0) as `name`, (xusersports.balias0) "
        + "as `name`, (yusersports.balias0) as `name`, sum((basecube.msr2)) as `sum(msr2)` FROM ",
      " join " + getDbName() + "c1_usertable userdim_1 on basecube.userid = userdim_1.id "
      + " join  (select user_interests_1.user_id as user_id, collect_set(usersports.name) as balias0 from "
      + getDbName() + "c1_user_interests_tbl user_interests_1 join " + getDbName() + "c1_sports_tbl usersports on "
      + "user_interests_1.sport_id = usersports.id group by user_interests_1.user_id) "
      + "usersports on userdim_1.id = usersports.user_id"
      + " join " + getDbName() + "c1_usertable userdim_0 on basecube.yuserid = userdim_0.id "
      + " join  (select user_interests_0.user_id as user_id,collect_set(yusersports.name) as balias0 from "
      + getDbName() + "c1_user_interests_tbl user_interests_0 join " + getDbName() + "c1_sports_tbl yusersports on "
      + " user_interests_0.sport_id = yusersports.id group by user_interests_0.user_id) yusersports on userdim_0.id ="
      + " yusersports.user_id join " + getDbName() + "c1_usertable userdim on basecube.xuserid = userdim.id"
      + " join  (select user_interests.user_id as user_id,collect_set(xusersports.name) as balias0 from "
      + getDbName() + "c1_user_interests_tbl user_interests join " + getDbName() + "c1_sports_tbl xusersports"
      + " on user_interests.sport_id = xusersports.id group by user_interests.user_id) xusersports on userdim.id = "
      + " xusersports.user_id", null, "group by usersports.balias0, xusersports.balias0, yusersports.balias0", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sports, xsports, ysports, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    expected = getExpectedQuery("basecube", "SELECT (usersports.balias0) as `sports`, (xusersports.balias0) "
            + "as `xsports`, (yusersports.balias0) as `ysports`, sum((basecube.msr2)) as `sum(msr2)` FROM ",
        " join " + getDbName() + "c1_usertable userdim_1 on basecube.userid = userdim_1.id "
            + " join  (select user_interests_1.user_id as user_id, collect_set(usersports.name) as balias0 from "
            + getDbName() + "c1_user_interests_tbl user_interests_1 join " + getDbName()
            + "c1_sports_tbl usersports on "
            + "user_interests_1.sport_id = usersports.id group by user_interests_1.user_id) "
            + "usersports on userdim_1.id = usersports.user_id"
            + " join " + getDbName() + "c1_usertable userdim_0 on basecube.yuserid = userdim_0.id "
            + " join  (select user_interests_0.user_id as user_id,collect_set(yusersports.name) as balias0 from "
            + getDbName() + "c1_user_interests_tbl user_interests_0 join " + getDbName()
            + "c1_sports_tbl yusersports on  user_interests_0.sport_id = yusersports.id group by "
            + "user_interests_0.user_id) yusersports on userdim_0.id ="
            + " yusersports.user_id join " + getDbName() + "c1_usertable userdim on basecube.xuserid = userdim.id"
            + " join  (select user_interests.user_id as user_id,collect_set(xusersports.name) as balias0 from "
            + getDbName() + "c1_user_interests_tbl user_interests join " + getDbName()
            + "c1_sports_tbl xusersports on user_interests.sport_id = xusersports.id "
            + "group by user_interests.user_id) xusersports on userdim.id = "
            + " xusersports.user_id",
        null, "group by usersports.balias0, xusersports.balias0, yusersports.balias0", null,
        getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    hqlQuery = rewrite(query, hConf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testBridgeTablesWithMultipleChainsWithJoinType() throws Exception {
    Configuration conf = new Configuration(hConf);
    conf.set(CubeQueryConfUtil.JOIN_TYPE_KEY, "LEFTOUTER");
    String query = "select usersports.name, xusersports.name, yusersports.name, sum(msr2) from basecube where "
      + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, conf);
    String expected = getExpectedQuery("basecube", "SELECT (usersports.balias0) as `name`, (xusersports.balias0) "
        + "as `name`, (yusersports.balias0) as `name`, sum((basecube.msr2)) as `sum(msr2)` FROM ",
      " left outer join " + getDbName() + "c1_usertable userdim_1 on basecube.userid = userdim_1.id "
      + " left outer join  (select user_interests_1.user_id as user_id, collect_set(usersports.name) as balias0 from "
      + getDbName() + "c1_user_interests_tbl user_interests_1 join " + getDbName() + "c1_sports_tbl usersports on "
      + "user_interests_1.sport_id = usersports.id group by user_interests_1.user_id) "
      + "usersports on userdim_1.id = usersports.user_id"
      + " left outer join " + getDbName() + "c1_usertable userdim_0 on basecube.yuserid = userdim_0.id "
      + " left outer join  (select user_interests_0.user_id as user_id,collect_set(yusersports.name) as balias0 from "
      + getDbName() + "c1_user_interests_tbl user_interests_0 join " + getDbName() + "c1_sports_tbl yusersports on "
      + " user_interests_0.sport_id = yusersports.id group by user_interests_0.user_id) yusersports on userdim_0.id ="
      + " yusersports.user_id left outer join " + getDbName()
      + "c1_usertable userdim on basecube.xuserid = userdim.id"
      + " left outer join  (select user_interests.user_id as user_id,collect_set(xusersports.name) as balias0 from "
      + getDbName() + "c1_user_interests_tbl user_interests join " + getDbName() + "c1_sports_tbl xusersports"
      + " on user_interests.sport_id = xusersports.id group by user_interests.user_id) xusersports on userdim.id = "
      + " xusersports.user_id", null, "group by usersports.balias0, xusersports.balias0, yusersports.balias0", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sports, xsports, ysports, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    expected = getExpectedQuery("basecube", "SELECT (usersports.balias0) as `sports`, (xusersports.balias0) "
            + "as `xsports`, (yusersports.balias0) as `ysports`, sum((basecube.msr2)) as `sum(msr2)` FROM ",
        " left outer join " + getDbName() + "c1_usertable userdim_1 on basecube.userid = userdim_1.id "
            + " left outer join  (select user_interests_1.user_id as user_id, "
            + "collect_set(usersports.name) as balias0 from "
            + getDbName() + "c1_user_interests_tbl user_interests_1 join "
            + getDbName() + "c1_sports_tbl usersports on "
            + "user_interests_1.sport_id = usersports.id group by user_interests_1.user_id) "
            + "usersports on userdim_1.id = usersports.user_id"
            + " left outer join " + getDbName() + "c1_usertable userdim_0 on basecube.yuserid = userdim_0.id "
            + " left outer join  (select user_interests_0.user_id as user_id,"
            + "collect_set(yusersports.name) as balias0 from "
            + getDbName() + "c1_user_interests_tbl user_interests_0 join " + getDbName()
            + "c1_sports_tbl yusersports on "
            + " user_interests_0.sport_id = yusersports.id group by user_interests_0.user_id) "
            + "yusersports on userdim_0.id = yusersports.user_id left outer join " + getDbName()
            + "c1_usertable userdim on basecube.xuserid = userdim.id"
            + " left outer join  (select user_interests.user_id as user_id,"
            + "collect_set(xusersports.name) as balias0 from " + getDbName()
            + "c1_user_interests_tbl user_interests join " + getDbName() + "c1_sports_tbl xusersports"
            + " on user_interests.sport_id = xusersports.id group by user_interests.user_id) "
            + "xusersports on userdim.id =  xusersports.user_id", null,
        "group by usersports.balias0, xusersports.balias0, yusersports.balias0", null,
        getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    hqlQuery = rewrite(query, conf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testBridgeTablesWithDimTablePartitioning() throws Exception {
    Configuration conf = new Configuration(hConf);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    String query = "select usersports.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, conf);
    String expected = getExpectedQuery("basecube", "SELECT (usersports.balias0) as `name`, sum((basecube.msr2)) "
        + "as `sum(msr2)` FROM ", " join " + getDbName()
        + "c2_usertable userdim ON basecube.userid = userdim.id and userdim.dt='latest' "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as balias0"
        + " from " + getDbName() + "c2_user_interests_tbl user_interests"
        + " join " + getDbName() + "c2_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " and usersports.dt='latest and user_interests.dt='latest'"
        + " group by user_interests.user_id) usersports"
        + " on userdim.id = usersports.user_id ",
      null, "group by usersports.balias0", null,
      getWhereForDailyAndHourly2days("basecube", "c2_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sports, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    expected = getExpectedQuery("basecube", "SELECT (usersports.balias0) as `sports`, sum((basecube.msr2)) "
            + "as `sum(msr2)` FROM ", " join " + getDbName()
            + "c2_usertable userdim ON basecube.userid = userdim.id and userdim.dt='latest' "
            + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as balias0"
            + " from " + getDbName() + "c2_user_interests_tbl user_interests"
            + " join " + getDbName() + "c2_sports_tbl usersports on user_interests.sport_id = usersports.id"
            + " and usersports.dt='latest and user_interests.dt='latest'"
            + " group by user_interests.user_id) usersports"
            + " on userdim.id = usersports.user_id ",
        null, "group by usersports.balias0", null,
        getWhereForDailyAndHourly2days("basecube", "c2_testfact1_base"));
    hqlQuery = rewrite(query, conf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testBridgeTablesWithNormalJoins() throws Exception {
    String query = "select usersports.name, cubestatecountry.name, cubecitystatecountry.name,"
      + " sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, hConf);
    String expected = getExpectedQuery("basecube", "SELECT (usersports.balias0) as `name`, (cubestatecountry.name) "
        + "as `name`, (cubecitystatecountry.name) as `name`, sum((basecube.msr2)) as `sum(msr2)` FROM ",
      " join " + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as balias0"
        + " from " + getDbName() + "c1_user_interests_tbl user_interests"
        + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports"
        + " on userdim.id = usersports.user_id "
        + " join " + getDbName() + "c1_citytable citydim on basecube.cityid = citydim.id and (citydim.dt = 'latest')"
        + " join " + getDbName()
        + "c1_statetable statedim_0 on citydim.stateid=statedim_0.id and statedim_0.dt='latest'"
        + " join " + getDbName()
        + "c1_countrytable cubecitystatecountry on statedim_0.countryid=cubecitystatecountry.id"
        + " join " + getDbName() + "c1_statetable statedim on basecube.stateid=statedim.id "
          + "and (statedim.dt = 'latest')"
        + " join " + getDbName() + "c1_countrytable cubestatecountry on statedim.countryid=cubestatecountry.id ",
      null, "group by usersports.balias0, cubestatecountry.name, cubecitystatecountry.name", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sports, statecountry, citycountry, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    expected = getExpectedQuery("basecube", "SELECT (usersports.balias0) as `sports`, (cubestatecountry.name) "
            + "as `statecountry`, (cubecitystatecountry.name) as `citycountry`, sum((basecube.msr2)) "
        + "as `sum(msr2)` FROM ",
        " join " + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
            + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as balias0"
            + " from " + getDbName() + "c1_user_interests_tbl user_interests"
            + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
            + " group by user_interests.user_id) usersports"
            + " on userdim.id = usersports.user_id "
            + " join " + getDbName() + "c1_citytable citydim on basecube.cityid = citydim.id "
            + "and (citydim.dt = 'latest') join " + getDbName()
            + "c1_statetable statedim_0 on citydim.stateid=statedim_0.id and statedim_0.dt='latest'"
            + " join " + getDbName()
            + "c1_countrytable cubecitystatecountry on statedim_0.countryid=cubecitystatecountry.id"
            + " join " + getDbName() + "c1_statetable statedim on basecube.stateid=statedim.id "
            + "and (statedim.dt = 'latest')"
            + " join " + getDbName() + "c1_countrytable cubestatecountry on statedim.countryid=cubestatecountry.id ",
        null, "group by usersports.balias0, cubestatecountry.name, cubecitystatecountry.name", null,
        getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    hqlQuery = rewrite(query, hConf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testBridgeTablesWithFilterBeforeFlattening() throws Exception {
    String query = "select usersports.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE
      + " and usersports.name = 'CRICKET' and usersports.name in ('BB', 'FOOTBALL')"
      + " and usersports.name != 'RANDOM' and usersports.name not in ('xyz', 'ABC')"
      + " and (some_filter(usersports.name, 'CRICKET') OR some_filter(usersports.name, 'FOOTBALL'))"
      + " and not (some_filter(usersports.name, 'ASD') OR some_filter(usersports.name, 'ZXC'))"
      + " and myfunc(usersports.name) = 'CRT' and substr(usersports.name, 3) in ('CRI')";
    String hqlQuery = rewrite(query, hConf);
    String expected = getExpectedQuery("basecube", "SELECT (usersports.balias0) as `name`, sum((basecube.msr2)) "
        + "as `sum(msr2)` FROM ", " join "
        + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as balias0,"
        + " collect_set(myfunc(usersports.name)) as balias1, collect_set(substr(usersports.name, 3)) as balias2"
        + " from " + getDbName() + "c1_user_interests_tbl user_interests"
        + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id "
        + " group by user_interests.user_id) usersports"
        + " on userdim.id = usersports.user_id ",
      null, " and array_contains(usersports.balias0,'CRICKET') and (array_contains(usersports.balias0, 'BB')"
      + " OR array_contains(usersports.balias0, 'FOOTBALL'))"
      + " and not array_contains(usersports.balias0, 'RANDOM'))"
      + " and not (array_contains(usersports.balias0, 'xyz') OR array_contains(usersports.balias0, 'ABC'))"
      + " and (some_filter(usersports.name, 'CRICKET') OR some_filter(usersports.name, 'FOOTBALL'))"
      + " and not (some_filter(usersports.name, 'ASD') OR some_filter(usersports.name, 'ZXC'))"
      + " and (array_contains(usersports.balias1, 'CRT') AND array_contains(usersports.balias2, 'CRI'))"
      + "group by usersports.balias0", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sports, sum(msr2) from basecube where " + TWO_DAYS_RANGE + " and sports = 'CRICKET' and "
      + " sports in ('BB', 'FOOTBALL') and sports != 'RANDOM' and sports not in ('xyz', 'ABC')"
      + " and (some_filter(sports, 'CRICKET') OR some_filter(sports, 'FOOTBALL'))"
      + " and not (some_filter(sports, 'ASD') OR some_filter(sports, 'ZXC'))"
      + " and myfunc(sports) = 'CRT' and sports_abbr in ('CRI')";
    expected = getExpectedQuery("basecube", "SELECT (usersports.balias0) as `sports`, sum((basecube.msr2)) "
            + "as `sum(msr2)` FROM ", " join "
            + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
            + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as balias0,"
            + " collect_set(myfunc(usersports.name)) as balias1, collect_set(substr(usersports.name, 3)) as balias2"
            + " from " + getDbName() + "c1_user_interests_tbl user_interests"
            + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id "
            + " group by user_interests.user_id) usersports"
            + " on userdim.id = usersports.user_id ",
        null, " and array_contains(usersports.balias0,'CRICKET') and (array_contains(usersports.balias0, 'BB')"
            + " OR array_contains(usersports.balias0, 'FOOTBALL'))"
            + " and not array_contains(usersports.balias0, 'RANDOM'))"
            + " and not (array_contains(usersports.balias0, 'xyz') OR array_contains(usersports.balias0, 'ABC'))"
            + " and (some_filter(usersports.name, 'CRICKET') OR some_filter(usersports.name, 'FOOTBALL'))"
            + " and not (some_filter(usersports.name, 'ASD') OR some_filter(usersports.name, 'ZXC'))"
            + " and (array_contains(usersports.balias1, 'CRT') AND array_contains(usersports.balias2, 'CRI'))"
            + "group by usersports.balias0", null,
        getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    hqlQuery = rewrite(query, hConf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testBridgeTablesWithFilterAndOrderby() throws Exception {
    String query = "select usersports.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE
        + " and usersports.name = 'CRICKET' order by usersports.name";
    String hqlQuery = rewrite(query, hConf);
    String expected = getExpectedQuery("basecube", "SELECT (usersports.balias0) as `name`, sum((basecube.msr2)) "
            + "as `sum(msr2)` FROM ", " join "
            + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
            + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as balias0"
            + " from " + getDbName() + "c1_user_interests_tbl user_interests"
            + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id "
            + " group by user_interests.user_id) usersports"
            + " on userdim.id = usersports.user_id ",
        null,
        " and array_contains(usersports.balias0, 'CRICKET') group by usersports.balias0 "
            + "order by name asc",
        null, getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sports, sum(msr2) from basecube where " + TWO_DAYS_RANGE + " and sports = 'CRICKET' order by "
        + "sports";
    expected = getExpectedQuery("basecube", "SELECT (usersports.balias0) as `sports`, sum((basecube.msr2)) "
            + "as `sum(msr2)` FROM ", " join "
            + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
            + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as balias0"
            + " from " + getDbName() + "c1_user_interests_tbl user_interests"
            + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id "
            + " group by user_interests.user_id) usersports"
            + " on userdim.id = usersports.user_id ",
        null,
        " and array_contains(usersports.balias0, 'CRICKET') group by usersports.balias0 "
            + "order by sports asc",
        null, getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    hqlQuery = rewrite(query, hConf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testFlattenBridgeTablesWithCustomFilter() throws Exception {
    Configuration conf = new Configuration(hConf);
    conf.set(CubeQueryConfUtil.BRIDGE_TABLE_FIELD_ARRAY_FILTER, "custom_filter");
    String query = "select usersports.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE
      + " and usersports.name in ('CRICKET','FOOTBALL')";
    String hqlQuery = rewrite(query, conf);
    String expected = getExpectedQuery("basecube", "SELECT (usersports.balias0) as `name`, sum((basecube.msr2)) "
        + "as `sum(msr2)` FROM ",
      " join " + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as balias0"
        + " from " + getDbName() + "c1_user_interests_tbl user_interests"
        + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports"
        + " on userdim.id = usersports.user_id ", null,
      " and (custom_filter(usersports.balias0, 'CRICKET') OR custom_filter(usersports.balias0, 'FOOTBALL'))"
        + "group by usersports.balias0",
      null, getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sports, sum(msr2) from basecube where " + TWO_DAYS_RANGE + " and sports in ('CRICKET','FOOTBALL')";
    expected = getExpectedQuery("basecube", "SELECT (usersports.balias0) as `sports`, sum((basecube.msr2)) "
            + "as `sum(msr2)` FROM ",
        " join " + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
            + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as balias0"
            + " from " + getDbName() + "c1_user_interests_tbl user_interests"
            + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
            + " group by user_interests.user_id) usersports"
            + " on userdim.id = usersports.user_id ", null,
        " and (custom_filter(usersports.balias0, 'CRICKET') OR custom_filter(usersports.balias0, 'FOOTBALL'))"
            + "group by usersports.balias0",
        null, getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    hqlQuery = rewrite(query, conf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testBridgeTablesWithFilterAfterFlattening() throws Exception {
    Configuration conf = new Configuration(hConf);
    conf.setBoolean(CubeQueryConfUtil.DO_FLATTENING_OF_BRIDGE_TABLE_EARLY, true);
    String query = "select usersports.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE
      + " and usersports.name = 'CRICKET,FOOTBALL'";
    String hqlQuery = rewrite(query, conf);
    String expected = getExpectedQuery("basecube", "SELECT (usersports.name) as `name`, sum((basecube.msr2)) "
        + "as `sum(msr2)` FROM ", " join "
        + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as name"
        + " from " + getDbName() + "c1_user_interests_tbl user_interests"
        + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports"
        + " on userdim.id = usersports.user_id ", null,
      " and usersports.name = 'CRICKET,FOOTBALL' group by usersports.name", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sports, sum(msr2) from basecube where " + TWO_DAYS_RANGE + " and sports = 'CRICKET,FOOTBALL'";
    expected = getExpectedQuery("basecube", "SELECT (usersports.name) as `sports`, sum((basecube.msr2)) "
            + "as `sum(msr2)` FROM ", " join "
            + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
            + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as name"
            + " from " + getDbName() + "c1_user_interests_tbl user_interests"
            + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
            + " group by user_interests.user_id) usersports"
            + " on userdim.id = usersports.user_id ", null,
        " and usersports.name = 'CRICKET,FOOTBALL' group by usersports.name", null,
        getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    hqlQuery = rewrite(query, conf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testBridgeTablesWithMultipleFactsWithFilterBeforeFlattening() throws Exception {
    String query = "select usersports.name, msr2, msr12 from basecube where " + TWO_DAYS_RANGE
      + " and usersports.name = 'CRICKET'";
    String hqlQuery = rewrite(query, hConf);
    String expected1 = getExpectedQuery("basecube",
      "SELECT (usersports.balias0) as `alias0`, sum((basecube.msr2)) as `alias1`, 0 "
          + "as `alias2` FROM ", " join " + getDbName()
        + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as balias0" + " from "
        + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
        + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + "group by user_interests.user_id) usersports on userdim.id = usersports.user_id ", null,
      "  and array_contains(usersports.balias0,'CRICKET') group by usersports.balias0", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    String expected2 = getExpectedQuery("basecube",
      "SELECT (usersports.balias0) as `alias0`, 0 as `alias1`, sum((basecube.msr12)) "
          + "as `alias2` FROM", " join " + getDbName()
        + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as balias0" + " from "
        + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
        + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports on userdim.id = usersports.user_id ", null,
      " and array_contains(usersports.balias0,'CRICKET') group by usersports.balias0", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact2_base"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    String lower = hqlQuery.toLowerCase();
    assertTrue(lower.startsWith("select (basecube.alias0) as `name`, sum((basecube.alias1)) as `msr2`, "
        + "sum((basecube.alias2)) as `msr12` from"), hqlQuery);

    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("GROUP BY (basecube.alias0)"),
      hqlQuery);
    // run with chain ref column
    query = "select sports, msr2, msr12 from basecube where " + TWO_DAYS_RANGE + " and sports = 'CRICKET'";
    hqlQuery = rewrite(query, hConf);
    expected1 = getExpectedQuery("basecube",
      "SELECT (usersports.balias0) as `alias0`, sum((basecube.msr2)) as `alias1`, 0 "
          + "as `alias2` FROM ", " join " + getDbName()
        + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as balias0" + " from "
        + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
        + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports on userdim.id = usersports.user_id ", null,
      "and array_contains(usersports.balias0,'CRICKET') group by usersports.balias0", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    expected2 = getExpectedQuery("basecube",
      "SELECT (usersports.balias0) as `alias0`, 0 as `alias1`, sum((basecube.msr12)) "
          + "as `alias2` FROM ", " join " + getDbName()
        + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as balias0" + " from "
        + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
        + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports on userdim.id = usersports.user_id ", null,
      " and array_contains(usersports.balias0,'CRICKET') group by usersports.balias0", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact2_base"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    lower = hqlQuery.toLowerCase();
    assertTrue(lower.startsWith("select (basecube.alias0) as `sports`, sum((basecube.alias1)) as `msr2`, "
        + "sum((basecube.alias2)) as `msr12` from"), hqlQuery);

    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("GROUP BY (basecube.alias0)"),
      hqlQuery);
  }

  @Test
  public void testBridgeTablesWithMultipleFactsWithFilterAfterFlattening() throws Exception {
    Configuration conf = new Configuration(hConf);
    conf.setBoolean(CubeQueryConfUtil.DO_FLATTENING_OF_BRIDGE_TABLE_EARLY, true);
    String query = "select usersports.name, msr2, msr12 from basecube where " + TWO_DAYS_RANGE
      + " and usersports.name = 'CRICKET,FOOTBALL'";
    String hqlQuery = rewrite(query, conf);
    String expected1 = getExpectedQuery("basecube",
      "SELECT (usersports.name) as `alias0`, sum((basecube.msr2)) as `alias1`, 0 as `alias2` FROM ",
        " join " + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as name" + " from "
        + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
        + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports" + " on userdim.id = usersports.user_id ", null,
      " and usersports.name = 'CRICKET,FOOTBALL' group by usersports.name", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    String expected2 = getExpectedQuery("basecube",
      "SELECT (usersports.name) as `alias0`, 0 as `alias1`, sum((basecube.msr12)) as `alias2` FROM ",
        " join " + getDbName()
        + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as name" + " from "
        + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
        + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports" + " on userdim.id = usersports.user_id ", null,
      " and usersports.name = 'CRICKET,FOOTBALL' group by usersports.name", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact2_base"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    String lower = hqlQuery.toLowerCase();
    assertTrue(lower.startsWith("select (basecube.alias0) as `name`, sum((basecube.alias1)) as `msr2`, "
        + "sum((basecube.alias2)) as `msr12` from"), hqlQuery);

    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("GROUP BY (basecube.alias0)"),
      hqlQuery);
    // run with chain ref column
    query = "select sports, msr2, msr12 from basecube where " + TWO_DAYS_RANGE
      + " and sports = 'CRICKET,FOOTBALL'";
    hqlQuery = rewrite(query, conf);
    expected1 = getExpectedQuery("basecube",
      "SELECT (usersports.name) as `alias0`, sum((basecube.msr2)) as `alias1`, 0 "
          + "as `alias2` FROM ", " join " + getDbName()
        + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as name" + " from "
        + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
        + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports" + " on userdim.id = usersports.user_id ", null,
      " and usersports.name = 'CRICKET,FOOTBALL' group by usersports.name", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    expected2 = getExpectedQuery("basecube",
      "SELECT (usersports.name) as `alias0`, 0 as `alias1`, sum((basecube.msr12)) "
          + "as `alias2` FROM ", " join " + getDbName()
        + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as name" + " from "
        + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
        + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports" + " on userdim.id = usersports.user_id ", null,
      " and usersports.name = 'CRICKET,FOOTBALL' group by usersports.name", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact2_base"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    lower = hqlQuery.toLowerCase();
    assertTrue(lower.startsWith("select (basecube.alias0) as `sports`, sum((basecube.alias1)) as `msr2`, "
        + "sum((basecube.alias2)) as `msr12` from"), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("GROUP BY (basecube.alias0)"),
      hqlQuery);
  }

  @Test
  public void testBridgeTablesWithExpressionBeforeFlattening() throws Exception {
    String query = "select substr(usersports.name, 3), sum(msr2) from basecube where " + TWO_DAYS_RANGE
      + " and usersports.name = 'CRICKET'";
    String hqlQuery = rewrite(query, hConf);
    String expected = getExpectedQuery("basecube", "SELECT (usersports.balias0) as `substr((usersports.name), 3)`, "
        + "sum((basecube.msr2)) as `sum(msr2)` FROM ",
      " join " + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(substr(usersports.name, 3)) as balias0"
        + " collect_set(( usersports . name )) as balias1 from " + getDbName() + "c1_user_interests_tbl user_interests"
        + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id "
        + " group by user_interests.user_id) usersports"
        + " on userdim.id = usersports.user_id ",
      null, " and array_contains(usersports.balias1, 'CRICKET') group by usersports.balias0", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sports_abbr, sum(msr2) from basecube where " + TWO_DAYS_RANGE + " and sports = 'CRICKET'";
    expected = getExpectedQuery("basecube", "SELECT (usersports.balias0) as `sports_abbr`, "
            + "sum((basecube.msr2)) as `sum(msr2)` FROM ",
        " join " + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
            + " join (select user_interests.user_id as user_id,collect_set(substr(usersports.name, 3)) as balias0"
            + " collect_set(( usersports . name )) as balias1 from " + getDbName()
            + "c1_user_interests_tbl user_interests"
            + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id "
            + " group by user_interests.user_id) usersports"
            + " on userdim.id = usersports.user_id ",
        null, " and array_contains(usersports.balias1, 'CRICKET') group by usersports.balias0", null,
        getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    hqlQuery = rewrite(query, hConf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testBridgeTablesWithExpressionAfterFlattening() throws Exception {
    Configuration conf = new Configuration(hConf);
    conf.setBoolean(CubeQueryConfUtil.DO_FLATTENING_OF_BRIDGE_TABLE_EARLY, true);
    String query = "select substr(usersports.name, 3), sum(msr2) from basecube where " + TWO_DAYS_RANGE
      + " and usersports.name = 'CRICKET,FOOTBALL'";
    String hqlQuery = rewrite(query, conf);
    String expected = getExpectedQuery("basecube", "SELECT substr((usersports.name), 3) as "
        + "`substr((usersports.name), 3)`, sum((basecube.msr2)) as `sum(msr2)` FROM ", " join "
        + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as name"
        + " from " + getDbName() + "c1_user_interests_tbl user_interests"
        + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports"
        + " on userdim.id = usersports.user_id ", null,
      " and usersports.name = 'CRICKET,FOOTBALL' group by substr(usersports.name, 3)", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sports_abbr, sum(msr2) from basecube where " + TWO_DAYS_RANGE
        + " and sports = 'CRICKET,FOOTBALL'";
    expected = getExpectedQuery("basecube", "SELECT substr((usersports.name), 3) as "
            + "`sports_abbr`, sum((basecube.msr2)) as `sum(msr2)` FROM ", " join "
            + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
            + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as name"
            + " from " + getDbName() + "c1_user_interests_tbl user_interests"
            + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
            + " group by user_interests.user_id) usersports"
            + " on userdim.id = usersports.user_id ", null,
        " and usersports.name = 'CRICKET,FOOTBALL' group by substr(usersports.name, 3)", null,
        getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    hqlQuery = rewrite(query, conf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testBridgeTablesWithExpressionAndAliasesBeforeFlattening() throws Exception {
    String query = "select userid as uid, usersports.name as uname, substr(usersports.name, 3) as `sub user`,"
      + " sum(msr2) from basecube where " + TWO_DAYS_RANGE
      + " and usersports.name = 'CRICKET' and substr(usersports.name, 3) = 'CRI' and (userid = 4 or userid = 5)";
    String hqlQuery = rewrite(query, hConf);
    String expected = getExpectedQuery("basecube", "SELECT (basecube.userid) as `uid`, (usersports.balias0) "
        + "as `uname`, (usersports.balias1) as `sub user`, sum((basecube.msr2)) as `sum(msr2)` FROM ",
      " join " + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id, collect_set(usersports.name) as balias0, "
        + "collect_set(substr(usersports.name, 3)) as balias1"
        + " from " + getDbName() + "c1_user_interests_tbl user_interests"
        + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id "
        + " group by user_interests.user_id) usersports"
        + " on userdim.id = usersports.user_id ",
      null, " and array_contains(usersports.balias0,'CRICKET') and (array_contains(usersports.balias1),'CRI')"
        + " and ((basecube.userid) = 4) or (( basecube . userid ) = 5 )) "
        + "group by basecube.userid, usersports.balias0, usersports.balias1", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select userid as uid, sports as uname, sports_abbr as `sub user`, sum(msr2) from basecube where "
      + TWO_DAYS_RANGE + " and sports = 'CRICKET' and sports_abbr = 'CRI' and (userid = 4 or userid = 5)";
    hqlQuery = rewrite(query, hConf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testBridgeTablesWithExpressionAndAliasesAfterFlattening() throws Exception {
    Configuration conf = new Configuration(hConf);
    conf.setBoolean(CubeQueryConfUtil.DO_FLATTENING_OF_BRIDGE_TABLE_EARLY, true);
    String query = "select usersports.name as uname, substr(usersports.name, 3) as `sub user`,"
      + " sum(msr2) from basecube where " + TWO_DAYS_RANGE
      + " and usersports.name = 'CRICKET,FOOTBALL'";
    String hqlQuery = rewrite(query, conf);
    String expected = getExpectedQuery("basecube", "SELECT (usersports.name) as `uname`, substr((usersports.name), 3) "
        + "as `sub user`, sum((basecube.msr2)) as `sum(msr2)` FROM ", " join "
        + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as name"
        + " from " + getDbName() + "c1_user_interests_tbl user_interests"
        + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports"
        + " on userdim.id = usersports.user_id ", null,
      " and usersports.name = 'CRICKET,FOOTBALL' group by usersports.name, substr(usersports.name, 3)", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sports as uname, sports_abbr as `sub user`, sum(msr2) from basecube where " + TWO_DAYS_RANGE
      + " and sports = 'CRICKET,FOOTBALL'";
    hqlQuery = rewrite(query, conf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }
  @Test
  public void testBridgeTablesWithMultipleFactsWithExprBeforeFlattening() throws Exception {
    String query = "select substr(usersports.name, 3), msr2, msr12 from basecube where " + TWO_DAYS_RANGE
      + " and usersports.name in ('CRICKET', 'FOOTBALL')";
    String hqlQuery = rewrite(query, hConf);
    String expected1 = getExpectedQuery("basecube",
      "SELECT (usersports.balias0) as `alias0`, sum((basecube.msr2)) as `alias1`, 0 "
          + "as `alias2` FROM ", " join " + getDbName()
        + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id, collect_set(substr(usersports.name, 3)) as balias0, "
        + " collect_set(usersports.name) as balias1 from"
        + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
        + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports on userdim.id = usersports.user_id ", null,
      " and ( array_contains(usersports.balias1,'CRICKET') OR array_contains(usersports.balias1,'FOOTBALL')"
        + " group by usersports.balias0", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    String expected2 = getExpectedQuery("basecube",
      "SELECT (usersports.balias0) as `alias0`, 0 as `alias1`, sum((basecube.msr12)) as `alias2` FROM  "
        , " join " + getDbName()
        + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id, collect_set(substr(usersports.name, 3)) as balias0, "
        + " collect_set(usersports.name) as balias1 from"
        + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
        + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports on userdim.id = usersports.user_id ", null,
      " and ( array_contains(usersports.balias1,'CRICKET') OR array_contains(usersports.balias1,'FOOTBALL')"
        + " group by usersports.balias0", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact2_base"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    String lower = hqlQuery.toLowerCase();
    assertTrue(lower.startsWith("select (basecube.alias0) as `substr((usersports.name), 3)`, "
        + "sum((basecube.alias1)) as `msr2`, sum((basecube.alias2)) as `msr12` from"),
      hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("GROUP BY (basecube.alias0)"),
      hqlQuery);

    // run with chain ref column
    query = "select sports_abbr, msr2, msr12 from basecube where " + TWO_DAYS_RANGE + " and sports in "
      + "('CRICKET', 'FOOTBALL')";
    hqlQuery = rewrite(query, hConf);
    expected1 = getExpectedQuery("basecube",
      "SELECT (usersports.balias0) as `alias0`, sum((basecube.msr2)) as `alias1`, 0 "
          + "as `alias2` FROM ", " join " + getDbName()
        + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id, collect_set(substr((usersports.name), 3)) as balias0, "
        + " collect_set(usersports.name) as balias1 from"
        + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
        + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports on userdim.id = usersports.user_id ", null,
      " and ( array_contains(usersports.balias1,'CRICKET') OR array_contains(usersports.balias1,'FOOTBALL')"
        + " group by usersports.balias0", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    expected2 = getExpectedQuery("basecube",
      "SELECT (usersports.balias0) as `alias0`, 0 as `alias1`, sum((basecube.msr12)) "
          + "as `alias2` FROM ", " join " + getDbName()
        + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id, collect_set(substr((usersports.name), 3)) as balias0,"
        + " collect_set(usersports.name) as balias1 from"
        + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
        + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports on userdim.id = usersports.user_id ", null,
      " and ( array_contains(usersports.balias1,'CRICKET') OR array_contains(usersports.balias1,'FOOTBALL')"
        + " group by usersports.balias0", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact2_base"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    lower = hqlQuery.toLowerCase();
    assertTrue(lower.startsWith(
        "select (basecube.alias0) as `sports_abbr`, sum((basecube.alias1)) as `msr2`, "
            + "sum((basecube.alias2)) as `msr12` from"), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL")
        && hqlQuery.endsWith("GROUP BY (basecube.alias0)"),
      hqlQuery);
  }

  @Test
  public void testBridgeTablesWithMultipleFactsWithExprAfterFlattening() throws Exception {
    Configuration conf = new Configuration(hConf);
    conf.setBoolean(CubeQueryConfUtil.DO_FLATTENING_OF_BRIDGE_TABLE_EARLY, true);
    String query = "select substr(usersports.name, 3), msr2, msr12 from basecube where " + TWO_DAYS_RANGE
      + " and usersports.name = 'CRICKET,FOOTBALL'";
    String hqlQuery = rewrite(query, conf);
    String expected1 = getExpectedQuery("basecube",
      "SELECT substr((usersports.name), 3) as `alias0`, sum((basecube.msr2)) as `alias1`, 0 "
          + "as `alias2` FROM ", " join " + getDbName()
        + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as name" + " from "
        + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
        + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports" + " on userdim.id = usersports.user_id ", null,
      " and usersports.name = 'CRICKET,FOOTBALL' group by substr(usersports.name, 3)", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    String expected2 = getExpectedQuery("basecube",
      "SELECT substr((usersports.name), 3) as `alias0`, 0 as `alias1`, sum((basecube.msr12)) "
          + "as `alias2` FROM ", " join " + getDbName()
        + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as name" + " from "
        + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
        + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports" + " on userdim.id = usersports.user_id ", null,
      " and usersports.name = 'CRICKET,FOOTBALL' group by substr(usersports.name, 3)", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact2_base"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    String lower = hqlQuery.toLowerCase();
    assertTrue(lower.startsWith("select (basecube.alias0) as `substr((usersports.name), 3)`, "
        + "sum((basecube.alias1)) as `msr2`, sum((basecube.alias2)) as `msr12` from"),
      hqlQuery);

    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("GROUP BY (basecube.alias0)"),
      hqlQuery);
    // run with chain ref column
    query = "select sports_abbr, msr2, msr12 from basecube where " + TWO_DAYS_RANGE + " and sports = "
      + "'CRICKET,FOOTBALL'";
    hqlQuery = rewrite(query, conf);
    expected1 = getExpectedQuery("basecube",
      "SELECT substr((usersports.name), 3) as `alias0`, sum((basecube.msr2)) as `alias1`, 0 "
        + "as `alias2` FROM ", " join " + getDbName()
        + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as name" + " from "
        + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
        + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports" + " on userdim.id = usersports.user_id ", null,
      " and usersports.name = 'CRICKET,FOOTBALL' group by substr(usersports.name, 3)", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    expected2 = getExpectedQuery("basecube",
      "SELECT substr((usersports.name), 3) as `alias0`, 0 as `alias1`, sum((basecube.msr12)) "
          + "as `alias2` FROM ", " join " + getDbName()
        + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as name" + " from "
        + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
        + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports" + " on userdim.id = usersports.user_id ", null,
      " and usersports.name = 'CRICKET,FOOTBALL' group by substr(usersports.name, 3)", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact2_base"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    lower = hqlQuery.toLowerCase();
    assertTrue(lower.startsWith(
      "select (basecube.alias0) as `sports_abbr`, sum((basecube.alias1)) as `msr2`, "
          + "sum((basecube.alias2)) as `msr12` from"), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL")
        && hqlQuery.endsWith("GROUP BY (basecube.alias0)"),
      hqlQuery);
  }

  @Test
  public void testBridgeTablesDimensionOnlyQuery() throws Exception {
    Configuration conf = new Configuration(hConf);
    conf.set(CubeQueryConfUtil.getValidFactTablesKey("basecube"), "testFact1_base");
    String query = "select userid as uid, usersports.name as uname, substr(usersports.name, 3) as `sub user`"
      + " from basecube where " + TWO_DAYS_RANGE
      + " and usersports.name = 'CRICKET' and substr(usersports.name, 3) = 'CRI' and (userid = 4 or userid = 5)";
    String hqlQuery = rewrite(query, conf);
    String expected = getExpectedQuery("basecube", "select distinct basecube.userid as `uid`, usersports.balias0 as "
        + "`uname`, (usersports.balias1) as `sub user` FROM ",
      " join " + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id, collect_set(usersports.name) as balias0, "
        + "collect_set(substr(usersports.name, 3)) as balias1"
        + " from " + getDbName() + "c1_user_interests_tbl user_interests"
        + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id "
        + " group by user_interests.user_id) usersports"
        + " on userdim.id = usersports.user_id ",
      null, " and array_contains(usersports.balias0,'CRICKET') and (array_contains(usersports.balias1),'CRI')"
        + " and ((basecube.userid) = 4) or (( basecube . userid ) = 5 )) ", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select userid as uid, sports as uname, sports_abbr as `sub user` from basecube where "
      + TWO_DAYS_RANGE + " and sports = 'CRICKET' and sports_abbr = 'CRI' and (userid = 4 or userid = 5)";
    hqlQuery = rewrite(query, conf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testBridgeTableQueryJoinColumns() throws Exception {
    Configuration conf = new Configuration(hConf);
    conf.set(CubeQueryConfUtil.getValidFactTablesKey("basecube"), "testFact1_base");
    String query = "select userid as uid, userchain.id as udid, userInterestIds.sport_id as uisid, "
      + "userInterestIds.user_id as uiuid, usersports.id as usid"
      + " from basecube where " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, conf);
    String expected = getExpectedQuery("basecube", "select distinct basecube.userid as `uid`,"
        + "userchain.id as `udid`, userinterestids.balias0 as `uisid`, userinterestids.balias1 as "
        + "`uiuid`, usersports . balias0 as `usid` FROM ",
      " join " + getDbName() + "c1_usertable userchain ON basecube.userid = userchain.id "
        + " join ( select userinterestids.user_id as user_id, collect_set(userinterestids.sport_id) as balias0,"
        + " collect_set(userinterestids.user_id) as balias1 from  " + getDbName() + "c1_user_interests_tbl "
        + " userinterestids group by userinterestids.user_id) userinterestids "
        + "on userchain.id = userinterestids.user_id"
        + " join  (select userinterestids.user_id as user_id, collect_set(usersports . id) as balias0 from"
        + getDbName() + " c1_user_interests_tbl userinterestids join " + getDbName() + "c1_sports_tbl"
        + " usersports on userinterestids.sport_id = usersports.id group by userinterestids.user_id) usersports"
        + " on userchain.id = usersports.user_id ",
      null, null, null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }
}

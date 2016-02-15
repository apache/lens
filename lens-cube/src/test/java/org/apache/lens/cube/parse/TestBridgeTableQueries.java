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
    String expected = getExpectedQuery("basecube", "select usersports.name, sum(basecube.msr2) FROM ",
      " join " + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as name"
        + " from " + getDbName() + "c1_user_interests_tbl user_interests"
        + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports"
        + " on userdim.id = usersports.user_id ",
      null, "group by usersports.name", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sports, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hConf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testBridgeTablesForExprFieldWithoutDimtablePartitioning() throws Exception {
    String query = "select substr(usersports.name, 10), sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, hConf);
    String expected = getExpectedQuery("basecube", "select substr(usersports.name, 10), sum(basecube.msr2) FROM ",
      " join " + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as name"
        + " from " + getDbName() + "c1_user_interests_tbl user_interests"
        + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports"
        + " on userdim.id = usersports.user_id ",
      null, "group by substr(( usersports . name ),  10 )", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select substrsprorts, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hConf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testFlattenBridgeTablesOFF() throws Exception {
    Configuration conf = new Configuration(hConf);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_FLATTENING_FOR_BRIDGETABLES, false);
    String query = "select usersports.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, conf);
    String expected = getExpectedQuery("basecube", "select usersports.name, sum(basecube.msr2) FROM ",
      " join " + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join " + getDbName() + "c1_user_interests_tbl user_interests on userdim.id = user_interests.user_id"
        + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id",
      null, "group by usersports.name", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sports, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, conf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testFlattenBridgeTablesWithCustomAggregate() throws Exception {
    Configuration conf = new Configuration(hConf);
    conf.set(CubeQueryConfUtil.BRIDGE_TABLE_FIELD_AGGREGATOR, "custom_aggr");
    String query = "select usersports.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, conf);
    String expected = getExpectedQuery("basecube", "select usersports.name, sum(basecube.msr2) FROM ",
      " join " + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,custom_aggr(usersports.name) as name"
        + " from " + getDbName() + "c1_user_interests_tbl user_interests"
        + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports"
        + " on userdim.id = usersports.user_id ",
      null, "group by usersports.name", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sports, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, conf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testBridgeTablesWithMegringChains() throws Exception {
    String query = "select userInterestIds.sport_id, usersports.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, hConf);
    String expected = getExpectedQuery("basecube", "select userInterestIds.sport_id, usersports.name,"
      + " sum(basecube.msr2) FROM ",
      " join " + getDbName() + "c1_usertable userdim on basecube.userid = userdim.id join (select userinterestids"
        + ".user_id as user_id,collect_set(userinterestids.sport_id) as sport_id from " + getDbName()
        + "c1_user_interests_tbl userinterestids group by userinterestids.user_id) userinterestids on userdim.id = "
        + "userinterestids.user_id join (select userinterestids.user_id as user_id,collect_set(usersports.name) as name"
        + " from " + getDbName() + "c1_user_interests_tbl userinterestids join "
        + getDbName() + "c1_sports_tbl usersports on userinterestids.sport_id = usersports.id"
        + " group by userinterestids.user_id) usersports on userdim.id = usersports.user_id",
       null, "group by userInterestIds.sport_id, usersports.name", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sportids, sports, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hConf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testBridgeTablesWithMultipleFacts() throws Exception {
    String query = "select usersports.name, sum(msr2), sum(msr12) from basecube where " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, hConf);
    String expected1 = getExpectedQuery("basecube",
        "select usersports.name as `name`, sum(basecube.msr2) as `msr2` FROM ", " join " + getDbName()
            + "c1_usertable userdim ON basecube.userid = userdim.id "
            + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as name" + " from "
            + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
            + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
            + " group by user_interests.user_id) usersports" + " on userdim.id = usersports.user_id ", null,
        "group by usersports.name", null, getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    String expected2 = getExpectedQuery("basecube",
        "select usersports.name as `name`, sum(basecube.msr12) as `msr12` FROM ", " join " + getDbName()
            + "c1_usertable userdim ON basecube.userid = userdim.id "
            + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as name" + " from "
            + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
            + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
            + " group by user_interests.user_id) usersports" + " on userdim.id = usersports.user_id ", null,
        "group by usersports.name", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact2_base"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    String lower = hqlQuery.toLowerCase();
    assertTrue(
      lower.startsWith("select coalesce(mq1.name, mq2.name) name, mq2.msr2 msr2, mq1.msr12 msr12 from ")
      || lower.startsWith("select coalesce(mq1.name, mq2.name) name, mq1.msr2 msr2, mq2.msr12 msr12 from "), hqlQuery);

    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.name <=> mq2.name"),
      hqlQuery);

    // run with chain ref column
    query = "select sports, sum(msr2), sum(msr12) from basecube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hConf);
    expected1 = getExpectedQuery("basecube",
      "select usersports.name as `sports`, sum(basecube.msr2) as `msr2` FROM ", " join " + getDbName()
        + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as name" + " from "
        + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
        + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports" + " on userdim.id = usersports.user_id ", null,
      "group by usersports.name", null, getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    expected2 = getExpectedQuery("basecube",
      "select usersports.name as `sports`, sum(basecube.msr12) as `msr12` FROM ", " join " + getDbName()
        + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as name" + " from "
        + getDbName() + "c1_user_interests_tbl user_interests" + " join " + getDbName()
        + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports" + " on userdim.id = usersports.user_id ", null,
      "group by usersports.name", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact2_base"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    lower = hqlQuery.toLowerCase();
    assertTrue(
      lower.startsWith("select coalesce(mq1.sports, mq2.sports) sports, mq2.msr2 msr2, mq1.msr12 msr12 from ")
        || lower.startsWith("select coalesce(mq1.sports, mq2.sports) sports, mq1.msr2 msr2, mq2.msr12 msr12 from "),
      hqlQuery);

    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.sports <=> mq2.sports"),
      hqlQuery);
  }

  @Test
  public void testBridgeTablesWithMultipleChains() throws Exception {
    String query = "select usersports.name, xusersports.name, yusersports.name, sum(msr2) from basecube where "
      + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, hConf);
    String expected = getExpectedQuery("basecube", "select usersports.name, xusersports.name, yusersports.name,"
      + " sum(basecube.msr2) FROM ",
      " join " + getDbName() + "c1_usertable userdim_1 on basecube.userid = userdim_1.id "
      + " join  (select user_interests_1.user_id as user_id, collect_set(usersports.name) as name from "
      + getDbName() + "c1_user_interests_tbl user_interests_1 join " + getDbName() + "c1_sports_tbl usersports on "
      + "user_interests_1.sport_id = usersports.id group by user_interests_1.user_id) "
      + "usersports on userdim_1.id = usersports.user_id"
      + " join " + getDbName() + "c1_usertable userdim_0 on basecube.yuserid = userdim_0.id "
      + " join  (select user_interests_0.user_id as user_id,collect_set(yusersports.name) as name from "
      + getDbName() + "c1_user_interests_tbl user_interests_0 join " + getDbName() + "c1_sports_tbl yusersports on "
      + " user_interests_0.sport_id = yusersports.id group by user_interests_0.user_id) yusersports on userdim_0.id ="
      + " yusersports.user_id join " + getDbName() + "c1_usertable userdim on basecube.xuserid = userdim.id"
      + " join  (select user_interests.user_id as user_id,collect_set(xusersports.name) as name from "
      + getDbName() + "c1_user_interests_tbl user_interests join " + getDbName() + "c1_sports_tbl xusersports"
      + " on user_interests.sport_id = xusersports.id group by user_interests.user_id) xusersports on userdim.id = "
      + " xusersports.user_id", null, "group by usersports.name, xusersports.name, yusersports.name", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sports, xsports, ysports, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
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
    String expected = getExpectedQuery("basecube", "select usersports.name, xusersports.name, yusersports.name,"
      + " sum(basecube.msr2) FROM ",
      " left outer join " + getDbName() + "c1_usertable userdim_1 on basecube.userid = userdim_1.id "
      + " left outer join  (select user_interests_1.user_id as user_id, collect_set(usersports.name) as name from "
      + getDbName() + "c1_user_interests_tbl user_interests_1 join " + getDbName() + "c1_sports_tbl usersports on "
      + "user_interests_1.sport_id = usersports.id group by user_interests_1.user_id) "
      + "usersports on userdim_1.id = usersports.user_id"
      + " left outer join " + getDbName() + "c1_usertable userdim_0 on basecube.yuserid = userdim_0.id "
      + " left outer join  (select user_interests_0.user_id as user_id,collect_set(yusersports.name) as name from "
      + getDbName() + "c1_user_interests_tbl user_interests_0 join " + getDbName() + "c1_sports_tbl yusersports on "
      + " user_interests_0.sport_id = yusersports.id group by user_interests_0.user_id) yusersports on userdim_0.id ="
      + " yusersports.user_id left outer join " + getDbName()
      + "c1_usertable userdim on basecube.xuserid = userdim.id"
      + " left outer join  (select user_interests.user_id as user_id,collect_set(xusersports.name) as name from "
      + getDbName() + "c1_user_interests_tbl user_interests join " + getDbName() + "c1_sports_tbl xusersports"
      + " on user_interests.sport_id = xusersports.id group by user_interests.user_id) xusersports on userdim.id = "
      + " xusersports.user_id", null, "group by usersports.name, xusersports.name, yusersports.name", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sports, xsports, ysports, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, conf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }
  @Test
  public void testBridgeTablesWithDimTablePartitioning() throws Exception {
    Configuration conf = new Configuration(hConf);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    String query = "select usersports.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, conf);
    String expected = getExpectedQuery("basecube", "select usersports.name, sum(basecube.msr2) FROM ",
      " join " + getDbName() + "c2_usertable userdim ON basecube.userid = userdim.id and userdim.dt='latest' "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as name"
        + " from " + getDbName() + "c2_user_interests_tbl user_interests"
        + " join " + getDbName() + "c2_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " and usersports.dt='latest and user_interests.dt='latest'"
        + " group by user_interests.user_id) usersports"
        + " on userdim.id = usersports.user_id ",
      null, "group by usersports.name", null,
      getWhereForDailyAndHourly2days("basecube", "c2_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sports, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, conf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testBridgeTablesWithNormalJoins() throws Exception {
    String query = "select usersports.name, cubestatecountry.name, cubecitystatecountry.name,"
      + " sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, hConf);
    String expected = getExpectedQuery("basecube", "select usersports.name, cubestatecountry.name, "
      + "cubecitystatecountry.name, sum(basecube.msr2) FROM ",
      " join " + getDbName() + "c1_usertable userdim ON basecube.userid = userdim.id "
        + " join (select user_interests.user_id as user_id,collect_set(usersports.name) as name"
        + " from " + getDbName() + "c1_user_interests_tbl user_interests"
        + " join " + getDbName() + "c1_sports_tbl usersports on user_interests.sport_id = usersports.id"
        + " group by user_interests.user_id) usersports"
        + " on userdim.id = usersports.user_id "
        + " join " + getDbName() + "c1_citytable citydim on basecube.cityid = citydim.id and (citydim.dt = 'latest')"
        + " join " + getDbName()
        + "c1_statetable statedim_0 on citydim.stateid=statedim_0.id and statedim_0.dt='latest'"
        + " join " + getDbName()
        + "c1_countrytable cubecitystatecountry on statedim_0.countryid=cubecitystatecountry.id"
        + " join " + getDbName() + "c1_statetable statedim on basecube.stateid=statedim.id and (statedim.dt = 'latest')"
        + " join " + getDbName() + "c1_countrytable cubestatecountry on statedim.countryid=cubestatecountry.id ",
      null, "group by usersports.name, cubestatecountry.name, cubecitystatecountry.name", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
    // run with chain ref column
    query = "select sports, statecountry, citycountry, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hConf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }
}

/*
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

import static org.apache.lens.cube.metadata.DateFactory.NOW;
import static org.apache.lens.cube.metadata.DateFactory.TWO_DAYS_RANGE;
import static org.apache.lens.cube.metadata.DateFactory.getDateWithOffset;
import static org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode.SEGMENTATION_PRUNED;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.DISABLE_AUTO_JOINS;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.ENABLE_FLATTENING_FOR_BRIDGETABLES;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.RESOLVE_SEGMENTATIONS;
import static org.apache.lens.cube.parse.CubeTestSetup.getDbName;
import static org.apache.lens.cube.parse.CubeTestSetup.getExpectedQuery;
import static org.apache.lens.cube.parse.CubeTestSetup.getWhereForDailyAndHourly2days;
import static org.apache.lens.cube.parse.CubeTestSetup.getWhereForHourly2days;
import static org.apache.lens.cube.parse.CubeTestSetup.getWhereForUpdatePeriods;
import static org.apache.lens.cube.parse.TestCubeRewriter.compareQueries;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import static com.google.common.collect.Lists.newArrayList;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lens.cube.error.NoCandidateFactAvailableException;
import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.server.api.LensServerAPITestUtil;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import junit.framework.Assert;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestCubeSegmentationRewriter extends TestQueryRewrite {

  private Configuration conf;

  @BeforeTest
  public void setupDriver() throws Exception {
    conf = LensServerAPITestUtil.getConfiguration(
      DRIVER_SUPPORTED_STORAGES, "C0,C1",
      DISABLE_AUTO_JOINS, false,
      ENABLE_SELECT_TO_GROUPBY, true,
      ENABLE_GROUP_BY_TO_SELECT, true,
      RESOLVE_SEGMENTATIONS, true,
      DISABLE_AGGREGATE_RESOLVER, false,
      ENABLE_FLATTENING_FOR_BRIDGETABLES, true);
  }

  @Override
  public Configuration getConf() {
    return new Configuration(conf);
  }

  private static String extractTableName(String query) {
    String l = query.toLowerCase();
    int fromIndex = l.indexOf("from");
    int toIndex = l.indexOf(" ", fromIndex + 5);
    return l.substring(fromIndex + 5, toIndex);
  }

  private static void compareUnionQuery(CubeQueryContext cubeql, String begin, String end, List<String> queries)
    throws LensException {
    final String actualLower = cubeql.toHQL().toLowerCase();
    queries.sort(Comparator.comparing(s -> actualLower.indexOf(extractTableName(s))));
    String expected = queries.stream().collect(Collectors.joining(" UNION ALL ", begin, end));
    compareQueries(actualLower, expected);
  }

  @Test
  public void testSegmentRewrite() throws Exception {
    CubeQueryContext ctx = rewriteCtx("select cityid, segmsr1 from testcube where " + TWO_DAYS_RANGE,
      getConf());
    String query1 = getExpectedQuery("testcube",
      "select testcube.cityid as alias0, sum(testcube.segmsr1) as alias1 FROM ", null,
      "group by testcube.cityid",
      getWhereForDailyAndHourly2days("testcube", "c1_b1fact1"));
    String query2 = getExpectedQuery("testcube",
      "select testcube.cityid as alias0, sum(testcube.segmsr1) as alias1 FROM ", null,
      "group by testcube.cityid",
      getWhereForDailyAndHourly2days("testcube", "c0_b2fact1"));
    compareUnionQuery(ctx,
      "SELECT (testcube.alias0) as `cityid`, sum((testcube.alias1)) as `segmsr1` FROM (",
      " ) as testcube GROUP BY (testcube.alias0)", newArrayList(query1, query2));
  }

  /*
  Asking for segmsr1 from testcube. segmsr1 is available in b1b2fact and seg1 split over time.
   Inside seg1, Two segments are there: b1cube and b2cube. b1cube has one fact b1fact which
   is split over time across two storages: c1 and c2. b2cube has one fact which answers complete range
   given to it. So total 4 storage candidates should be there.
   */
  @Test
  public void testFactUnionSegmentWithInnerUnion() throws Exception {
    CubeQueryContext ctx = rewriteCtx("select cityid, segmsr1 from testcube where "
        + "time_range_in(d_time, 'now.day-40day', 'now.day')", getConf());
    String query1, query2, query3, query4;
    query1 = getExpectedQuery("testcube", "select testcube.cityid as alias0, sum(testcube.segmsr1) as alias1 from ",
      null, "group by testcube.cityid",
      getWhereForUpdatePeriods("testcube", "c0_b1fact1",
        getDateWithOffset(UpdatePeriod.DAILY, -31), getDateWithOffset(UpdatePeriod.DAILY, -10),
        Sets.newHashSet(UpdatePeriod.MONTHLY, UpdatePeriod.DAILY)));
    query2 = getExpectedQuery("testcube", "select testcube.cityid as alias0, sum(testcube.segmsr1) as alias1 from ",
      null, "group by testcube.cityid",
      getWhereForUpdatePeriods("testcube", "c1_b1fact1",
        getDateWithOffset(UpdatePeriod.DAILY, -11), NOW,
        Sets.newHashSet(UpdatePeriod.MONTHLY, UpdatePeriod.DAILY)));
    query3 = getExpectedQuery("testcube", "select testcube.cityid as alias0, sum(testcube.segmsr1) as alias1 from ",
      null, "group by testcube.cityid",
      getWhereForUpdatePeriods("testcube", "c0_b2fact1",
        getDateWithOffset(UpdatePeriod.DAILY, -31), NOW,
        Sets.newHashSet(UpdatePeriod.MONTHLY, UpdatePeriod.DAILY)));
    query4 = getExpectedQuery("testcube", "select testcube.cityid as alias0, sum(testcube.segmsr1) as alias1 from ",
      null, "group by testcube.cityid",
      getWhereForUpdatePeriods("testcube", "c0_b1b2fact1",
        getDateWithOffset(UpdatePeriod.DAILY, -41),
        getDateWithOffset(UpdatePeriod.DAILY, -30),
        Sets.newHashSet(UpdatePeriod.MONTHLY, UpdatePeriod.DAILY)));
    compareUnionQuery(ctx, "select testcube.alias0 as cityid, sum(testcube.alias1) as segmsr1 from (",
      ") AS testcube GROUP BY (testcube.alias0)", newArrayList(query1, query2, query3, query4));
  }

  @Test
  public void testFactJoinSegmentWithInnerUnion() throws Exception {
    CubeQueryContext ctx = rewriteCtx("select cityid, msr2, segmsr1 from testcube where " + TWO_DAYS_RANGE,
      getConf());
    String query1, query2, query3;
    query1 = getExpectedQuery("testcube",
      "select testcube.cityid as alias0, 0 as alias1, sum(testcube.segmsr1) as alias2 FROM ", null,
      "group by testcube.cityid",
      getWhereForDailyAndHourly2days("testcube", "c1_b1fact1"));
    query2 = getExpectedQuery("testcube",
      "select testcube.cityid as alias0, 0 as alias1, sum(testcube.segmsr1) as alias2 FROM ", null,
      "group by testcube.cityid",
      getWhereForDailyAndHourly2days("testcube", "c0_b2fact1"));
    query3 = getExpectedQuery("testcube",
      "select testcube.cityid as alias0, sum(testcube.msr2) as alias1, 0 as alias2 FROM ", null,
      "group by testcube.cityid",
      getWhereForHourly2days("testcube", "c1_testfact2"));
    compareUnionQuery(ctx,
      "select testcube.alias0 as cityid, sum(testcube.alias1) as msr2, sum(testcube.alias2) as segmsr1 from ( ",
      ") as testcube group by testcube.alias0", newArrayList(query1, query2, query3));
  }

  @Test
  public void testFieldWithDifferentDescriptions() throws LensException {
    NoCandidateFactAvailableException e = getLensExceptionInRewrite("select invmsr1 from testcube where "
      + TWO_DAYS_RANGE, getConf());
    assertEquals(e.getJsonMessage().getBrief(), "Columns [invmsr1] are not present in any table");
  }

  @Test
  public void testExpressions() throws Exception {
    CubeQueryContext ctx = rewriteCtx("select singlecolchainfield, segmsr1 from testcube where " + TWO_DAYS_RANGE,
      getConf());
    String joinExpr = " JOIN " + getDbName()
      + "c1_citytable cubecity ON testcube.cityid = cubecity.id AND (cubecity.dt = 'latest')";
    String query1, query2;
    query1 = getExpectedQuery("testcube",
      "SELECT (cubecity.name) AS `alias0`, sum((testcube.segmsr1)) AS `alias1` from",
      joinExpr,
      null, "group by cubecity.name", null, getWhereForDailyAndHourly2days("testcube", "c1_b1fact1"));
    query2 = getExpectedQuery("testcube",
      "SELECT (cubecity.name) AS `alias0`, sum((testcube.segmsr1)) AS `alias1` from",
      joinExpr,
      null, "group by cubecity.name", null, getWhereForDailyAndHourly2days("testcube", "c0_b2fact1"));
    compareUnionQuery(ctx,
      "SELECT (testcube.alias0) AS `singlecolchainfield`, sum((testcube.alias1)) AS `segmsr1` from (",
      "as testcube group by testcube.alias0", newArrayList(query1, query2));
  }

  @Test
  public void testQueryWithWhereHavingGroupbyOrderby() throws Exception {
    String userQuery = "select cityid, msr2, segmsr1 from testcube where cityname='blah' and "
      + TWO_DAYS_RANGE + " group by cityid having segmsr1 > 1 and msr2 > 2";
    CubeQueryContext ctx = rewriteCtx(userQuery, getConf());
    String join1, join2, join3;
    String query1, query2, query3;
    join1 = "join " + getDbName()
      + "c1_citytable cubecity1 ON testcube.cityid1 = cubecity1.id AND (cubecity1.dt = 'latest')";
    join2 = "join " + getDbName()
      + "c1_citytable cubecity2 ON testcube.cityid2 = cubecity2.id AND (cubecity2.dt = 'latest')";
    join3 = "join " + getDbName()
      + "c1_citytable cubecity ON testcube.cityid = cubecity.id AND (cubecity.dt = 'latest')";
    query1 = getExpectedQuery("testcube",
      "select testcube.cityid as alias0, 0 as alias1, sum(testcube.segmsr1) as alias2 FROM ", join1,
      "cubecity1.name='blah'", "group by testcube.cityid", null,
      getWhereForDailyAndHourly2days("testcube", "c1_b1fact1"));
    query2 = getExpectedQuery("testcube",
      "select testcube.cityid as alias0, 0 as alias1, sum(testcube.segmsr1) as alias2 FROM ", join2,
      "cubecity2.name='blah'", "group by testcube.cityid", null,
      getWhereForDailyAndHourly2days("testcube", "c0_b2fact1"));
    query3 = getExpectedQuery("testcube",
      "select testcube.cityid as alias0, sum(testcube.msr2) as alias1, 0 as alias2 FROM ", join3,
      "cubecity.name='blah'", "group by testcube.cityid", null,
      getWhereForHourly2days("testcube", "c1_testfact2"));
    compareUnionQuery(ctx,
      "select testcube.alias0 as cityid, sum(testcube.alias1) as msr2, sum(testcube.alias2) as segmsr1 from ( ",
      ") as testcube group by testcube.alias0 having ((sum((testcube.alias2)) > 1) and (sum((testcube.alias1)) > 2)",
      newArrayList(query1, query2, query3));

    // Expression in having
    userQuery = "select cityid, segmsr1 from testcube where cityname='blah' and "
        + TWO_DAYS_RANGE + " having citysegmsr1 > 20";
    String rewrittenQuery = rewrite(userQuery, getConf());
    assertTrue(rewrittenQuery.toLowerCase().endsWith("(sum((testcube.alias2)) > 20)"));

    // Order by on alias
    userQuery = "select cityid as `city_id_alias`, segmsr1 from testcube where cityname='blah' and "
        + TWO_DAYS_RANGE + " order by city_id_alias";
    rewrittenQuery = rewrite(userQuery, getConf());
    assertTrue(rewrittenQuery.toLowerCase().endsWith("order by city_id_alias asc"));

    // Order by on column but the final query rewritten with alias
    userQuery = "select cityid as `city_id_alias`, segmsr1 from testcube where cityname='blah' and "
        + TWO_DAYS_RANGE + " order by cityid";
    rewrittenQuery = rewrite(userQuery, getConf());
    assertTrue(rewrittenQuery.toLowerCase().endsWith("order by city_id_alias asc"));
  }

  @Test
  public void testQueryWithManyToMany() throws LensException {
    String userQuery = "select usersports.name, xusersports.name, yusersports.name, segmsr1, msr2 from testcube where "
      + TWO_DAYS_RANGE;
    CubeQueryContext ctx = rewriteCtx(userQuery, getConf());
    String query1, query2, query3;
    String joinExpr = " join " + getDbName() + "c1_usertable userdim_1 on testcube.userid = userdim_1.id "
      + " join  (select user_interests_1.user_id as user_id, collect_set(usersports.name) as balias0 from "
      + getDbName() + "c1_user_interests_tbl user_interests_1 join " + getDbName() + "c1_sports_tbl usersports on "
      + "user_interests_1.sport_id = usersports.id group by user_interests_1.user_id) "
      + "usersports on userdim_1.id = usersports.user_id"
      + " join " + getDbName() + "c1_usertable userdim_0 on testcube.yuserid = userdim_0.id "
      + " join  (select user_interests_0.user_id as user_id,collect_set(yusersports.name) as balias0 from "
      + getDbName() + "c1_user_interests_tbl user_interests_0 join " + getDbName() + "c1_sports_tbl yusersports on "
      + " user_interests_0.sport_id = yusersports.id group by user_interests_0.user_id) yusersports on userdim_0.id ="
      + " yusersports.user_id join " + getDbName() + "c1_usertable userdim on testcube.xuserid = userdim.id"
      + " join  (select user_interests.user_id as user_id,collect_set(xusersports.name) as balias0 from "
      + getDbName() + "c1_user_interests_tbl user_interests join " + getDbName() + "c1_sports_tbl xusersports"
      + " on user_interests.sport_id = xusersports.id group by user_interests.user_id) xusersports on userdim.id = "
      + " xusersports.user_id";
    query1 = getExpectedQuery("testcube",
      "select (usersports.balias0) AS `alias0`, (xusersports.balias0) AS `alias1`, (yusersports.balias0) AS `alias2`, "
        + "sum((testcube.segmsr1)) AS `alias3`, 0 AS `alias4` FROM ", joinExpr, null,
      "group by (usersports.balias0), (xusersports.balias0), (yusersports.balias0), ", null,
      getWhereForDailyAndHourly2days("testcube", "c1_b1fact1"));
    query2 = getExpectedQuery("testcube",
      "select (usersports.balias0) AS `alias0`, (xusersports.balias0) AS `alias1`, (yusersports.balias0) AS `alias2`, "
        + "sum((testcube.segmsr1)) AS `alias3`, 0 AS `alias4` FROM ", joinExpr, null,
      "group by (usersports.balias0), (xusersports.balias0), (yusersports.balias0)", null,
      getWhereForDailyAndHourly2days("testcube", "c0_b2fact1"));
    query3 = getExpectedQuery("testcube",
      "select (usersports.balias0) AS `alias0`, (xusersports.balias0) AS `alias1`, (yusersports.balias0) AS `alias2`, "
        + "0 AS `alias3`, sum(testcube.msr2) AS `alias4` FROM ", joinExpr, null,
      "group by (usersports.balias0), (xusersports.balias0), (yusersports.balias0)", null,
      getWhereForHourly2days("testcube", "c1_testfact2"));
    compareUnionQuery(ctx,
      "select testcube.alias0 AS `name`,testcube.alias1 AS `name`, testcube.alias2 AS `name`, "
        + "sum((testcube.alias3)) AS `segmsr1`, sum((testcube.alias4)) AS `msr2` from ( ",
      ") as testcube group by testcube.alias0, testcube.alias1, testcube.alias2",
      newArrayList(query1, query2, query3));
  }

  @Test
  public void testQueryWithHavingOnInnerMeasure() throws LensException {
    String userQuery = "select cityid from testcube where " + TWO_DAYS_RANGE + " having msr2 > 2 and segmsr1 > 1";
    CubeQueryContext ctx = rewriteCtx(userQuery, getConf());
    String query1, query2, query3;
    query1 = getExpectedQuery("testcube",
      "select testcube.cityid as alias0, 0 as alias1, sum(testcube.segmsr1) as alias2 FROM ", null,
      "group by testcube.cityid",
      getWhereForDailyAndHourly2days("testcube", "c1_b1fact1"));
    query2 = getExpectedQuery("testcube",
      "select testcube.cityid as alias0, 0 as alias1, sum(testcube.segmsr1) as alias2 FROM ", null,
      "group by testcube.cityid",
      getWhereForDailyAndHourly2days("testcube", "c0_b2fact1"));
    query3 = getExpectedQuery("testcube",
      "select testcube.cityid as alias0, sum(testcube.msr2) as alias1, 0 as alias2 FROM ", null,
      "group by testcube.cityid",
      getWhereForHourly2days("testcube", "c1_testfact2"));
    compareUnionQuery(ctx,
      "select testcube.alias0 as cityid from ( ",
      ") as testcube group by testcube.alias0 having sum(testcube.alias1) > 2 and sum(testcube.alias2) > 1",
      newArrayList(query1, query2, query3));
  }

  @Test
  public void testSegmentationWithSingleSegment() throws LensException {
    String userQuery = "select zipcode, segmsr1 from basecube where " + TWO_DAYS_RANGE + " having segmsr1 > 10";
    String actual = rewrite(userQuery, getConf());
    String expected = getExpectedQuery("basecube",
      "select basecube.zipcode, sum(basecube.segmsr1) FROM ", null,
      "group by basecube.zipcode having sum(basecube.segmsr1) > 10",
      getWhereForDailyAndHourly2days("basecube", "c1_b1fact1"));
    compareQueries(actual, expected);
  }
  @Test
  public void testSegmentationPruningWithPruneCause() throws LensException {
    String userQuery = "select segsegmsr1 from testcube where " + TWO_DAYS_RANGE;
    PruneCauses<Candidate> pruneCauses = getBriefAndDetailedError(userQuery, getConf());
    Assert.assertEquals(pruneCauses.getMaxCause(), SEGMENTATION_PRUNED);
    Map<String, String> innerCauses = pruneCauses.getCompact().get("SEG[b1cube; b2cube]")
      .iterator().next().getInnerCauses();
    Assert.assertEquals(innerCauses.size(), 2);
    Assert.assertTrue(innerCauses.get("b1cube").equals("Columns [segsegmsr1] are not present in any table"));
    Assert.assertTrue(innerCauses.get("b2cube").equals("Columns [segsegmsr1] are not present in any table"));
  }
}

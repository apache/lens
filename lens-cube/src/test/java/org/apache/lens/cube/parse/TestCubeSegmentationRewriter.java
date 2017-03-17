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

import static org.apache.lens.cube.metadata.DateFactory.NOW;
import static org.apache.lens.cube.metadata.DateFactory.TWO_DAYS_RANGE;
import static org.apache.lens.cube.metadata.DateFactory.TWO_MONTHS_RANGE_UPTO_DAYS;
import static org.apache.lens.cube.metadata.DateFactory.getDateWithOffset;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.DISABLE_AUTO_JOINS;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY;
import static org.apache.lens.cube.parse.CubeTestSetup.getExpectedQuery;
import static org.apache.lens.cube.parse.CubeTestSetup.getWhereForDailyAndHourly2days;
import static org.apache.lens.cube.parse.CubeTestSetup.getWhereForHourly2days;
import static org.apache.lens.cube.parse.CubeTestSetup.getWhereForUpdatePeriods;
import static org.apache.lens.cube.parse.TestCubeRewriter.compareQueries;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.server.api.LensServerAPITestUtil;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestCubeSegmentationRewriter extends TestQueryRewrite {

  private Configuration conf;

  @BeforeTest
  public void setupDriver() throws Exception {
    conf = LensServerAPITestUtil.getConfiguration(
      DRIVER_SUPPORTED_STORAGES, "C0,C1",
      DISABLE_AUTO_JOINS, true,
      ENABLE_SELECT_TO_GROUPBY, true,
      ENABLE_GROUP_BY_TO_SELECT, true,
      DISABLE_AGGREGATE_RESOLVER, false);
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

  private static void compareUnionQuery(CubeQueryContext cubeql, String begin, String end, List<String> queries) throws LensException {
    final String actualLower = cubeql.toHQL().toLowerCase();
    queries.sort(Comparator.comparing(s -> actualLower.indexOf(extractTableName(s))));
    String expected = queries.stream().collect(Collectors.joining(" UNION ALL ", begin, end));
    compareQueries(actualLower, expected);
  }

  @Test
  public void testSegmentRewrite() throws Exception {
    CubeQueryContext ctx = rewriteCtx("select cityid, segmsr1 from testcube where " + TWO_DAYS_RANGE,
      getConf());
    String query1 = getExpectedQuery("b1cube",
      "select b1cube.cityid as alias0, sum(b1cube.segmsr1) as alias1 FROM ", null,
      "group by b1cube.cityid",
      getWhereForDailyAndHourly2days("b1cube", "c1_b1fact1"));
    String query2 = getExpectedQuery("b2cube",
      "select b2cube.cityid as alias0, sum(b2cube.segmsr1) as alias1 FROM ", null,
      "group by b2cube.cityid",
      getWhereForDailyAndHourly2days("b2cube", "c0_b2fact1"));
    compareUnionQuery(ctx,
      "SELECT (testcube.alias0) as `cityid`, sum((testcube.alias1)) as `segmsr1` FROM (",
      " ) as testcube GROUP BY (testcube.alias0)", Lists.newArrayList(query1, query2));
  }
  // TODO in the end after running other tests
  @Test
  public void testSegmentRewriteWithDifferentStartTimes() throws Exception {

  }

  /*
  Asking for segmsr1 from testcube. segmsr1 is available in b1b2fact and seg1 split over time.
   Inside seg1, Two segments are there: b1cube and b2cube. b1cube has one fact b1fact which
   is split over time across two storages: c1 and c2. b2cube has one fact which answers complete range
   given to it. So total 4 storage candidates should be there.
   */
  @Test
  public void testFactUnionSegmentWithInnerUnion() throws Exception {
    CubeQueryContext ctx = rewriteCtx("select cityid, segmsr1 from testcube where " + TWO_MONTHS_RANGE_UPTO_DAYS,
      getConf());
    String query1,query2,query3,query4;
    query1 = getExpectedQuery("b1cube", "select b1cube.cityid as alias0, sum(b1cube.segmsr1) as alias1 from ",
      null, "group by b1cube.cityid",
      getWhereForUpdatePeriods("b1cube", "c0_b1fact1",
        getDateWithOffset(UpdatePeriod.DAILY, -31), getDateWithOffset(UpdatePeriod.DAILY, -10),
        Sets.newHashSet(UpdatePeriod.MONTHLY, UpdatePeriod.DAILY)));
    query2 = getExpectedQuery("b1cube", "select b1cube.cityid as alias0, sum(b1cube.segmsr1) as alias1 from ",
      null, "group by b1cube.cityid",
      getWhereForUpdatePeriods("b1cube", "c1_b1fact1",
        getDateWithOffset(UpdatePeriod.DAILY, -11), NOW,
        Sets.newHashSet(UpdatePeriod.MONTHLY, UpdatePeriod.DAILY)));
    query3 = getExpectedQuery("b2cube", "select b2cube.cityid as alias0, sum(b2cube.segmsr1) as alias1 from ",
      null, "group by b2cube.cityid",
      getWhereForUpdatePeriods("b2cube", "c0_b2fact1",
        getDateWithOffset(UpdatePeriod.DAILY, -31), NOW,
        Sets.newHashSet(UpdatePeriod.MONTHLY, UpdatePeriod.DAILY)));
    query4 = getExpectedQuery("testcube", "select testcube.cityid as alias0, sum(testcube.segmsr1) as alias1 from ",
      null, "group by testcube.cityid",
      getWhereForUpdatePeriods("testcube", "c0_b1b2fact1",
        getDateWithOffset(UpdatePeriod.DAILY, -60), getDateWithOffset(UpdatePeriod.DAILY, -30),
        Sets.newHashSet(UpdatePeriod.MONTHLY, UpdatePeriod.DAILY)));
    compareUnionQuery(ctx, "select testcube.alias0 as cityid, sum(testcube.alias1) as segmsr1 from (",
      ") AS testcube GROUP BY (testcube.alias0)", Lists.newArrayList(query1, query2, query3, query4));
  }

  @Test
  public void testFactJoinSegmentWithInnerUnion() throws Exception {
    CubeQueryContext ctx = rewriteCtx("select cityid, msr2, segmsr1 from testcube where " + TWO_DAYS_RANGE,
      getConf());
    String query1, query2, query3;
    query1 = getExpectedQuery("b1cube",
      "select b1cube.cityid as alias0, sum(0.0) as alias1, sum(b1cube.segmsr1) as alias2 FROM ", null,
      "group by b1cube.cityid",
      getWhereForDailyAndHourly2days("b1cube", "c1_b1fact1"));
    query2 = getExpectedQuery("b2cube",
      "select b2cube.cityid as alias0, sum(0.0) as alias1, sum(b2cube.segmsr1) as alias2 FROM ", null,
      "group by b2cube.cityid",
      getWhereForDailyAndHourly2days("b2cube", "c0_b2fact1"));
    query3 = getExpectedQuery("testcube",
      "select testcube.cityid as alias0, sum(testcube.msr2) as alias1, sum(0.0) as alias2 FROM ", null,
      "group by testcube.cityid",
      getWhereForHourly2days("testcube", "c1_testfact2"));
    compareUnionQuery(ctx,
      "select testcube.alias0 as cityid, sum(testcube.alias1) as msr2, sum(testcube.alias2) as segmsr1 from ( ",
      ") as testcube group by testcube.alias0", Lists.newArrayList(query1, query2, query3));
  }

  @Test
  public void testExpressionBreakdown() throws Exception {
    CubeQueryContext ctx = rewriteCtx("select cityid, msr2 + segmsr1 from testcube where " + TWO_DAYS_RANGE,
      getConf());

    System.out.println(ctx.toHQL());
  }
}

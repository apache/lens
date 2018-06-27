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
import static org.apache.lens.cube.parse.CubeQueryConfUtil.*;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER;
import static org.apache.lens.cube.parse.CubeTestSetup.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.server.api.LensServerAPITestUtil;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestVirtualFactQueries extends TestQueryRewrite {

  private Configuration conf;

  @BeforeTest
  public void setupDriver() throws Exception {
    conf = LensServerAPITestUtil.getConfiguration(
      DRIVER_SUPPORTED_STORAGES, "C1",
      DISABLE_AUTO_JOINS, false,
      ENABLE_SELECT_TO_GROUPBY, true,
      ENABLE_GROUP_BY_TO_SELECT, true,
      DISABLE_AGGREGATE_RESOLVER, false);
  }

  @Override
  public Configuration getConf() {
    return new Configuration(conf);
  }

  @Test
  public void testVirtualFactDayQuery() throws Exception {
    CubeQueryContext rewrittenQuery =
      rewriteCtx("select SUM(msr1) from virtualCube where " + TWO_DAYS_RANGE, getConfWithStorages("C1"));
    String expected = getExpectedQuery(VIRTUAL_CUBE_NAME, "select sum(virtualcube.msr1) as `sum(msr1)` FROM ",
      null, "AND ( dim1 = 10 )", getWhereForDailyAndHourly2days(VIRTUAL_CUBE_NAME,
        "C1_testfact9_base"));
    String hql = rewrittenQuery.toHQL();
    compareQueries(hql, expected);
  }


  @Test
  public void testVirtualFactColStartTimeQuery() {
    try {
        rewriteCtx("select dim1,SUM(msr1) from virtualcube where " + TWO_DAYS_RANGE, getConfWithStorages("C1"));
        Assert.fail("Should not come here..Column Start time feature is failing..");
    }catch (LensException e) {
      if(e.getErrorCode() == 3024) {
        System.out.println("Expected flow :" + e.getMessage());
      }else {
        Assert.fail("Exception not as expected");
      }
    }
  }

  @Test
  public void testVirtualFactColEndTimeQuery() {
    try {
      rewriteCtx("select dim2,SUM(msr1) from virtualcube where " + TWO_DAYS_RANGE, getConfWithStorages("C1"));
      Assert.fail("Should not come here..Column End time feature is failing..");
    }catch (LensException e) {
      if(e.getErrorCode() == 3024) {
        System.out.println("Expected flow :" + e.getMessage());
      }else {
        Assert.fail("Exception not as expected");
      }
    }
  }


  @Test
  public void testVirtualFactMonthQuery() throws Exception {

    CubeQueryContext rewrittenQuery =
      rewriteCtx("select SUM(msr1) from virtualCube where " + TWO_MONTHS_RANGE_UPTO_HOURS, getConfWithStorages("C1"));
    String expected = getExpectedQuery(VIRTUAL_CUBE_NAME, "select sum(virtualcube.msr1) as `sum(msr1)` FROM ",
      null, "AND ( dim1 = 10 )", getWhereForMonthlyDailyAndHourly2months("virtualcube",
        "C1_testfact9_base"));
    String hql = rewrittenQuery.toHQL();
    compareQueries(hql, expected);
  }

  static void compareQueries(String actual, String expected) {
    assertEquals(new TestQuery(actual), new TestQuery(expected));
  }

  @Test
  public void testVirtualFactUnionQuery() throws Exception {

    String expectedInnerSelect = getExpectedQuery("virtualcube", "SELECT (virtualcube.cityid) AS `alias0`,"
        + " sum((virtualcube.msr2)) AS `alias1`,0 AS `alias2` FROM ",
      null, null, "GROUP BY (virtualcube.cityid)", null,
      getWhereForDailyAndHourly2days("virtualcube", "c1_testfact8_base"))
      + " UNION ALL " + getExpectedQuery("virtualcube", "SELECT (virtualcube.cityid) AS `alias0`,"
        + "0 AS `alias1`, sum((virtualcube.msr3)) AS `alias2` FROM ",
      null, null, "GROUP BY (virtualcube.cityid)", null,
      getWhereForDailyAndHourly2days("virtualcube", "c1_testfact7_base"));


    String expected = "SELECT (virtualcube.alias0) AS `cityid`,"
      + " sum((virtualcube.alias1)) AS `sum(msr2)`, sum((virtualcube.alias2)) AS `sum(msr3)`"
      + " FROM (" + expectedInnerSelect + ") AS virtualcube GROUP BY (virtualcube.alias0)";

    CubeQueryContext rewrittenQuery =
      rewriteCtx("select cityid as `cityid`, SUM(msr2), SUM(msr3) from virtualcube where " + TWO_DAYS_RANGE,
        getConfWithStorages("C1"));
    String hql = rewrittenQuery.toHQL();
    compareQueries(hql, expected);
  }

  @Test
  public void testVirtualFactJoinQuery() throws Exception {
    String query, hqlQuery, expected;

    // Single joinchain with direct link
    query = "select cubestate.name, sum(msr2) from virtualcube where " + TWO_DAYS_RANGE + " group by cubestate.name";
    hqlQuery = rewrite(query, conf);
    expected = getExpectedQuery("virtualcube", "SELECT (cubestate.name) as `name`, sum((virtualcube.msr2)) "
        + "as `sum(msr2)` FROM ",
      " join " + getDbName() + "c1_statetable cubestate ON virtualcube.stateid=cubeState.id and cubeState.dt= 'latest'",
      null, "group by cubestate.name",
      null, getWhereForDailyAndHourly2days("virtualcube", "c1_testfact8_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

  }

  @Test
  public void testVirtualFactValidColumns() throws Exception {

    try {
      rewriteCtx("select SUM(msr4) from virtualCube where " + TWO_DAYS_RANGE, getConfWithStorages("C1"));
      fail("Rewrite should not succeed here");
    } catch (LensException exc) {
      assertEquals(exc.getErrorCode(), LensCubeErrorCode.COLUMN_NOT_FOUND.getLensErrorInfo().getErrorCode());
    }
  }
}


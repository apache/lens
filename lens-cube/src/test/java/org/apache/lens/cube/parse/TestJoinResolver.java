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
import static org.apache.lens.cube.parse.CubeTestSetup.*;
import static org.apache.lens.cube.parse.TestCubeRewriter.compareQueries;

import static org.testng.Assert.*;

import java.util.*;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.*;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ParseException;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestJoinResolver extends TestQueryRewrite {

  private static HiveConf hconf = new HiveConf(TestJoinResolver.class);

  @BeforeTest
  public void setupInstance() throws Exception {
    hconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    hconf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
    hconf.setBoolean(CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT, true);
    hconf.setBoolean(CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY, true);
    hconf.setBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER, false);
    hconf.setBoolean(CubeQueryConfUtil.ENABLE_FLATTENING_FOR_BRIDGETABLES, true);
  }

  @AfterTest
  public void closeInstance() throws Exception {
  }

  private String getAutoResolvedFromString(CubeQueryContext query) throws LensException {
    return query.getHqlContext().getFrom();
  }

  @Test
  public void testAutoJoinResolver() throws Exception {
    HiveConf conf = new HiveConf(hconf);
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER, true);
    // Test 1 Cube + dim
    String query = "select cubeCity.name, dim2chain.name, dim4chain.name, msr2 from testCube where " + TWO_DAYS_RANGE;
    CubeQueryRewriter driver = new CubeQueryRewriter(conf, conf);
    CubeQueryContext rewrittenQuery = driver.rewrite(query);
    String hql = rewrittenQuery.toHQL();
    System.out.println("testAutoJoinResolverauto join HQL:" + hql);
    System.out.println("testAutoJoinResolver@@Resolved join chain:[" + getAutoResolvedFromString(rewrittenQuery) + "]");
    List<String> expectedClauses = new ArrayList<String>();
    expectedClauses.add(getDbName() + "c1_testfact2_raw testcube");
    expectedClauses.add(getDbName()
      + "c1_citytable cubecity on testcube.cityid = cubecity.id and (cubecity.dt = 'latest')");
    expectedClauses.add(getDbName()
      + "c1_testdim2tbl dim2chain on testcube.dim2 = dim2chain.id and (dim2chain.dt = 'latest')");
    expectedClauses.add(getDbName()
      + "c1_testdim3tbl testdim3 on dim2chain.testdim3id = testdim3.id and (testdim3.dt = 'latest')");
    expectedClauses.add(getDbName()
      + "c1_testdim4tbl dim4chain on testdim3.testdim4id = dim4chain.id and (dim4chain.dt = 'latest')");

    List<String> actualClauses = new ArrayList<>();
    for (String clause : StringUtils.splitByWholeSeparator(getAutoResolvedFromString(rewrittenQuery), "join")) {
      if (StringUtils.isNotBlank(clause)) {
        actualClauses.add(clause.trim());
      }
    }
    System.out.println("testAutoJoinResolverExpected1" + expectedClauses);
    System.out.println("testAutoJoinResolverActual1" + actualClauses);
    Assert.assertEqualsNoOrder(expectedClauses.toArray(), actualClauses.toArray());

    // Test 2 Dim only query
    expectedClauses.clear();
    actualClauses.clear();
    String dimOnlyQuery = "select testDim2.name, dim4chain.name FROM testDim2 where " + TWO_DAYS_RANGE;
    rewrittenQuery = driver.rewrite(dimOnlyQuery);
    hql = rewrittenQuery.toHQL();
    System.out.println("testAutoJoinResolverauto join HQL:" + hql);
    System.out.println("testAutoJoinResolver@@Resolved join chain:[" + getAutoResolvedFromString(rewrittenQuery) + "]");
    expectedClauses.add(getDbName() + "c1_testdim2tbl testdim2");
    expectedClauses.add(getDbName()
      + "c1_testdim3tbl testdim3 on testdim2.testdim3id = testdim3.id and (testdim3.dt = 'latest')");
    expectedClauses.add(getDbName()
      + "c1_testdim4tbl dim4chain on testdim3.testdim4id = dim4chain.id and (dim4chain.dt = 'latest')");
    for (String clause : StringUtils.splitByWholeSeparator(getAutoResolvedFromString(rewrittenQuery), "join")) {
      if (StringUtils.isNotBlank(clause)) {
        actualClauses.add(clause.trim());
      }
    }
    System.out.println("testAutoJoinResolverExpected2" + expectedClauses);
    System.out.println("testAutoJoinResolverActual2" + actualClauses);
    Assert.assertEquals(expectedClauses, actualClauses);

    // Test 3 Dim only query should throw error
    String errDimOnlyQuery = "select citydim.id, testDim4.name FROM citydim where " + TWO_DAYS_RANGE;
    getLensExceptionInRewrite(errDimOnlyQuery, hconf);
  }

  @Test
  public void testJoinFilters() throws Exception {
    String query =
      "SELECT citydim.name, testDim4.name, msr2 FROM testCube "
        + " left outer join citydim ON testcube.cityid = citydim .id and citydim.name = 'FOOBAR'"
        + " right outer join testdim2 on testcube.dim2 = testdim2.id "
        + " right outer join testdim3 on testdim2.testdim3id = testdim3.id "
        + " right outer join testDim4 on testdim3.testdim4id = testdim4.id and testDim4.name='TESTDIM4NAME'"
        + " WHERE " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, hconf);
    String expected = getExpectedQuery("testcube", "select citydim.name, testDim4.name, sum(testcube.msr2) FROM ",
      " left outer JOIN " + getDbName() + "c1_citytable citydim on testcube.cityid = citydim.id +"
        + " and (( citydim . name ) =  'FOOBAR' ) and (citydim.dt = 'latest')"
        + " right outer join " + getDbName()
        + "c1_testdim2tbl testdim2 on testcube.dim2 = testdim2.id and (testdim2.dt = 'latest')"
        + " right outer join " + getDbName() + "c1_testdim3tbl testdim3 on testdim2.testdim3id = testdim3.id and "
        + "(testdim3.dt = 'latest') "
        + " right outer join " + getDbName() + "c1_testdim4tbl testdim4 on testdim3.testdim4id = testdim4.id and "
        + "(( testdim4 . name ) =  'TESTDIM4NAME' ) and (testdim4.dt = 'latest')",
      null, "group by citydim.name, testdim4.name", null,
      getWhereForDailyAndHourly2days("testcube", "c1_summary3"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testJoinNotRequired() throws Exception {
    String query = "SELECT msr2 FROM testCube WHERE " + TWO_DAYS_RANGE;
    CubeQueryRewriter driver = new CubeQueryRewriter(hconf, hconf);
    CubeQueryContext ctx = driver.rewrite(query);
    Assert.assertTrue(ctx.getAutoJoinCtx() == null);
  }

  @Test
  public void testJoinWithoutCondition() throws Exception {
    assertLensExceptionInRewrite("SELECT citydim.name, msr2 FROM testCube WHERE " + TWO_DAYS_RANGE, hconf,
      LensCubeErrorCode.NO_JOIN_CONDITION_AVAILABLE);
    assertLensExceptionInRewrite("select cubeState.name, citydim.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE,
      hconf, LensCubeErrorCode.NO_JOIN_CONDITION_AVAILABLE);
    assertLensExceptionInRewrite("select citydim.name, statedim.name from citydim limit 10",
      hconf, LensCubeErrorCode.NO_JOIN_CONDITION_AVAILABLE);
    assertLensExceptionInRewrite("select countrydim.name, citystate.name from citydim limit 10",
      hconf, LensCubeErrorCode.NO_JOIN_CONDITION_AVAILABLE);
  }


  @Test
  public void testJoinTypeConf() throws Exception {
    HiveConf tConf = new HiveConf(hconf);
    tConf.set(CubeQueryConfUtil.JOIN_TYPE_KEY, "LEFTOUTER");
    String query = "select cubecity.name, msr2 FROM testCube WHERE " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, tConf);
    // Check that aliases are preserved in the join clause
    String expected = getExpectedQuery("testcube", "select cubecity.name, sum(testcube.msr2) FROM ",
      " left outer join " + getDbName()
        + "c1_citytable cubecity ON testcube.cityid = cubecity.id and (cubecity.dt = 'latest')",
      null, " group by cubecity.name", null, getWhereForHourly2days("testcube", "c1_testfact2"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    tConf.set(CubeQueryConfUtil.JOIN_TYPE_KEY, "FULLOUTER");
    hqlQuery = rewrite(query, tConf);
    // Check that aliases are preserved in the join clause
    expected = getExpectedQuery("testcube", "select cubecity.name, sum(testcube.msr2) FROM ",
      " full outer join " + getDbName()
        + "c1_citytable cubecity ON testcube.cityid = cubecity.id and (cubecity.dt = 'latest')",
      null, " group by cubecity.name", null, getWhereForHourly2days("testcube", "c1_testfact2"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    tConf.set(CubeQueryConfUtil.JOIN_TYPE_KEY, "RIGHTOUTER");
    hqlQuery = rewrite(query, tConf);
    // Check that aliases are preserved in the join clause
    expected = getExpectedQuery("testcube", "select cubecity.name, sum(testcube.msr2) FROM ",
      " right outer join " + getDbName()
        + "c1_citytable cubecity ON testcube.cityid = cubecity.id",
      null, " and (cubecity.dt = 'latest') group by cubecity.name", null,
      getWhereForHourly2days("testcube", "c1_testfact2"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testPreserveTableAliasWithFullJoin() throws Exception {
    HiveConf tConf = new HiveConf(hconf);
    tConf.set(CubeQueryConfUtil.JOIN_TYPE_KEY, "LEFTOUTER");
    String query = "select c.name, t.msr2 FROM testCube t join citydim c on t.cityid = c.id WHERE " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, tConf);
    // Check that aliases are preserved in the join clause
    // Conf will be ignored in this case since user has specified the join condition
    String expected = getExpectedQuery("t", "select c.name, sum(t.msr2) FROM ",
      " inner join " + getDbName() + "c1_citytable c ON t.cityid = c.id and c.dt = 'latest'",
      null, " group by c.name", null, getWhereForHourly2days("t", "c1_testfact2"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testPreserveTableAliasWithAutoJoin() throws Exception {
    HiveConf tConf = new HiveConf(hconf);
    tConf.set(CubeQueryConfUtil.JOIN_TYPE_KEY, "LEFTOUTER");
    String query = "select cubecity.name, t.msr2 FROM testCube t WHERE " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, tConf);
    // Check that aliases are preserved in the join clause
    String expected = getExpectedQuery("t", "select cubecity.name, sum(t.msr2) FROM ",
      " left outer join " + getDbName()
        + "c1_citytable cubecity ON t.cityid = cubecity.id and (cubecity.dt = 'latest')",
      null, " group by cubecity.name", null, getWhereForHourly2days("t", "c1_testfact2"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testDimOnlyQueryWithAutoJoin() throws Exception {
    HiveConf tConf = new HiveConf(hconf);
    tConf.set(CubeQueryConfUtil.JOIN_TYPE_KEY, "INNER");
    String query = "select citydim.name, citystate.name from citydim limit 10";
    String hqlQuery = rewrite(query, tConf);
    String expected =
      getExpectedQuery("citydim", "select citydim.name, citystate.name from ", " inner join " + getDbName()
          + "c1_statetable citystate on citydim.stateid = citystate.id and (citystate.dt = 'latest')",
        null, " limit 10", "c1_citytable", true);
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testDimOnlyQueryWithFullJoin() throws Exception {
    HiveConf tConf = new HiveConf(hconf);
    tConf.set(CubeQueryConfUtil.JOIN_TYPE_KEY, "INNER");
    String queryWithJoin = "select citydim.name, statedim.name from citydim join statedim on citydim.stateid = "
      + "statedim.id";

    String hqlQuery = rewrite(queryWithJoin, tConf);
    String expected =
      getExpectedQuery("citydim", "select citydim.name, statedim.name from ", " inner join " + getDbName()
          + "c1_statetable statedim on citydim.stateid = statedim.id and citydim.dt='latest' and statedim.dt='latest'",
        null, null, "c1_citytable", false);
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testStorageFilterPushdownWithFullJoin() throws Exception {
    String q1 = "SELECT citydim.name, statedim.name FROM citydim left outer join statedim on citydim.stateid = "
      + "statedim.id";
    String hqlQuery = rewrite(q1, hconf);
    String expected =
      getExpectedQuery("citydim", "select citydim.name, statedim.name from ", " left outer join " + getDbName()
          + "c1_statetable statedim on citydim.stateid = statedim.id and citydim.dt='latest' and statedim.dt='latest'",
        null, null, "c1_citytable", false);
    compareQueries(hqlQuery, expected);

    String q2 = "SELECT citydim.name, statedim.name FROM citydim right outer join statedim on citydim.stateid = "
      + "statedim.id";
    hqlQuery = rewrite(q2, hconf);
    expected =
      getExpectedQuery("citydim", "select citydim.name, statedim.name from ", " right outer join " + getDbName()
          + "c1_statetable statedim on citydim.stateid = statedim.id and citydim.dt='latest' and statedim.dt='latest'",
        null, null, "c1_citytable", false);
    compareQueries(hqlQuery, expected);

    String q3 = "SELECT citydim.name, statedim.name FROM citydim full outer join statedim on citydim.stateid = "
      + "statedim.id";
    hqlQuery = rewrite(q3, hconf);
    expected =
      getExpectedQuery("citydim", "select citydim.name, statedim.name from ", " full outer join " + getDbName()
          + "c1_statetable statedim on citydim.stateid = statedim.id and citydim.dt='latest' and statedim.dt='latest'",
        null, null, "c1_citytable", false);
    compareQueries(hqlQuery, expected);

  }

  @Test
  public void testStorageFilterPushdownWithAutoJoin() throws Exception {
    String q = "SELECT citydim.name, citystate.name FROM citydim limit 10";
    HiveConf conf = new HiveConf(hconf);
    conf.set(CubeQueryConfUtil.JOIN_TYPE_KEY, "LEFTOUTER");
    String hqlQuery = rewrite(q, conf);
    String expected =
      getExpectedQuery("citydim", "select citydim.name, citystate.name from ", " left outer join " + getDbName()
          + "c1_statetable citystate on citydim.stateid = citystate.id and (citystate.dt = 'latest')",
        null, " limit 10", "c1_citytable", true);
    compareQueries(hqlQuery, expected);

    conf.set(CubeQueryConfUtil.JOIN_TYPE_KEY, "RIGHTOUTER");
    hqlQuery = rewrite(q, conf);
    expected =
      getExpectedQuery("citydim", "select citydim.name, citystate.name from ", " right outer join " + getDbName()
          + "c1_statetable citystate on citydim.stateid = citystate.id and (citydim.dt = 'latest')",
        " citystate.dt='latest' ", "limit 10", "c1_citytable", false);
    compareQueries(hqlQuery, expected);

    conf.set(CubeQueryConfUtil.JOIN_TYPE_KEY, "FULLOUTER");
    hqlQuery = rewrite(q, conf);
    expected =
      getExpectedQuery("citydim", "select citydim.name, citystate.name from ", " full outer join " + getDbName()
        + "c1_statetable citystate on citydim.stateid = citystate.id and (citydim.dt = 'latest')"
        + " and citystate.dt='latest'", null, "limit 10", "c1_citytable", false);
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testJoinChains() throws ParseException, LensException, HiveException {
    String query, hqlQuery, expected;

    // Single joinchain with direct link
    query = "select cubestate.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE + " group by cubestate.name";
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("basecube", "select cubestate.name, sum(basecube.msr2) FROM ",
      " join " + getDbName() + "c1_statetable cubestate ON basecube.stateid=cubeState.id and cubeState.dt= 'latest'",
      null, "group by cubestate.name",
      null, getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // Single joinchain with two chains
    query = "select citystate.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE + " group by citystate.name";
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("basecube", "select citystate.name, sum(basecube.msr2) FROM ",
      " join " + getDbName() + "c1_citytable citydim ON baseCube.cityid = citydim.id and citydim.dt = 'latest'"
        + " join " + getDbName() + "c1_statetable cityState ON citydim.stateid=cityState.id and cityState.dt= 'latest'",
      null, "group by citystate.name",
      null, getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // Single joinchain with two chains, accessed as refcolumn
    query = "select cityStateCapital, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("basecube", "select citystate.capital, sum(basecube.msr2) FROM ",
      " join " + getDbName() + "c1_citytable citydim ON baseCube.cityid = citydim.id and citydim.dt = 'latest'"
        + " join " + getDbName() + "c1_statetable cityState ON citydim.stateid=cityState.id and cityState.dt= 'latest'",
      null, "group by citystate.capital",
      null, getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // Same test, Accessing refcol as a column of cube
    query = "select basecube.cityStateCapital, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // Adding Order by
    query = "select cityStateCapital, sum(msr2) from basecube where " + TWO_DAYS_RANGE + " order by cityStateCapital";
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("basecube", "select citystate.capital, sum(basecube.msr2) FROM ",
      " join " + getDbName() + "c1_citytable citydim ON baseCube.cityid = citydim.id and citydim.dt = 'latest'"
        + " join " + getDbName() + "c1_statetable cityState ON citydim.stateid=cityState.id and cityState.dt= 'latest'",
      null, "group by citystate.capital order by citystate.capital asc",
      null, getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // Single joinchain, but one column accessed as refcol and another as chain.column
    query = "select citystate.name, cityStateCapital, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("basecube", "select citystate.name, citystate.capital, sum(basecube.msr2) FROM ",
      " join " + getDbName() + "c1_citytable citydim ON baseCube.cityid = citydim.id and citydim.dt = 'latest'"
        + " join " + getDbName() + "c1_statetable cityState ON citydim.stateid=cityState.id and cityState.dt= 'latest'",
      null, "group by citystate.name, citystate.capital",
      null, getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // Two unrelated join chains
    query = "select cubeState.name, cubecity.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("basecube",
      "select cubestate.name, cubecity.name, sum(basecube.msr2) FROM ",
      " join " + getDbName() + "c1_statetable cubestate on basecube.stateid = cubestate.id and cubestate.dt = 'latest'"
        + " join " + getDbName() + "c1_citytable cubecity on basecube.cityid = cubecity.id and cubecity.dt = 'latest'",
      null, "group by cubestate.name,cubecity.name", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base")
    );
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // Multiple join chains with same destination table
    query = "select cityState.name, cubeState.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("basecube", "select citystate.name, cubestate.name, sum(basecube.msr2) FROM ",
      " join " + getDbName() + "c1_statetable cubestate on basecube.stateid=cubestate.id and cubestate.dt='latest'"
        + " join " + getDbName() + "c1_citytable citydim on basecube.cityid = citydim.id and "
        + "citydim.dt = 'latest'"
        + " join " + getDbName() + "c1_statetable citystate on citydim.stateid = citystate.id and "
        + "citystate.dt = 'latest'",
      null, "group by citystate.name,cubestate.name", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base")
    );
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // Two joinchains, one accessed as refcol.
    query = "select cubestate.name, cityStateCapital, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("basecube",
      "select cubestate.name, citystate.capital, sum(basecube.msr2) FROM ",
      ""
        + " join " + getDbName() + "c1_statetable cubestate on basecube.stateid=cubestate.id and cubestate.dt='latest'"
        + " join " + getDbName() + "c1_citytable citydim on basecube.cityid = citydim.id and citydim.dt = 'latest'"
        + " join " + getDbName() + "c1_statetable citystate on citydim.stateid=citystate.id and citystate.dt='latest'"
        + ""
      , null, "group by cubestate.name, citystate.capital", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base")
    );
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // Two joinchains with initial path common. Testing merging of chains
    query = "select cityState.name, cityZip.f1, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("basecube",
      "select citystate.name, cityzip.f1, sum(basecube.msr2) FROM ",
      " join " + getDbName() + "c1_citytable citydim on basecube.cityid = citydim.id and "
        + "citydim.dt = 'latest'"
        + " join " + getDbName() + "c1_statetable citystate on citydim.stateid = citystate.id and "
        + "citystate.dt = 'latest'"
        + " join " + getDbName() + "c1_ziptable cityzip on citydim.zipcode = cityzip.code and "
        + "cityzip.dt = 'latest'"
      , null, "group by citystate.name,cityzip.f1", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base")
    );
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // Two joinchains with common intermediate dimension, but different paths to that common dimension
    // checking aliasing
    query = "select cubeStateCountry.name, cubeCityStateCountry.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("basecube",
      "select cubestatecountry.name, cubecitystatecountry.name, sum(basecube.msr2) FROM ",
      ""
        + " join " + getDbName() + "c1_citytable citydim on basecube.cityid = citydim.id and (citydim.dt = 'latest')"
        + " join " + getDbName()
        + "c1_statetable statedim_0 on citydim.stateid=statedim_0.id and statedim_0.dt='latest'"
        + " join " + getDbName()
        + "c1_countrytable cubecitystatecountry on statedim_0.countryid=cubecitystatecountry.id"
        + " join " + getDbName() + "c1_statetable statedim on basecube.stateid=statedim.id and (statedim.dt = 'latest')"
        + " join " + getDbName() + "c1_countrytable cubestatecountry on statedim.countryid=cubestatecountry.id "
        + "", null, "group by cubestatecountry.name, cubecitystatecountry.name", null,
      getWhereForDailyAndHourly2days("basecube", "c1_testfact1_base")
    );
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // Test 4 Dim only query with join chains

    List<String> expectedClauses = new ArrayList<>();
    List<String> actualClauses = new ArrayList<>();
    String dimOnlyQuery = "select testDim2.name, testDim2.cityStateCapital FROM testDim2 where " + TWO_DAYS_RANGE;
    CubeQueryRewriter driver = new CubeQueryRewriter(hconf, hconf);
    CubeQueryContext rewrittenQuery = driver.rewrite(dimOnlyQuery);
    String hql = rewrittenQuery.toHQL();
    System.out.println("testAutoJoinResolverauto join HQL:" + hql);
    System.out.println("testAutoJoinResolver@@Resolved join chain:[" + getAutoResolvedFromString(rewrittenQuery) + "]");
    expectedClauses.add(getDbName() + "c1_testdim2tbl testdim2");
    expectedClauses.add(getDbName()
      + "c1_citytable citydim on testdim2.cityid = citydim.id and (citydim.dt = 'latest')");
    expectedClauses.add(getDbName()
      + "c1_statetable citystate on citydim.stateid = citystate.id and (citystate.dt = 'latest')");


    for (String clause : StringUtils.splitByWholeSeparator(getAutoResolvedFromString(rewrittenQuery), "join")) {
      if (StringUtils.isNotBlank(clause)) {
        actualClauses.add(clause.trim());
      }
    }
    System.out.println("testDimOnlyJoinChainExpected1 : " + expectedClauses);
    System.out.println("testDimOnlyJoinChainActual1 : " + actualClauses);
    Assert.assertEquals(expectedClauses, actualClauses);

    //Dim only join chain query without qualified tableName for join chain ref column
    actualClauses.clear();
    dimOnlyQuery = "select name, cityStateCapital FROM testDim2 where " + TWO_DAYS_RANGE;
    driver = new CubeQueryRewriter(hconf, hconf);
    rewrittenQuery = driver.rewrite(dimOnlyQuery);
    hql = rewrittenQuery.toHQL();
    System.out.println("testAutoJoinResolverauto join HQL:" + hql);
    System.out.println("testAutoJoinResolver@@Resolved join chain:[" + getAutoResolvedFromString(rewrittenQuery) + "]");


    for (String clause : StringUtils.splitByWholeSeparator(getAutoResolvedFromString(rewrittenQuery), "join")) {
      if (StringUtils.isNotBlank(clause)) {
        actualClauses.add(clause.trim());
      }
    }
    System.out.println("testDimOnlyJoinChainExpected1 : " + expectedClauses);
    System.out.println("testDimOnlyJoinChainActual1 : " + actualClauses);
    Assert.assertEquals(expectedClauses, actualClauses);


    //With ChainRef.col
    actualClauses.clear();
    dimOnlyQuery = "select testDim2.name, cityState.capital FROM testDim2 where " + TWO_DAYS_RANGE;
    driver = new CubeQueryRewriter(hconf, hconf);
    rewrittenQuery = driver.rewrite(dimOnlyQuery);
    hql = rewrittenQuery.toHQL();
    System.out.println("testAutoJoinResolverauto join HQL:" + hql);
    System.out.println("testAutoJoinResolver@@Resolved join chain:[" + getAutoResolvedFromString(rewrittenQuery) + "]");


    for (String clause : StringUtils.splitByWholeSeparator(getAutoResolvedFromString(rewrittenQuery), "join")) {
      if (StringUtils.isNotBlank(clause)) {
        actualClauses.add(clause.trim());
      }
    }
    System.out.println("testDimOnlyJoinChainExpected1 : " + expectedClauses);
    System.out.println("testDimOnlyJoinChainActual1 : " + actualClauses);
    Assert.assertEquals(expectedClauses, actualClauses);
  }

  @Test
  public void testConflictingJoins() throws ParseException, LensException, HiveException {
    // Single joinchain with two paths, intermediate dimension accessed separately by name.
    String query = "select cityState.name, citydim.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    assertLensExceptionInRewrite(query, hconf, LensCubeErrorCode.NO_JOIN_CONDITION_AVAILABLE);

    // Multi joinchains + a dimension part of one of the chains.
    query = "select cityState.name, cubeState.name, citydim.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    assertLensExceptionInRewrite(query, hconf, LensCubeErrorCode.NO_JOIN_CONDITION_AVAILABLE);

    // this test case should pass when default qualifiers for dimensions' chains are added
    // Two joinchains with same destination, and the destination table accessed separately
    query = "select cityState.name, cubeState.name, statedim.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    assertLensExceptionInRewrite(query, hconf, LensCubeErrorCode.NO_JOIN_CONDITION_AVAILABLE);

    // this test case should pass when default qualifiers for dimensions' chains are added
    // Two Single joinchain, And dest table accessed separately.
    query = "select cubeState.name, statedim.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    assertLensExceptionInRewrite(query, hconf, LensCubeErrorCode.NO_JOIN_CONDITION_AVAILABLE);

    // this should pass when default qualifiers are added
    query = "select cityStateCapital, statedim.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    assertLensExceptionInRewrite(query, hconf, LensCubeErrorCode.NO_JOIN_CONDITION_AVAILABLE);

    // table accessed through denorm column and chain column
    Configuration conf = new Configuration(hconf);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C3, C4");
    String failingQuery = "select testDim2.cityname, testDim2.cityStateCapital FROM testDim2 where " + TWO_DAYS_RANGE;
    assertLensExceptionInRewrite(failingQuery, conf, LensCubeErrorCode.NO_REF_COL_AVAILABLE);
  }

  @Test
  public void testMultiPaths() throws ParseException, LensException, HiveException {
    String query, hqlQuery, expected;

    query = "select dim3chain.name, sum(msr2) from testcube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("testcube", "select dim3chain.name, sum(testcube.msr2) FROM ",
      " join " + getDbName() + "c1_testdim3tbl dim3chain ON testcube.testdim3id=dim3chain.id and dim3chain.dt='latest'",
      null, "group by dim3chain.name",
      null, getWhereForDailyAndHourly2days("testcube", "c1_summary1"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // hit a fact where there is no direct path
    query = "select dim3chain.name, avg(msr2) from testcube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("testcube", "select dim3chain.name, avg(testcube.msr2) FROM ",
      " join " + getDbName() + "c1_testdim2tbl testdim2 ON testcube.dim2 = testdim2.id and testdim2.dt = 'latest'"
        + " join " + getDbName() + "c1_testdim3tbl dim3chain "
        + "ON testdim2.testdim3id = dim3chain.id and dim3chain.dt = 'latest'",
      null, "group by dim3chain.name",
      null, getWhereForHourly2days("testcube", "c1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // resolve denorm variable through multi hop chain paths
    query = "select testdim3id, avg(msr2) from testcube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("testcube", "select dim3chain.id, avg(testcube.msr2) FROM ",
      " join " + getDbName() + "c1_testdim2tbl testdim2 ON testcube.dim2 = testdim2.id and testdim2.dt = 'latest'"
        + " join " + getDbName() + "c1_testdim3tbl dim3chain "
        + "ON testdim2.testdim3id = dim3chain.id and dim3chain.dt = 'latest'",
      null, "group by dim3chain.id",
      null, getWhereForHourly2days("testcube", "c1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // tests from multiple different chains
    query = "select dim4chain.name, testdim3id, avg(msr2) from testcube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("testcube", "select dim4chain.name, dim3chain.id, avg(testcube.msr2) FROM ",
      " join " + getDbName() + "c1_testdim2tbl testdim2 ON testcube.dim2 = testdim2.id and testdim2.dt = 'latest'"
        + " join " + getDbName()
        + "c1_testdim3tbl dim3chain ON testdim2.testdim3id=dim3chain.id and dim3chain.dt='latest'"
        + " join " + getDbName() + "c1_testdim4tbl dim4chain ON dim3chain.testDim4id = dim4chain.id and"
        + " dim4chain.dt = 'latest'", null, "group by dim4chain.name, dim3chain.id", null,
      getWhereForHourly2days("testcube", "c1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    query = "select cubecity.name, dim4chain.name, testdim3id, avg(msr2) from testcube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("testcube", "select cubecity.name, dim4chain.name, dim3chain.id, avg(testcube.msr2) "
        + "FROM ",
      " join " + getDbName() + "c1_testdim2tbl testdim2 ON testcube.dim2 = testdim2.id and testdim2.dt = 'latest'"
        + " join " + getDbName()
        + "c1_testdim3tbl dim3chain ON testdim2.testdim3id=dim3chain.id and dim3chain.dt='latest'"
        + " join " + getDbName() + "c1_testdim4tbl dim4chain ON dim3chain.testDim4id = dim4chain.id and"
        + " dim4chain.dt = 'latest'"
        + " join " + getDbName() + "c1_citytable cubecity ON testcube.cityid = cubecity.id and cubecity.dt = 'latest'"
      , null, "group by cubecity.name, dim4chain.name, dim3chain.id", null,
      getWhereForHourly2days("testcube", "c1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // test multi hops
    query = "select dim4chain.name, avg(msr2) from testcube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("testcube", "select dim4chain.name, avg(testcube.msr2) FROM ",
      " join " + getDbName() + "c1_testdim2tbl testdim2 ON testcube.dim2 = testdim2.id and testdim2.dt = 'latest'"
        + " join " + getDbName() + "c1_testdim3tbl testdim3 ON testdim2.testdim3id=testdim3.id and testdim3.dt='latest'"
        + " join " + getDbName() + "c1_testdim4tbl dim4chain ON testdim3.testDim4id = dim4chain.id and"
        + " dim4chain.dt = 'latest'", null, "group by dim4chain.name", null,
      getWhereForHourly2days("testcube", "c1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    query = "select dim4chain.name, sum(msr2) from testcube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("testcube", "select dim4chain.name, sum(testcube.msr2) FROM ",
      " join " + getDbName() + "c1_testdim3tbl testdim3 ON testcube.testdim3id = testdim3.id and testdim3.dt = 'latest'"
        + " join " + getDbName() + "c1_testdim4tbl dim4chain ON testdim3.testDim4id = dim4chain.id and"
        + " dim4chain.dt = 'latest'", null, "group by dim4chain.name", null,
      getWhereForDailyAndHourly2days("testcube", "c1_summary1"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);
  }

  @Test
  public void testChainsWithMultipleStorage() throws ParseException, HiveException, LensException {
    Configuration conf = new Configuration(hconf);
    conf.unset(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES); // supports all storages
    String dimOnlyQuery = "select testDim2.name, testDim2.cityStateCapital FROM testDim2 where " + TWO_DAYS_RANGE;
    CubeQueryRewriter driver = new CubeQueryRewriter(conf, hconf);
    CubeQueryContext rewrittenQuery = driver.rewrite(dimOnlyQuery);
    rewrittenQuery.toHQL();
    Dimension citydim = CubeMetastoreClient.getInstance(hconf).getDimension("citydim");
    Set<String> cdimTables = new HashSet<>();
    for (CandidateDim cdim : rewrittenQuery.getCandidateDims().get(citydim)) {
      cdimTables.add(cdim.getName());
    }
    Assert.assertTrue(cdimTables.contains("citytable"));
    Assert.assertTrue(cdimTables.contains("citytable2"));
    Assert.assertFalse(cdimTables.contains("citytable3"));
    Assert.assertFalse(cdimTables.contains("citytable4"));
  }

  @Test
  public void testUnreachableDim() throws ParseException, LensException, HiveException {
    assertLensExceptionInRewrite("select urdimid from testdim2", hconf, LensCubeErrorCode.NO_DIM_HAS_COLUMN);
    assertLensExceptionInRewrite("select urdimid from testcube where " + TWO_DAYS_RANGE, hconf,
      LensCubeErrorCode.NO_FACT_HAS_COLUMN);
    assertLensExceptionInRewrite("select unreachableName from testdim2", hconf,
      LensCubeErrorCode.NO_DIM_HAS_COLUMN);
    assertLensExceptionInRewrite("select unreachableName from testcube where " + TWO_DAYS_RANGE, hconf,
      LensCubeErrorCode.NO_CANDIDATE_FACT_AVAILABLE);
    assertLensExceptionInRewrite("select unreachableDim_chain.name from testdim2", hconf,
      LensCubeErrorCode.NO_JOIN_PATH);
    assertLensExceptionInRewrite("select unreachableDim_chain.name from testcube where " + TWO_DAYS_RANGE, hconf,
      LensCubeErrorCode.NO_FACT_HAS_COLUMN);
  }
}

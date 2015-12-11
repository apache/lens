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

import static org.testng.Assert.*;

import java.util.*;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.*;
import org.apache.lens.cube.metadata.SchemaGraph.TableRelationship;
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
  private CubeMetastoreClient metastore;

  @BeforeTest
  public void setupInstance() throws Exception {
    hconf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    hconf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
    hconf.setBoolean(CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT, true);
    hconf.setBoolean(CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY, true);
    hconf.setBoolean(CubeQueryConfUtil.ENABLE_FLATTENING_FOR_BRIDGETABLES, true);
    this.metastore = CubeMetastoreClient.getInstance(hconf);
  }

  @AfterTest
  public void closeInstance() throws Exception {
  }

  // testBuildGraph - graph correctness
  @Test
  public void testBuildGraph() throws Exception {
    SchemaGraph schemaGraph = metastore.getSchemaGraph();
    CubeInterface cube = metastore.getCube(CubeTestSetup.TEST_CUBE_NAME);
    Map<AbstractCubeTable, Set<TableRelationship>> graph = schemaGraph.getCubeGraph(cube);
    printGraph(graph);
    Assert.assertNotNull(graph);

    // Let's do some lookups
    Set<TableRelationship> dim4Edges = graph.get(metastore.getDimension("testdim4"));
    Assert.assertNull(dim4Edges);
    dim4Edges = graph.get(metastore.getDimension("testdim3"));
    Assert.assertNotNull(dim4Edges);
    Assert.assertEquals(1, dim4Edges.size());

    List<TableRelationship> edges = new ArrayList<TableRelationship>(dim4Edges);
    TableRelationship dim4edge = edges.get(0);
    Assert.assertEquals("id", dim4edge.getToColumn());
    Assert.assertEquals(metastore.getDimension("testdim4"), dim4edge.getToTable());
    Assert.assertEquals("testdim4id", dim4edge.getFromColumn());
    Assert.assertEquals(metastore.getDimension("testdim3"), dim4edge.getFromTable());
  }

  private void searchPaths(AbstractCubeTable source, AbstractCubeTable target, SchemaGraph graph) {
    SchemaGraph.GraphSearch search = new SchemaGraph.GraphSearch(source, target, graph);
    List<SchemaGraph.JoinPath> joinPaths = search.findAllPathsToTarget();

    System.out.println("@@ " + source + " ==> " + target + " paths =");
    int i = 0;
    for (SchemaGraph.JoinPath jp : joinPaths) {
      Assert.assertEquals(jp.getEdges().get(0).getToTable(), source);
      Assert.assertEquals(jp.getEdges().get(jp.getEdges().size() - 1).getFromTable(), target);
      Collections.reverse(jp.getEdges());
      System.out.println(++i + " " + jp.getEdges());
    }
  }

  @Test
  public void testFindChain() throws Exception {
    SchemaGraph schemaGraph = metastore.getSchemaGraph();
    schemaGraph.print();

    // Search For all cubes and all dims to make sure that search terminates
    for (CubeInterface cube : metastore.getAllCubes()) {
      for (Dimension dim : metastore.getAllDimensions()) {
        searchPaths(dim, (AbstractCubeTable) cube, schemaGraph);
      }
    }

    for (Dimension dim : metastore.getAllDimensions()) {
      for (Dimension otherDim : metastore.getAllDimensions()) {
        if (otherDim != dim) {
          searchPaths(dim, otherDim, schemaGraph);
        }
      }
    }

    // Assert for testcube
    CubeInterface testCube = metastore.getCube("testcube");
    Dimension zipDim = metastore.getDimension("zipdim");
    Dimension cityDim = metastore.getDimension("citydim");
    Dimension testDim2 = metastore.getDimension("testDim2");

    SchemaGraph.GraphSearch search = new SchemaGraph.GraphSearch(zipDim, (AbstractCubeTable) testCube, schemaGraph);

    List<SchemaGraph.JoinPath> paths = search.findAllPathsToTarget();
    Assert.assertEquals(6, paths.size());
    validatePath(paths.get(0), zipDim, (AbstractCubeTable) testCube);
    validatePath(paths.get(1), zipDim, cityDim, (AbstractCubeTable) testCube);
    validatePath(paths.get(2), zipDim, cityDim, testDim2, (AbstractCubeTable) testCube);
    validatePath(paths.get(3), zipDim, cityDim, testDim2, (AbstractCubeTable) testCube);
    validatePath(paths.get(4), zipDim, cityDim, testDim2, (AbstractCubeTable) testCube);
    validatePath(paths.get(5), zipDim, cityDim, testDim2, (AbstractCubeTable) testCube);
  }

  private void validatePath(SchemaGraph.JoinPath jp, AbstractCubeTable... tables) {
    Assert.assertTrue(!jp.getEdges().isEmpty());
    Set<AbstractCubeTable> expected = new HashSet<AbstractCubeTable>(Arrays.asList(tables));
    Set<AbstractCubeTable> actual = new HashSet<AbstractCubeTable>();
    for (TableRelationship edge : jp.getEdges()) {
      if (edge.getFromTable() != null) {
        actual.add(edge.getFromTable());
      }
      if (edge.getToTable() != null) {
        actual.add(edge.getToTable());
      }
    }

    Assert.assertEquals(expected, actual,
      "Edges: " + jp.getEdges().toString() + " Expected Tables: " + Arrays.toString(tables) + " Actual Tables: "
        + actual.toString());
  }

  private void printGraph(Map<AbstractCubeTable, Set<TableRelationship>> graph) {
    System.out.println("--Graph-Nodes=" + graph.size());
    for (AbstractCubeTable tab : graph.keySet()) {
      System.out.println(tab.getName() + "::" + graph.get(tab));
    }
  }

  private String getAutoResolvedFromString(CubeQueryContext query) throws LensException {
    return query.getHqlContext().getFrom();
  }

  @Test
  public void testAutoJoinResolver() throws Exception {
    // Test 1 Cube + dim
    String query = "select citydim.name, testDim2.name, testDim4.name, msr2 from testCube where " + TWO_DAYS_RANGE;
    CubeQueryRewriter driver = new CubeQueryRewriter(hconf, hconf);
    CubeQueryContext rewrittenQuery = driver.rewrite(query);
    String hql = rewrittenQuery.toHQL();
    System.out.println("testAutoJoinResolverauto join HQL:" + hql);
    System.out.println("testAutoJoinResolver@@Resolved join chain:[" + getAutoResolvedFromString(rewrittenQuery) + "]");
    List<String> expectedClauses = new ArrayList<String>();
    expectedClauses.add(getDbName() + "c1_testfact2_raw testcube");
    expectedClauses.add(getDbName()
      + "c1_citytable citydim on testcube.cityid = citydim.id and (citydim.dt = 'latest')");
    expectedClauses.add(getDbName()
      + "c1_testdim2tbl testdim2 on testcube.dim2 = testdim2.id and (testdim2.dt = 'latest')");
    expectedClauses.add(getDbName()
      + "c1_testdim3tbl testdim3 on testdim2.testdim3id = testdim3.id and (testdim3.dt = 'latest')");
    expectedClauses.add(getDbName()
      + "c1_testdim4tbl testdim4 on testdim3.testdim4id = testdim4.id and (testdim4.dt = 'latest')");

    List<String> actualClauses = new ArrayList<String>();
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
    String dimOnlyQuery = "select testDim2.name, testDim4.name FROM testDim2 where " + TWO_DAYS_RANGE;
    rewrittenQuery = driver.rewrite(dimOnlyQuery);
    hql = rewrittenQuery.toHQL();
    System.out.println("testAutoJoinResolverauto join HQL:" + hql);
    System.out.println("testAutoJoinResolver@@Resolved join chain:[" + getAutoResolvedFromString(rewrittenQuery) + "]");
    expectedClauses.add(getDbName() + "c1_testdim2tbl testdim2");
    expectedClauses.add(getDbName()
      + "c1_testdim3tbl testdim3 on testdim2.testdim3id = testdim3.id and (testdim3.dt = 'latest')");
    expectedClauses.add(getDbName()
      + "c1_testdim4tbl testdim4 on testdim3.testdim4id = testdim4.id and (testdim4.dt = 'latest')");
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
  public void testPartialJoinResolver() throws Exception {
    String query =
      "SELECT citydim.name, testDim4.name, msr2 "
        + "FROM testCube left outer join citydim ON citydim.name = 'FOOBAR'"
        + " right outer join testDim4 on testDim4.name='TESTDIM4NAME'" + " WHERE " + TWO_DAYS_RANGE;
    CubeQueryRewriter driver = new CubeQueryRewriter(hconf, hconf);
    CubeQueryContext rewrittenQuery = driver.rewrite(query);
    String hql = rewrittenQuery.toHQL();
    System.out.println("testPartialJoinResolver Partial join hql: " + hql);
    String partSQL =
      " left outer join " + getDbName() + "c1_citytable citydim on testcube.cityid "
        + "= citydim.id and (( citydim . name ) =  'FOOBAR' ) " + "and (citydim.dt = 'latest')";
    Assert.assertTrue(hql.contains(partSQL));
    partSQL =
      " right outer join " + getDbName() + "c1_testdim2tbl testdim2 on "
        + "testcube.dim2 = testdim2.id right outer join " + getDbName()
        + "c1_testdim3tbl testdim3 on testdim2.testdim3id = testdim3.id and "
        + "(testdim2.dt = 'latest') right outer join " + getDbName()
        + "c1_testdim4tbl testdim4 on testdim3.testdim4id = testdim4.id and "
        + "(( testdim4 . name ) =  'TESTDIM4NAME' ) and (testdim3.dt = 'latest')";

    Assert.assertTrue(hql.contains(partSQL));
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
    String query = "SELECT citydim.name, msr2 FROM testCube WHERE " + TWO_DAYS_RANGE;
    CubeQueryRewriter driver = new CubeQueryRewriter(hconf, hconf);
    CubeQueryContext ctx = driver.rewrite(query);
    String hql = ctx.toHQL();
    String joinClause = getAutoResolvedFromString(ctx);
    System.out.println("@Resolved join clause " + joinClause);
    Assert.assertEquals(getDbName() + "c1_testfact2_raw testcube join " + getDbName() + "c1_citytable citydim on "
      + "testcube.cityid = citydim.id and (citydim.dt = 'latest')", joinClause.trim());
  }

  @Test
  public void testJoinTypeConf() throws Exception {
    HiveConf tConf = new HiveConf(hconf);
    tConf.set(CubeQueryConfUtil.JOIN_TYPE_KEY, "LEFTOUTER");
    System.out.println("@@Set join type to " + hconf.get(CubeQueryConfUtil.JOIN_TYPE_KEY));
    CubeQueryRewriter driver = new CubeQueryRewriter(tConf, hconf);
    String query = "select citydim.name, msr2 FROM testCube WHERE " + TWO_DAYS_RANGE;
    CubeQueryContext ctx = driver.rewrite(query);
    String hql = ctx.toHQL();
    System.out.println("testJoinTypeConf@@Resolved join clause1 - " + getAutoResolvedFromString(ctx));
    Assert.assertEquals(getDbName() + "c1_testfact2_raw testcube left outer join " + getDbName()
        + "c1_citytable citydim on testcube.cityid = citydim.id and (citydim.dt = 'latest')",
      getAutoResolvedFromString(ctx).trim());

    tConf.set(CubeQueryConfUtil.JOIN_TYPE_KEY, "FULLOUTER");
    System.out.println("@@Set join type to " + hconf.get(CubeQueryConfUtil.JOIN_TYPE_KEY));
    driver = new CubeQueryRewriter(tConf, hconf);
    ctx = driver.rewrite(query);
    hql = ctx.toHQL();
    System.out.println("testJoinTypeConf@@Resolved join clause2 - " + getAutoResolvedFromString(ctx));
    Assert.assertEquals(getDbName() + "c1_testfact2_raw testcube full outer join " + getDbName()
        + "c1_citytable citydim on testcube.cityid = citydim.id and (citydim.dt = 'latest')",
      getAutoResolvedFromString(ctx).trim());
  }

  @Test
  public void testPreserveTableAlias() throws Exception {
    HiveConf tConf = new HiveConf(hconf);
    tConf.set(CubeQueryConfUtil.JOIN_TYPE_KEY, "LEFTOUTER");
    String query = "select c.name, t.msr2 FROM testCube t join citydim c WHERE " + TWO_DAYS_RANGE;
    CubeQueryRewriter driver = new CubeQueryRewriter(tConf, hconf);
    CubeQueryContext ctx = driver.rewrite(query);
    String hql = ctx.toHQL();
    System.out.println("testPreserveTableAlias@@HQL:" + hql);
    System.out.println("testPreserveTableAlias@@Resolved join clause - " + getAutoResolvedFromString(ctx));
    // Check that aliases are preserved in the join clause
    // Conf will be ignored in this case since user has specified partial join
    Assert.assertEquals(getDbName() + "c1_testfact2_raw t inner join " + getDbName()
      + "c1_citytable c on t.cityid = c.id and (c.dt = 'latest')", getAutoResolvedFromString(ctx).trim());
    String whereClause = hql.substring(hql.indexOf("WHERE"));
    // Check that the partition condition is not added again in where clause
    Assert.assertFalse(whereClause.contains("c.dt = 'latest'"));
  }

  @Test
  public void testDimOnlyQuery() throws Exception {
    HiveConf tConf = new HiveConf(hconf);
    tConf.set(CubeQueryConfUtil.JOIN_TYPE_KEY, "INNER");
    String query = "select citydim.name, statedim.name from citydim limit 10";
    HiveConf dimOnlyConf = new HiveConf(tConf);
    CubeQueryRewriter rewriter = new CubeQueryRewriter(dimOnlyConf, hconf);
    CubeQueryContext ctx = rewriter.rewrite(query);
    String hql = ctx.toHQL();
    System.out.println("testDimOnlyQuery@@@HQL:" + hql);
    System.out.println("testDimOnlyQuery@@@Resolved join clause: " + getAutoResolvedFromString(ctx));
    Assert.assertTrue(hql.matches(".*?WHERE\\W+citydim.dt = 'latest'\\W+LIMIT 10.*?"));
    Assert.assertEquals(getDbName() + "c1_citytable citydim inner join " + getDbName()
        + "c1_statetable statedim on citydim.stateid = statedim.id and (statedim.dt = 'latest')",
      getAutoResolvedFromString(ctx).trim());

    String queryWithJoin = "select citydim.name, statedim.name from citydim join statedim";
    ctx = rewriter.rewrite(queryWithJoin);
    hql = ctx.toHQL();
    System.out.println("testDimOnlyQuery@@@HQL2:" + hql);
    HQLParser.parseHQL(hql, tConf);
    Assert.assertEquals(getDbName() + "c1_citytable citydim inner join " + getDbName()
        + "c1_statetable statedim on citydim.stateid = statedim.id and (statedim.dt = 'latest')",
      getAutoResolvedFromString(ctx).trim());
  }

  @Test
  public void testStorageFilterPushdown() throws Exception {
    String q = "SELECT citydim.name, statedim.name FROM citydim";
    HiveConf conf = new HiveConf(hconf);
    conf.set(CubeQueryConfUtil.JOIN_TYPE_KEY, "LEFTOUTER");
    CubeQueryRewriter rewriter = new CubeQueryRewriter(conf, hconf);
    CubeQueryContext context = rewriter.rewrite(q);
    String hql = context.toHQL();
    System.out.println("##1 hql " + hql);
    System.out.println("##1 " + getAutoResolvedFromString(context));
    Assert.assertEquals(getDbName() + "c1_citytable citydim left outer join " + getDbName()
        + "c1_statetable statedim on citydim.stateid = statedim.id" + " and (statedim.dt = 'latest')",
      getAutoResolvedFromString(context).trim());
    Assert.assertTrue(hql.matches(".*?WHERE\\W+citydim.dt = 'latest'\\W+.*?"));

    conf.set(CubeQueryConfUtil.JOIN_TYPE_KEY, "RIGHTOUTER");
    rewriter = new CubeQueryRewriter(conf, hconf);
    context = rewriter.rewrite(q);
    hql = context.toHQL();
    System.out.println("##2 hql " + hql);
    System.out.println("##2 " + getAutoResolvedFromString(context));
    Assert.assertEquals(getDbName() + "c1_citytable citydim right outer join " + getDbName()
        + "c1_statetable statedim on citydim.stateid = statedim.id " + "and (citydim.dt = 'latest')",
      getAutoResolvedFromString(context).trim());
    Assert.assertTrue(hql.matches(".*?WHERE\\W+statedim.dt = 'latest'\\W+.*?"));

    conf.set(CubeQueryConfUtil.JOIN_TYPE_KEY, "FULLOUTER");
    rewriter = new CubeQueryRewriter(conf, hconf);
    context = rewriter.rewrite(q);
    hql = context.toHQL();
    System.out.println("##3 hql " + hql);
    System.out.println("##3 " + getAutoResolvedFromString(context));
    Assert.assertEquals(getDbName() + "c1_citytable citydim full outer join " + getDbName()
      + "c1_statetable statedim on citydim.stateid = statedim.id "
      + "and (citydim.dt = 'latest') and (statedim.dt = 'latest')", getAutoResolvedFromString(context).trim());
    Assert.assertTrue(!hql.contains("WHERE"));
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

    // Single join chain and an unrelated dimension
    query = "select cubeState.name, citydim.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("basecube",
      "select cubestate.name, citydim.name, sum(basecube.msr2) FROM ",
      " join " + getDbName() + "c1_statetable cubestate on basecube.stateid = cubestate.id and cubestate.dt = 'latest'"
        + " join " + getDbName() + "c1_citytable citydim on basecube.cityid = citydim.id and citydim.dt = 'latest'",
      null, "group by cubestate.name,citydim.name", null,
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

    List<String> expectedClauses = new ArrayList<String>();
    List<String> actualClauses = new ArrayList<String>();
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
    try {
      rewrite(query, hconf);
      Assert.fail("Should have failed. "
        + "The table citydim is getting accessed as both chain and without chain ");
    } catch (LensException e) {
      Assert.assertEquals(e.getMessage().toLowerCase(),
        "Table citydim is getting accessed via joinchain: citystate and no chain at all".toLowerCase());
    }

    // Multi joinchains + a dimension part of one of the chains.
    query = "select cityState.name, cubeState.name, citydim.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    try {
      rewrite(query, hconf);
      Assert.fail("Should have failed. "
        + "The table citydim is getting accessed as both chain and without chain ");
    } catch (LensException e) {
      Assert.assertEquals(e.getMessage().toLowerCase(),
        "Table citydim is getting accessed via joinchain: citystate and no chain at all".toLowerCase());
    }

    // this test case should pass when default qualifiers for dimensions' chains are added
    // Two joinchains with same destination, and the destination table accessed separately
    query = "select cityState.name, cubeState.name, statedim.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    try {
      rewrite(query, hconf);
      Assert.fail("Should have failed. "
        + "It's not possible to resolve which statedim is being asked for when cityState and cubeState both end at"
        + " statedim table.");
    } catch (LensException e) {
      Assert.assertEquals(
        e.getMessage().indexOf("Table statedim has 2 different paths through joinchains"), 0);
    }

    // this test case should pass when default qualifiers for dimensions' chains are added
    // Two Single joinchain, And dest table accessed separately.
    query = "select cubeState.name, statedim.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    try {
      rewrite(query, hconf);
      Assert.fail("Should have failed. "
        + "The table statedim is getting accessed as both cubeState and statedim ");
    } catch (LensException e) {
      Assert.assertEquals(e.getMessage().toLowerCase(),
        "Table statedim is getting accessed via two different names: [cubestate, statedim]".toLowerCase());
    }
    // this should pass when default qualifiers are added
    query = "select cityStateCapital, statedim.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    try {
      rewrite(query, hconf);
      Assert.fail("Should have failed. "
        + "The table statedim is getting accessed as both cubeState and statedim ");
    } catch (LensException e) {
      Assert.assertEquals(e.getMessage().toLowerCase(),
        "Table statedim is getting accessed via two different names: [citystate, statedim]".toLowerCase());
    }

    // table accessed through denorm column and chain column
    Configuration conf = new Configuration(hconf);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C3, C4");
    String failingQuery = "select testDim2.cityname, testDim2.cityStateCapital FROM testDim2 where " + TWO_DAYS_RANGE;
    try {
      rewrite(failingQuery, conf);
      Assert.fail("Should have failed. "
        + "The table citydim is getting accessed as both chain and without chain ");
    } catch (LensException e) {
      Assert.assertEquals(e.getMessage().toLowerCase(),
        "Table citydim is getting accessed via joinchain: citystate and no chain at all".toLowerCase());
    }
  }

  @Test
  public void testMultiPaths() throws ParseException, LensException, HiveException {
    String query, hqlQuery, expected;

    query = "select testdim3.name, sum(msr2) from testcube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("testcube", "select testdim3.name, sum(testcube.msr2) FROM ",
      " join " + getDbName() + "c1_testdim3tbl testdim3 ON testcube.testdim3id=testdim3.id and testdim3.dt='latest'",
      null, "group by testdim3.name",
      null, getWhereForDailyAndHourly2days("testcube", "c1_summary1"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // hit a fact where there is no direct path
    query = "select testdim3.name, avg(msr2) from testcube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("testcube", "select testdim3.name, avg(testcube.msr2) FROM ",
      " join " + getDbName() + "c1_testdim2tbl testdim2 ON testcube.dim2 = testdim2.id and testdim2.dt = 'latest'"
        + " join " + getDbName() + "c1_testdim3tbl testdim3 "
        + "ON testdim2.testdim3id = testdim3.id and testdim3.dt = 'latest'",
      null, "group by testdim3.name",
      null, getWhereForHourly2days("testcube", "c1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // resolve denorm variable through multi hop chain paths
    query = "select testdim3id, avg(msr2) from testcube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("testcube", "select testdim3.id, avg(testcube.msr2) FROM ",
      " join " + getDbName() + "c1_testdim2tbl testdim2 ON testcube.dim2 = testdim2.id and testdim2.dt = 'latest'"
        + " join " + getDbName() + "c1_testdim3tbl testdim3 "
        + "ON testdim2.testdim3id = testdim3.id and testdim3.dt = 'latest'",
      null, "group by testdim3.id",
      null, getWhereForHourly2days("testcube", "c1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // tests from multiple different chains
    query = "select testdim4.name, testdim3id, avg(msr2) from testcube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("testcube", "select testdim4.name, testdim3.id, avg(testcube.msr2) FROM ",
      " join " + getDbName() + "c1_testdim2tbl testdim2 ON testcube.dim2 = testdim2.id and testdim2.dt = 'latest'"
        + " join " + getDbName() + "c1_testdim3tbl testdim3 ON testdim2.testdim3id=testdim3.id and testdim3.dt='latest'"
        + " join " + getDbName() + "c1_testdim4tbl testdim4 ON testdim3.testDim4id = testdim4.id and"
        + " testdim4.dt = 'latest'", null, "group by testdim4.name, testdim3.id", null,
      getWhereForHourly2days("testcube", "c1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    query = "select citydim.name, testdim4.name, testdim3id, avg(msr2) from testcube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("testcube", "select citydim.name, testdim4.name, testdim3.id, avg(testcube.msr2) FROM ",
      " join " + getDbName() + "c1_testdim2tbl testdim2 ON testcube.dim2 = testdim2.id and testdim2.dt = 'latest'"
        + " join " + getDbName() + "c1_testdim3tbl testdim3 ON testdim2.testdim3id=testdim3.id and testdim3.dt='latest'"
        + " join " + getDbName() + "c1_testdim4tbl testdim4 ON testdim3.testDim4id = testdim4.id and"
        + " testdim4.dt = 'latest'"
        + " join " + getDbName() + "c1_citytable citydim ON testcube.cityid = citydim.id and citydim.dt = 'latest'"
      , null, "group by citydim.name, testdim4.name, testdim3.id", null,
      getWhereForHourly2days("testcube", "c1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    // test multi hops
    query = "select testdim4.name, avg(msr2) from testcube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("testcube", "select testdim4.name, avg(testcube.msr2) FROM ",
      " join " + getDbName() + "c1_testdim2tbl testdim2 ON testcube.dim2 = testdim2.id and testdim2.dt = 'latest'"
        + " join " + getDbName() + "c1_testdim3tbl testdim3 ON testdim2.testdim3id=testdim3.id and testdim3.dt='latest'"
        + " join " + getDbName() + "c1_testdim4tbl testdim4 ON testdim3.testDim4id = testdim4.id and"
        + " testdim4.dt = 'latest'", null, "group by testdim4.name", null,
      getWhereForHourly2days("testcube", "c1_testfact2_raw"));
    TestCubeRewriter.compareQueries(hqlQuery, expected);

    query = "select testdim4.name, sum(msr2) from testcube where " + TWO_DAYS_RANGE;
    hqlQuery = rewrite(query, hconf);
    expected = getExpectedQuery("testcube", "select testdim4.name, sum(testcube.msr2) FROM ",
      " join " + getDbName() + "c1_testdim3tbl testdim3 ON testcube.testdim3id = testdim3.id and testdim3.dt = 'latest'"
        + " join " + getDbName() + "c1_testdim4tbl testdim4 ON testdim3.testDim4id = testdim4.id and"
        + " testdim4.dt = 'latest'", null, "group by testdim4.name", null,
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
    Set<String> cdimTables = new HashSet<String>();
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
    LensException e1 = getLensExceptionInRewrite("select urdimid from testdim2", hconf);
    assertNotNull(e1);
    assertEquals(e1.getErrorCode(), LensCubeErrorCode.NO_DIM_HAS_COLUMN.getLensErrorInfo().getErrorCode());

    LensException e2 = getLensExceptionInRewrite("select urdimid from testcube where " + TWO_DAYS_RANGE, hconf);
    assertNotNull(e2);
    assertEquals(e2.getErrorCode(), LensCubeErrorCode.NO_CANDIDATE_FACT_AVAILABLE.getLensErrorInfo().getErrorCode());
  }

  @Test
  public void testBridgeTablesWithoutDimtablePartitioning() throws Exception {
    Configuration conf = new Configuration(hconf);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    String query = "select usersports.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, conf);
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
  }

  @Test
  public void testFlattenBridgeTablesOFF() throws Exception {
    Configuration conf = new Configuration(hconf);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
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
  }

  @Test
  public void testFlattenBridgeTablesWithCustomAggregate() throws Exception {
    Configuration conf = new Configuration(hconf);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
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
  }

  @Test
  public void testBridgeTablesWithMegringChains() throws Exception {
    Configuration conf = new Configuration(hconf);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    String query = "select userInterestIds.sport_id, usersports.name, sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, conf);
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
  }

  @Test
  public void testBridgeTablesWithMultipleFacts() throws Exception {
    Configuration conf = new Configuration(hconf);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    String query = "select usersports.name, sum(msr2), sum(msr12) from basecube where " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, conf);
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
  }

  @Test
  public void testBridgeTablesWithMultipleChains() throws Exception {
    Configuration conf = new Configuration(hconf);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    String query = "select usersports.name, xusersports.name, yusersports.name, sum(msr2) from basecube where "
      + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, conf);
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
  }
  @Test
  public void testBridgeTablesWithDimTablePartitioning() throws Exception {
    Configuration conf = new Configuration(hconf);
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
  }

  @Test
  public void testBridgeTablesWithNormalJoins() throws Exception {
    Configuration conf = new Configuration(hconf);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    String query = "select usersports.name, cubestatecountry.name, cubecitystatecountry.name,"
      + " sum(msr2) from basecube where " + TWO_DAYS_RANGE;
    String hqlQuery = rewrite(query, conf);
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
  }
}

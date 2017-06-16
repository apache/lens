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
package org.apache.lens.server.rewrite;

import java.util.*;

import org.apache.lens.api.LensConf;
import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.cube.parse.CubeQueryContext;
import org.apache.lens.cube.parse.CubeQueryRewriter;
import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.cube.parse.QueryWriterContext;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.driver.MockDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.metrics.LensMetricsRegistry;
import org.apache.lens.server.api.query.QueryContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockObjectFactory;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Maps;

/**
 * The Class TestRewriting.
 */
@PrepareForTest(RewriteUtil.class)
@PowerMockIgnore({"org.apache.log4j.*", "javax.management.*", "javax.xml.*",
  "org.apache.hadoop.*", "org.apache.hive.*", "org.antlr.*",
  "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*", "org.w3c.dom*", "org.mockito.*"})
public class TestRewriting {
  /**
   * We need a special {@link IObjectFactory}.
   *
   * @return {@link PowerMockObjectFactory}.
   */
  @ObjectFactory
  public IObjectFactory getObjectFactory() {
    return new PowerMockObjectFactory();
  }

  private HiveConf hconf = new HiveConf();

  static int i = 0;
  // number of successful queries through mock rewriter
  // we use this number to mock failures after successful queries
  // change the number, if more tests for success needs to be added
  static final int NUM_SUCCESS = 40;

  public static CubeMetastoreClient getMockedClient() {
    CubeMetastoreClient client = Mockito.mock(CubeMetastoreClient.class);
    Mockito.when(client.isLensQueryableTable(Matchers.any(String.class))).thenAnswer(new Answer<Boolean>() {

      @Override
      public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
        return invocationOnMock.getArguments()[0].toString().toLowerCase().contains("cube");
      }
    });
    return client;
  }

  private CubeQueryRewriter getMockedRewriter() throws ParseException, LensException, HiveException {
    CubeQueryRewriter mockwriter = Mockito.mock(CubeQueryRewriter.class);
    Mockito.when(mockwriter.rewrite(Matchers.any(String.class))).thenAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      // return query for first NUM_SUCCESS calls and fail later
      if (++i <= NUM_SUCCESS) {
        return getMockedCubeContext((String) args[0]);
      } else {
        throw new RuntimeException("Mock fail");
      }
    });
    Mockito.when(mockwriter.rewrite(Matchers.any(ASTNode.class))).thenAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      return getMockedCubeContext((ASTNode) args[0]);
    });
    Mockito.doCallRealMethod().when(mockwriter).clear();
    return mockwriter;
  }

  /**
   * Gets the mocked cube context.
   *
   * @param query the query
   * @return the mocked cube context
   * @throws LensException the lens exception
   * @throws ParseException    the parse exception
   */
  private CubeQueryContext getMockedCubeContext(String query)
    throws ParseException, LensException {
    CubeQueryContext context = Mockito.mock(CubeQueryContext.class);
    QueryWriterContext mockQueryWriterContext = Mockito.mock(QueryWriterContext.class);
    Mockito.when(mockQueryWriterContext.getDimsToQuery()).thenReturn(Maps.newHashMap());
    Mockito.when(context.getQueryWriterContext()).thenReturn(mockQueryWriterContext);
    Mockito.when(context.toHQL()).thenReturn(query.substring(4));
    Mockito.when(context.toAST(Matchers.any(Context.class)))
      .thenReturn(HQLParser.parseHQL(query.toLowerCase().replaceFirst("^cube", ""), hconf));
    return context;
  }

  /**
   * Gets the mocked cube context.
   *
   * @param ast the ast
   * @return the mocked cube context
   * @throws ParseException    the parse exception
   * @throws LensException  the lens exception
   */
  private CubeQueryContext getMockedCubeContext(ASTNode ast) throws ParseException, LensException {
    CubeQueryContext context = Mockito.mock(CubeQueryContext.class);
    if (ast.getToken().getType() == HiveParser.TOK_QUERY) {
      if (((ASTNode) ast.getChild(0)).getToken().getType() == HiveParser.KW_CUBE) {
        // remove cube child from AST
        for (int i = 0; i < ast.getChildCount() - 1; i++) {
          ast.setChild(i, ast.getChild(i + 1));
        }
        ast.deleteChild(ast.getChildCount() - 1);
      }
    }
    StringBuilder builder = new StringBuilder();
    HQLParser.toInfixString(ast, builder);
    Mockito.when(context.toHQL()).thenReturn(builder.toString());
    Mockito.when(context.toAST(Matchers.any(Context.class))).thenReturn(ast);
    return context;
  }

  private void runRewrites(Map<LensDriver, RewriteUtil.DriverRewriterRunnable> runnableMap) {
    for (LensDriver driver : runnableMap.keySet()) {
      RewriteUtil.DriverRewriterRunnable r = runnableMap.get(driver);
      r.run();
      Assert.assertTrue(r.getDriver() == driver);

      // Failure cause should be set only when rewrite fails
      if (r.isSucceeded()) {
        Assert.assertNull(r.getFailureCause(), driver
          + " succeeded but failure cause is set to " + r.getFailureCause());
        Assert.assertNotNull(r.getRewrittenQuery(), driver + " succeeded but rewritten query is not set");
      } else {
        Assert.assertNotNull(r.getFailureCause(), driver + " failed but failure cause is not set");
        Assert.assertNull(r.getRewrittenQuery(),
          driver + " failed but rewritten query is set to " + r.getRewrittenQuery());
      }

    }
  }

  /**
   * Test cube query.
   *
   * @throws ParseException    the parse exception
   * @throws LensException     the lens exception
   * @throws HiveException
   */
  @Test
  public void testCubeQuery() throws ParseException, LensException, HiveException {
    SessionState.start(hconf);
    List<LensDriver> drivers = new ArrayList<LensDriver>();
    MockDriver driver = new MockDriver();
    LensConf lensConf = new LensConf();
    Configuration conf = new Configuration();
    driver.configure(conf, null, null);
    drivers.add(driver);
    List<RewriteUtil.CubeQueryInfo> cubeQueries;
    QueryContext ctx;
    String q1, q2;
    CubeQueryRewriter mockWriter = getMockedRewriter();
    CubeMetastoreClient mockClient = getMockedClient();
    PowerMockito.stub(PowerMockito.method(RewriteUtil.class, "getCubeRewriter")).toReturn(mockWriter);
    PowerMockito.stub(PowerMockito.method(RewriteUtil.class, "getClient")).toReturn(mockClient);

    q1 = "select name from table";
    cubeQueries = RewriteUtil.findCubePositions(q1, hconf);
    Assert.assertEquals(cubeQueries.size(), 0);
    ctx = new QueryContext(q1, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));

    conf.set(LensConfConstants.QUERY_METRIC_UNIQUE_ID_CONF_KEY, TestRewriting.class.getSimpleName());
    driver.configure(conf, null, null);
    q2 = "select name from cube_table";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, q2);
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));
    MetricRegistry reg = LensMetricsRegistry.getStaticRegistry();

    Assert.assertTrue(reg.getGauges().keySet().containsAll(Arrays.asList(
      "lens.MethodMetricGauge.TestRewriting-" + driver.getFullyQualifiedName() + "-RewriteUtil-rewriteQuery",
      "lens.MethodMetricGauge.TestRewriting-" + driver.getFullyQualifiedName() + "-1-RewriteUtil-rewriteQuery-toHQL")));
    conf.unset(LensConfConstants.QUERY_METRIC_UNIQUE_ID_CONF_KEY);

    assertIsCubeQuery("select name from cube_table", lensConf, conf, drivers);
    assertIsCubeQuery("select cube_table.name from cube_table", lensConf, conf, drivers);
    assertIsCubeQuery("select cube_dim.name, cube_dim2.name from cube_dim "
      + "join cube_dim2 on cube_dim.d2id=cube_dim2.id", lensConf, conf, drivers);
    assertIsCubeQuery("select distinct cube_dim.name, cube_dim2.name from cube_dim full outer "
      + "join cube_dim2 on cube_dim.d2id=cube_dim2.id", lensConf, conf, drivers);
    assertIsCubeQuery("select cube_dim.name, cube_dim2.name from cube_dim left outer "
      + "join cube_dim2 on cube_dim.d2id=cube_dim2.id", lensConf, conf, drivers);
    assertIsCubeQuery("select cube_dim.name, cube_dim2.name from cube_dim inner "
      + "join cube_dim2 on cube_dim.d2id=cube_dim2.id", lensConf, conf, drivers);

    q2 = "select * from (select name from cube_table) table1";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "select name from cube_table");
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));

    q2 = "insert overwrite directory 'target/rewrite' select name from cube_table";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "select name from cube_table");
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));

    q2 = "insert overwrite local directory 'target/example-output' select id,name from cube_dim_table";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "select id,name from cube_dim_table");
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));

    q2 = "explain select name from cube_table";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "select name from cube_table");
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));

    q2 = "select * from (select name from cube_table) a";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "select name from cube_table");
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));

    q2 = "insert overwrite directory 'target/rewrite' select * from (select name from cube_table) a";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "select name from cube_table");
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));

    q2 = "select * from (select name from cube_table)a";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "select name from cube_table");
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));

    q2 = "select * from  (  select name from cube_table   )     a";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "select name from cube_table");
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));

    q2 = "select * from  (  select name from cube_table   )     a";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "select name from cube_table");
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));

    q2 = "select * from (      select name from cube_table where"
      + " (name = 'ABC'||name = 'XYZ')&&(key=100)   )       a";
    cubeQueries = RewriteUtil.findCubePositions(RewriteUtil.getReplacedQuery(q2), hconf);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "select name from"
      + " cube_table where (name = 'ABC' OR name = 'XYZ') AND (key=100)");
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));

    conf.set(LensConfConstants.QUERY_METRIC_UNIQUE_ID_CONF_KEY, TestRewriting.class.getSimpleName() + "-multiple");
    q2 = "select * from (select name from cube_table) a join (select" + " name2 from cube_table2) b";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    Assert.assertEquals(cubeQueries.size(), 2);
    Assert.assertEquals(cubeQueries.get(0).query, "select name from cube_table");
    Assert.assertEquals(cubeQueries.get(1).query, "select name2 from cube_table2");
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));
    reg = LensMetricsRegistry.getStaticRegistry();
    Assert.assertTrue(reg.getGauges().keySet().containsAll(Arrays.asList(
      "lens.MethodMetricGauge.TestRewriting-" + driver.getFullyQualifiedName() + "-1-RewriteUtil-rewriteQuery-toHQL",
      "lens.MethodMetricGauge.TestRewriting-multiple-" + driver.getFullyQualifiedName()
        + "-2-RewriteUtil-rewriteQuery-toHQL",
      "lens.MethodMetricGauge.TestRewriting-multiple-" + driver.getFullyQualifiedName()
        + "-RewriteUtil-rewriteQuery")));
    conf.unset(LensConfConstants.QUERY_METRIC_UNIQUE_ID_CONF_KEY);

    q2 = "select * from (select name from cube_table) a full outer join"
      + " (select name2 from cube_table2) b on a.name=b.name2";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    Assert.assertEquals(cubeQueries.size(), 2);
    Assert.assertEquals(cubeQueries.get(0).query, "select name from cube_table");
    Assert.assertEquals(cubeQueries.get(1).query, "select name2 from cube_table2");
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));

    q2 = "select * from (select name from cube_table) a join (select name2 from table2) b";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "select name from cube_table");
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));

    q2 = "select * from (select name from cube_table) a join (select name2 from cube_table2) b";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    Assert.assertEquals(cubeQueries.size(), 2);
    Assert.assertEquals(cubeQueries.get(0).query, "select name from cube_table");
    Assert.assertEquals(cubeQueries.get(1).query, "select name2 from cube_table2");
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));

    q2 = "select * from (select name from cube_table union all select name2 from cube_table2) u";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));
    Assert.assertEquals(cubeQueries.size(), 2);
    Assert.assertEquals(cubeQueries.get(0).query, "select name from cube_table");
    Assert.assertEquals(cubeQueries.get(1).query, "select name2 from cube_table2");

    q2 = "select * from (select name from cube_table union all select distinct name2 from cube_table2) u";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));
    Assert.assertEquals(cubeQueries.size(), 2);
    Assert.assertEquals(cubeQueries.get(0).query, "select name from cube_table");
    Assert.assertEquals(cubeQueries.get(1).query, "select distinct name2 from cube_table2");

    q2 = "select * from (select distinct name from cube_table union all select name2 from cube_table2) u";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));
    Assert.assertEquals(cubeQueries.size(), 2);
    Assert.assertEquals(cubeQueries.get(0).query, "select distinct name from cube_table");
    Assert.assertEquals(cubeQueries.get(1).query, "select name2 from cube_table2");

    q2 = "insert overwrite directory 'target/rewrite' "
      + "select * from (select name from cube_table union all select name2 from cube_table2) u";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));
    Assert.assertEquals(cubeQueries.size(), 2);
    Assert.assertEquals(cubeQueries.get(0).query, "select name from cube_table");
    Assert.assertEquals(cubeQueries.get(1).query, "select name2 from cube_table2");

    q2 = "select u.* from (select name from table    union all       select name2 from cube_table2)   u";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "select name2 from cube_table2");
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));

    q2 = "select u.* from (select name from table union all select name2 from cube_table2)u";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "select name2 from cube_table2");
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));

    q2 = "select * from (select name from cube_table union all select name2"
      + " from cube_table2 union all select name3 from cube_table3) u";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));
    Assert.assertEquals(cubeQueries.size(), 3);
    Assert.assertEquals(cubeQueries.get(0).query, "select name from cube_table");
    Assert.assertEquals(cubeQueries.get(1).query, "select name2 from cube_table2");
    Assert.assertEquals(cubeQueries.get(2).query, "select name3 from cube_table3");

    q2 = "select * from (select name from cube_table union all select" + " name2 from cube_table2) u group by u.name";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    Assert.assertEquals(cubeQueries.size(), 2);
    Assert.assertEquals(cubeQueries.get(0).query, "select name from cube_table");
    Assert.assertEquals(cubeQueries.get(1).query, "select name2 from cube_table2");
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));

    q2 = "select * from (select name from cube_table union all select" + " name2 from cube_table2)  u group by u.name";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));
    Assert.assertEquals(cubeQueries.size(), 2);
    Assert.assertEquals(cubeQueries.get(0).query, "select name from cube_table");
    Assert.assertEquals(cubeQueries.get(1).query, "select name2 from cube_table2");

    q2 = "create table temp1 as select name from cube_table";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "select name from cube_table");

    q2 = "create table temp1 as select * from (select name from cube_table union all select"
      + " name2 from cube_table2)  u group by u.name";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));
    Assert.assertEquals(cubeQueries.size(), 2);
    Assert.assertEquals(cubeQueries.get(0).query, "select name from cube_table");
    Assert.assertEquals(cubeQueries.get(1).query, "select name2 from cube_table2");

    q2 = "create table temp1 as select name from cube_table where"
      + " time_range_in('dt', '2014-06-24-23', '2014-06-25-00')";
    cubeQueries = RewriteUtil.findCubePositions(q2, hconf);
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query,
      "select name from cube_table where time_range_in('dt', '2014-06-24-23', '2014-06-25-00')");

    q2 = "SELECT case when a='b' then 'c' else 'd' end some_field from cube_table";
    cubeQueries = RewriteUtil.findCubePositions(q2 + " union all " + q2, hconf);

    Assert.assertEquals(cubeQueries.size(), 2);
    Assert.assertEquals(cubeQueries.get(0).query, q2);
    Assert.assertEquals(cubeQueries.get(1).query, q2);

    // failing query for second driver
    MockDriver driver2 = new MockDriver();
    driver2.configure(conf, null, null);
    drivers.add(driver2);

    Assert.assertEquals(drivers.size(), 2);
    Assert.assertTrue(drivers.contains(driver) && drivers.contains(driver2));
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    Map<LensDriver, RewriteUtil.DriverRewriterRunnable> dQueries = RewriteUtil.rewriteQuery(ctx);
    Assert.assertEquals(dQueries.size(), 2);

    // Check both driver runnables are present
    Assert.assertNotNull(dQueries.get(driver), driver + " runnable not found");
    Assert.assertEquals(dQueries.get(driver).getDriver(), driver);

    Assert.assertNotNull(dQueries.get(driver2), driver2 + " runnable not found");
    Assert.assertEquals(dQueries.get(driver2).getDriver(), driver2);

    runRewrites(dQueries);

    // We have to verify that query failed for the second driver
    // First driver passes
    Iterator<LensDriver> itr = dQueries.keySet().iterator();
    itr.next();
    LensDriver failedDriver = itr.next();
    Assert.assertFalse(dQueries.get(failedDriver).isSucceeded(), failedDriver + " rewrite should have failed");
    Assert.assertNull(dQueries.get(failedDriver).getRewrittenQuery(),
      failedDriver + " rewritten query should not be set");
    // running again will fail on both drivers
    ctx = new QueryContext(q2, null, lensConf, conf, drivers);
    Map<LensDriver, RewriteUtil.DriverRewriterRunnable> runnables = RewriteUtil.rewriteQuery(ctx);
    runRewrites(runnables);

    Assert.assertFalse(runnables.get(driver).isSucceeded());
    Assert.assertNotNull(runnables.get(driver).getFailureCause());
    Assert.assertNull(runnables.get(driver).getRewrittenQuery());
    Assert.assertNotNull(ctx.getDriverRewriteError(driver));

    Assert.assertFalse(runnables.get(driver2).isSucceeded());
    Assert.assertNotNull(runnables.get(driver2).getFailureCause());
    Assert.assertNull(runnables.get(driver2).getRewrittenQuery());
    Assert.assertNotNull(ctx.getDriverRewriteError(driver2));
  }

  private void assertIsCubeQuery(String query, LensConf lensConf, Configuration conf, List<LensDriver> drivers)
    throws LensException {
    List<RewriteUtil.CubeQueryInfo> cubeQueries = RewriteUtil.findCubePositions(query, hconf);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, query);
    QueryContext ctx = new QueryContext(query, null, lensConf, conf, drivers);
    runRewrites(RewriteUtil.rewriteQuery(ctx));
  }
}

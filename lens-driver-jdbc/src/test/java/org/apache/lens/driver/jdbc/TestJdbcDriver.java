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
package org.apache.lens.driver.jdbc;

import static org.testng.Assert.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensException;
import org.apache.lens.api.query.QueryCost;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.ResultRow;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.*;
import org.apache.lens.server.api.driver.DriverQueryStatus.DriverQueryState;
import org.apache.lens.server.api.metrics.LensMetricsRegistry;
import org.apache.lens.server.api.query.ExplainQueryContext;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.util.LensUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.ColumnDescriptor;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;
import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * The Class TestJdbcDriver.
 */
public class TestJdbcDriver {
  public static final Log LOG = LogFactory.getLog(TestJdbcDriver.class);

  /** The base conf. */
  Configuration baseConf;

  HiveConf hConf;
  /** The driver. */
  JDBCDriver driver;

  Collection<LensDriver> drivers;

  /**
   * Test create jdbc driver.
   *
   * @throws Exception the exception
   */
  @BeforeTest
  public void testCreateJdbcDriver() throws Exception {
    baseConf = new Configuration();
    baseConf.set(JDBCDriverConfConstants.JDBC_DRIVER_CLASS, "org.hsqldb.jdbc.JDBCDriver");
    baseConf.set(JDBCDriverConfConstants.JDBC_DB_URI, "jdbc:hsqldb:mem:jdbcTestDB");
    baseConf.set(JDBCDriverConfConstants.JDBC_USER, "SA");
    baseConf.set(JDBCDriverConfConstants.JDBC_PASSWORD, "");
    baseConf.set(JDBCDriverConfConstants.JDBC_EXPLAIN_KEYWORD_PARAM, "explain plan for ");
    hConf = new HiveConf(baseConf, this.getClass());

    driver = new JDBCDriver();
    driver.configure(baseConf);
    assertNotNull(driver);
    assertTrue(driver.configured);

    drivers = new ArrayList<LensDriver>() {
      {
        add(driver);
      }
    };
  }

  /**
   * Close.
   *
   * @throws Exception the exception
   */
  @AfterTest
  public void close() throws Exception {
    driver.close();
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    if (SessionState.get() == null) {
      SessionState.start(new HiveConf(baseConf, TestJdbcDriver.class));
    }
  }

  private QueryContext createQueryContext(final String query) throws LensException {
    return createQueryContext(query, baseConf);
  }

  private QueryContext createQueryContext(final String query, Configuration conf) throws LensException {
    QueryContext context = new QueryContext(query, "SA", new LensConf(), conf, drivers);
    return context;
  }

  protected ExplainQueryContext createExplainContext(final String query, Configuration conf) {
    ExplainQueryContext ectx = new ExplainQueryContext(query, "testuser", null, conf, drivers);
    return ectx;
  }

  /**
   * Creates the table.
   *
   * @param table the table
   * @throws Exception the exception
   */
  synchronized void createTable(String table) throws Exception {
    createTable(table, null);
  }

  synchronized void createTable(String table, Connection conn) throws Exception {
    Statement stmt = null;
    try {
      if (conn == null) {
        conn = driver.getConnection();
      }
      stmt = conn.createStatement();
      stmt.execute("CREATE TABLE " + table + " (ID INT)");

      conn.commit();
    } finally {
      if (stmt != null) {
        stmt.close();
      }
      if (conn != null) {
        conn.close();
      }
    }
  }


  void insertData(String table) throws Exception {
    insertData(table, null);
  }

  /**
   * Insert data.
   *
   * @param table the table
   * @throws Exception the exception
   */
  void insertData(String table, Connection conn) throws Exception {
    PreparedStatement stmt = null;
    try {
      if (conn == null) {
        conn = driver.getConnection();
      }
      stmt = conn.prepareStatement("INSERT INTO " + table + " VALUES(?)");

      for (int i = 0; i < 10; i++) {
        stmt.setInt(1, i);
        stmt.executeUpdate();
      }

      conn.commit();
    } finally {
      if (stmt != null) {
        stmt.close();
      }
      if (conn != null) {
        conn.close();
      }
    }
  }

  /**
   * Test ddl queries.
   */
  @Test
  public void testDDLQueries() {
    String query = "DROP TABLE TEMP";

    Throwable th = null;
    try {
      driver.rewriteQuery(createQueryContext(query));
    } catch (LensException e) {
      e.printStackTrace();
      th = e;
    }
    Assert.assertNotNull(th);

    query = "create table temp(name string, msr int)";

    th = null;
    try {
      driver.rewriteQuery(createQueryContext(query));
    } catch (LensException e) {
      e.printStackTrace();
      th = e;
    }
    Assert.assertNotNull(th);

    query = "insert overwrite table temp SELECT * FROM execute_test";

    th = null;
    try {
      driver.rewriteQuery(createQueryContext(query));
    } catch (LensException e) {
      e.printStackTrace();
      th = e;
    }
    Assert.assertNotNull(th);

    query = "create table temp2 as SELECT * FROM execute_test";

    th = null;
    try {
      driver.rewriteQuery(createQueryContext(query));
    } catch (LensException e) {
      e.printStackTrace();
      th = e;
    }
    Assert.assertNotNull(th);
  }

  /**
   * Test estimate.
   *
   * @throws Exception the exception
   */
  @Test
  public void testEstimate() throws Exception {
    createTable("estimate_test", driver.getEstimateConnection()); // Create table
    insertData("estimate_test", driver.getEstimateConnection()); // Insert some data into table
    String query1 = "SELECT * FROM estimate_test"; // Select query against existing table
    ExplainQueryContext ctx = createExplainContext(query1, baseConf);
    Assert.assertNull(ctx.getFinalDriverQuery(driver));
    QueryCost cost = driver.estimate(ctx);
    Assert.assertEquals(cost.getEstimatedExecTimeMillis(), 0);
    Assert.assertEquals(cost.getEstimatedResourceUsage(), 0.0);
    Assert.assertNotNull(ctx.getFinalDriverQuery(driver));
  }

  /**
   * Test estimate failing
   *
   * @throws Exception the exception
   */
  @Test
  public void testEstimateFailing() throws Exception {
    String query2 = "SELECT * FROM estimate_test2"; // Select query against non existing table
    try {
      driver.estimate(createExplainContext(query2, baseConf));
      Assert.fail("Running estimate on a non existing table.");
    } catch (LensException ex) {
      Assert.assertEquals(LensUtil.getCauseMessage(ex), "user lacks privilege or object not found: ESTIMATE_TEST2");
    }
  }

  /**
   * Test estimate failing
   *
   * @throws Exception the exception
   */
  @Test
  public void testEstimateGauges() throws Exception {
    createTable("estimate_test_gauge", driver.getEstimateConnection()); // Create table
    insertData("estimate_test_gauge", driver.getEstimateConnection()); // Insert some data into table
    String query1 = "SELECT * FROM estimate_test_gauge"; // Select query against existing table
    Configuration metricConf = new Configuration(baseConf);
    metricConf.set(LensConfConstants.QUERY_METRIC_UNIQUE_ID_CONF_KEY, TestJdbcDriver.class.getSimpleName());
    driver.estimate(createExplainContext(query1, metricConf));
    MetricRegistry reg = LensMetricsRegistry.getStaticRegistry();

    Assert.assertTrue(reg.getGauges().keySet().containsAll(Arrays.asList(
      "lens.MethodMetricGauge.TestJdbcDriver-JDBCDriver-columnar-sql-rewrite",
      "lens.MethodMetricGauge.TestJdbcDriver-JDBCDriver-jdbc-prepare-statement",
      "lens.MethodMetricGauge.TestJdbcDriver-JDBCDriver-validate-thru-prepare",
      "lens.MethodMetricGauge.TestJdbcDriver-JDBCDriver-jdbc-check-allowed-query")));
  }

  @Test
  public void testMetricsEnabled() throws Exception {
    createTable("test_metrics", driver.getEstimateConnection()); // Create table
    insertData("test_metrics", driver.getEstimateConnection()); // Insert some data into table
    createTable("test_metrics"); // Create table
    insertData("test_metrics"); // Insert some data into table
    String query1 = "SELECT * FROM test_metrics"; // Select query against existing table
    Configuration metricConf = new Configuration(baseConf);
    metricConf.setBoolean(LensConfConstants.ENABLE_QUERY_METRICS, true);
    QueryContext ctx = createQueryContext(query1, metricConf);
    driver.estimate(ctx);
    driver.execute(ctx);
  }

  /**
   * Test explain.
   *
   * @throws Exception the exception
   */
  @Test
  public void testExplain() throws Exception {
    createTable("explain_test"); // Create table
    insertData("explain_test"); // Insert some data into table
    String query1 = "SELECT * FROM explain_test"; // Select query against existing table
    String query2 = "SELECT * FROM explain_test1"; // Select query against non existing table
    ExplainQueryContext ctx = createExplainContext(query1, baseConf);
    Assert.assertNull(ctx.getFinalDriverQuery(driver));
    driver.explain(ctx);
    Assert.assertNotNull(ctx.getFinalDriverQuery(driver));

    try {
      driver.explain(createExplainContext(query2, baseConf));
      Assert.fail("Running explain on a non existing table.");
    } catch (LensException ex) {
      System.out.println("Error : " + ex);
    }
  }


  /**
   * Test execute.
   *
   * @throws Exception the exception
   */
  @Test
  public void testExecute() throws Exception {
    createTable("execute_test");

    // Insert some data into table
    insertData("execute_test");

    // Query
    final String query = "SELECT * FROM execute_test";

    QueryContext context = createQueryContext(query);
    LensResultSet resultSet = driver.execute(context);
    assertNotNull(resultSet);
    if (resultSet instanceof InMemoryResultSet) {
      InMemoryResultSet rs = (InMemoryResultSet) resultSet;
      LensResultSetMetadata rsMeta = rs.getMetadata();
      assertEquals(rsMeta.getColumns().size(), 1);

      ColumnDescriptor col1 = rsMeta.getColumns().get(0);
      assertEquals(col1.getTypeName().toLowerCase(), "int");
      assertEquals(col1.getName(), "ID");

      while (rs.hasNext()) {
        ResultRow row = rs.next();
        List<Object> rowObjects = row.getValues();
      }

      if (rs instanceof JDBCResultSet) {
        ((JDBCResultSet) rs).close();
      }
    }
  }

  /**
   * Test prepare.
   *
   * @throws Exception the exception
   */
  @Test
  public void testPrepare() throws Exception {
    // In this test we are testing both prepare and validate. Since in the test config
    // We are using different DBs for estimate pool and query pool, we have to create
    // tables in both DBs.
    createTable("prepare_test");
    createTable("prepare_test", driver.getEstimateConnection());
    insertData("prepare_test");
    insertData("prepare_test", driver.getEstimateConnection());

    final String query = "SELECT * from prepare_test";
    PreparedQueryContext pContext = new PreparedQueryContext(query, "SA", baseConf, drivers);
    //run prepare
    driver.prepare(pContext);
    //run validate
    driver.validate(pContext);
  }

  /**
   * Test prepare failing
   *
   * @throws Exception the exception
   */
  @Test
  public void testPrepareFailing() throws Exception {
    String query = "SELECT * FROM prepare_test2"; // Select query against non existing table
    try {
      PreparedQueryContext pContext = new PreparedQueryContext(query, "SA", baseConf, drivers);
      driver.prepare(pContext);
      Assert.fail("Running prepare on a non existing table.");
    } catch (LensException ex) {
      Assert.assertEquals(LensUtil.getCauseMessage(ex), "user lacks privilege or object not found: PREPARE_TEST2");
    }
  }

  /**
   * Test execute async.
   *
   * @throws Exception the exception
   */
  @Test
  public void testExecuteAsync() throws Exception {
    createTable("execute_async_test");
    insertData("execute_async_test");
    final String query = "SELECT * FROM execute_async_test";
    QueryContext context = createQueryContext(query);
    System.out.println("@@@ Test_execute_async:" + context.getQueryHandle());
    final CountDownLatch listenerNotificationLatch = new CountDownLatch(1);

    QueryCompletionListener listener = new QueryCompletionListener() {
      @Override
      public void onError(QueryHandle handle, String error) {
        fail("Query failed " + handle + " message" + error);
      }

      @Override
      public void onCompletion(QueryHandle handle) {
        System.out.println("@@@@ Query is complete " + handle);
        listenerNotificationLatch.countDown();
      }
    };

    driver.executeAsync(context);
    QueryHandle handle = context.getQueryHandle();
    driver.registerForCompletionNotification(handle, 0, listener);

    while (true) {
      driver.updateStatus(context);
      System.out.println("Query: " + handle + " Status: " + context.getDriverStatus());
      if (context.getDriverStatus().isFinished()) {
        assertEquals(context.getDriverStatus().getState(), DriverQueryState.SUCCESSFUL);
        assertEquals(context.getDriverStatus().getProgress(), 1.0);
        break;
      }
      Thread.sleep(500);
    }
    assertTrue(context.getDriverStatus().getDriverStartTime() > 0);
    assertTrue(context.getDriverStatus().getDriverFinishTime() > 0);
    // make sure query completion listener was called with onCompletion
    try {
      listenerNotificationLatch.await(1, TimeUnit.SECONDS);
    } catch (Exception e) {
      fail("query completion listener was not notified - " + e.getMessage());
      e.printStackTrace();
    }

    LensResultSet grs = driver.fetchResultSet(context);

    // Check multiple fetchResultSet return same object
    for (int i = 0; i < 5; i++) {
      assertTrue(grs == driver.fetchResultSet(context));
    }

    assertNotNull(grs);
    if (grs instanceof InMemoryResultSet) {
      InMemoryResultSet rs = (InMemoryResultSet) grs;
      LensResultSetMetadata rsMeta = rs.getMetadata();
      assertEquals(rsMeta.getColumns().size(), 1);

      ColumnDescriptor col1 = rsMeta.getColumns().get(0);
      assertEquals(col1.getTypeName().toLowerCase(), "int");
      assertEquals(col1.getName(), "ID");
      System.out.println("Matched metadata");

      while (rs.hasNext()) {
        List<Object> vals = rs.next().getValues();
        assertEquals(vals.size(), 1);
        assertEquals(vals.get(0).getClass(), Integer.class);
      }

      driver.closeQuery(handle);
      // Close again, should get not found
      try {
        driver.closeQuery(handle);
        fail("Close again should have thrown exception");
      } catch (LensException ex) {
        assertTrue(ex.getMessage().contains("not found") && ex.getMessage().contains(handle.getHandleId().toString()));
        System.out.println("Matched exception");
      }
    } else {
      fail("Only in memory result set is supported as of now");
    }

  }

  /**
   * Test connection close for failed queries.
   *
   * @throws Exception the exception
   */
  @Test
  public void testConnectionCloseForFailedQueries() throws Exception {
    createTable("invalid_conn_close");
    insertData("invalid_conn_close");

    final String query = "SELECT * from invalid_conn_close2";
    QueryContext ctx = new QueryContext(query, "SA", new LensConf(), baseConf, drivers);

    for (int i = 0; i < JDBCDriverConfConstants.JDBC_POOL_MAX_SIZE_DEFAULT; i++) {
      driver.executeAsync(ctx);
      driver.updateStatus(ctx);
      System.out.println("@@@@ QUERY " + (i + 1));
    }

    String validQuery = "SELECT * FROM invalid_conn_close";
    QueryContext validCtx = createQueryContext(validQuery);
    System.out.println("@@@ Submitting valid query");
    driver.executeAsync(validCtx);

    // Wait for query to finish
    while (true) {
      driver.updateStatus(validCtx);
      if (validCtx.getDriverStatus().isFinished()) {
        break;
      }
      Thread.sleep(1000);
    }

    driver.closeQuery(validCtx.getQueryHandle());
  }

  /**
   * Test connection close for successful queries.
   *
   * @throws Exception the exception
   */
  @Test
  public void testConnectionCloseForSuccessfulQueries() throws Exception {
    createTable("valid_conn_close");
    insertData("valid_conn_close");

    final String query = "SELECT * from valid_conn_close";
    QueryContext ctx = createQueryContext(query);

    for (int i = 0; i < JDBCDriverConfConstants.JDBC_POOL_MAX_SIZE_DEFAULT; i++) {
      LensResultSet resultSet = driver.execute(ctx);
      assertNotNull(resultSet);
      if (resultSet instanceof InMemoryResultSet) {
        InMemoryResultSet rs = (InMemoryResultSet) resultSet;
        LensResultSetMetadata rsMeta = rs.getMetadata();
        assertEquals(rsMeta.getColumns().size(), 1);

        ColumnDescriptor col1 = rsMeta.getColumns().get(0);
        assertEquals(col1.getTypeName().toLowerCase(), "int");
        assertEquals(col1.getName(), "ID");

        while (rs.hasNext()) {
          ResultRow row = rs.next();
          List<Object> rowObjects = row.getValues();
        }
      }
      System.out.println("@@@@ QUERY " + (i + 1));
    }

    String validQuery = "SELECT * FROM valid_conn_close";
    QueryContext validCtx = createQueryContext(validQuery);
    System.out.println("@@@ Submitting query after pool quota used");
    driver.execute(validCtx);
  }

  /**
   * Test cancel query.
   *
   * @throws Exception the exception
   */
  @Test
  public void testCancelQuery() throws Exception {
    createTable("cancel_query_test");
    insertData("cancel_query_test");
    final String query = "SELECT * FROM cancel_query_test";
    QueryContext context = createQueryContext(query);
    System.out.println("@@@ test_cancel:" + context.getQueryHandle());
    driver.executeAsync(context);
    QueryHandle handle = context.getQueryHandle();
    boolean isCancelled = driver.cancelQuery(handle);
    driver.updateStatus(context);

    if (isCancelled) {
      assertEquals(context.getDriverStatus().getState(), DriverQueryState.CANCELED);
    } else {
      // Query completed before cancelQuery call
      assertEquals(context.getDriverStatus().getState(), DriverQueryState.SUCCESSFUL);
    }

    assertTrue(context.getDriverStatus().getDriverStartTime() > 0);
    assertTrue(context.getDriverStatus().getDriverFinishTime() > 0);
    driver.closeQuery(handle);
  }

  /**
   * Test invalid query.
   *
   * @throws Exception the exception
   */
  @Test
  public void testInvalidQuery() throws Exception {
    final String query = "SELECT * FROM invalid_table";
    QueryContext ctx = new QueryContext(query, "SA", new LensConf(), baseConf, drivers);
    try {
      LensResultSet rs = driver.execute(ctx);
      fail("Should have thrown exception");
    } catch (LensException e) {
      e.printStackTrace();
    }

    final CountDownLatch listenerNotificationLatch = new CountDownLatch(1);

    QueryCompletionListener listener = new QueryCompletionListener() {
      @Override
      public void onError(QueryHandle handle, String error) {
        listenerNotificationLatch.countDown();
      }

      @Override
      public void onCompletion(QueryHandle handle) {
        fail("Was expecting this query to fail " + handle);
      }
    };

    driver.executeAsync(ctx);
    QueryHandle handle = ctx.getQueryHandle();
    driver.registerForCompletionNotification(handle, 0, listener);

    while (true) {
      driver.updateStatus(ctx);
      System.out.println("Query: " + handle + " Status: " + ctx.getDriverStatus());
      if (ctx.getDriverStatus().isFinished()) {
        assertEquals(ctx.getDriverStatus().getState(), DriverQueryState.FAILED);
        assertEquals(ctx.getDriverStatus().getProgress(), 1.0);
        break;
      }
      Thread.sleep(500);
    }
    assertTrue(ctx.getDriverStatus().getDriverStartTime() > 0);
    assertTrue(ctx.getDriverStatus().getDriverFinishTime() > 0);

    listenerNotificationLatch.await(1, TimeUnit.SECONDS);
    // fetch result should throw error
    try {
      driver.fetchResultSet(ctx);
      fail("should have thrown error");
    } catch (LensException e) {
      e.printStackTrace();
    }
    driver.closeQuery(handle);
  }

  @Test
  public void testEstimateConf() {
    Configuration estimateConf = driver.getEstimateConnectionConf();
    assertNotNull(estimateConf);
    assertTrue(estimateConf != driver.getConf());

    // Validate overridden conf
    assertEquals(estimateConf.get(JDBCDriverConfConstants.JDBC_USER), "estimateUser");
    assertEquals(estimateConf.get(JDBCDriverConfConstants.JDBC_POOL_MAX_SIZE), "50");
    assertEquals(estimateConf.get(JDBCDriverConfConstants.JDBC_POOL_IDLE_TIME), "800");
    assertEquals(estimateConf.get(JDBCDriverConfConstants.JDBC_GET_CONNECTION_TIMEOUT), "25000");
    assertEquals(estimateConf.get(JDBCDriverConfConstants.JDBC_MAX_STATEMENTS_PER_CONNECTION),
      "15");
  }

  @Test
  public void testEstimateConnectionPool() throws Exception {
    assertNotNull(driver.getEstimateConnectionProvider());
    assertTrue(driver.getEstimateConnectionProvider() != driver.getConnectionProvider());

    ConnectionProvider connectionProvider = driver.getEstimateConnectionProvider();
    assertTrue(connectionProvider instanceof DataSourceConnectionProvider);

    DataSourceConnectionProvider estimateCp = (DataSourceConnectionProvider) connectionProvider;
    DataSourceConnectionProvider queryCp = (DataSourceConnectionProvider) driver.getConnectionProvider();

    assertTrue(estimateCp != queryCp);

    DataSourceConnectionProvider.DriverConfig estimateCfg =
      estimateCp.getDriverConfigfromConf(driver.getEstimateConnectionConf());
    DataSourceConnectionProvider.DriverConfig queryCfg =
      queryCp.getDriverConfigfromConf(driver.getConf());

    LOG.info("@@@ ESTIMATE_CFG " + estimateCfg);
    LOG.info("@@@ QUERY CFG " + queryCfg);

    // Get connection from each so that pools get initialized
    try {
      Connection estimateConn = estimateCp.getConnection(driver.getEstimateConnectionConf());
      estimateConn.close();
    } catch (SQLException e) {
      // Ignore exception
      LOG.error("Error getting connection from estimate pool", e);
    }

    try {
      Connection queryConn = queryCp.getConnection(driver.getConf());
      queryConn.close();
    } catch (SQLException e) {
      LOG.error("Error getting connection from query pool", e);
    }


    ComboPooledDataSource estimatePool = estimateCp.getDataSource(driver.getEstimateConnectionConf());
    ComboPooledDataSource queryPool = queryCp.getDataSource(driver.getConf());

    assertTrue(estimatePool != queryPool);

    // Validate config on estimatePool
    assertEquals(estimatePool.getMaxPoolSize(), 50);
    assertEquals(estimatePool.getMaxIdleTime(), 800);
    assertEquals(estimatePool.getCheckoutTimeout(), 25000);
    assertEquals(estimatePool.getMaxStatementsPerConnection(), 15);
  }

}

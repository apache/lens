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

import static org.apache.lens.driver.jdbc.JDBCDriverConfConstants.*;
import static org.apache.lens.driver.jdbc.JDBCDriverConfConstants.ConnectionPoolProperties.*;

import static org.testng.Assert.*;

import java.sql.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.ResultRow;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.*;
import org.apache.lens.server.api.driver.DriverQueryStatus.DriverQueryState;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.metrics.LensMetricsRegistry;
import org.apache.lens.server.api.query.ExplainQueryContext;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.cost.QueryCost;
import org.apache.lens.server.api.user.MockDriverQueryHook;
import org.apache.lens.server.api.util.LensUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.ColumnDescriptor;

import org.testng.Assert;
import org.testng.annotations.*;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import lombok.extern.slf4j.Slf4j;

/**
 * The Class TestJdbcDriver.
 */
@Slf4j
public class TestJdbcDriver {

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
    baseConf.set(JDBC_DRIVER_CLASS, "org.hsqldb.jdbc.JDBCDriver");
    baseConf.set(JDBC_DB_URI, "jdbc:hsqldb:mem:jdbcTestDB");
    baseConf.set(JDBC_USER, "SA");
    baseConf.set(JDBC_PASSWORD, "");
    baseConf.set(JDBC_EXPLAIN_KEYWORD_PARAM, "explain plan for ");
    baseConf.setClass(JDBC_QUERY_HOOK_CLASS, MockDriverQueryHook.class, DriverQueryHook.class);
    hConf = new HiveConf(baseConf, this.getClass());

    driver = new JDBCDriver();
    driver.configure(baseConf, "jdbc", "jdbc1");

    assertNotNull(driver);
    assertTrue(driver.configured);

    drivers = Lists.<LensDriver>newArrayList(driver);
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
    ExplainQueryContext ectx = new ExplainQueryContext(UUID.randomUUID().toString(), query, "testuser", null, conf,
      drivers);
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
      log.error("Error running DDL query.", e);
      th = e;
    }
    Assert.assertNotNull(th);

    query = "create table temp(name string, msr int)";

    th = null;
    try {
      driver.rewriteQuery(createQueryContext(query));
    } catch (LensException e) {
      log.error("Error running DDL query", e);
      th = e;
    }
    Assert.assertNotNull(th);

    query = "insert overwrite table temp SELECT * FROM execute_test";

    th = null;
    try {
      driver.rewriteQuery(createQueryContext(query));
    } catch (LensException e) {
      log.error("Error running DDL query", e);
      th = e;
    }
    Assert.assertNotNull(th);

    query = "create table temp2 as SELECT * FROM execute_test";

    th = null;
    try {
      driver.rewriteQuery(createQueryContext(query));
    } catch (LensException e) {
      log.error("Error running DDL query", e);
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
    Assert.assertEquals(cost, JDBCDriver.JDBC_DRIVER_COST);
    Assert.assertNotNull(ctx.getFinalDriverQuery(driver));

    // Test connection leak for estimate
    final int maxEstimateConnections =
      driver.getEstimateConnectionConf().getInt(JDBC_POOL_MAX_SIZE.getConfigKey(), 50);
    for (int i = 0; i < maxEstimateConnections + 10; i++) {
      try {
        log.info("Iteration#{}", (i + 1));
        String query = i > maxEstimateConnections ? "SELECT * FROM estimate_test" : "CREATE TABLE FOO(ID INT)";
        ExplainQueryContext context = createExplainContext(query, baseConf);
        cost = driver.estimate(context);
      } catch (LensException exc) {
        Throwable th = exc.getCause();
        while (th != null) {
          assertFalse(th instanceof SQLException);
          th = th.getCause();
        }
      }
    }

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
    String driverQualifiledName = driver.getFullyQualifiedName();
    Assert.assertTrue(reg.getGauges().keySet().containsAll(Arrays.asList(
      "lens.MethodMetricGauge.TestJdbcDriver-"+driverQualifiledName+"-validate-columnar-sql-rewrite",
      "lens.MethodMetricGauge.TestJdbcDriver-"+driverQualifiledName+"-validate-jdbc-prepare-statement",
      "lens.MethodMetricGauge.TestJdbcDriver-"+driverQualifiledName+"-validate-thru-prepare",
      "lens.MethodMetricGauge.TestJdbcDriver-"+driverQualifiledName+"-jdbc-check-allowed-query")));
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
    // run estimate and execute - because server would first run estimate and then execute with same context
    QueryContext ctx = createQueryContext(query1, metricConf);
    QueryCost cost = driver.estimate(ctx);
    Assert.assertEquals(cost, JDBCDriver.JDBC_DRIVER_COST);
    LensResultSet result = driver.execute(ctx);
    Assert.assertNotNull(result);

    // test prepare
    // run estimate and prepare - because server would first run estimate and then prepare with same context
    PreparedQueryContext pContext = new PreparedQueryContext(query1, "SA", metricConf, drivers);
    cost = driver.estimate(pContext);
    Assert.assertEquals(cost, JDBCDriver.JDBC_DRIVER_COST);
    driver.prepare(pContext);

    // test explain and prepare
    PreparedQueryContext pContext2 = new PreparedQueryContext(query1, "SA", metricConf, drivers);
    cost = driver.estimate(pContext2);
    Assert.assertEquals(cost, JDBCDriver.JDBC_DRIVER_COST);
    driver.prepare(pContext2);
    driver.explainAndPrepare(pContext2);
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
   * Test type casting of char, varchar, nvarchar and decimal type
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void tesDecimalCharCasting() throws Exception {

    Statement stmt = null;
    Connection conn = null;
    try {
      conn = driver.getConnection();
      stmt = conn.createStatement();
      // Create table with char, varchar, nvarchar and decimal data type
      stmt.execute("CREATE TABLE test_casting(c1 decimal(10,2), c2 varchar(20), c3 nvarchar(20), c4 char(10))");
      // Insert data
      stmt.execute("INSERT INTO test_casting VALUES(34.56,'abc','def','ghi')");
      stmt.execute("INSERT INTO test_casting VALUES(78.50,'abc1','def1','ghi1')");
      stmt.execute("INSERT INTO test_casting VALUES(48.89,'abc2','def2','ghi2')");
      conn.commit();

      // Query
      final String query = "SELECT * FROM test_casting";
      QueryContext context = createQueryContext(query);
      LensResultSet resultSet = driver.execute(context);
      assertNotNull(resultSet);

      if (resultSet instanceof InMemoryResultSet) {
        InMemoryResultSet rs = (InMemoryResultSet) resultSet;
        LensResultSetMetadata rsMeta = rs.getMetadata();
        assertEquals(rsMeta.getColumns().size(), 4);

        ColumnDescriptor col1 = rsMeta.getColumns().get(0);
        assertEquals(col1.getTypeName().toLowerCase(), "double");
        assertEquals(col1.getName(), "C1");

        ColumnDescriptor col2 = rsMeta.getColumns().get(1);
        assertEquals(col2.getTypeName().toLowerCase(), "string");
        assertEquals(col2.getName(), "C2");

        ColumnDescriptor col3 = rsMeta.getColumns().get(2);
        assertEquals(col3.getTypeName().toLowerCase(), "string");
        assertEquals(col3.getName(), "C3");

        ColumnDescriptor col4 = rsMeta.getColumns().get(3);
        assertEquals(col4.getTypeName().toLowerCase(), "string");
        assertEquals(col4.getName(), "C4");


        while (rs.hasNext()) {
          ResultRow row = rs.next();
          List<Object> rowObjects = row.getValues();
        }
        if (rs instanceof JDBCResultSet) {
          ((JDBCResultSet) rs).close();
        }
      }

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
    //run validate
    driver.validate(pContext);
    //run prepare
    driver.prepare(pContext);
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

    executeAsync(context);
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
      fail("Query completion listener was not notified - " + e.getMessage());
      log.error("Query completion listener was not notified.", e);
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

    for (int i = 0; i < JDBC_POOL_MAX_SIZE.getDefaultValue(); i++) {
      executeAsync(ctx);
      driver.updateStatus(ctx);
      System.out.println("@@@@ QUERY " + (i + 1));
    }

    String validQuery = "SELECT * FROM invalid_conn_close";
    QueryContext validCtx = createQueryContext(validQuery);
    System.out.println("@@@ Submitting valid query");
    executeAsync(validCtx);

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

  private void executeAsync(QueryContext ctx) throws LensException {
    driver.executeAsync(ctx);
    assertEquals(ctx.getSelectedDriverConf().get(MockDriverQueryHook.KEY), MockDriverQueryHook.VALUE);
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

    for (int i = 0; i < ConnectionPoolProperties.JDBC_POOL_MAX_SIZE.getDefaultValue(); i++) {
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
    executeAsync(context);
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
      log.error("Encountered Lens exception.", e);
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

    executeAsync(ctx);
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
      log.error("Encountered Lens exception", e);
    }
    driver.closeQuery(handle);
  }

  @Test
  public void testEstimateConf() {
    Configuration estimateConf = driver.getEstimateConnectionConf();
    assertNotNull(estimateConf);
    assertTrue(estimateConf != driver.getConf());

    // Validate overridden conf
    assertEquals(estimateConf.get(JDBC_USER), "estimateUser");
    assertEquals(estimateConf.get(JDBC_POOL_MAX_SIZE.getConfigKey()), "50");
    assertEquals(estimateConf.get(JDBC_POOL_IDLE_TIME.getConfigKey()), "800");
    assertEquals(estimateConf.get(JDBC_GET_CONNECTION_TIMEOUT.getConfigKey()), "25000");
    assertEquals(estimateConf.get(JDBC_MAX_STATEMENTS_PER_CONNECTION.getConfigKey()), "15");
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

    log.info("@@@ ESTIMATE_CFG {}", estimateCfg);
    log.info("@@@ QUERY CFG {}", queryCfg);

    // Get connection from each so that pools get initialized
    try {
      Connection estimateConn = estimateCp.getConnection(driver.getEstimateConnectionConf());
      estimateConn.close();
    } catch (SQLException e) {
      // Ignore exception
      log.error("Error getting connection from estimate pool", e);
    }

    try {
      Connection queryConn = queryCp.getConnection(driver.getConf());
      queryConn.close();
    } catch (SQLException e) {
      log.error("Error getting connection from query pool", e);
    }


    ComboPooledDataSource estimatePool = estimateCp.getDataSource(driver.getEstimateConnectionConf());
    ComboPooledDataSource queryPool = queryCp.getDataSource(driver.getConf());

    assertTrue(estimatePool != queryPool);

    // Validate config on estimatePool
    assertEquals(estimatePool.getMaxPoolSize(), 50);
    assertEquals(estimatePool.getMaxIdleTime(), 800);
    assertEquals(estimatePool.getCheckoutTimeout(), 25000);
    assertEquals(estimatePool.getMaxStatementsPerConnection(), 15);
    assertEquals(estimatePool.getProperties().get("random_key"), "random_value");
  }

}

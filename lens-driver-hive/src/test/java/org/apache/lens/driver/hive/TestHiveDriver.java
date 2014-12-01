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
package org.apache.lens.driver.hive;

import static org.testng.Assert.*;

import java.io.*;
import java.util.*;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.HiveDriverRunHook;
import org.apache.hadoop.hive.ql.HiveDriverRunHookContext;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensException;
import org.apache.lens.api.Priority;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.*;
import org.apache.lens.server.api.driver.DriverQueryStatus.DriverQueryState;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * The Class TestHiveDriver.
 */
public class TestHiveDriver {

  /** The Constant TEST_DATA_FILE. */
  public static final String TEST_DATA_FILE = "testdata/testdata1.txt";

  /** The test output dir. */
  public final String TEST_OUTPUT_DIR = "target/" + this.getClass().getSimpleName() + "/test-output";

  /** The conf. */
  protected HiveConf conf;

  /** The driver. */
  protected HiveDriver driver;

  /** Driver list **/
  protected Collection<LensDriver> drivers;

  /** The data base. */
  public String DATA_BASE = this.getClass().getSimpleName().toLowerCase();

  protected String sessionid;
  protected SessionState ss;

  /**
   * Before test.
   *
   * @throws Exception
   *           the exception
   */
  @BeforeTest
  public void beforeTest() throws Exception {
    // Check if hadoop property set
    System.out.println("###HADOOP_PATH " + System.getProperty("hadoop.bin.path"));
    assertNotNull(System.getProperty("hadoop.bin.path"));

    createDriver();
    ss = new SessionState(conf, "testuser");
    SessionState.start(ss);
    Hive client = Hive.get(conf);
    Database database = new Database();
    database.setName(DATA_BASE);
    client.createDatabase(database, true);
    SessionState.get().setCurrentDatabase(DATA_BASE);
    sessionid = SessionState.get().getSessionId();

    conf.setBoolean(LensConfConstants.QUERY_ADD_INSERT_OVEWRITE, false);
    conf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    QueryContext context = createContext("USE " + DATA_BASE, conf);
    driver.execute(context);
    conf.setBoolean(LensConfConstants.QUERY_ADD_INSERT_OVEWRITE, true);
    conf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, true);
  }

  protected void createDriver() throws LensException {
    conf = new HiveConf();
    conf.addResource("hivedriver-site.xml");
    conf.setClass(HiveDriver.HIVE_CONNECTION_CLASS, EmbeddedThriftConnection.class, ThriftConnection.class);
    conf.set("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    conf.setBoolean(HiveDriver.HS2_CALCULATE_PRIORITY, false);
    driver = new HiveDriver();
    driver.configure(conf);
    drivers = new ArrayList<LensDriver>() {{ add
      (driver);}};
    System.out.println("TestHiveDriver created");
  }

  @BeforeMethod
  public void setDB() {
    SessionState.get().setCurrentDatabase(DATA_BASE);
  }

  protected QueryContext createContext(final String query, Configuration conf) throws LensException {
    QueryContext context = new QueryContext(query, "testuser", conf, drivers);
    context.getDriverContext().setDriverQueriesAndPlans(new HashMap<LensDriver, String>() {{ put(driver, query); }} );
    context.setSelectedDriver(driver);
    context.setLensSessionIdentifier(sessionid);
    return context;
  }

  protected QueryContext createContext(PreparedQueryContext query, Configuration conf) {
    QueryContext context = new QueryContext(query, "testuser", conf);
    context.setLensSessionIdentifier(sessionid);
    return context;
  }

  /**
   * After test.
   *
   * @throws Exception
   *           the exception
   */
  @AfterTest
  public void afterTest() throws Exception {
    driver.close();
    Hive.get(conf).dropDatabase(DATA_BASE, true, true, true);
  }

  /**
   * Creates the test table.
   *
   * @param tableName
   *          the table name
   * @throws Exception
   *           the exception
   */
  protected void createTestTable(String tableName) throws Exception {
    System.out.println("Hadoop Location: " + System.getProperty("hadoop.bin.path"));
    String createTable = "CREATE TABLE IF NOT EXISTS " + tableName + "(ID STRING)" + " TBLPROPERTIES ('"
        + LensConfConstants.STORAGE_COST + "'='500')";
    conf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    // Craete again
    QueryContext context = createContext(createTable, conf);
    LensResultSet resultSet = driver.execute(context);
    assertNull(resultSet);

    // Load some data into the table
    String dataLoad = "LOAD DATA LOCAL INPATH '" + TEST_DATA_FILE + "' OVERWRITE INTO TABLE " + tableName;
    context = createContext(dataLoad, conf);
    resultSet = driver.execute(context);
    assertNull(resultSet);
    Assert.assertEquals(0, driver.getHiveHandleSize());
  }

  /**
   * Creates the test table.
   *
   * @param tableName
   *          the table name
   * @throws Exception
   *           the exception
   */
  protected void createPartitionedTable(String tableName) throws Exception {
    System.out.println("Hadoop Location: " + System.getProperty("hadoop.bin.path"));
    String createTable = "CREATE TABLE IF NOT EXISTS " + tableName + "(ID STRING)" + " PARTITIONED BY (dt string) TBLPROPERTIES ('"
        + LensConfConstants.STORAGE_COST + "'='500')";
    conf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    // Craete again
    QueryContext context = createContext(createTable, conf);
    LensResultSet resultSet = driver.execute(context);
    assertNull(resultSet);

    // Load some data into the table
    String dataLoad = "LOAD DATA LOCAL INPATH '" + TEST_DATA_FILE + "' OVERWRITE INTO TABLE " + tableName
        + " partition (dt='today')";
    context = createContext(dataLoad, conf);
    resultSet = driver.execute(context);
    assertNull(resultSet);
    Assert.assertEquals(0, driver.getHiveHandleSize());
  }
  
  // Tests
  /**
   * Test insert overwrite conf.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testInsertOverwriteConf() throws Exception {
    createTestTable("test_insert_overwrite");
    conf.setBoolean(LensConfConstants.QUERY_ADD_INSERT_OVEWRITE, false);
    String query = "SELECT ID FROM test_insert_overwrite";
    QueryContext context = createContext(query, conf);
    driver.addPersistentPath(context);
    assertEquals(context.getUserQuery(), query);
    assertNotNull(context.getDriverContext().getDriverQuery(driver));
    assertEquals(context.getDriverContext().getDriverQuery(driver), context.getUserQuery());
  }

  /**
   * Test temptable.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testTemptable() throws Exception {
    createTestTable("test_temp");
    conf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    Hive.get(conf).dropTable("test_temp_output");
    String query = "CREATE TABLE test_temp_output AS SELECT ID FROM test_temp";
    QueryContext context = createContext(query, conf);
    LensResultSet resultSet = driver.execute(context);
    assertNull(resultSet);
    Assert.assertEquals(0, driver.getHiveHandleSize());

    // fetch results from temp table
    String select = "SELECT * FROM test_temp_output";
    context = createContext(select, conf);
    resultSet = driver.execute(context);
    Assert.assertEquals(0, driver.getHiveHandleSize());
    validateInMemoryResult(resultSet, "test_temp_output");
    Assert.assertEquals(0, driver.getHiveHandleSize());
  }

  /**
   * Test execute query.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testExecuteQuery() throws Exception {
    createTestTable("test_execute");
    LensResultSet resultSet = null;
    // Execute a select query
    conf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    String select = "SELECT ID FROM test_execute";
    QueryContext context = createContext(select, conf);
    resultSet = driver.execute(context);
    validateInMemoryResult(resultSet);
    conf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, true);
    context = createContext(select, conf);
    resultSet = driver.execute(context);
    validatePersistentResult(resultSet, TEST_DATA_FILE, context.getHDFSResultDir(), false);
    conf.set(LensConfConstants.QUERY_OUTPUT_DIRECTORY_FORMAT,
        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'"
            + " WITH SERDEPROPERTIES ('serialization.null.format'='-NA-',"
            + " 'field.delim'=','  ) STORED AS TEXTFILE ");
    select = "SELECT ID, null, ID FROM test_execute";
    context = createContext(select, conf);
    resultSet = driver.execute(context);
    validatePersistentResult(resultSet, TEST_DATA_FILE, context.getHDFSResultDir(), true);
    Assert.assertEquals(0, driver.getHiveHandleSize());
  }

  /**
   * Validate in memory result.
   *
   * @param resultSet
   *          the result set
   * @throws LensException
   *           the lens exception
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void validateInMemoryResult(LensResultSet resultSet) throws LensException, IOException {
    validateInMemoryResult(resultSet, null);
  }

  /**
   * Validate in memory result.
   *
   * @param resultSet
   *          the result set
   * @param outputTable
   *          the output table
   * @throws LensException
   *           the lens exception
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void validateInMemoryResult(LensResultSet resultSet, String outputTable) throws LensException, IOException {
    assertNotNull(resultSet);
    assertTrue(resultSet instanceof HiveInMemoryResultSet);
    HiveInMemoryResultSet inmemrs = (HiveInMemoryResultSet) resultSet;

    // check metadata
    LensResultSetMetadata rsMeta = inmemrs.getMetadata();
    List<ColumnDescriptor> columns = rsMeta.getColumns();
    assertNotNull(columns);
    assertEquals(columns.size(), 1);
    String expectedCol = "";
    if (outputTable != null) {
      expectedCol += outputTable + ".";
    }
    expectedCol += "ID";
    assertTrue(columns.get(0).getName().toLowerCase().equals(expectedCol.toLowerCase())
        || columns.get(0).getName().toLowerCase().equals("ID".toLowerCase()));
    assertEquals(columns.get(0).getTypeName().toLowerCase(), "STRING".toLowerCase());

    List<String> expectedRows = new ArrayList<String>();
    // Read data from the test file into expectedRows
    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(TEST_DATA_FILE)));
    String line = "";
    while ((line = br.readLine()) != null) {
      expectedRows.add(line.trim());
    }
    br.close();

    List<String> actualRows = new ArrayList<String>();
    while (inmemrs.hasNext()) {
      List<Object> row = inmemrs.next().getValues();
      actualRows.add((String) row.get(0));
    }
    assertEquals(actualRows, expectedRows);
  }

  /**
   * The Class FailHook.
   */
  public static class FailHook implements HiveDriverRunHook {

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.hive.ql.HiveDriverRunHook#postDriverRun(org.apache.hadoop.hive.ql.HiveDriverRunHookContext)
     */
    @Override
    public void postDriverRun(HiveDriverRunHookContext arg0) throws Exception {
      // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.hive.ql.HiveDriverRunHook#preDriverRun(org.apache.hadoop.hive.ql.HiveDriverRunHookContext)
     */
    @Override
    public void preDriverRun(HiveDriverRunHookContext arg0) throws Exception {
      throw new LensException("Failing this run");
    }

  }

  // executeAsync
  /**
   * Test execute query async.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testExecuteQueryAsync() throws Exception {
    createTestTable("test_execute_sync");

    // Now run a command that would fail
    String expectFail = "SELECT ID FROM test_execute_sync";
    conf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, true);
    conf.set("hive.exec.driver.run.hooks", FailHook.class.getCanonicalName());
    QueryContext context = createContext(expectFail, conf);
    driver.executeAsync(context);
    Assert.assertEquals(1, driver.getHiveHandleSize());
    validateExecuteAsync(context, DriverQueryState.FAILED, true, false);
    Assert.assertEquals(1, driver.getHiveHandleSize());
    driver.closeQuery(context.getQueryHandle());
    Assert.assertEquals(0, driver.getHiveHandleSize());

    conf.set("hive.exec.driver.run.hooks", "");
    // Async select query
    String select = "SELECT ID FROM test_execute_sync";
    conf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    context = createContext(select, conf);
    driver.executeAsync(context);
    Assert.assertEquals(1, driver.getHiveHandleSize());
    validateExecuteAsync(context, DriverQueryState.SUCCESSFUL, false, false);
    driver.closeQuery(context.getQueryHandle());
    Assert.assertEquals(0, driver.getHiveHandleSize());

    conf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, true);
    context = createContext(select, conf);
    driver.executeAsync(context);
    Assert.assertEquals(1, driver.getHiveHandleSize());
    validateExecuteAsync(context, DriverQueryState.SUCCESSFUL, true, false);
    driver.closeQuery(context.getQueryHandle());
    Assert.assertEquals(0, driver.getHiveHandleSize());

    conf.set(LensConfConstants.QUERY_OUTPUT_DIRECTORY_FORMAT,
        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'"
            + " WITH SERDEPROPERTIES ('serialization.null.format'='-NA-',"
            + " 'field.delim'=','  ) STORED AS TEXTFILE ");
    select = "SELECT ID, null, ID FROM test_execute_sync";
    context = createContext(select, conf);
    driver.executeAsync(context);
    Assert.assertEquals(1, driver.getHiveHandleSize());
    validateExecuteAsync(context, DriverQueryState.SUCCESSFUL, true, true);
    driver.closeQuery(context.getQueryHandle());
    Assert.assertEquals(0, driver.getHiveHandleSize());
  }

  /**
   * Validate execute async.
   *
   * @param ctx
   *          the ctx
   * @param finalState
   *          the final state
   * @param isPersistent
   *          the is persistent
   * @param formatNulls
   *          the format nulls
   * @param driver
   *          the driver
   * @throws Exception
   *           the exception
   */
  protected void validateExecuteAsync(QueryContext ctx, DriverQueryState finalState, boolean isPersistent,
      boolean formatNulls, HiveDriver driver) throws Exception {
    waitForAsyncQuery(ctx, driver);
    driver.updateStatus(ctx);
    assertEquals(ctx.getDriverStatus().getState(), finalState, "Expected query to finish with" + finalState);
    assertTrue(ctx.getDriverStatus().getDriverFinishTime() > 0);
    if (finalState.equals(DriverQueryState.SUCCESSFUL)) {
      System.out.println("Progress:" + ctx.getDriverStatus().getProgressMessage());
      assertNotNull(ctx.getDriverStatus().getProgressMessage());
      if (!isPersistent) {
        validateInMemoryResult(driver.fetchResultSet(ctx));
      } else {
        validatePersistentResult(driver.fetchResultSet(ctx), TEST_DATA_FILE, ctx.getHDFSResultDir(), formatNulls);
      }
    } else if (finalState.equals(DriverQueryState.FAILED)) {
      System.out.println("Error:" + ctx.getDriverStatus().getErrorMessage());
      System.out.println("Status:" + ctx.getDriverStatus().getStatusMessage());
      assertNotNull(ctx.getDriverStatus().getErrorMessage());
    }
  }

  /**
   * Validate execute async.
   *
   * @param ctx
   *          the ctx
   * @param finalState
   *          the final state
   * @param isPersistent
   *          the is persistent
   * @param formatNulls
   *          the format nulls
   * @throws Exception
   *           the exception
   */
  protected void validateExecuteAsync(QueryContext ctx, DriverQueryState finalState, boolean isPersistent,
      boolean formatNulls) throws Exception {
    validateExecuteAsync(ctx, finalState, isPersistent, formatNulls, driver);
  }

  /**
   * Test cancel async query.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testCancelAsyncQuery() throws Exception {
    createTestTable("test_cancel_async");
    conf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    QueryContext context = createContext("SELECT ID FROM test_cancel_async", conf);
    driver.executeAsync(context);
    driver.cancelQuery(context.getQueryHandle());
    driver.updateStatus(context);
    assertEquals(context.getDriverStatus().getState(), DriverQueryState.CANCELED, "Expecting query to be cancelled");
    driver.closeQuery(context.getQueryHandle());
    Assert.assertEquals(0, driver.getHiveHandleSize());

    try {
      driver.cancelQuery(context.getQueryHandle());
      fail("Cancel on closed query should throw error");
    } catch (LensException exc) {
      assertTrue(exc.getMessage().startsWith("Query not found"));
    }
  }

  /**
   * Validate persistent result.
   *
   * @param resultSet
   *          the result set
   * @param dataFile
   *          the data file
   * @param outptuDir
   *          the outptu dir
   * @param formatNulls
   *          the format nulls
   * @throws Exception
   *           the exception
   */
  private void validatePersistentResult(LensResultSet resultSet, String dataFile, Path outptuDir, boolean formatNulls)
      throws Exception {
    assertTrue(resultSet instanceof HivePersistentResultSet);
    HivePersistentResultSet persistentResultSet = (HivePersistentResultSet) resultSet;
    String path = persistentResultSet.getOutputPath();

    Path actualPath = new Path(path);
    FileSystem fs = actualPath.getFileSystem(conf);
    assertEquals(actualPath, fs.makeQualified(outptuDir));
    List<String> actualRows = new ArrayList<String>();
    for (FileStatus stat : fs.listStatus(actualPath)) {
      FSDataInputStream in = fs.open(stat.getPath());
      BufferedReader br = null;
      try {
        br = new BufferedReader(new InputStreamReader(in));
        String line = "";

        while ((line = br.readLine()) != null) {
          System.out.println("Actual:" + line);
          actualRows.add(line.trim());
        }
      } finally {
        if (br != null) {
          br.close();
        }
      }
    }

    BufferedReader br = null;
    List<String> expectedRows = new ArrayList<String>();

    try {
      br = new BufferedReader(new FileReader(new File(dataFile)));
      String line = "";
      while ((line = br.readLine()) != null) {
        String row = line.trim();
        if (formatNulls) {
          row += ",-NA-,";
          row += line.trim();
        }
        expectedRows.add(row);
      }
    } finally {
      if (br != null) {
        br.close();
      }
    }
    assertEquals(actualRows, expectedRows);
  }

  /**
   * Test persistent result set.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testPersistentResultSet() throws Exception {
    createTestTable("test_persistent_result_set");
    conf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, true);
    conf.setBoolean(LensConfConstants.QUERY_ADD_INSERT_OVEWRITE, true);
    conf.set(LensConfConstants.RESULT_SET_PARENT_DIR, TEST_OUTPUT_DIR);
    QueryContext ctx = createContext("SELECT ID FROM test_persistent_result_set", conf);
    LensResultSet resultSet = driver.execute(ctx);
    validatePersistentResult(resultSet, TEST_DATA_FILE, ctx.getHDFSResultDir(), false);
    Assert.assertEquals(0, driver.getHiveHandleSize());

    ctx = createContext("SELECT ID FROM test_persistent_result_set", conf);
    driver.executeAsync(ctx);
    Assert.assertEquals(1, driver.getHiveHandleSize());
    validateExecuteAsync(ctx, DriverQueryState.SUCCESSFUL, true, false);
    driver.closeQuery(ctx.getQueryHandle());
    Assert.assertEquals(0, driver.getHiveHandleSize());

    conf.set(LensConfConstants.QUERY_OUTPUT_DIRECTORY_FORMAT,
        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'"
            + " WITH SERDEPROPERTIES ('serialization.null.format'='-NA-',"
            + " 'field.delim'=','  ) STORED AS TEXTFILE ");
    ctx = createContext("SELECT ID, null, ID FROM test_persistent_result_set", conf);
    resultSet = driver.execute(ctx);
    Assert.assertEquals(0, driver.getHiveHandleSize());
    validatePersistentResult(resultSet, TEST_DATA_FILE, ctx.getHDFSResultDir(), true);
    driver.closeQuery(ctx.getQueryHandle());
    Assert.assertEquals(0, driver.getHiveHandleSize());

    ctx = createContext("SELECT ID, null, ID FROM test_persistent_result_set", conf);
    driver.executeAsync(ctx);
    Assert.assertEquals(1, driver.getHiveHandleSize());
    validateExecuteAsync(ctx, DriverQueryState.SUCCESSFUL, true, true);
    driver.closeQuery(ctx.getQueryHandle());
    Assert.assertEquals(0, driver.getHiveHandleSize());
  }

  /**
   * Wait for async query.
   *
   * @param ctx
   *          the ctx
   * @param driver
   *          the driver
   * @throws Exception
   *           the exception
   */
  private void waitForAsyncQuery(QueryContext ctx, HiveDriver driver) throws Exception {
    while (true) {
      driver.updateStatus(ctx);
      System.out.println("#W Waiting for query " + ctx.getQueryHandle() + " status: "
          + ctx.getDriverStatus().getState());
      assertNotNull(ctx.getDriverStatus());
      if (ctx.getDriverStatus().isFinished()) {
        assertTrue(ctx.getDriverStatus().getDriverFinishTime() > 0);
        break;
      }
      System.out.println("Progress:" + ctx.getDriverStatus().getProgressMessage());
      Thread.sleep(1000);
      assertTrue(ctx.getDriverStatus().getDriverStartTime() > 0);
    }
  }

  // explain
  /**
   * Test explain.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testExplain() throws Exception {
    createTestTable("test_explain");
    SessionState.setCurrentSessionState(ss);
    DriverQueryPlan plan = driver.explain("SELECT ID FROM test_explain", conf);
    assertTrue(plan instanceof HiveQueryPlan);
    assertEquals(plan.getTableWeight(DATA_BASE + ".test_explain"), 500.0);
    Assert.assertEquals(0, driver.getHiveHandleSize());

    // test execute prepare
    PreparedQueryContext pctx = new PreparedQueryContext("SELECT ID FROM test_explain", null, conf, drivers);
    pctx.setSelectedDriver(driver);

    SessionState.setCurrentSessionState(ss);
    plan = driver.explainAndPrepare(pctx);
    QueryContext qctx = createContext(pctx, conf);
    LensResultSet result = driver.execute(qctx);
    Assert.assertEquals(0, driver.getHiveHandleSize());
    validateInMemoryResult(result);

    // test execute prepare async
    qctx = createContext(pctx, conf);
    driver.executeAsync(qctx);
    assertNotNull(qctx.getDriverOpHandle());
    validateExecuteAsync(qctx, DriverQueryState.SUCCESSFUL, false, false);
    Assert.assertEquals(1, driver.getHiveHandleSize());

    driver.closeQuery(qctx.getQueryHandle());
    Assert.assertEquals(0, driver.getHiveHandleSize());

    // for backward compatibility
    qctx = createContext(pctx, conf);
    qctx.setQueryHandle(new QueryHandle(pctx.getPrepareHandle().getPrepareHandleId()));
    result = driver.execute(qctx);
    assertNotNull(qctx.getDriverOpHandle());
    Assert.assertEquals(0, driver.getHiveHandleSize());
    validateInMemoryResult(result);
    // test execute prepare async
    qctx = createContext(pctx, conf);
    qctx.setQueryHandle(new QueryHandle(pctx.getPrepareHandle().getPrepareHandleId()));
    driver.executeAsync(qctx);
    Assert.assertEquals(1, driver.getHiveHandleSize());
    validateExecuteAsync(qctx, DriverQueryState.SUCCESSFUL, false, false);

    driver.closeQuery(qctx.getQueryHandle());
    driver.closePreparedQuery(pctx.getPrepareHandle());
    Assert.assertEquals(0, driver.getHiveHandleSize());
  }

  /**
   * Test explain partitioned table
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testExplainPartitionedTable() throws Exception {
    createPartitionedTable("test_part_table");
    // acquire
    SessionState.setCurrentSessionState(ss);
    DriverQueryPlan plan = driver.explain("SELECT ID FROM test_part_table", conf);
    Assert.assertEquals(0, driver.getHiveHandleSize());
    assertTrue(plan instanceof HiveQueryPlan);
    assertNotNull(plan.getTablesQueried());
    assertEquals(plan.getTablesQueried().size(), 1);
    System.out.println("Tables:" + plan.getTablesQueried());
    assertEquals(plan.getTableWeight(DATA_BASE + ".test_part_table"), 500.0);
    System.out.println("Parts:" + plan.getPartitions());
    assertFalse(plan.getPartitions().isEmpty());
    assertTrue(plan.getPartitions().get(DATA_BASE + ".test_part_table").get(0).contains("today"));
    assertTrue(plan.getPartitions().get(DATA_BASE + ".test_part_table").get(0).contains("dt"));
  }

  /**
   * Test explain output.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testExplainOutput() throws Exception {
    createTestTable("explain_test_1");
    createTestTable("explain_test_2");

    SessionState.setCurrentSessionState(ss);
    DriverQueryPlan plan = driver.explain("SELECT explain_test_1.ID, count(1) FROM "
        + " explain_test_1  join explain_test_2 on explain_test_1.ID = explain_test_2.ID"
        + " WHERE explain_test_1.ID = 'foo' or explain_test_2.ID = 'bar'" + " GROUP BY explain_test_1.ID", conf);

    Assert.assertEquals(0, driver.getHiveHandleSize());
    assertTrue(plan instanceof HiveQueryPlan);
    assertNotNull(plan.getTablesQueried());
    assertEquals(plan.getTablesQueried().size(), 2);
    assertNotNull(plan.getTableWeights());
    assertTrue(plan.getTableWeights().containsKey(DATA_BASE + ".explain_test_1"));
    assertTrue(plan.getTableWeights().containsKey(DATA_BASE + ".explain_test_2"));
    assertEquals(plan.getNumJoins(), 1);
    assertTrue(plan.getPlan() != null && !plan.getPlan().isEmpty());
    driver.closeQuery(plan.getHandle());
  }

  /**
   * Test explain output persistent.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testExplainOutputPersistent() throws Exception {
    createTestTable("explain_test_1");
    conf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, true);
    SessionState.setCurrentSessionState(ss);
    String query2 = "SELECT DISTINCT ID FROM explain_test_1";
    PreparedQueryContext pctx = new PreparedQueryContext(query2, null, conf, drivers);
    pctx.setSelectedDriver(driver);
    DriverQueryPlan plan2 = driver.explainAndPrepare(pctx);
    // assertNotNull(plan2.getResultDestination());
    Assert.assertEquals(0, driver.getHiveHandleSize());
    assertNotNull(plan2.getTablesQueried());
    assertEquals(plan2.getTablesQueried().size(), 1);
    assertTrue(plan2.getTableWeights().containsKey(DATA_BASE + ".explain_test_1"));
    assertEquals(plan2.getNumSels(), 1);
    QueryContext ctx = createContext(pctx, conf);
    LensResultSet resultSet = driver.execute(ctx);
    Assert.assertEquals(0, driver.getHiveHandleSize());
    HivePersistentResultSet persistentResultSet = (HivePersistentResultSet) resultSet;
    String path = persistentResultSet.getOutputPath();
    assertEquals(ctx.getHdfsoutPath(), path);
    driver.closeQuery(plan2.getHandle());
  }

  /**
   * Testing Duration Based Priority Logic by mocking everything except partitions.
   * @throws IOException
   * @throws LensException
   */
  @Test
  public void testPriority() throws IOException, LensException {
    Configuration conf = new Configuration();
    final MockDriver mockDriver = new MockDriver();
    mockDriver.configure(conf);

    BufferedReader br = new BufferedReader(new InputStreamReader(TestHiveDriver.class.getResourceAsStream("/priority_tests.txt")));
    String line;
    while((line = br.readLine()) != null) {
      String[] kv = line.split("\\s*:\\s*");

      final List<String> partitions = Arrays.asList(kv[0].trim().split("\\s*,\\s*"));
      final Priority expected = Priority.valueOf(kv[1]);
      final HashMap<LensDriver, String> driverQuery1 = new HashMap<LensDriver, String>() {
        {
          put(mockDriver, "driverQuery1");
        }
      };
      AbstractQueryContext ctx = new MockQueryContext("driverQuery1", new LensConf(), conf,
                                                      driverQuery1.keySet());
      ctx.getDriverContext().setDriverQueriesAndPlans(driverQuery1);
      ctx.setSelectedDriver(mockDriver);

      ((MockDriver.MockQueryPlan)ctx.getDriverContext().getDriverQueryPlan(mockDriver)).setPartitions
        (new HashMap<String,
          List<String>>
          () {
          {
            put("table1", partitions);
        }});
      Assert.assertEquals(expected, driver.queryPriorityDecider.decidePriority(ctx));
    }
  }
}

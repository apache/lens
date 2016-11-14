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
import java.text.ParseException;
import java.util.*;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.Priority;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.*;
import org.apache.lens.server.api.driver.DriverQueryStatus.DriverQueryState;
import org.apache.lens.server.api.driver.hooks.DriverQueryHook;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.ExplainQueryContext;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.cost.QueryCost;
import org.apache.lens.server.api.query.priority.CostRangePriorityDecider;
import org.apache.lens.server.api.query.priority.CostToPriorityRangeConf;
import org.apache.lens.server.api.user.MockDriverQueryHook;
import org.apache.lens.server.api.util.LensUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.HiveDriverRunHook;
import org.apache.hadoop.hive.ql.HiveDriverRunHookContext;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.ColumnDescriptor;

import org.testng.annotations.*;

import com.beust.jcommander.internal.Maps;
import com.google.common.collect.Lists;


/**
 * The Class TestHiveDriver.
 */
public class TestHiveDriver {

  /** The Constant TEST_DATA_FILE. */
  public static final String TEST_DATA_FILE = "testdata/testdata1.data";

  /** The test output dir. */
  private final String testOutputDir = "target/" + this.getClass().getSimpleName() + "/test-output";

  /** The conf. */
  protected Configuration driverConf = new Configuration();
  protected HiveConf hiveConf = new HiveConf();
  protected Configuration queryConf = new Configuration();

  /** The driver. */
  protected HiveDriver driver;

  /** Driver list * */
  protected Collection<LensDriver> drivers;

  /** The data base. */
  String dataBase = this.getClass().getSimpleName().toLowerCase();

  protected String sessionid;
  protected SessionState ss;
  private CostRangePriorityDecider alwaysNormalPriorityDecider
    = new CostRangePriorityDecider(new CostToPriorityRangeConf(""));

  /**
   * Before test.
   *
   * @throws Exception the exception
   */
  @BeforeTest
  public void beforeTest() throws Exception {
    // Check if hadoop property set
    System.out.println("###HADOOP_PATH " + System.getProperty("hadoop.bin.path"));
    assertNotNull(System.getProperty("hadoop.bin.path"));
    createDriver();
    ss = new SessionState(hiveConf, "testuser");
    SessionState.start(ss);
    Hive client = Hive.get(hiveConf);
    Database database = new Database();
    database.setName(dataBase);
    client.createDatabase(database, true);
    SessionState.get().setCurrentDatabase(dataBase);
    sessionid = SessionState.get().getSessionId();
    driverConf.setBoolean(LensConfConstants.QUERY_ADD_INSERT_OVEWRITE, false);
    QueryContext context = createContext("USE " + dataBase, this.queryConf);
    driver.execute(context);
    driverConf.setBoolean(LensConfConstants.QUERY_ADD_INSERT_OVEWRITE, true);
    driverConf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, true);
  }

  protected void createDriver() throws LensException {
    driverConf.addResource("drivers/hive/hive1/hivedriver-site.xml");
    driverConf.setClass(HiveDriver.HIVE_CONNECTION_CLASS, EmbeddedThriftConnection.class, ThriftConnection.class);
    driverConf.setClass(LensConfConstants.DRIVER_HOOK_CLASSES_SFX, MockDriverQueryHook.class, DriverQueryHook.class);
    driverConf.set("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    driverConf.setBoolean(HiveDriver.HS2_CALCULATE_PRIORITY, true);
    driver = new HiveDriver();
    driver.configure(driverConf, "hive", "hive1");
    drivers = Lists.<LensDriver>newArrayList(driver);
    System.out.println("TestHiveDriver created");
  }

  @BeforeMethod
  public void setDB() {
    SessionState.setCurrentSessionState(ss);
  }

  protected QueryContext createContext(final String query, Configuration conf) throws LensException {
    QueryContext context = new QueryContext(query, "testuser", new LensConf(), conf, drivers);
    // session id has to be set before calling setDriverQueriesAndPlans
    context.setLensSessionIdentifier(sessionid);
    return context;
  }

  protected QueryContext createContext(final String query, Configuration conf, LensDriver driver) throws LensException {
    QueryContext context = new QueryContext(query, "testuser", new LensConf(), conf, Arrays.asList(driver));
    // session id has to be set before calling setDriverQueriesAndPlans
    context.setLensSessionIdentifier(sessionid);
    return context;
  }

  protected QueryContext createContext(PreparedQueryContext query, Configuration conf) {
    QueryContext context = new QueryContext(query, "testuser", new LensConf(), conf);
    context.setLensSessionIdentifier(sessionid);
    return context;
  }

  protected ExplainQueryContext createExplainContext(final String query, Configuration conf) {
    ExplainQueryContext ectx = new ExplainQueryContext(UUID.randomUUID().toString(), query, "testuser", null, conf,
      drivers);
    ectx.setLensSessionIdentifier(sessionid);
    return ectx;
  }

  /**
   * After test.
   *
   * @throws Exception the exception
   */
  @AfterTest
  public void afterTest() throws Exception {
    verifyThriftLogs();
    driver.close();
    Hive.get(hiveConf).dropDatabase(dataBase, true, true, true);
  }

  private void verifyThriftLogs() throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(new File("target/test.log")));
    for (String line = br.readLine(); line != null; line = br.readLine()) {
      if (line.contains("Update from hive")) {
        return;
      }
    }
    fail("No updates from hive found in the logs");
  }

  /**
   * Creates the test table.
   *
   * @param tableName the table name
   * @throws Exception the exception
   */
  protected void createTestTable(String tableName) throws Exception {
    int handleSize = getHandleSize();
    System.out.println("Hadoop Location: " + System.getProperty("hadoop.bin.path"));
    String createTable = "CREATE TABLE IF NOT EXISTS " + tableName + "(ID STRING)" + " TBLPROPERTIES ('"
      + LensConfConstants.STORAGE_COST + "'='500')";
    String dataLoad = "LOAD DATA LOCAL INPATH '" + TEST_DATA_FILE + "' OVERWRITE INTO TABLE " + tableName;
    // Create test table
    QueryContext context = createContext(createTable, queryConf);
    LensResultSet resultSet = driver.execute(context);
    assertNull(resultSet);
    // Load some data into the table
    context = createContext(dataLoad, queryConf);
    resultSet = driver.execute(context);
    assertNull(resultSet);
    assertHandleSize(handleSize);
  }

  /**
   * Creates the test table.
   *
   * @param tableName the table name
   * @throws Exception the exception
   */
  protected void createPartitionedTable(String tableName) throws Exception {
    int handleSize = getHandleSize();
    System.out.println("Hadoop Location: " + System.getProperty("hadoop.bin.path"));
    String createTable = "CREATE TABLE IF NOT EXISTS " + tableName + "(ID STRING)"
      + " PARTITIONED BY (dt string) TBLPROPERTIES ('"
      + LensConfConstants.STORAGE_COST + "'='500')";
    queryConf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    // Craete again
    QueryContext context = createContext(createTable, queryConf);
    LensResultSet resultSet = driver.execute(context);
    assertNull(resultSet);

    // Load some data into the table
    String dataLoad = "LOAD DATA LOCAL INPATH '" + TEST_DATA_FILE + "' OVERWRITE INTO TABLE " + tableName
      + " partition (dt='today')";
    context = createContext(dataLoad, queryConf);
    resultSet = driver.execute(context);
    assertNull(resultSet);
    assertHandleSize(handleSize);
  }

  // Tests

  /**
   * Test insert overwrite conf.
   *
   * @throws Exception the exception
   */
  @Test
  public void testInsertOverwriteConf() throws Exception {
    createTestTable("test_insert_overwrite");
    queryConf.setBoolean(LensConfConstants.QUERY_ADD_INSERT_OVEWRITE, false);
    String query = "SELECT ID FROM test_insert_overwrite";
    QueryContext context = createContext(query, queryConf);
    driver.addPersistentPath(context);
    assertEquals(context.getUserQuery(), query);
    assertNotNull(context.getDriverContext().getDriverQuery(driver));
    assertEquals(context.getDriverContext().getDriverQuery(driver), context.getUserQuery());
  }

  /**
   * Test temptable.
   *
   * @throws Exception the exception
   */
  @Test
  public void testTemptable() throws Exception {
    int handleSize = getHandleSize();
    createTestTable("test_temp");
    queryConf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    Hive.get(hiveConf).dropTable("test_temp_output");
    String query = "CREATE TABLE test_temp_output AS SELECT ID FROM test_temp";
    QueryContext context = createContext(query, queryConf);
    LensResultSet resultSet = driver.execute(context);
    assertNull(resultSet);
    assertHandleSize(handleSize);

    // fetch results from temp table
    String select = "SELECT * FROM test_temp_output";
    context = createContext(select, queryConf);
    resultSet = driver.execute(context);
    assertHandleSize(handleSize);
    validateInMemoryResult(resultSet, "test_temp_output");
    assertHandleSize(handleSize);
  }

  /**
   * Test execute query.
   *
   * @throws Exception the exception
   */
  @Test
  public void testExecuteQuery() throws Exception {
    int handleSize = getHandleSize();
    createTestTable("test_execute");
    LensResultSet resultSet = null;
    // Execute a select query
    queryConf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    String select = "SELECT ID FROM test_execute";
    QueryContext context = createContext(select, queryConf);
    resultSet = driver.execute(context);
    assertNotNull(context.getDriverConf(driver).get("mapred.job.name"));
    validateInMemoryResult(resultSet);
    queryConf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, true);
    context = createContext(select, queryConf);
    resultSet = driver.execute(context);
    validatePersistentResult(resultSet, TEST_DATA_FILE, context.getHDFSResultDir(), false);
    queryConf.set(LensConfConstants.QUERY_OUTPUT_DIRECTORY_FORMAT,
      "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'"
        + " WITH SERDEPROPERTIES ('serialization.null.format'='-NA-',"
        + " 'field.delim'=','  ) STORED AS TEXTFILE ");
    select = "SELECT ID, null, ID FROM test_execute";
    context = createContext(select, queryConf);
    resultSet = driver.execute(context);
    validatePersistentResult(resultSet, TEST_DATA_FILE, context.getHDFSResultDir(), true);
    assertHandleSize(handleSize);
  }

  /**
   * Validate in memory result.
   *
   * @param resultSet the result set
   * @throws LensException the lens exception
   * @throws IOException   Signals that an I/O exception has occurred.
   */
  private void validateInMemoryResult(LensResultSet resultSet) throws LensException, IOException {
    validateInMemoryResult(resultSet, null);
  }

  /**
   * Validate in memory result.
   *
   * @param resultSet   the result set
   * @param outputTable the output table
   * @throws LensException the lens exception
   * @throws IOException   Signals that an I/O exception has occurred.
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
   * @throws Exception the exception
   */
  @Test
  public void testExecuteQueryAsync() throws Exception {
    int handleSize = getHandleSize();
    createTestTable("test_execute_sync");

    // Now run a command that would fail
    String expectFail = "SELECT ID FROM test_execute_sync";
    queryConf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, true);
    Configuration failConf = new Configuration(queryConf);
    failConf.set("hive.exec.driver.run.hooks", FailHook.class.getName());
    QueryContext context = createContext(expectFail, failConf);
    driver.executeAsync(context);
    assertHandleSize(handleSize + 1);
    validateExecuteAsync(context, DriverQueryState.FAILED, true, false);
    assertHandleSize(handleSize + 1);
    driver.closeQuery(context.getQueryHandle());
    assertHandleSize(handleSize);
    // Async select query
    String select = "SELECT ID FROM test_execute_sync";
    queryConf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    context = createContext(select, queryConf);
    driver.executeAsync(context);
    assertNotNull(context.getDriverConf(driver).get("mapred.job.name"));
    assertNotNull(context.getDriverConf(driver).get("mapred.job.priority"));
    assertHandleSize(handleSize + 1);
    validateExecuteAsync(context, DriverQueryState.SUCCESSFUL, false, false);
    driver.closeQuery(context.getQueryHandle());
    assertHandleSize(handleSize);

    queryConf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, true);
    context = createContext(select, queryConf);
    driver.executeAsync(context);
    assertHandleSize(handleSize + 1);
    validateExecuteAsync(context, DriverQueryState.SUCCESSFUL, true, false);
    driver.closeQuery(context.getQueryHandle());
    assertHandleSize(handleSize);

    queryConf.set(LensConfConstants.QUERY_OUTPUT_DIRECTORY_FORMAT,
      "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'"
        + " WITH SERDEPROPERTIES ('serialization.null.format'='-NA-',"
        + " 'field.delim'=','  ) STORED AS TEXTFILE ");
    select = "SELECT ID, null, ID FROM test_execute_sync";
    context = createContext(select, queryConf);
    driver.executeAsync(context);
    assertHandleSize(handleSize + 1);
    validateExecuteAsync(context, DriverQueryState.SUCCESSFUL, true, true);
    driver.closeQuery(context.getQueryHandle());
    assertHandleSize(handleSize);
  }

  /**
   * Validate execute async.
   *
   * @param ctx          the ctx
   * @param finalState   the final state
   * @param isPersistent the is persistent
   * @param formatNulls  the format nulls
   * @param driver       the driver
   * @throws Exception the exception
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
   * @param ctx          the ctx
   * @param finalState   the final state
   * @param isPersistent the is persistent
   * @param formatNulls  the format nulls
   * @throws Exception the exception
   */
  protected void validateExecuteAsync(QueryContext ctx, DriverQueryState finalState, boolean isPersistent,
    boolean formatNulls) throws Exception {
    validateExecuteAsync(ctx, finalState, isPersistent, formatNulls, driver);
  }

  /**
   * Test cancel async query.
   *
   * @throws Exception the exception
   */
  @Test
  public void testCancelAsyncQuery() throws Exception {
    int handleSize = getHandleSize();
    createTestTable("test_cancel_async");
    queryConf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    QueryContext context = createContext("select a.id aid, b.id bid from "
        + "((SELECT ID FROM test_cancel_async) a full outer join (select id from test_cancel_async) b)",
      queryConf);
    driver.executeAsync(context);
    driver.cancelQuery(context.getQueryHandle());
    driver.updateStatus(context);
    assertEquals(context.getDriverStatus().getState(), DriverQueryState.CANCELED, "Expecting query to be cancelled");
    driver.closeQuery(context.getQueryHandle());
    assertHandleSize(handleSize);

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
   * @param resultSet   the result set
   * @param dataFile    the data file
   * @param outptuDir   the outptu dir
   * @param formatNulls the format nulls
   * @throws Exception the exception
   */
  private void validatePersistentResult(LensResultSet resultSet, String dataFile, Path outptuDir, boolean formatNulls)
    throws Exception {
    assertTrue(resultSet instanceof HivePersistentResultSet, "resultset class: " + resultSet.getClass().getName());
    HivePersistentResultSet persistentResultSet = (HivePersistentResultSet) resultSet;
    String path = persistentResultSet.getOutputPath();

    Path actualPath = new Path(path);
    FileSystem fs = actualPath.getFileSystem(driverConf);
    assertEquals(actualPath, fs.makeQualified(outptuDir));
    List<String> actualRows = new ArrayList<String>();
    for (FileStatus stat : fs.listStatus(actualPath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return !new File(path.toUri()).isDirectory();
      }
    })) {
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
   * @throws Exception the exception
   */
  @Test
  public void testPersistentResultSet() throws Exception {
    int handleSize = getHandleSize();
    createTestTable("test_persistent_result_set");
    queryConf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, true);
    queryConf.setBoolean(LensConfConstants.QUERY_ADD_INSERT_OVEWRITE, true);
    queryConf.set(LensConfConstants.RESULT_SET_PARENT_DIR, testOutputDir);
    QueryContext ctx = createContext("SELECT ID FROM test_persistent_result_set", queryConf);
    LensResultSet resultSet = driver.execute(ctx);
    validatePersistentResult(resultSet, TEST_DATA_FILE, ctx.getHDFSResultDir(), false);
    assertHandleSize(handleSize);

    ctx = createContext("SELECT ID FROM test_persistent_result_set", queryConf);
    driver.executeAsync(ctx);
    assertHandleSize(handleSize + 1);
    validateExecuteAsync(ctx, DriverQueryState.SUCCESSFUL, true, false);
    driver.closeQuery(ctx.getQueryHandle());
    assertHandleSize(handleSize);

    queryConf.set(LensConfConstants.QUERY_OUTPUT_DIRECTORY_FORMAT,
      "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'"
        + " WITH SERDEPROPERTIES ('serialization.null.format'='-NA-',"
        + " 'field.delim'=','  ) STORED AS TEXTFILE ");
    ctx = createContext("SELECT ID, null, ID FROM test_persistent_result_set", queryConf);
    resultSet = driver.execute(ctx);
    assertHandleSize(handleSize);
    validatePersistentResult(resultSet, TEST_DATA_FILE, ctx.getHDFSResultDir(), true);
    driver.closeQuery(ctx.getQueryHandle());
    assertHandleSize(handleSize);

    ctx = createContext("SELECT ID, null, ID FROM test_persistent_result_set", queryConf);
    driver.executeAsync(ctx);
    assertHandleSize(handleSize + 1);
    validateExecuteAsync(ctx, DriverQueryState.SUCCESSFUL, true, true);
    driver.closeQuery(ctx.getQueryHandle());
    assertHandleSize(handleSize);
  }

  /**
   * Wait for async query.
   *
   * @param ctx    the ctx
   * @param driver the driver
   * @throws Exception the exception
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

  @Test(expectedExceptions = {UnsupportedOperationException.class})
  public void testEstimateNativeQuery() throws Exception {
    createTestTable("test_estimate");
    SessionState.setCurrentSessionState(ss);
    QueryCost cost = driver.estimate(createExplainContext("SELECT ID FROM test_estimate", queryConf));
    assertEquals(cost.getEstimatedResourceUsage(), Double.MAX_VALUE);
    cost.getEstimatedExecTimeMillis();

  }

  @Test(expectedExceptions = {UnsupportedOperationException.class})
  public void testEstimateOlapQuery() throws Exception {
    SessionState.setCurrentSessionState(ss);
    ExplainQueryContext ctx = createExplainContext("cube SELECT ID FROM test_cube", queryConf);
    ctx.setOlapQuery(true);
    ctx.getDriverContext().setDriverRewriterPlan(driver, new DriverQueryPlan() {
      @Override
      public String getPlan() {
        return null;
      }

      @Override
      public QueryCost getCost() {
        return null;
      }

      @Override
      public Map<String, Set<?>> getPartitions() {
        return Maps.newHashMap();
      }
    });
    QueryCost cost = driver.estimate(ctx);
    assertEquals(cost.getEstimatedResourceUsage(), 0.0);
    cost.getEstimatedExecTimeMillis();
  }

  @Test
  public void testExplainNativeFailingQuery() throws Exception {
    SessionState.setCurrentSessionState(ss);
    try {
      driver.estimate(createExplainContext("SELECT ID FROM nonexist", queryConf));
      fail("Should not reach here");
    } catch (LensException e) {
      assertTrue(LensUtil.getCauseMessage(e).contains("Line 1:32 Table not found 'nonexist'"));
    }
  }

  // explain

  /**
   * Test explain.
   *
   * @throws Exception the exception
   */
  @Test
  public void testExplain() throws Exception {
    int handleSize = getHandleSize();
    SessionState.setCurrentSessionState(ss);
    SessionState.get().setCurrentDatabase(dataBase);
    createTestTable("test_explain");
    DriverQueryPlan plan = driver.explain(createExplainContext("SELECT ID FROM test_explain", queryConf));
    assertTrue(plan instanceof HiveQueryPlan);
    assertEquals(plan.getTableWeight(dataBase + ".test_explain"), 500.0);
    assertHandleSize(handleSize);

    // test execute prepare
    PreparedQueryContext pctx = new PreparedQueryContext("SELECT ID FROM test_explain", null, queryConf, drivers);
    pctx.setSelectedDriver(driver);
    pctx.setLensSessionIdentifier(sessionid);

    SessionState.setCurrentSessionState(ss);
    Configuration inConf = new Configuration(queryConf);
    inConf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    plan = driver.explainAndPrepare(pctx);
    QueryContext qctx = createContext(pctx, inConf);
    LensResultSet result = driver.execute(qctx);
    assertHandleSize(handleSize);
    validateInMemoryResult(result);

    // test execute prepare async
    queryConf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, true);
    qctx = createContext(pctx, queryConf);
    driver.executeAsync(qctx);
    assertNotNull(qctx.getDriverOpHandle());
    validateExecuteAsync(qctx, DriverQueryState.SUCCESSFUL, true, false);
    assertHandleSize(handleSize + 1);

    driver.closeQuery(qctx.getQueryHandle());
    assertHandleSize(handleSize);

    // for backward compatibility
    qctx = createContext(pctx, inConf);
    qctx.setQueryHandle(new QueryHandle(pctx.getPrepareHandle().getPrepareHandleId()));
    result = driver.execute(qctx);
    assertNotNull(qctx.getDriverOpHandle());
    assertHandleSize(handleSize);
    validateInMemoryResult(result);
    // test execute prepare async
    qctx = createContext(pctx, queryConf);
    qctx.setQueryHandle(new QueryHandle(pctx.getPrepareHandle().getPrepareHandleId()));
    driver.executeAsync(qctx);
    assertHandleSize(handleSize + 1);
    validateExecuteAsync(qctx, DriverQueryState.SUCCESSFUL, true, false);

    driver.closeQuery(qctx.getQueryHandle());
    driver.closePreparedQuery(pctx.getPrepareHandle());
    assertHandleSize(handleSize);
  }

  /**
   * Test explain partitioned table
   *
   * @throws Exception the exception
   */
  @Test
  public void testExplainPartitionedTable() throws Exception {
    int handleSize = getHandleSize();
    createPartitionedTable("test_part_table");
    // acquire
    SessionState.setCurrentSessionState(ss);
    DriverQueryPlan plan = driver.explain(createExplainContext("SELECT ID FROM test_part_table", queryConf));
    assertHandleSize(handleSize);
    assertTrue(plan instanceof HiveQueryPlan);
    assertNotNull(plan.getTablesQueried());
    assertEquals(plan.getTablesQueried().size(), 1);
    System.out.println("Tables:" + plan.getTablesQueried());
    assertEquals(plan.getTableWeight(dataBase + ".test_part_table"), 500.0);
    System.out.println("Parts:" + plan.getPartitions());
    assertFalse(plan.getPartitions().isEmpty());
    assertEquals(plan.getPartitions().size(), 1);
    assertTrue(((String) plan.getPartitions().get(dataBase + ".test_part_table").iterator().next()).contains("today"));
    assertTrue(((String) plan.getPartitions().get(dataBase + ".test_part_table").iterator().next()).contains("dt"));
  }

  /**
   * Test explain output.
   *
   * @throws Exception the exception
   */
  @Test
  public void testExplainOutput() throws Exception {
    int handleSize = getHandleSize();
    createTestTable("explain_test_1");
    createTestTable("explain_test_2");
    SessionState.setCurrentSessionState(ss);
    DriverQueryPlan plan = driver.explain(createExplainContext("SELECT explain_test_1.ID, count(1) FROM "
        + " explain_test_1  join explain_test_2 on explain_test_1.ID = explain_test_2.ID"
        + " WHERE explain_test_1.ID = 'foo' or explain_test_2.ID = 'bar'" + " GROUP BY explain_test_1.ID",
      queryConf));

    assertHandleSize(handleSize);
    assertTrue(plan instanceof HiveQueryPlan);
    assertNotNull(plan.getTablesQueried());
    assertEquals(plan.getTablesQueried().size(), 2);
    assertNotNull(plan.getTableWeights());
    assertTrue(plan.getTableWeights().containsKey(dataBase + ".explain_test_1"));
    assertTrue(plan.getTableWeights().containsKey(dataBase + ".explain_test_2"));
    assertTrue(plan.getPlan() != null && !plan.getPlan().isEmpty());
    driver.closeQuery(plan.getHandle());
  }

  /**
   * Test explain output persistent.
   *
   * @throws Exception the exception
   */
  @Test
  public void testExplainOutputPersistent() throws Exception {
    int handleSize = getHandleSize();
    createTestTable("explain_test_1");
    queryConf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, true);
    SessionState.setCurrentSessionState(ss);
    String query2 = "SELECT DISTINCT ID FROM explain_test_1";
    PreparedQueryContext pctx = createPreparedQueryContext(query2);
    pctx.setSelectedDriver(driver);
    pctx.setLensSessionIdentifier(sessionid);
    DriverQueryPlan plan2 = driver.explainAndPrepare(pctx);
    // assertNotNull(plan2.getResultDestination());
    assertHandleSize(handleSize);
    assertNotNull(plan2.getTablesQueried());
    assertEquals(plan2.getTablesQueried().size(), 1);
    assertTrue(plan2.getTableWeights().containsKey(dataBase + ".explain_test_1"));
    QueryContext ctx = createContext(pctx, queryConf);
    LensResultSet resultSet = driver.execute(ctx);
    assertHandleSize(handleSize);
    HivePersistentResultSet persistentResultSet = (HivePersistentResultSet) resultSet;
    String path = persistentResultSet.getOutputPath();
    assertEquals(ctx.getDriverResultPath(), path);
    driver.closeQuery(plan2.getHandle());
  }

  private PreparedQueryContext createPreparedQueryContext(String query2) {
    PreparedQueryContext pctx = new PreparedQueryContext(query2, null, queryConf, drivers);
    pctx.setSelectedDriver(driver);
    pctx.setLensSessionIdentifier(sessionid);
    return pctx;
  }

  @DataProvider
  public Object[][] priorityDataProvider() throws IOException, ParseException {
    BufferedReader br = new BufferedReader(new InputStreamReader(
      TestHiveDriver.class.getResourceAsStream("/priority_tests.data")));
    String line;
    int numTests = Integer.parseInt(br.readLine());
    Object[][] data = new Object[numTests][2];
    for (int i = 0; i < numTests; i++) {
      String[] kv = br.readLine().split("\\s*:\\s*");
      final Set<FactPartition> partitions = getFactParts(Arrays.asList(kv[0].trim().split("\\s*,\\s*")));
      final Priority expected = Priority.valueOf(kv[1]);
      data[i] = new Object[]{partitions, expected};
    }
    return data;
  }


  /**
   * Testing Duration Based Priority Logic by mocking everything except partitions.
   *
   * @throws IOException
   * @throws LensException
   * @throws ParseException
   */
  @Test(dataProvider = "priorityDataProvider")
  public void testPriority(final Set<FactPartition> partitions, Priority expected) throws Exception {
    Configuration conf = new Configuration();
    QueryContext ctx = createContext("test priority query", conf);
    ctx.getDriverContext().setDriverRewriterPlan(driver, new DriverQueryPlan() {

      @Override
      public String getPlan() {
        return null;
      }

      @Override
      public QueryCost getCost() {
        return null;
      }
    });

    ctx.getDriverContext().getDriverRewriterPlan(driver).getPartitions().putAll(
      new HashMap<String, Set<FactPartition>>() {
        {
          put("table1", partitions);
        }
      });
    // table weights only for first calculation
    ctx.getDriverContext().getDriverRewriterPlan(driver).getTableWeights().putAll(
      new HashMap<String, Double>() {
        {
          put("table1", 1.0);
        }
      });
    ctx.setOlapQuery(true);
    Priority priority = driver.decidePriority(ctx);
    assertEquals(priority, expected, "cost: " + ctx.getDriverQueryCost(driver) + "priority: " + priority);
    assertEquals(ctx.getConf().get("mapred.job.priority"), priority.toString());
    assertEquals(driver.decidePriority(ctx, alwaysNormalPriorityDecider), Priority.NORMAL);
  }

  @Test
  public void testPriorityWithoutFactPartitions() throws LensException {
    // test priority without fact partitions
    QueryContext ctx = createContext("test priority query", queryConf);
    ctx.getDriverContext().setDriverRewriterPlan(driver, new DriverQueryPlan() {

      @Override
      public String getPlan() {
        return null;
      }

      @Override
      public QueryCost getCost() {
        return null;
      }
    });

    ctx.getDriverContext().getDriverRewriterPlan(driver).getPartitions().putAll(
      new HashMap<String, Set<String>>() {
        {
          put("table1", new HashSet<String>());
        }
      });
    ctx.getDriverContext().getDriverRewriterPlan(driver).getTableWeights().putAll(
      new HashMap<String, Double>() {
        {
          put("table1", 1.0);
        }
      });
    ctx.setDriverCost(driver, driver.queryCostCalculator.calculateCost(ctx, driver));
    assertEquals(driver.decidePriority(ctx, driver.queryPriorityDecider), Priority.VERY_HIGH);
    assertEquals(driver.decidePriority(ctx, alwaysNormalPriorityDecider), Priority.NORMAL);

    // test priority without rewriter plan
    ctx = createContext("test priority query", queryConf);
    ctx.getDriverContext().setDriverRewriterPlan(driver, new DriverQueryPlan() {
      @Override
      public String getPlan() {
        return null;
      }

      @Override
      public QueryCost getCost() {
        return null;
      }
    });
    ctx.setDriverCost(driver, driver.queryCostCalculator.calculateCost(ctx, driver));
    assertEquals(driver.decidePriority(ctx), Priority.VERY_HIGH);
    assertEquals(alwaysNormalPriorityDecider.decidePriority(ctx.getDriverQueryCost(driver)), Priority.NORMAL);
  }

  private Set<FactPartition> getFactParts(List<String> partStrings) throws ParseException {
    Set<FactPartition> factParts = new HashSet<FactPartition>();
    for (String partStr : partStrings) {
      String[] partEls = partStr.split(" ");
      UpdatePeriod p = null;
      String partSpec = partEls[1];
      switch (partSpec.length()) {
      case 7: //monthly
        p = UpdatePeriod.MONTHLY;
        break;
      case 10: // daily
        p = UpdatePeriod.DAILY;
        break;
      case 13: // hourly
        p = UpdatePeriod.HOURLY;
        break;
      }
      FactPartition part = new FactPartition(partEls[0], p.parse(partSpec), p, null, p.format(),
        Collections.singleton("table1"));
      factParts.add(part);
    }
    return factParts;
  }

  private int getHandleSize() {
    return driver.getHiveHandleSize();
  }

  private void assertHandleSize(int handleSize) {
    assertEquals(getHandleSize(), handleSize, "Unexpected handle size, all handles: "
      + driver.getHiveHandles());
  }
}

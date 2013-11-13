package com.inmobi.grill.driver.hive;

import static org.testng.Assert.*;

import java.io.*;
import java.util.*;

import com.inmobi.grill.api.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.thrift.TStringValue;
import org.testng.annotations.*;

import com.inmobi.grill.api.QueryStatus.Status;
import com.inmobi.grill.exception.GrillException;

public class TestHiveDriver {
  public static final String TEST_DATA_FILE = "testdata/testdata1.txt";
  public static final String TEST_OUTPUT_DIR = "test-output";
  protected static HiveConf conf = new HiveConf();
  protected HiveDriver driver;
  public static final String DATA_BASE = "test_hive_driver";

  @BeforeTest
  public void beforeTest() throws Exception {
    // Check if hadoop property set
    System.out.println("###HADOOP_PATH " + System.getProperty("hadoop.bin.path"));
    assertNotNull(System.getProperty("hadoop.bin.path"));
    conf.setClass(HiveDriver.GRILL_HIVE_CONNECTION_CLASS,
        EmbeddedThriftConnection.class, 
        ThriftConnection.class);
    conf.set(HiveDriver.GRILL_PASSWORD_KEY, "password");
    conf.set(HiveDriver.GRILL_USER_NAME_KEY, "user");
    conf.set("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");

    SessionState.start(conf);
    Hive client = Hive.get(conf);
    Database database = new Database();
    database.setName(TestHiveDriver.class.getSimpleName());
    client.createDatabase(database, true);
    SessionState.get().setCurrentDatabase(TestHiveDriver.class.getSimpleName());

    driver = new HiveDriver();
    driver.configure(conf);
    conf.setBoolean(HiveDriver.GRILL_ADD_INSERT_OVEWRITE, false);
    conf.setBoolean(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, false);
    QueryContext context = new QueryContext(
        "USE " + TestHiveDriver.class.getSimpleName(), null, conf);
    driver.execute(context);
    conf.setBoolean(HiveDriver.GRILL_ADD_INSERT_OVEWRITE, true);
    conf.setBoolean(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, true);
    System.out.println("Driver created");
  }

  @AfterTest
  public void afterTest() throws Exception {
    driver.close();
    Hive.get(conf).dropDatabase(TestHiveDriver.class.getSimpleName(), true, true, true);
  }


  private void createTestTable(String tableName) throws Exception {
    System.out.println("Hadoop Location: " + System.getProperty("hadoop.bin.path"));
    String createTable = "CREATE TABLE IF NOT EXISTS " + tableName  +"(ID STRING)" +
        " TBLPROPERTIES ('" + GrillConfConstants.STORAGE_COST + "'='500')";
    conf.setBoolean(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, false);
    // Craete again
    QueryContext context = new QueryContext(createTable, null, conf);
    GrillResultSet resultSet = driver.execute(context);
    assertNotNull(resultSet);
    assertTrue(resultSet instanceof HiveInMemoryResultSet, "expecting in-memory result set");

    // Load some data into the table
    String dataLoad = "LOAD DATA LOCAL INPATH '"+ TEST_DATA_FILE +"' OVERWRITE INTO TABLE " + tableName;
    context = new QueryContext(dataLoad, null, conf);
    resultSet = driver.execute(context);
  }

  // Tests
  @Test
  public void testInsertOverwriteConf() throws Exception {
    createTestTable("test_insert_overwrite");
    conf.setBoolean(HiveDriver.GRILL_ADD_INSERT_OVEWRITE, false);
    String query = "SELECT ID FROM test_insert_overwrite";
    QueryContext context = new QueryContext(query, null, conf);
    driver.addPersistentPath(context);
    assertEquals(context.getUserQuery(), query);
    assertNotNull(context.getDriverQuery());
    assertEquals(context.getDriverQuery(), context.getUserQuery());
  }

  @Test
  public void testExecuteQuery() throws Exception {
    createTestTable("test_execute");
    GrillResultSet resultSet = null;
    // Execute a select query
    System.err.println("Execute select");
    String select = "SELECT ID FROM test_execute";
    QueryContext context = new QueryContext(select, null, conf);
    resultSet = driver.execute(context);
    validateExecuteSync(resultSet);
  }

  private void validateExecuteSync(GrillResultSet resultSet)
      throws GrillException, IOException {
    assertNotNull(resultSet);
    assertTrue(resultSet instanceof HiveInMemoryResultSet);
    HiveInMemoryResultSet inmemrs = (HiveInMemoryResultSet) resultSet;

    // check metadata
    GrillResultSetMetadata rsMeta = inmemrs.getMetadata();
    List<ResultColumn>  columns = rsMeta.getColumns();
    assertNotNull(columns);
    assertEquals(columns.size(), 1);
    assertEquals("ID".toLowerCase(), columns.get(0).getName().toLowerCase());
    assertEquals("STRING".toLowerCase(), columns.get(0).getType().toLowerCase());

    List<String> expectedRows = new ArrayList<String>();
    // Read data from the test file into expectedRows
    BufferedReader br = new BufferedReader(new InputStreamReader(
        new FileInputStream(TEST_DATA_FILE)));
    String line = "";
    while ((line = br.readLine()) != null) {
      expectedRows.add(line.trim());
    }
    br.close();

    List<String> actualRows = new ArrayList<String>();
    while (inmemrs.hasNext()) {
      List<Object> row = inmemrs.next();
      TStringValue thriftString = (TStringValue) row.get(0);
      actualRows.add(thriftString.getValue());
    }
    System.out.print(actualRows);
    assertEquals(actualRows, expectedRows);
  }

  // executeAsync
  @Test
  public void testExecuteQueryAsync()  throws Exception {
    createTestTable("test_execute_sync");

    // Now run a command that would fail
    String expectFail = "SELECT * FROM FOO_BAR";
    conf.setBoolean(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, true);
    QueryContext context = new QueryContext(expectFail, null, conf);
    driver.executeAsync(context);
    validateExecuteAsync(context.getQueryHandle(), Status.FAILED);
    driver.closeQuery(context.getQueryHandle());


    //  Async select query
    String select = "SELECT ID FROM test_execute_sync";
    conf.setBoolean(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, false);
    context = new QueryContext(select, null, conf);
    driver.executeAsync(context);
    validateExecuteAsync(context.getQueryHandle(), Status.SUCCESSFUL);
    driver.closeQuery(context.getQueryHandle());
  }

  private void validateExecuteAsync(QueryHandle handle, Status finalState)
      throws Exception {
    waitForAsyncQuery(handle, driver);
    QueryStatus status = driver.getStatus(handle);
    assertEquals(status.getStatus(), finalState, "Expected query to finish with"
        + finalState);
  }

  @Test
  public void testCancelAsyncQuery() throws Exception {
    createTestTable("test_cancel_async");
    conf.setBoolean(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, false);
    QueryContext context = new QueryContext("SELECT ID FROM test_cancel_async", null, conf);
    driver.executeAsync(context);
    driver.cancelQuery(context.getQueryHandle());
    QueryStatus status = driver.getStatus(context.getQueryHandle());
    assertEquals(status.getStatus(), Status.CANCELED, "Expecting query to be cancelled");
    driver.closeQuery(context.getQueryHandle());

    try {
      driver.cancelQuery(context.getQueryHandle());
      fail("Cancel on closed query should throw error");
    } catch (GrillException exc) {
      assertTrue(exc.getMessage().startsWith("Query not found"));
    }
  }

  @Test
  public void testPersistentResultSet() throws Exception {
    createTestTable("test_persistent_result_set");
    conf.setBoolean(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, true);
    conf.setBoolean(HiveDriver.GRILL_ADD_INSERT_OVEWRITE, true);
    conf.set(GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR, TEST_OUTPUT_DIR);
    QueryContext context = new QueryContext(
        "SELECT ID FROM test_persistent_result_set", null, conf);
    GrillResultSet resultSet = driver.execute(context);
    assertTrue(resultSet instanceof HivePersistentResultSet);
    HivePersistentResultSet persistentResultSet = (HivePersistentResultSet) resultSet;
    String path = persistentResultSet.getOutputPath();
    QueryHandle handle = persistentResultSet.getQueryHandle();

    FileSystem fs = FileSystem.get(conf);
    Path actualPath = new Path(path);
    assertEquals(actualPath, new Path(TEST_OUTPUT_DIR, handle.toString()).makeQualified(fs));
    assertTrue(FileSystem.get(conf).exists(actualPath));
    validatePersistentResult(actualPath, TEST_DATA_FILE);
    fs.delete(actualPath, true);
  }

  public static void validatePersistentResult(Path actualPath, String dataFile) throws IOException {
    // read in data from output
    FileSystem fs = actualPath.getFileSystem(conf);
    Set<String> actualRows = new HashSet<String>();
    for (FileStatus stat : fs.listStatus(actualPath)) {
      FSDataInputStream in = fs.open(stat.getPath());
      BufferedReader br = null;
      try {
        br = new BufferedReader(new InputStreamReader(in));
        String line = "";

        while ((line = br.readLine()) != null) {
          actualRows.add(line.trim());
        }
      } finally {
        if (br != null) {
          br.close();
        }
      }
    }


    BufferedReader br = null;
    Set<String> expectedRows = new HashSet<String>();

    try {
      br = new BufferedReader(new FileReader(new File(dataFile)));
      String line = "";
      while ((line = br.readLine()) != null) {
        expectedRows.add(line.trim());
      }
    } finally {
      if (br != null) {
        br.close();
      }
    }
    assertEquals(actualRows, expectedRows);
  }

  private void waitForAsyncQuery(QueryHandle handle, HiveDriver driver) throws Exception {
    Set<Status> terminationStates =
        EnumSet.of(Status.CANCELED, Status.CLOSED, Status.FAILED,
            Status.SUCCESSFUL);

    while (true) {
      QueryStatus status = driver.getStatus(handle);
      assertNotNull(status);
      if (terminationStates.contains(status.getStatus())) {
        break;
      }
      Thread.sleep(1000);
    }
  }

  // explain
  @Test
  public void testExplain() throws Exception {
    createTestTable("test_explain");
    QueryPlan plan = driver.explain("SELECT ID FROM test_explain", conf);
    assertTrue(plan instanceof HiveQueryPlan);
    assertEquals(plan.getTableWeight("test_explain"), 500.0);

    // test execute prepare
    PreparedQueryContext pctx = new PreparedQueryContext(
        "SELECT ID FROM test_explain", null, conf);
    plan = driver.explainAndPrepare(pctx);
    QueryContext qctx = new QueryContext(pctx, null, conf);
    GrillResultSet result = driver.execute(qctx);
    validateExecuteSync(result);

    // test execute prepare async
    driver.executeAsync(qctx);
    validateExecuteAsync(qctx.getQueryHandle(), Status.SUCCESSFUL);

    driver.closeQuery(qctx.getQueryHandle());

    // for backward compatibility
    qctx = new QueryContext(pctx, null, conf);
    qctx.setQueryHandle(new QueryHandle(pctx.getPrepareHandle().getHandleId()));
    result = driver.execute(qctx);
    validateExecuteSync(result);
    // test execute prepare async
    driver.executeAsync(qctx);
    validateExecuteAsync(plan.getHandle(), Status.SUCCESSFUL);

    driver.closeQuery(plan.getHandle());
    driver.closePreparedQuery(pctx.getPrepareHandle());

    conf.setBoolean(GrillConfConstants.PREPARE_ON_EXPLAIN, false);
    plan = driver.explain("SELECT ID FROM test_explain", conf);
    assertTrue(plan instanceof HiveQueryPlan);
    assertNull(plan.getHandle());
    conf.setBoolean(GrillConfConstants.PREPARE_ON_EXPLAIN, true);
  }

  @Test
  public void testExplainOutput() throws Exception {
    createTestTable("explain_test_1");
    createTestTable("explain_test_2");

    QueryPlan plan = driver.explain("SELECT explain_test_1.ID, count(1) FROM " +
        " explain_test_1  join explain_test_2 on explain_test_1.ID = explain_test_2.ID" +
        " WHERE explain_test_1.ID = 'foo' or explain_test_2.ID = 'bar'" +
        " GROUP BY explain_test_1.ID", conf);

    assertTrue(plan instanceof  HiveQueryPlan);
    assertNotNull(plan.getTablesQueried());
    assertEquals(plan.getTablesQueried().size(), 2);
    assertNotNull(plan.getTableWeights());
    assertTrue(plan.getTableWeights().containsKey("explain_test_1"));
    assertTrue(plan.getTableWeights().containsKey("explain_test_2"));
    assertEquals(plan.getNumJoins(), 1);
    driver.closeQuery(plan.getHandle());
  }

  @Test
  public void testExplainOutputPersistent() throws Exception {
    createTestTable("explain_test_1");
    conf.setBoolean(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, true);
    String query2 = "SELECT DISTINCT ID FROM explain_test_1";
    PreparedQueryContext pctx = new PreparedQueryContext(query2, null, conf);
    QueryPlan plan2 = driver.explainAndPrepare(pctx);
    //assertNotNull(plan2.getResultDestination());
    assertNotNull(plan2.getTablesQueried());
    assertEquals(plan2.getTablesQueried().size(), 1);
    assertTrue(plan2.getTableWeights().containsKey("explain_test_1"));
    assertEquals(plan2.getNumSels(), 1);
    QueryContext ctx = new QueryContext(pctx, null, conf);
    GrillResultSet resultSet = driver.execute(ctx);
    HivePersistentResultSet persistentResultSet = (HivePersistentResultSet) resultSet;
    String path = persistentResultSet.getOutputPath();
    assertEquals(ctx.getResultSetPath(), path);
    driver.closeQuery(plan2.getHandle());
  }
}

package com.inmobi.grill.driver.hive;

import static org.testng.Assert.*;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.testng.annotations.*;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.api.query.ResultColumn;
import com.inmobi.grill.api.query.QueryStatus.Status;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.driver.DriverQueryPlan;
import com.inmobi.grill.server.api.driver.GrillResultSet;
import com.inmobi.grill.server.api.driver.GrillResultSetMetadata;
import com.inmobi.grill.server.api.query.PreparedQueryContext;
import com.inmobi.grill.server.api.query.QueryContext;

public class TestHiveDriver {
  public static final String TEST_DATA_FILE = "testdata/testdata1.txt";
  public final String TEST_OUTPUT_DIR = this.getClass().getSimpleName() + "/test-output";
  protected HiveConf conf = new HiveConf();
  protected HiveDriver driver;
  public final String DATA_BASE = this.getClass().getSimpleName();

  @BeforeTest
  public void beforeTest() throws Exception {
    // Check if hadoop property set
    System.out.println("###HADOOP_PATH " + System.getProperty("hadoop.bin.path"));
    assertNotNull(System.getProperty("hadoop.bin.path"));
    conf.setClass(HiveDriver.GRILL_HIVE_CONNECTION_CLASS,
        EmbeddedThriftConnection.class, 
        ThriftConnection.class);
    conf.set("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    conf.setLong(HiveDriver.GRILL_CONNECTION_EXPIRY_DELAY, 10000);
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


  protected void createTestTable(String tableName) throws Exception {
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
    conf.setBoolean(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, false);
    String select = "SELECT ID FROM test_execute";
    QueryContext context = new QueryContext(select, null, conf);
    resultSet = driver.execute(context);
    validateInMemoryResult(resultSet);
    conf.setBoolean(HiveDriver.GRILL_PERSISTENT_RESULT_SET, true);
    context = new QueryContext(select, null, conf);
    resultSet = driver.execute(context);
    validatePersistentResult(resultSet, TEST_DATA_FILE, HiveDriver.GRILL_RESULT_SET_PARENT_DIR_DEFAULT, false);
    conf.set(HiveDriver.GRILL_OUTPUT_DIRECTORY_FORMAT,
        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'" +
            " WITH SERDEPROPERTIES ('serialization.null.format'='-NA-'," +
        " 'field.delim'=','  ) STORED AS TEXTFILE ");
    select = "SELECT ID, null, ID FROM test_execute";
    context = new QueryContext(select, null, conf);
    resultSet = driver.execute(context);
    validatePersistentResult(resultSet, TEST_DATA_FILE, HiveDriver.GRILL_RESULT_SET_PARENT_DIR_DEFAULT, true);
  }

  private void validateInMemoryResult(GrillResultSet resultSet)
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
    assertEquals("STRING".toLowerCase(), columns.get(0).getType().name().toLowerCase());

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
      List<Object> row = inmemrs.next().getValues();
      actualRows.add((String)row.get(0));
    }
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
    validateExecuteAsync(context, Status.FAILED, true, null, false);
    driver.closeQuery(context.getQueryHandle());


    //  Async select query
    String select = "SELECT ID FROM test_execute_sync";
    conf.setBoolean(HiveDriver.GRILL_PERSISTENT_RESULT_SET, false);
    context = new QueryContext(select, null, conf);
    driver.executeAsync(context);
    validateExecuteAsync(context, Status.SUCCESSFUL, false, null, false);
    driver.closeQuery(context.getQueryHandle());

    conf.setBoolean(HiveDriver.GRILL_PERSISTENT_RESULT_SET, true);
    context = new QueryContext(select, null, conf);
    driver.executeAsync(context);
    validateExecuteAsync(context, Status.SUCCESSFUL, true,
        HiveDriver.GRILL_RESULT_SET_PARENT_DIR_DEFAULT, false);
    driver.closeQuery(context.getQueryHandle());

    conf.set(HiveDriver.GRILL_OUTPUT_DIRECTORY_FORMAT,
        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'" +
            " WITH SERDEPROPERTIES ('serialization.null.format'='-NA-'," +
        " 'field.delim'=','  ) STORED AS TEXTFILE ");
    select = "SELECT ID, null, ID FROM test_execute_sync";
    context = new QueryContext(select, null, conf);
    driver.executeAsync(context);
    validateExecuteAsync(context, Status.SUCCESSFUL, true,
        HiveDriver.GRILL_RESULT_SET_PARENT_DIR_DEFAULT, true);
    driver.closeQuery(context.getQueryHandle());

  }

  private void validateExecuteAsync(QueryContext ctx, Status finalState,
      boolean isPersistent, String outputDir, boolean formatNulls) throws Exception {
    waitForAsyncQuery(ctx.getQueryHandle(), driver);
    QueryStatus status = driver.getStatus(ctx.getQueryHandle());
    assertEquals(status.getStatus(), finalState, "Expected query to finish with"
        + finalState);
    if (finalState.equals(Status.SUCCESSFUL)) {
      System.out.println("Progress:" + status.getProgressMessage());
      assertNotNull(status.getProgressMessage());
      if (!isPersistent) {
        validateInMemoryResult(driver.fetchResultSet(ctx));
      } else{
        validatePersistentResult(driver.fetchResultSet(ctx), TEST_DATA_FILE, outputDir, formatNulls);
      }
    } else if (finalState.equals(Status.FAILED)) {
      System.out.println("Error:" + status.getErrorMessage());
      System.out.println("Error:" + status.getStatusMessage());
      assertNotNull(status.getErrorMessage());
    }
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

  private void validatePersistentResult(GrillResultSet resultSet, String dataFile, 
      String outptuDir, boolean formatNulls) throws Exception {
    assertTrue(resultSet instanceof HivePersistentResultSet);
    HivePersistentResultSet persistentResultSet = (HivePersistentResultSet) resultSet;
    String path = persistentResultSet.getOutputPath();
    QueryHandle handle = persistentResultSet.getQueryHandle();

    Path actualPath = new Path(path);
    FileSystem fs = actualPath.getFileSystem(conf);
    assertEquals(actualPath, fs.makeQualified(new Path(outptuDir, handle.toString())));
    List<String> actualRows = new ArrayList<String>();
    for (FileStatus stat : fs.listStatus(actualPath)) {
      FSDataInputStream in = fs.open(stat.getPath());
      BufferedReader br = null;
      try {
        br = new BufferedReader(new InputStreamReader(in));
        String line = "";

        while ((line = br.readLine()) != null) {
          System.out.println("Actual:" +line);
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

  @Test
  public void testPersistentResultSet() throws Exception {
    createTestTable("test_persistent_result_set");
    conf.setBoolean(HiveDriver.GRILL_PERSISTENT_RESULT_SET, true);
    conf.setBoolean(HiveDriver.GRILL_ADD_INSERT_OVEWRITE, true);
    conf.set(HiveDriver.GRILL_RESULT_SET_PARENT_DIR, TEST_OUTPUT_DIR);
    QueryContext ctx = new QueryContext("SELECT ID FROM test_persistent_result_set", null, conf);
    GrillResultSet resultSet = driver.execute(ctx);
    validatePersistentResult(resultSet, TEST_DATA_FILE, TEST_OUTPUT_DIR, false);

    ctx = new QueryContext("SELECT ID FROM test_persistent_result_set", null, conf);
    driver.executeAsync(ctx);
    validateExecuteAsync(ctx, Status.SUCCESSFUL, true, TEST_OUTPUT_DIR, false);
    driver.closeQuery(ctx.getQueryHandle());

    conf.set(HiveDriver.GRILL_OUTPUT_DIRECTORY_FORMAT,
        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'" +
            " WITH SERDEPROPERTIES ('serialization.null.format'='-NA-'," +
        " 'field.delim'=','  ) STORED AS TEXTFILE ");
    ctx = new QueryContext("SELECT ID, null, ID FROM test_persistent_result_set", null, conf);
    resultSet = driver.execute(ctx);
    validatePersistentResult(resultSet, TEST_DATA_FILE, TEST_OUTPUT_DIR, true);
    driver.closeQuery(ctx.getQueryHandle());

    ctx = new QueryContext("SELECT ID, null, ID FROM test_persistent_result_set", null, conf);
    driver.executeAsync(ctx);
    validateExecuteAsync(ctx, Status.SUCCESSFUL, true, TEST_OUTPUT_DIR, true);
    driver.closeQuery(ctx.getQueryHandle());
  }

  private void waitForAsyncQuery(QueryHandle handle, HiveDriver driver) throws Exception {
    Set<Status> terminationStates =
        EnumSet.of(Status.CANCELED, Status.CLOSED, Status.FAILED,
            Status.SUCCESSFUL);

    while (true) {
      QueryStatus status = driver.getStatus(handle);
      System.out.println("#W Waiting for query " + handle + " status: " + status.getStatus());
      assertNotNull(status);
      if (terminationStates.contains(status.getStatus())) {
        break;
      }
      System.out.println("Progress:" + status.getProgressMessage());
      Thread.sleep(1000);
    }
  }

  // explain
  @Test
  public void testExplain() throws Exception {
    createTestTable("test_explain");
    DriverQueryPlan plan = driver.explain("SELECT ID FROM test_explain", conf);
    assertTrue(plan instanceof HiveQueryPlan);
    assertEquals(plan.getTableWeight("test_explain"), 500.0);

    // test execute prepare
    PreparedQueryContext pctx = new PreparedQueryContext(
        "SELECT ID FROM test_explain", null, conf);
    plan = driver.explainAndPrepare(pctx);
    QueryContext qctx = new QueryContext(pctx, null, conf);
    GrillResultSet result = driver.execute(qctx);
    validateExecuteAsync(qctx, Status.SUCCESSFUL, false, null, false);

    // test execute prepare async
    driver.executeAsync(qctx);
    assertNotNull(qctx.getDriverOpHandle());
    validateExecuteAsync(qctx, Status.SUCCESSFUL, false, null, false);

    driver.closeQuery(qctx.getQueryHandle());

    // for backward compatibility
    qctx = new QueryContext(pctx, null, conf);
    qctx.setQueryHandle(new QueryHandle(pctx.getPrepareHandle().getPrepareHandleId()));
    result = driver.execute(qctx);
    assertNotNull(qctx.getDriverOpHandle());
    validateExecuteAsync(qctx, Status.SUCCESSFUL, false, null, false);
    // test execute prepare async
    driver.executeAsync(qctx);
    validateExecuteAsync(qctx, Status.SUCCESSFUL, false, null, false);

    driver.closeQuery(qctx.getQueryHandle());
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

    DriverQueryPlan plan = driver.explain("SELECT explain_test_1.ID, count(1) FROM " +
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
    DriverQueryPlan plan2 = driver.explainAndPrepare(pctx);
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

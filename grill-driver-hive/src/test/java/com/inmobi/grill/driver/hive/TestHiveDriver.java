package com.inmobi.grill.driver.hive;

import static org.testng.Assert.*;

import java.io.*;
import java.util.*;

import com.inmobi.grill.api.*;
import org.apache.hadoop.conf.Configuration;
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
import com.inmobi.grill.api.GrillConfUtil;

public class TestHiveDriver {
  public static final String TEST_DATA_FILE = "testdata/testdata1.txt";
  public static final String TEST_OUTPUT_DIR = "test-output";
  protected static HiveConf conf;
  protected HiveDriver driver;
  public static final String DATA_BASE = "test_hive_driver";

  @BeforeTest
  public void beforeTest() throws Exception {
    // Check if hadoop property set
    System.out.println("###HADOOP_PATH " + System.getProperty("hadoop.bin.path"));
    assertNotNull(System.getProperty("hadoop.bin.path"));
    conf = new HiveConf();
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
    conf.setBoolean(HiveDriver.GRILL_PERSISTENT_RESULT_SET, false);
    driver.execute("USE " + TestHiveDriver.class.getSimpleName(), conf);
    conf.setBoolean(HiveDriver.GRILL_ADD_INSERT_OVEWRITE, true);
    conf.setBoolean(HiveDriver.GRILL_PERSISTENT_RESULT_SET, true);
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
        " TBLPROPERTIES ('" + GrillConfUtil.STORAGE_COST + "'='500')";
    conf.setBoolean(HiveDriver.GRILL_PERSISTENT_RESULT_SET, false);
    // Craete again
    GrillResultSet resultSet = driver.execute(createTable, conf);
    assertNotNull(resultSet);
    assertTrue(resultSet instanceof HiveInMemoryResultSet, "expecting in-memory result set");

    // Load some data into the table
    String dataLoad = "LOAD DATA LOCAL INPATH '"+ TEST_DATA_FILE +"' OVERWRITE INTO TABLE " + tableName;
    resultSet = driver.execute(dataLoad, conf);
  }

  // Tests
  @Test
  public void testInsertOverwriteConf() throws Exception {
    createTestTable("test_insert_overwrite");
    conf.setBoolean(HiveDriver.GRILL_ADD_INSERT_OVEWRITE, false);
    String query = "SELECT ID FROM test_insert_overwrite";
    HiveDriver.QueryContext ctx = driver.createQueryContext(query, conf);
    assertEquals(ctx.userQuery, query);
    assertNotNull(ctx.hiveQuery);
    assertEquals(ctx.hiveQuery, ctx.userQuery);
  }

  @Test
  public void testExecuteQuery() throws Exception {
    createTestTable("test_execute");
    GrillResultSet resultSet = null;
    // Execute a select query
    System.err.println("Execute select");
    String select = "SELECT ID FROM test_execute";
    resultSet = driver.execute(select, conf);
    validateExecuteSync(resultSet);
  }

  private void validateExecuteSync(GrillResultSet resultSet)
      throws GrillException, IOException {
    assertNotNull(resultSet);
    assertTrue(resultSet instanceof HiveInMemoryResultSet);
    HiveInMemoryResultSet inmemrs = (HiveInMemoryResultSet) resultSet;

    // check metadata
    GrillResultSetMetadata rsMeta = inmemrs.getMetadata();
    List<GrillResultSetMetadata.Column>  columns = rsMeta.getColumns();
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
    // Load some data into the table
    QueryHandle handle = null;

    // Now run a command that would fail
    String expectFail = "SELECT * FROM FOO_BAR";
    conf.setBoolean(HiveDriver.GRILL_PERSISTENT_RESULT_SET, true);
    handle = driver.executeAsync(expectFail, conf);
    validateExecuteAsync(handle, Status.FAILED);
    driver.closeQuery(handle);


    //  Async select query
    String select = "SELECT ID FROM test_execute_sync";
    conf.setBoolean(HiveDriver.GRILL_PERSISTENT_RESULT_SET, false);
    handle = driver.executeAsync(select, conf);
    validateExecuteAsync(handle, Status.SUCCESSFUL);
    driver.closeQuery(handle);
  }

  private void validateExecuteAsync(QueryHandle handle, Status finalState) throws Exception {
    waitForAsyncQuery(handle, driver);
    QueryStatus status = driver.getStatus(handle);
    assertEquals(status.getStatus(), finalState, "Expected query to finish with"
        + finalState);
  }

  @Test
  public void testCancelAsyncQuery() throws Exception {
    createTestTable("test_cancel_async");
    conf.setBoolean(HiveDriver.GRILL_PERSISTENT_RESULT_SET, false);
    QueryHandle handle = driver.executeAsync("SELECT ID FROM test_cancel_async", conf);
    driver.cancelQuery(handle);
    QueryStatus status = driver.getStatus(handle);
    assertEquals(status.getStatus(), Status.CANCELED, "Expecting query to be cancelled");
    driver.closeQuery(handle);

    try {
      driver.cancelQuery(handle);
      fail("Cancel on closed query should throw error");
    } catch (GrillException exc) {
      assertTrue(exc.getMessage().startsWith("Query not found"));
    }
  }

  @Test
  public void testPersistentResultSet() throws Exception {
    createTestTable("test_persistent_result_set");
    conf.setBoolean(HiveDriver.GRILL_PERSISTENT_RESULT_SET, true);
    conf.setBoolean(HiveDriver.GRILL_ADD_INSERT_OVEWRITE, true);
    conf.set(HiveDriver.GRILL_RESULT_SET_PARENT_DIR, TEST_OUTPUT_DIR);
    GrillResultSet resultSet = driver.execute("SELECT ID FROM test_persistent_result_set", conf);
    assertTrue(resultSet instanceof HivePersistentResultSet);
    HivePersistentResultSet persistentResultSet = (HivePersistentResultSet) resultSet;
    String path = persistentResultSet.getOutputPath();
    QueryHandle handle = persistentResultSet.getQueryHandle();

    Path actualPath = new Path(path);
    assertEquals(actualPath, new Path(TEST_OUTPUT_DIR, handle.toString()));
    assertTrue(FileSystem.get(conf).exists(actualPath));

    // read in data from output
    FileSystem fs = FileSystem.get(conf);
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
      br = new BufferedReader(new FileReader(new File(TEST_DATA_FILE)));
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
    fs.delete(actualPath, true);
  }

  private void waitForAsyncQuery(QueryHandle handle, HiveDriver driver) throws Exception {
    Set<Status> terminationStates =
        EnumSet.of(Status.CANCELED, Status.CLOSED, Status.FAILED,
            Status.SUCCESSFUL);

    while (true) {
      QueryStatus status = driver.getStatus(handle);
      System.out.println("#W Waiting for query " + handle + " status: " + status.getStatus() + " driverOpHandle:"
        + status.getDriverOpHandle());
      assertNotNull(status);
      assertNotNull(status.getDriverOpHandle());
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
    GrillResultSet result = driver.executePrepare(plan.getHandle(), conf);
    validateExecuteSync(result);

    // test execute prepare async
    driver.executePrepareAsync(plan.getHandle(), conf);
    validateExecuteAsync(plan.getHandle(), Status.SUCCESSFUL);

    driver.closeQuery(plan.getHandle());

    conf.setBoolean(GrillConfConstatnts.PREPARE_ON_EXPLAIN, false);
    plan = driver.explain("SELECT ID FROM test_explain", conf);
    assertTrue(plan instanceof HiveQueryPlan);
    assertNull(plan.getHandle());
    conf.setBoolean(GrillConfConstatnts.PREPARE_ON_EXPLAIN, true);
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
    assertNull(plan.getResultDestination());
    assertNotNull(plan.getTablesQueried());
    System.out.println("@@Result Destination " + plan.getResultDestination());
    System.out.println("@@Tables Queried " + plan.getTablesQueried());
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
    conf.setBoolean(HiveDriver.GRILL_PERSISTENT_RESULT_SET, true);
    String query2 = "SELECT DISTINCT ID FROM explain_test_1";
    QueryPlan plan2 = driver.explain(query2, conf);
    assertNotNull(plan2.getResultDestination());
    assertNotNull(plan2.getTablesQueried());
    assertEquals(plan2.getTablesQueried().size(), 1);
    assertTrue(plan2.getTableWeights().containsKey("explain_test_1"));
    assertEquals(plan2.getNumSels(), 1);
    GrillResultSet resultSet = driver.executePrepare(plan2.getHandle(), conf);
    HivePersistentResultSet persistentResultSet = (HivePersistentResultSet) resultSet;
    String path = persistentResultSet.getOutputPath();
    assertEquals(plan2.getResultDestination(), path);
    driver.closeQuery(plan2.getHandle());
  }
}

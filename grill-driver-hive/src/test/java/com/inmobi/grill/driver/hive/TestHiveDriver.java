package com.inmobi.grill.driver.hive;

import static org.testng.Assert.*;

import java.io.*;
import java.util.*;

import com.inmobi.grill.api.*;
import con.inmobi.grill.driver.hive.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.service.cli.thrift.TStringValue;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.grill.api.QueryStatus.Status;
import com.inmobi.grill.exception.GrillException;


public class TestHiveDriver {
	public static final String TEST_DATA_FILE = "testdata/testdata1.txt";
  public static final String TEST_OUTPUT_DIR = "test-output";
  public static final String TBL = "HIVE_TEST_TABLE";
	protected Configuration conf;
  protected HiveDriver driver;
	
	@BeforeTest
	public void beforeTest() throws Exception {
    // Check if hadoop property set
    System.out.println("###HADOOP_PATH " + System.getProperty("hadoop.bin.path"));
    assertNotNull(System.getProperty("hadoop.bin.path"));
		conf = new Configuration();
		conf.setClass(HiveDriver.GRILL_HIVE_CONNECTION_CLASS, EmbeddedThriftConnection.class, 
				ThriftConnection.class);
		conf.set(HiveDriver.GRILL_PASSWORD_KEY, "password");
		conf.set(HiveDriver.GRILL_USER_NAME_KEY, "user");
		conf.set("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
		
		driver = new HiveDriver(conf);
		System.out.println("Driver created");
	}
	
	@AfterTest
	public void afterTest() throws Exception {
		driver.close();
	}
	
	private void createTestTable() throws Exception {
    System.out.println("Hadoop Location: " + System.getProperty("hadoop.bin.path"));
    String dropTable = "DROP TABLE IF EXISTS " + TBL;
    String createTable = "CREATE TABLE " + TBL  +"(ID STRING)";
    conf.setBoolean(HiveDriver.GRILL_RESULT_SET_TYPE_KEY, false);
    GrillResultSet resultSet = driver.execute(dropTable, conf);

    assertNotNull(resultSet);
    assertTrue(resultSet instanceof HiveInMemoryResultSet, "expecting in-memory result set");
    // Craete again
    resultSet = driver.execute(createTable, conf);
    assertNotNull(resultSet);
    assertTrue(resultSet instanceof HiveInMemoryResultSet, "expecting in-memory result set");

    // Load some data into the table
    String dataLoad = "LOAD DATA LOCAL INPATH '"+ TEST_DATA_FILE +"' OVERWRITE INTO TABLE " + TBL;
    resultSet = driver.execute(dataLoad, conf);
  }

	// Tests
	@Test
	public void testExecuteQuery() throws Exception {
    createTestTable();
		GrillResultSet resultSet = null;
		// Execute a select query
		System.err.println("Execute select");
		String select = "SELECT ID FROM " + TBL;
		try {
			resultSet = driver.execute(select, conf);
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
      BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(TEST_DATA_FILE)));
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
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	// executeAsync
	@Test
	public void testExecuteQueryAsync()  throws Exception {
    createTestTable();
		// Load some data into the table
		QueryHandle handle = null;
		Set<Status> expectedStates =
				new LinkedHashSet<Status>(Arrays.asList(Status.RUNNING, Status.SUCCESSFUL));
		Set<Status> actualStates = new LinkedHashSet<Status>();

		// Now run a command that would fail
		String expectFail = "SELECT * FROM FOO_BAR";
		conf.setBoolean(HiveDriver.GRILL_RESULT_SET_TYPE_KEY, true);
		handle = driver.executeAsync(expectFail, conf);
    actualStates.clear();
    waitForAsyncQuery(handle, actualStates, driver);
		QueryStatus status = driver.getStatus(handle);
		assertEquals(status.getStatus(), Status.FAILED, "Expecting query to fail");
		driver.closeQuery(handle);


    //  Async select query
    String select = "SELECT ID FROM " + TBL;
    conf.setBoolean(HiveDriver.GRILL_RESULT_SET_TYPE_KEY, false);
    handle = driver.executeAsync(select, conf);
    actualStates.clear();
    waitForAsyncQuery(handle, actualStates, driver);
    status = driver.getStatus(handle);
    assertEquals(status.getStatus(), Status.SUCCESSFUL, "Expected query to finish successfully");
    assertEquals(actualStates, expectedStates);
    driver.closeQuery(handle);
	}

  @Test
  public void testCancelAsyncQuery() throws Exception {
    createTestTable();
    conf.setBoolean(HiveDriver.GRILL_RESULT_SET_TYPE_KEY, false);
    QueryHandle handle = driver.executeAsync("SELECT ID FROM " + TBL, conf);
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
    createTestTable();
    conf.setBoolean(HiveDriver.GRILL_RESULT_SET_TYPE_KEY, true);
    conf.set(HiveDriver.GRILL_RESULT_SET_PARENT_DIR, TEST_OUTPUT_DIR);
    GrillResultSet resultSet = driver.execute("SELECT ID FROM " + TBL, conf);
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

  private void waitForAsyncQuery(QueryHandle handle, Set<Status> actualStates, HiveDriver driver) throws Exception {
    Set<Status> terminationStates =
      EnumSet.of(Status.CANCELED, Status.CLOSED, Status.FAILED, Status.SUCCESSFUL);

    while (true) {
      QueryStatus status = driver.getStatus(handle);
      actualStates.add(status.getStatus());
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
    createTestTable();
    QueryPlan plan = driver.explain("SELECT ID FROM " + TBL, conf);
    assertTrue(plan instanceof  HiveQueryPlan);
    System.out.println("#####\n" + plan.getPlan());
  }
 
}

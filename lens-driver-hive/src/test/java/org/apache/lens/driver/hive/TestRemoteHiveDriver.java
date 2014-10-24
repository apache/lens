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

import static org.testng.Assert.assertEquals;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.server.HiveServer2;
import org.apache.lens.api.LensException;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.driver.hive.HiveDriver;
import org.apache.lens.driver.hive.RemoteThriftConnection;
import org.apache.lens.driver.hive.ThriftConnection;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.DriverQueryPlan;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.driver.DriverQueryStatus.DriverQueryState;
import org.apache.lens.server.api.query.QueryContext;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * The Class TestRemoteHiveDriver.
 */
public class TestRemoteHiveDriver extends TestHiveDriver {

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(TestRemoteHiveDriver.class);

  /** The Constant HS2_HOST. */
  static final String HS2_HOST = "localhost";

  /** The Constant HS2_PORT. */
  static final int HS2_PORT = 12345;

  /** The server. */
  public static HiveServer2 server;

  /** The remote conf. */
  private static HiveConf remoteConf = new HiveConf();

  /**
   * Setup test.
   *
   * @throws Exception
   *           the exception
   */
  @BeforeClass
  public static void setupTest() throws Exception {
    createHS2Service();

    SessionState ss = new SessionState(remoteConf, "testuser");
    SessionState.start(ss);
    Hive client = Hive.get(remoteConf);
    Database database = new Database();
    database.setName(TestRemoteHiveDriver.class.getSimpleName());
    client.createDatabase(database, true);
    SessionState.get().setCurrentDatabase(TestRemoteHiveDriver.class.getSimpleName());
  }

  /**
   * Creates the h s2 service.
   *
   * @throws Exception
   *           the exception
   */
  public static void createHS2Service() throws Exception {
    remoteConf.setClass(HiveDriver.HIVE_CONNECTION_CLASS, RemoteThriftConnection.class, ThriftConnection.class);
    remoteConf.set("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    remoteConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, HS2_HOST);
    remoteConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, HS2_PORT);
    remoteConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_CONNECTION_RETRY_LIMIT, 3);
    remoteConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_RETRY_LIMIT, 3);
    remoteConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_RETRY_DELAY_SECONDS, 10);
    remoteConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT, 1);
    remoteConf.setIntVar(HiveConf.ConfVars.SERVER_READ_SOCKET_TIMEOUT, 60000);
    remoteConf.setLong(HiveDriver.HS2_CONNECTION_EXPIRY_DELAY, 10000);
    server = new HiveServer2();
    server.init(remoteConf);
    server.start();
    // TODO figure out a better way to wait for thrift service to start
    Thread.sleep(7000);
  }

  /**
   * Cleanup test.
   *
   * @throws Exception
   *           the exception
   */
  @AfterClass
  public static void cleanupTest() throws Exception {
    stopHS2Service();
    Hive.get(remoteConf).dropDatabase(TestRemoteHiveDriver.class.getSimpleName(), true, true, true);
  }

  /**
   * Stop h s2 service.
   *
   * @throws Exception
   *           the exception
   */
  public static void stopHS2Service() throws Exception {
    try {
      server.stop();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.driver.hive.TestHiveDriver#beforeTest()
   */
  @BeforeMethod
  @Override
  public void beforeTest() throws Exception {
    conf = new HiveConf(remoteConf);
    conf.addResource("hivedriver-site.xml");
    // Check if hadoop property set
    System.out.println("###HADOOP_PATH " + System.getProperty("hadoop.bin.path"));
    Assert.assertNotNull(System.getProperty("hadoop.bin.path"));
    driver = new HiveDriver();
    driver.configure(conf);
    conf.setBoolean(LensConfConstants.QUERY_ADD_INSERT_OVEWRITE, false);
    conf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    driver.execute(new QueryContext("USE " + TestRemoteHiveDriver.class.getSimpleName(), null, conf));
    conf.setBoolean(LensConfConstants.QUERY_ADD_INSERT_OVEWRITE, true);
    conf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, true);
    Assert.assertEquals(0, driver.getHiveHandleSize());
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.driver.hive.TestHiveDriver#afterTest()
   */
  @AfterMethod
  @Override
  public void afterTest() throws Exception {
    LOG.info("Test finished, closing driver");
    driver.close();
  }

  /**
   * Test multi thread client.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testMultiThreadClient() throws Exception {
    LOG.info("@@ Starting multi thread test");
    // Launch two threads
    createTestTable("test_multithreads");
    HiveConf thConf = new HiveConf(conf, TestRemoteHiveDriver.class);
    thConf.setLong(HiveDriver.HS2_CONNECTION_EXPIRY_DELAY, 10000);
    final HiveDriver thrDriver = new HiveDriver();
    thrDriver.configure(thConf);
    QueryContext ctx = new QueryContext("USE " + TestRemoteHiveDriver.class.getSimpleName(), null, conf);
    thrDriver.execute(ctx);

    // Launch a select query
    final int QUERIES = 5;
    int launchedQueries = 0;
    final int THREADS = 5;
    final long POLL_DELAY = 500;
    List<Thread> thrs = new ArrayList<Thread>();
    final AtomicInteger errCount = new AtomicInteger();
    for (int q = 0; q < QUERIES; q++) {
      final QueryContext qctx;
      try {
        qctx = new QueryContext("SELECT * FROM test_multithreads", null, conf);
        thrDriver.executeAsync(qctx);
      } catch (LensException e) {
        errCount.incrementAndGet();
        LOG.info(q + " executeAsync error: " + e.getCause());
        continue;
      }
      LOG.info("@@ Launched query: " + q + " " + qctx.getQueryHandle());
      launchedQueries++;
      // Launch many threads to poll for status
      final QueryHandle handle = qctx.getQueryHandle();

      for (int i = 0; i < THREADS; i++) {
        int thid = q * THREADS + i;
        Thread th = new Thread(new Runnable() {
          @Override
          public void run() {
            for (int i = 0; i < 1000; i++) {
              try {
                thrDriver.updateStatus(qctx);
                if (qctx.getDriverStatus().isFinished()) {
                  LOG.info("@@ " + handle.getHandleId() + " >> " + qctx.getDriverStatus().getState());
                  thrDriver.closeQuery(handle);
                  break;
                }
                Thread.sleep(POLL_DELAY);
              } catch (LensException e) {
                LOG.error("Got Exception", e.getCause());
                e.printStackTrace();
                errCount.incrementAndGet();
                break;
              } catch (InterruptedException e) {
                e.printStackTrace();
                break;
              }
            }
          }
        });
        thrs.add(th);
        th.setName("Poller#" + (thid));
        th.start();
      }
    }

    for (Thread th : thrs) {
      try {
        th.join(10000);
      } catch (InterruptedException e) {
        LOG.warn("Not ended yet: " + th.getName());
      }
    }
    Assert.assertEquals(0, thrDriver.getHiveHandleSize());
    LOG.info("@@ Completed all pollers. Total thrift errors: " + errCount.get());
    assertEquals(launchedQueries, QUERIES);
    assertEquals(thrs.size(), QUERIES * THREADS);
    assertEquals(errCount.get(), 0);
  }

  /**
   * Test hive driver persistence.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testHiveDriverPersistence() throws Exception {
    System.out.println("@@@@ start_persistence_test");
    HiveConf driverConf = new HiveConf(conf, TestRemoteHiveDriver.class);
    driverConf.setLong(HiveDriver.HS2_CONNECTION_EXPIRY_DELAY, 10000);

    final HiveDriver oldDriver = new HiveDriver();
    oldDriver.configure(driverConf);

    driverConf.setBoolean(LensConfConstants.QUERY_ADD_INSERT_OVEWRITE, false);
    driverConf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    QueryContext ctx = new QueryContext("USE " + TestRemoteHiveDriver.class.getSimpleName(), null, driverConf);
    oldDriver.execute(ctx);
    Assert.assertEquals(0, oldDriver.getHiveHandleSize());

    String tableName = "test_hive_driver_persistence";

    // Create some ops with a driver
    String createTable = "CREATE TABLE IF NOT EXISTS " + tableName + "(ID STRING)";
    ctx = new QueryContext(createTable, null, driverConf);
    oldDriver.execute(ctx);

    // Load some data into the table
    String dataLoad = "LOAD DATA LOCAL INPATH '" + TEST_DATA_FILE + "' OVERWRITE INTO TABLE " + tableName;
    ctx = new QueryContext(dataLoad, null, driverConf);
    oldDriver.execute(ctx);

    driverConf.setBoolean(LensConfConstants.QUERY_ADD_INSERT_OVEWRITE, true);
    driverConf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, true);
    // Fire two queries
    QueryContext ctx1 = new QueryContext("SELECT * FROM " + tableName, null, driverConf);
    oldDriver.executeAsync(ctx1);
    QueryContext ctx2 = new QueryContext("SELECT ID FROM " + tableName, null, driverConf);
    oldDriver.executeAsync(ctx2);
    Assert.assertEquals(2, oldDriver.getHiveHandleSize());

    byte[] ctx1bytes = persistContext(ctx1);
    byte[] ctx2bytes = persistContext(ctx2);

    // Write driver to stream
    ByteArrayOutputStream driverBytes = new ByteArrayOutputStream();
    try {
      oldDriver.writeExternal(new ObjectOutputStream(driverBytes));
    } finally {
      driverBytes.close();
    }

    // Create another driver from the stream
    ByteArrayInputStream driverInput = new ByteArrayInputStream(driverBytes.toByteArray());
    HiveDriver newDriver = new HiveDriver();
    newDriver.readExternal(new ObjectInputStream(driverInput));
    newDriver.configure(driverConf);
    driverInput.close();

    ctx1 = readContext(ctx1bytes, newDriver);
    ctx2 = readContext(ctx2bytes, newDriver);

    Assert.assertEquals(2, newDriver.getHiveHandleSize());

    validateExecuteAsync(ctx1, DriverQueryState.SUCCESSFUL, true, false, newDriver);
    validateExecuteAsync(ctx2, DriverQueryState.SUCCESSFUL, true, false, newDriver);
  }

  /**
   * Persist context.
   *
   * @param ctx
   *          the ctx
   * @return the byte[]
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private byte[] persistContext(QueryContext ctx) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(baos);
    try {
      out.writeObject(ctx);
      boolean isDriverAvailable = (ctx.getSelectedDriver() != null);
      out.writeBoolean(isDriverAvailable);
      if (isDriverAvailable) {
        out.writeUTF(ctx.getSelectedDriver().getClass().getName());
      }
    } finally {
      out.flush();
      out.close();
      baos.close();
    }

    return baos.toByteArray();
  }

  /**
   * Read context.
   *
   * @param bytes
   *          the bytes
   * @param driver
   *          the driver
   * @return the query context
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   * @throws ClassNotFoundException
   *           the class not found exception
   */
  private QueryContext readContext(byte[] bytes, LensDriver driver) throws IOException, ClassNotFoundException {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectInputStream in = new ObjectInputStream(bais);
    QueryContext ctx;
    try {
      ctx = (QueryContext) in.readObject();
      ctx.setConf(driver.getConf());
      boolean driverAvailable = in.readBoolean();
      if (driverAvailable) {
        String clsName = in.readUTF();
        ctx.setSelectedDriver(driver);
      }
    } finally {
      in.close();
      bais.close();
    }
    return ctx;
  }

  /**
   * Creates the partitioned table.
   *
   * @param tableName
   *          the table name
   * @param partitions
   *          the partitions
   * @throws Exception
   *           the exception
   */
  private void createPartitionedTable(String tableName, int partitions) throws Exception {
    conf.setBoolean(LensConfConstants.QUERY_ADD_INSERT_OVEWRITE, false);
    conf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);

    QueryContext ctx = new QueryContext("CREATE EXTERNAL TABLE IF NOT EXISTS " + tableName
        + " (ID STRING) PARTITIONED BY (DT STRING, ET STRING)", null, conf);

    driver.execute(ctx);
    Assert.assertEquals(0, driver.getHiveHandleSize());

    File dataDir = new File("target/partdata");
    dataDir.mkdir();

    // Add partitions
    for (int i = 0; i < partitions; i++) {
      // Create partition paths
      File tableDir = new File(dataDir, tableName);
      tableDir.mkdir();
      File partDir = new File(tableDir, "p" + i);
      partDir.mkdir();

      // Create data file
      File data = new File(partDir, "data.txt");
      FileUtils.writeLines(data, Arrays.asList("one", "two", "three", "four", "five"));

      System.out.println("@@ Adding partition " + i);
      QueryContext partCtx = new QueryContext("ALTER TABLE " + tableName + " ADD IF NOT EXISTS PARTITION (DT='p" + i
          + "', ET='1') LOCATION '" + partDir.getPath() + "'", null, conf);
      driver.execute(partCtx);
    }
  }

  /**
   * Test partition in query plan.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testPartitionInQueryPlan() throws Exception {
    // Create tables with 10 & 1 partitions respectively
    createPartitionedTable("table_1", 10);
    createPartitionedTable("table_2", 1);

    // Query should select 5 partitions of table 1 and 1 partitions of table 2
    String explainQuery = "SELECT table_1.ID  "
        + "FROM table_1 LEFT OUTER JOIN table_2 ON table_1.ID = table_2.ID AND table_2.DT='p0' "
        + "WHERE table_1.DT='p0' OR table_1.DT='p1' OR table_1.DT='p2' OR table_1.DT='p3' OR table_1.DT='p4' "
        + "AND table_1.ET='1'";

    DriverQueryPlan plan = driver.explain(explainQuery, conf);

    Assert.assertEquals(0, driver.getHiveHandleSize());
    System.out.println("@@ partitions" + plan.getPartitions());

    Assert.assertEquals(plan.getPartitions().size(), 2);

    String dbName = TestRemoteHiveDriver.class.getSimpleName().toLowerCase();
    Assert.assertTrue(plan.getPartitions().containsKey(dbName + ".table_1"));
    Assert.assertEquals(plan.getPartitions().get(dbName + ".table_1").size(), 5);

    Assert.assertTrue(plan.getPartitions().containsKey(dbName + ".table_2"));
    Assert.assertEquals(plan.getPartitions().get(dbName + ".table_2").size(), 1);

    FileUtils.deleteDirectory(new File("target/partdata"));
  }
}

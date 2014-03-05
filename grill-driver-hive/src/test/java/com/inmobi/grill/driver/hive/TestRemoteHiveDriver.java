package com.inmobi.grill.driver.hive;


import static org.junit.Assert.assertNotNull;
import static org.testng.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.server.HiveServer2;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.api.QueryStatus.Status;
import com.inmobi.grill.driver.hive.HiveDriver.QueryContext;
import com.inmobi.grill.exception.GrillException;

public class TestRemoteHiveDriver extends TestHiveDriver {
  public static final Log LOG = LogFactory.getLog(TestRemoteHiveDriver.class);
  static final String HS2_HOST = "localhost";
  static final int  HS2_PORT = 12345;
  static HiveServer2 server;
  private static HiveConf remoteConf = new HiveConf();

  @BeforeClass
  public static void createHS2Service() throws Exception {
    remoteConf.setClass(HiveDriver.GRILL_HIVE_CONNECTION_CLASS, RemoteThriftConnection.class,
        ThriftConnection.class);
    remoteConf.set("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    remoteConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, HS2_HOST);
    remoteConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, HS2_PORT);
    remoteConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_CONNECTION_RETRY_LIMIT, 3);
    remoteConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_RETRY_LIMIT, 3);
    remoteConf.setIntVar(HiveConf.ConfVars.SERVER_READ_SOCKET_TIMEOUT, 60000);
    remoteConf.setLong(HiveDriver.GRILL_CONNECTION_EXPIRY_DELAY, 10000);

    SessionState.start(remoteConf);
    Hive client = Hive.get(remoteConf);
    Database database = new Database();
    database.setName(TestRemoteHiveDriver.class.getSimpleName());
    client.createDatabase(database, true);
    SessionState.get().setCurrentDatabase(TestRemoteHiveDriver.class.getSimpleName());

    server = new HiveServer2();
    server.init(remoteConf);
    server.start();
    // TODO figure out a better way to wait for thrift service to start
    Thread.sleep(7000);
  }

  @AfterClass
  public static void stopHS2Service() throws Exception  {
    try {
      server.stop();
    } catch (Exception e) {
      e.printStackTrace();
    }
    Hive.get(remoteConf).dropDatabase(TestRemoteHiveDriver.class.getSimpleName(), true, true, true);
  }

  @BeforeMethod
  @Override
  public void beforeTest() throws Exception {
    conf = new HiveConf(remoteConf);
    // Check if hadoop property set
    System.out.println("###HADOOP_PATH " + System.getProperty("hadoop.bin.path"));
    assertNotNull(System.getProperty("hadoop.bin.path"));
    driver = new HiveDriver();
    driver.configure(conf);
    conf.setBoolean(HiveDriver.GRILL_ADD_INSERT_OVEWRITE, false);
    conf.setBoolean(HiveDriver.GRILL_PERSISTENT_RESULT_SET, false);
    driver.execute("USE " + TestRemoteHiveDriver.class.getSimpleName(), conf);
    conf.setBoolean(HiveDriver.GRILL_ADD_INSERT_OVEWRITE, true);
    conf.setBoolean(HiveDriver.GRILL_PERSISTENT_RESULT_SET, true);
  }

  @AfterMethod
  @Override
  public void afterTest() throws Exception {
    driver.close();
  }

  @Test
  public void testMultiThreadClient() throws Exception {
    LOG.info("@@ Starting multi thread test");
    // Launch two threads
    createTestTable("test_multithreads");
    HiveConf thConf = new HiveConf(conf, TestRemoteHiveDriver.class);
    thConf.setLong(HiveDriver.GRILL_CONNECTION_EXPIRY_DELAY, 10000);
    final HiveDriver thrDriver = new HiveDriver();
    thrDriver.configure(thConf);
    thrDriver.execute("USE " + TestRemoteHiveDriver.class.getSimpleName(), conf);

    // Launch a select query
    final int QUERIES = 5;
    int launchedQueries = 0;
    final int THREADS = 5;
    final long POLL_DELAY = 500;
    List<Thread> thrs = new ArrayList<Thread>();
    final AtomicInteger errCount = new AtomicInteger();

    for (int q = 0; q < QUERIES; q++) {
      QueryHandle qhandle;
      try {
        qhandle = thrDriver.executeAsync("SELECT * FROM test_multithreads", conf);
      } catch (GrillException e) {
        errCount.incrementAndGet();
        LOG.info(q + " executeAsync error: " + e.getCause());
        continue;
      }
      LOG.info("@@ Launched query: " + q + " " + qhandle.getHandleId());
      launchedQueries++;
      // Launch many threads to poll for status
      final QueryHandle handle = qhandle;

      for (int i = 0; i < THREADS; i++) {
        int thid = q * THREADS + i;
        Thread th = new Thread(new Runnable() {
          @Override
          public void run() {
            for (int i = 0; i < 1000; i++) {
              try {
                QueryStatus status = thrDriver.getStatus(handle);
                if (status.getStatus() == QueryStatus.Status.CANCELED
                    || status.getStatus() == QueryStatus.Status.CLOSED
                    || status.getStatus() == QueryStatus.Status.SUCCESSFUL
                    || status.getStatus() == QueryStatus.Status.FAILED) {
                  LOG.info("@@ " + handle.getHandleId() + " >> " + status.getStatus());
                  break;
                }

                Thread.sleep(POLL_DELAY);
              } catch (GrillException e) {
                LOG.error("Got Exception", e.getCause());
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
    LOG.info("@@ Completed all pollers. Total thrift errors: " + errCount.get());
    assertEquals(launchedQueries, QUERIES);
    assertEquals(thrs.size(), QUERIES * THREADS);
    assertEquals(errCount.get(), 0);
  }
  
  @Test
  public void testHiveDriverPersistence() throws Exception {
    System.out.println("@@@@ start_persistence_test");
    HiveConf driverConf = new HiveConf(remoteConf, TestRemoteHiveDriver.class);
    driverConf.setLong(HiveDriver.GRILL_CONNECTION_EXPIRY_DELAY, 10000);
    
    final HiveDriver oldDriver = new HiveDriver();
    oldDriver.configure(driverConf);
    
    driverConf.setBoolean(HiveDriver.GRILL_ADD_INSERT_OVEWRITE, false);
    driverConf.setBoolean(HiveDriver.GRILL_PERSISTENT_RESULT_SET, false);
    oldDriver.execute("USE " + TestRemoteHiveDriver.class.getSimpleName(), driverConf);
    
    String tableName = "test_hive_driver_persistence";

    // Create some ops with a driver
    String createTable = "CREATE TABLE IF NOT EXISTS " + tableName  +"(ID STRING)";
    oldDriver.execute(createTable, driverConf);
    
    // Load some data into the table
    String dataLoad = "LOAD DATA LOCAL INPATH '"+ TEST_DATA_FILE +"' OVERWRITE INTO TABLE " + tableName;
    oldDriver.execute(dataLoad, driverConf);
    
    driverConf.setBoolean(HiveDriver.GRILL_ADD_INSERT_OVEWRITE, true);
    driverConf.setBoolean(HiveDriver.GRILL_PERSISTENT_RESULT_SET, true);
    // Fire two queries
    QueryHandle query1 = oldDriver.executeAsync("SELECT * FROM " + tableName, driverConf);
    
    QueryHandle query2 = oldDriver.executeAsync("SELECT ID FROM " + tableName, driverConf);
    
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
    
    // Check status from the new driver, should get all statuses back.
    while (true) {
      QueryStatus stat1 = newDriver.getStatus(query1);
      assertNotNull(stat1);
      System.out.println("@@ stat1 " + query1 + " > " + stat1.getStatus());
      QueryStatus stat2 = newDriver.getStatus(query2);
      assertNotNull(stat2);
      System.out.println("@@ stat2 " + query2 + " > " + stat2.getStatus());
      
      if (isFinished(stat1) && isFinished(stat2)) {
        break;
      } else {
        Thread.sleep(1000);
      }
    }
  }
  
  private boolean isFinished(QueryStatus stat) {
    QueryStatus.Status s = stat.getStatus();
    return s == Status.CANCELED || s == Status.FAILED || s == Status.CLOSED || s == Status.SUCCESSFUL;
  }
}

package com.inmobi.grill.driver.hive;


import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.server.HiveServer2;
import org.apache.thrift.TException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.driver.hive.HiveDriver.ExpirableConnection;
import com.inmobi.grill.exception.GrillException;

import static org.testng.Assert.*;

public class TestRemoteHiveDriver extends TestHiveDriver {
	public static final Log LOG = LogFactory.getLog(TestRemoteHiveDriver.class);
  static final String HS2_HOST = "localhost";
  static final int  HS2_PORT = 12345;
  static HiveServer2 server;

  @BeforeClass
  public static void createHS2Service() throws Exception {
    conf = new HiveConf();
    conf.setClass(HiveDriver.GRILL_HIVE_CONNECTION_CLASS, RemoteThriftConnection.class,
      ThriftConnection.class);
    conf.set("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, HS2_HOST);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, HS2_PORT);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_CONNECTION_RETRY_LIMIT, 3);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_RETRY_LIMIT, 3);
    conf.setIntVar(HiveConf.ConfVars.SERVER_READ_SOCKET_TIMEOUT, 60000);
    conf.setLong(HiveDriver.GRILL_CONNECTION_EXPIRY_DELAY, 10000);
    
    SessionState.start(conf);
    Hive client = Hive.get(conf);
    Database database = new Database();
    database.setName(TestRemoteHiveDriver.class.getSimpleName());
    client.createDatabase(database, true);
    SessionState.get().setCurrentDatabase(TestRemoteHiveDriver.class.getSimpleName());

    server = new HiveServer2();
    server.init(conf);
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
    Hive.get(conf).dropDatabase(TestRemoteHiveDriver.class.getSimpleName(), true, true, true);
    
    // After this open connections should be closed.
    long waitTime = 11000;
   	Thread.sleep(waitTime);
    if (conf.getLong(HiveDriver.GRILL_CONNECTION_EXPIRY_DELAY, 
    		HiveDriver.DEFAULT_EXPIRY_DELAY) <= waitTime) {
    	assertEquals(HiveDriver.openConnections(), 0, "Expected all connections to be closed");
    }
  }

  @BeforeMethod
  @Override
  public void beforeTest() throws Exception {
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
  	final long POLL_DELAY = 100;
  	List<Thread> thrs = new ArrayList<Thread>();
  	final AtomicInteger errCount = new AtomicInteger();
  	
  	for (int q = 0; q < QUERIES; q++) {
	  	QueryHandle qhandle;
			try {
				qhandle = thrDriver.executeAsync("SELECT * FROM test_multithreads", conf);
			} catch (GrillException e) {
				if (e.getCause() instanceof HiveSQLException 
						&& e.getCause().getCause() instanceof TException) {
					errCount.incrementAndGet();
				}
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
								LOG.error("@@" + e.getCause());
								if (e.getCause() instanceof HiveSQLException 
										&& e.getCause().getCause() instanceof TException) {
									errCount.incrementAndGet();
									LOG.info("Got TException");
									break;
								}
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
}

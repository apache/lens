package com.inmobi.grill.driver.hive;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.server.HiveServer2;
import org.testng.annotations.*;

import static org.testng.Assert.assertNotNull;

public class TestRemoteHiveDriver extends TestHiveDriver {
  static final String HS2_HOST = "localhost";
  static final int  HS2_PORT = 12345;
  static HiveServer2 server;

  @BeforeClass
  public static void createHS2Service() throws Exception {
    conf = new HiveConf(TestRemoteHiveDriver.class);
    conf.set("hive.server2.thrift.bind.host", HS2_HOST);
    conf.setInt("hive.server2.thrift.port", HS2_PORT);
    conf = new HiveConf();
    conf.setClass(HiveDriver.GRILL_HIVE_CONNECTION_CLASS, RemoteThriftConnection.class,
      ThriftConnection.class);
    conf.set("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    conf.set("hive.server2.thrift.bind.host", HS2_HOST);
    conf.setInt("hive.server2.thrift.port", HS2_PORT);

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
    Thread.sleep(5000);
  }

  @AfterClass
  public static void stopHS2Service() throws Exception  {
    try {
      server.stop();
    } catch (Exception e) {
      e.printStackTrace();
    }
    Hive.get(conf).dropDatabase(TestRemoteHiveDriver.class.getSimpleName(), true, true, true);
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
}

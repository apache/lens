package com.inmobi.grill.driver.hive;


import con.inmobi.grill.driver.hive.HiveDriver;
import con.inmobi.grill.driver.hive.RemoteThriftConnection;
import con.inmobi.grill.driver.hive.ThriftConnection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;

import static org.testng.Assert.assertNotNull;

public class TestRemoteHiveDriver extends TestHiveDriver {
  static final String HS2_HOST = "localhost";
  static final int  HS2_PORT = 12345;
  static HiveServer2 server;

  @BeforeClass
  public static void createHS2Service() throws Exception {
    final HiveConf conf = new HiveConf(TestRemoteHiveDriver.class);
    conf.set("hive.server2.thrift.bind.host", HS2_HOST);
    conf.setInt("hive.server2.thrift.port", HS2_PORT);

    server = new HiveServer2();
    server.init(conf);
    server.start();
    // TODO figure out a better way to wait for thrift service to start
    Thread.sleep(5000);
  }

  @AfterClass
  public static void stopHS2Service() throws Exception  {
    server.stop();
  }

  @BeforeTest
  @Override
  public void beforeTest() throws Exception {
    // Check if hadoop property set
    System.out.println("###HADOOP_PATH " + System.getProperty("hadoop.bin.path"));
    assertNotNull(System.getProperty("hadoop.bin.path"));
    conf = new Configuration();
    conf.setClass(HiveDriver.GRILL_HIVE_CONNECTION_CLASS, RemoteThriftConnection.class,
      ThriftConnection.class);
    conf.set("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    conf.set("hive.server2.thrift.bind.host", HS2_HOST);
    conf.setInt("hive.server2.thrift.port", HS2_PORT);
    driver = new HiveDriver(conf);
    driver = new HiveDriver(conf);
  }
}

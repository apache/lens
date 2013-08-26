package com.inmobi.yoda.hive;


import com.google.common.io.Files;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.EmbeddedThriftCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class TestForwardingUDF {
  public static final String TEST_TBL = "udfTest.test_yoda_udf_table";
  private HiveConf conf;
  private ThriftCLIServiceClient hiveClient;
  private SessionHandle session;
  private Map<String, String> confOverlay;
  private File testFile;
  public static final int NUM_LINES = 100;

  @BeforeTest
  public void setup() throws Exception {
    conf = new HiveConf(TestForwardingUDF.class);
    conf.set("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    hiveClient = new ThriftCLIServiceClient(new EmbeddedThriftCLIService());
    session = hiveClient.openSession(conf.getUser(), "");
    confOverlay = new HashMap<String, String>();
    hiveClient.executeStatement(session, "DROP DATABASE IF EXISTS udfTest", confOverlay);
    hiveClient.executeStatement(session, "CREATE DATABASE udfTest", confOverlay);
    hiveClient.executeStatement(session, "SET hive.lock.manager=org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager",
      confOverlay);
    hiveClient.executeStatement(session, "DROP TABLE IF EXISTS " + TEST_TBL, confOverlay);
    hiveClient.executeStatement(session, "CREATE TABLE " + TEST_TBL + "(ID STRING)", confOverlay);

    // Create test input
    PrintWriter out = null;
    try {
      testFile = File.createTempFile("grill_yoda_test", ".txt");
      out = new PrintWriter(testFile);
      for (int i = 0; i < NUM_LINES; i++) {
        out.println(i);
      }
    } catch (Exception ex) {
      throw ex;
    } finally {
       if (out != null) {
         out.close();
       }
    }

    String testFilePath = testFile.getPath();
    hiveClient.executeStatement(session,
      "LOAD DATA LOCAL INPATH '" + testFilePath + "' OVERWRITE INTO TABLE " + TEST_TBL,
      confOverlay);

  }

  @Test
  public void testUdfForward() throws Exception {
    hiveClient.executeStatement(session,
      "CREATE TEMPORARY FUNCTION yoda_udf AS 'com.inmobi.yoda.hive.ForwardingUDF'",
      confOverlay);
    //yoda_udf('md5', ID)
    OperationHandle opHandle = hiveClient.executeStatement(session,
      "INSERT OVERWRITE LOCAL DIRECTORY 'target/udfTestOutput' SELECT yoda_udf('md5', ID) " + TEST_TBL,
      confOverlay);
    File outputDir = new File("target/udfTestOutput");
    List<String> lines = new ArrayList<String>();
    for (File f : outputDir.listFiles()) {
      if (!f.getName().endsWith(".crc")) {
        lines.addAll(Files.readLines(f, Charset.defaultCharset()));
      }
    }
    assertEquals(lines.size(), NUM_LINES);
  }

  @AfterTest
  public void tearDown() throws Exception {
    if (testFile != null)  {
      testFile.delete();
    }
    //hiveClient.closeSession(session);
  }
}

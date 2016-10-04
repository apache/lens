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

package org.apache.lens.regression;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;

import javax.ws.rs.client.WebTarget;
import javax.xml.bind.JAXBException;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.query.*;
import org.apache.lens.cube.parse.CubeQueryConfUtil;
import org.apache.lens.regression.core.constants.DriverConfig;
import org.apache.lens.regression.core.constants.QueryInventory;
import org.apache.lens.regression.core.helpers.*;
import org.apache.lens.regression.core.testHelper.BaseTestClass;
import org.apache.lens.regression.util.Util;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.util.LensUtil;

import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.*;


public class ITRestartTests extends BaseTestClass {

  WebTarget servLens;
  String sessionHandleString;

  private static String hiveDriver = "hive/hive1";
  private static String jdbcDriver = "jdbc/jdbc1";

  private String lensSiteConf = lens.getServerDir() + "/conf/lens-site.xml";
  private String hiveDriverConf = lens.getServerDir() + "/conf/drivers/hive/hive1/hivedriver-site.xml";
  private String jdbcDriverConf = lens.getServerDir() + "/conf/drivers/jdbc/jdbc1/jdbcdriver-site.xml";

  public static final String SLEEP_QUERY = QueryInventory.getSleepQuery("5");
  public static final String COST_95 = String.format(QueryInventory.getQueryFromInventory("HIVE.SLEEP_COST_95"), "7");
  public static final String COST_60 = String.format(QueryInventory.getQueryFromInventory("HIVE.SLEEP_COST_60"), "7");
  public static final String COST_30 = String.format(QueryInventory.getQueryFromInventory("HIVE.SLEEP_COST_30"), "6");
  public static final String COST_20 = String.format(QueryInventory.getQueryFromInventory("HIVE.SLEEP_COST_20"), "6");
  public static final String COST_10 = String.format(QueryInventory.getQueryFromInventory("HIVE.SLEEP_COST_10"), "6");
  public static final String COST_5 = String.format(QueryInventory.getQueryFromInventory("HIVE.SLEEP_COST_5"), "5");

  private static Logger logger = Logger.getLogger(ITRestartTests.class);

  @BeforeClass(alwaysRun = true)
  public void initialize() throws IOException, JAXBException, LensException {
    servLens = ServiceManagerHelper.init();
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp(Method method) throws Exception {
    logger.info("Test Name: " + method.getName());
    logger.info("Creating a new Session");
    sessionHandleString = sHelper.openSession(lens.getCurrentDB());
  }

  @AfterMethod(alwaysRun = true)
  public void closeSession() throws Exception {
    logger.info("Closing Session");
    if (sessionHandleString!=null) {
      sHelper.closeSession();
    }
  }


  //LENS-924
  @Test(enabled = true)
  public void testSessionConfRetainedOnRestart()  throws Exception {

    String mailNotify = LensConfConstants.QUERY_MAIL_NOTIFY;
    HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.QUERY_MAIL_NOTIFY, "true");
    Util.changeConfig(map, lensSiteConf);
    lens.restart();

    String query = QueryInventory.getSleepQuery("10");

    HashMap<String, String> confMap = LensUtil.getHashMap(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true",
        LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false", mailNotify, "false",
        CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "false", LensConfConstants.PURGE_INTERVAL, "5000");

    LensConf lensConf = new LensConf();
    lensConf.addProperties(confMap);
    String session1 = sHelper.openSession("diff1", "diff1", lens.getCurrentDB());
    sHelper.setAndValidateParam(confMap, session1);

    List<QueryHandle> qList = new ArrayList<QueryHandle>();
    for(int i=1; i<3; i++){
      qList.add((QueryHandle)qHelper.executeQuery(query, null, null, session1, lensConf).getData());
    }

    String session2 = sHelper.openSession("diff2", "diff2", lens.getCurrentDB());
    String session3 = sHelper.openSession("diff3", "diff3", lens.getCurrentDB());

    qHelper.waitForCompletion(qList.get(0));
    Thread.sleep(5000);                         // wait till query gets persisted
    sHelper.closeSession(session1);          // required to hit that flow
    lens.restart();

    Assert.assertTrue(Boolean.parseBoolean(sHelper.getSessionParam(session2,
        LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER)));
    Assert.assertTrue(Boolean.parseBoolean(sHelper.getSessionParam(session3,
        LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER)));
    Assert.assertTrue(Boolean.parseBoolean(sHelper.getSessionParam(session2, mailNotify)));
    Assert.assertTrue(Boolean.parseBoolean(sHelper.getSessionParam(session3, mailNotify)));

    for(int i=4; i<8; i++){
      String user = "diff" + Integer.toString(i);
      String session = sHelper.openSession(user, user, lens.getCurrentDB());
      String isDriverPersist = sHelper.getSessionParam(session, LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER);
      String isMailNotify = sHelper.getSessionParam(session, mailNotify);
      logger.info(user + " session  : " + isDriverPersist);
      Assert.assertTrue(Boolean.parseBoolean(isDriverPersist));
      Assert.assertTrue(Boolean.parseBoolean(isMailNotify));
    }
  }

  //TODO : add dataprovider for all persistent combination
  @Test(enabled = true)
  public void testGetFinishedResultSetAfterRestart() throws Exception {

    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");

    QueryHandle handle1 = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_CUBE_QUERY).getData();
    QueryHandle handle2 = (QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_CUBE_QUERY).getData();

    LensQuery lq1 = qHelper.waitForCompletion(handle1);
    LensQuery lq2 = qHelper.waitForCompletion(handle2);

    Assert.assertEquals(lq1.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
    Assert.assertEquals(lq2.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    // Result should get purged and then server should be restarted
    Thread.sleep(10000);
    lens.restart();

    PersistentQueryResult result1 = (PersistentQueryResult)qHelper.getResultSet(handle1);
    Assert.assertNotNull(result1);
    Assert.assertEquals(result1.getNumRows().intValue(), 2);

    PersistentQueryResult result2 = (PersistentQueryResult)qHelper.getResultSet(handle2);
    Assert.assertNotNull(result2);
    Assert.assertEquals(result2.getNumRows().intValue(), 8);
  }

  /*
   Once lens server restarts, the map join param tuning fails because rewriterplan would have been lost.
  */

  @Test(enabled = true)
  public void mapJoinTuneOnRestart() throws Exception {

    sHelper.setAndValidateParam(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "false");
    String memoryConfKey = "mapreduce.map.memory.mb";
    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES, "1");

    try {
      Util.changeConfig(map, hiveDriverConf);
      lens.restart();

      QueryHandle handle  = (QueryHandle) qHelper.executeQuery(COST_5).getData();
      QueryHandle handle1 = (QueryHandle) qHelper.executeQuery(COST_5).getData();
      QueryHandle handle2 = (QueryHandle) qHelper.executeQuery(COST_5).getData();
      QueryHandle handle3 = (QueryHandle) qHelper.executeQuery(COST_5).getData();

      LensQuery lq = qHelper.getLensQuery(sessionHandleString, handle);
      String expected = lq.getQueryConf().getProperties().get(memoryConfKey);

      lens.restart();

      LensQuery lq1 = qHelper.getLensQuery(sessionHandleString, handle1);
      LensQuery lq2 = qHelper.getLensQuery(sessionHandleString, handle2);
      LensQuery lq3 = qHelper.getLensQuery(sessionHandleString, handle3);

      Assert.assertEquals(lq1.getQueryConf().getProperties().get(memoryConfKey), expected);
      Assert.assertEquals(lq2.getQueryConf().getProperties().get(memoryConfKey), expected);
      Assert.assertEquals(lq3.getQueryConf().getProperties().get(memoryConfKey), expected);

    } finally {
      Util.changeConfig(hiveDriverConf);
      lens.restart();
    }
  }


  @Test(enabled = true)
  public void testHiveQueryOnRestart() throws Exception {

    sHelper.setAndValidateParam(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "false");
    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES, "4");
    try {
      Util.changeConfig(map, hiveDriverConf);
      lens.restart();

      List<QueryHandle> handleList = new ArrayList<QueryHandle>();
      for (int i = 1; i <= 3; i++) {
        handleList.add((QueryHandle) qHelper.executeQuery(COST_95).getData());
        handleList.add((QueryHandle) qHelper.executeQuery(COST_20).getData());
      }

      qHelper.waitForQueryToRun(handleList.get(0));
      lens.restart();

      for (int i = 1; i <= 2; i++) {
        handleList.add((QueryHandle) qHelper.executeQuery(COST_10).getData());
        handleList.add((QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_CUBE_QUERY).getData());
      }

      for (QueryHandle q : handleList) {
        LensQuery lq = qHelper.waitForCompletion(q);
        Assert.assertEquals(lq.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
      }

    } finally {
      Util.changeConfig(hiveDriverConf);
      lens.restart();
    }
  }

  //TODO : Add long running jdbc query
  @Test(enabled = true)
  public void testJdbcQueryOnRestart() throws Exception {

    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES, "4",
        DriverConfig.JDBC_POOL_SIZE, "4");

    try {
      Util.changeConfig(map, jdbcDriverConf);
      lens.restart();

      List<QueryHandle> handleList = new ArrayList<QueryHandle>();
      for (int i = 1; i <= 8; i++) {
        qHelper.executeQuery(QueryInventory.JDBC_CUBE_QUERY).getData();
        qHelper.executeQuery(QueryInventory.JDBC_DIM_QUERY).getData();
      }

      handleList = qHelper.getQueryHandleList(null, "QUEUED", "all", sessionHandleString, null, null, jdbcDriver);
      lens.restart();

      for (int i = 1; i <= 2; i++) {
        handleList.add((QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_CUBE_QUERY).getData());
        handleList.add((QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_DIM_QUERY).getData());
      }

      for (QueryHandle q : handleList) {
        LensQuery lq = qHelper.waitForCompletion(q);
        Assert.assertEquals(lq.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
      }

    } finally {
      Util.changeConfig(jdbcDriverConf);
      lens.restart();
    }
  }
}

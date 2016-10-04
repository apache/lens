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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.ws.rs.client.WebTarget;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.Priority;
import org.apache.lens.api.query.*;
import org.apache.lens.cube.parse.CubeQueryConfUtil;
import org.apache.lens.driver.hive.HiveDriver;
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

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

public class ITPriorityTests extends BaseTestClass{

  WebTarget servLens;
  String sessionHandleString;

  LensServerHelper lens = getLensServerHelper();
  MetastoreHelper mHelper = getMetastoreHelper();
  SessionHelper sHelper = getSessionHelper();
  QueryHelper qHelper = getQueryHelper();

  String hiveDriver = "hive/hive1";
  String hiveDriverConfPath  = lens.getServerDir() + "/conf/drivers/" + hiveDriver + "/hivedriver-site.xml";
  String lensSiteConfPath = lens.getServerDir() + "/conf/lens-site.xml";
  private String jobUrl = lens.getJobConfUrl();

  public static final String COST_95 = QueryInventory.getQueryFromInventory("HIVE.COST_95");
  public static final String COST_60 = QueryInventory.getQueryFromInventory("HIVE.COST_60");
  public static final String COST_20 = QueryInventory.getQueryFromInventory("HIVE.COST_20");
  public static final String COST_5 = QueryInventory.getQueryFromInventory("HIVE.COST_5");

  private static Logger logger = Logger.getLogger(ITPriorityTests.class);


  @BeforeClass(alwaysRun = true)
  public void initialize() throws Exception {
    servLens = ServiceManagerHelper.init();
    HashMap<String, String> map = LensUtil.getHashMap(HiveDriver.HS2_PRIORITY_RANGES,
        "HIGH,7,NORMAL,30,LOW,90,VERY_LOW");
    Util.changeConfig(map, hiveDriverConfPath);
    lens.restart();

    sessionHandleString=sHelper.openSession(lens.getCurrentDB());
    sHelper.setAndValidateParam(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "false");
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp(Method method) throws Exception {
    logger.info("Test Name: " + method.getName());
  }

  @AfterMethod(alwaysRun=true)
  public void restoreConfig() throws JSchException, IOException, LensException{
  }

  @AfterClass(alwaysRun = true)
  public void cleanup() throws IOException, JSchException, LensException, InterruptedException, SftpException {
    logger.info("Closing Session");
    sHelper.closeSession();
    Util.changeConfig(hiveDriverConfPath);
    lens.restart();
  }


  @DataProvider(name = "priority_check")
  public Object[][] priorityCheck() {
    Object[][] testData = {{COST_5, Priority.HIGH}, {COST_20, Priority.NORMAL}, {COST_60, Priority.LOW},
      {COST_95, Priority.VERY_LOW}, };
    return testData;
  }

  @Test(enabled=true, dataProvider = "priority_check")
  public void testPriority(String query, Priority priority) throws Exception {
    QueryHandle qh = (QueryHandle) qHelper.executeQuery(query).getData();
    qHelper.waitForQueryToRun(qh);
    LensQuery lq = qHelper.getLensQuery(sessionHandleString, qh);
    String progressMsg = lq.getStatus().getProgressMessage();
    logger.info("Progress msg : " + progressMsg);
    String jobId = Util.getJobIdFromProgressMsg(progressMsg);
    Assert.assertEquals(lq.getPriority(), priority);
    Assert.assertEquals(Util.getMapredJobPrority(jobUrl, jobId), priority.toString());
  }


  @Test(enabled = true)
  public void testPriorityAfterQueryPurge() throws Exception {

    try{
      HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.PURGE_INTERVAL, "5000");
      Util.changeConfig(map, lensSiteConfPath);
      lens.restart();

      QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_CUBE_QUERY).getData();
      LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
      Thread.sleep(6000);
      lensQuery =  qHelper.getLensQuery(sessionHandleString, queryHandle);
      Assert.assertEquals(lensQuery.getPriority(), Priority.HIGH);

    }catch(Exception e){
      Util.changeConfig(lensSiteConfPath);
      lens.restart();
    }
  }

  /*
   When query executed with TIMEOUT is in running state then all queued queries will fail with
   null pointer execption when getting prioity. This is becuasue there was a bug in queryWithTimeout
   execution flow. It was not going in the normal execution path where priority was set
  */

  @Test(enabled = true)
  public void testPriorityForTimeoutQuery() throws Exception {

    String cost60 = QueryInventory.getQueryFromInventory("HIVE.COST_60");
    List<QueryHandle> list = new ArrayList<QueryHandle>();

    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");

    LensConf lensConf = new LensConf();
    lensConf.addProperty(LensConfConstants.CANCEL_QUERY_ON_TIMEOUT, "false");

    QueryHandleWithResultSet qhr = (QueryHandleWithResultSet) qHelper.executeQueryTimeout(cost60, "10000",
        null, sessionHandleString, lensConf).getData();

    for(int i = 0; i < 6; i++){
      list.add((QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_CUBE_QUERY).getData());
    }

    LensQuery lq = qHelper.waitForCompletion(qhr.getQueryHandle());
    Assert.assertEquals(lq.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    for(QueryHandle q : list){
      LensQuery lensQuery = qHelper.waitForCompletion(q);
      Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
    }
  }


  /*
    LENS-1016 : When query is executed with "mapreduce.job.priority" set by user, then all queued queries will fail with
    null pointer execption when getting prioity. This is becuasue priority is not set for that query.
 */

  @Test(enabled = true)
  public void testExplicitPrioritySettingByUser() throws Exception {

    List<QueryHandle> handleList = new ArrayList<QueryHandle>();
    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES, "4",
        DriverConfig.PRIORITY_MAX_CONCURRENT, "LOW=2,VERY_LOW=1,HIGH=4");
    String sleepQuery = QueryInventory.getSleepQuery("10");

    try {
      Util.changeConfig(map, hiveDriverConfPath);
      lens.restart();

      LensConf lensConf = new LensConf();
      lensConf.addProperty("mapreduce.job.priority", "HIGH");

      handleList.add((QueryHandle) qHelper.executeQuery(sleepQuery, null, null, sessionHandleString,
          lensConf).getData());

      for(int i=0; i<6; i++){
        handleList.add((QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_CUBE_QUERY).getData());
      }

      for(int i=0; i<handleList.size(); i++){
        LensQuery lq = qHelper.waitForCompletion(handleList.get(i));
        Assert.assertEquals(lq.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
      }
    } finally {
      Util.changeConfig(hiveDriverConfPath);
      lens.restart();
    }
  }
}

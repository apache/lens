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

package org.apache.lens.regression.throttling;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.WebTarget;

import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryPlan;
import org.apache.lens.api.query.QueryStatus;
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

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

public class ITCostTests extends BaseTestClass {

  private WebTarget servLens;
  private String sessionHandleString;

  public static final String COST_95 = QueryInventory.getQueryFromInventory("HIVE.COST_95");
  public static final String COST_60 = QueryInventory.getQueryFromInventory("HIVE.COST_60");
  public static final String COST_30 = QueryInventory.getQueryFromInventory("HIVE.COST_30");
  public static final String COST_20 = QueryInventory.getQueryFromInventory("HIVE.COST_20");
  public static final String COST_10 = QueryInventory.getQueryFromInventory("HIVE.COST_10");
  public static final String COST_5 = QueryInventory.getQueryFromInventory("HIVE.COST_5");
  public static final String COST_3 = QueryInventory.getQueryFromInventory("HIVE.COST_3");
  public static final String COST_2 = QueryInventory.getQueryFromInventory("HIVE.COST_2");
  public static final String JDBC_QUERY1 = QueryInventory.getQueryFromInventory("JDBC.QUERY1");

  private static String hiveDriver = "hive/hive1";
  private String hiveDriverSitePath  = lens.getServerDir() + "/conf/drivers/hive/hive1/hivedriver-site.xml";
  private static final long SECONDS_IN_A_MINUTE = 60;

  private static Logger logger = Logger.getLogger(ITCostTests.class);

  @BeforeClass(alwaysRun = true)
  public void initialize() throws Exception {
    servLens = ServiceManagerHelper.init();
    logger.info("Creating a new Session");
    sessionHandleString = sHelper.openSession(lens.getCurrentDB());
    sHelper.setAndValidateParam(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "false");
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp(Method method) throws Exception {
    logger.info("Test Name: " + method.getName());
  }

  @AfterMethod(alwaysRun = true)
  public void afterMethod(Method method) throws Exception {
    logger.info("Test Name: " + method.getName());
    qHelper.killQuery(null, "QUEUED", "all");
    qHelper.killQuery(null, "RUNNING", "all");
    qHelper.killQuery(null, "EXECUTED", "all");
  }

  @AfterClass(alwaysRun = true)
  public void closeSession() throws Exception {
    logger.info("Closing Session");
    sHelper.closeSession();
  }

  @BeforeGroups("user-cost-ceiling")
  public void setUserCeilingconfig() throws Exception {
    try{
      HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.TOTAL_QUERY_COST_CEILING_PER_USER_KEY, "60",
          DriverConfig.HIVE_CONSTRAINT_FACTORIES,
          DriverConfig.MAX_CONCURRENT_CONSTRAINT_FACTORY + "," + DriverConfig.USER_COST_CONSTRAINT_FACTORY,
          DriverConfig.MAX_CONCURRENT_QUERIES, "10");
      Util.changeConfig(map, hiveDriverSitePath);
      lens.restart();
    }catch (Exception e){
      logger.info(e);
    }
  }

  @AfterGroups("user-cost-ceiling")
  public void restoreConfig() throws SftpException, JSchException, InterruptedException, LensException, IOException {
    Util.changeConfig(hiveDriverSitePath);
    lens.restart();
  }


  @Test(enabled = true, groups= "user-cost-ceiling")
  public void testUserCostCeiling() throws Exception {

    QueryHandle h1 = (QueryHandle) qHelper.executeQuery(COST_60).getData();
    QueryHandle h2 = (QueryHandle) qHelper.executeQuery(COST_95).getData();

    Assert.assertEquals(qHelper.getQueryStatus(h1).getStatus(), QueryStatus.Status.RUNNING);
    Assert.assertEquals(qHelper.getQueryStatus(h2).getStatus(), QueryStatus.Status.QUEUED);

    LensQuery lq1 = qHelper.waitForCompletion(h1);
    LensQuery lq2 = qHelper.waitForCompletion(h2);

    Assert.assertEquals(lq1.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
    Assert.assertEquals(lq2.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
  }

  @Test(enabled = true, groups= "user-cost-ceiling")
  public void hiveJdbcUserCostCeiling() throws Exception {

    List<QueryStatus.Status> passStatus = Arrays.asList(QueryStatus.Status.SUCCESSFUL, QueryStatus.Status.RUNNING,
        QueryStatus.Status.EXECUTED);

    QueryHandle h1 = (QueryHandle) qHelper.executeQuery(COST_10).getData();
    QueryHandle jq1 = (QueryHandle) qHelper.executeQuery(JDBC_QUERY1).getData();
    QueryHandle h2 = (QueryHandle) qHelper.executeQuery(COST_60).getData();
    QueryHandle jq2 = (QueryHandle) qHelper.executeQuery(JDBC_QUERY1).getData();
    QueryHandle h3 = (QueryHandle) qHelper.executeQuery(COST_30).getData();

    Assert.assertEquals(qHelper.getQueryStatus(h1).getStatus(), QueryStatus.Status.RUNNING);
    Assert.assertEquals(qHelper.getQueryStatus(h2).getStatus(), QueryStatus.Status.RUNNING);
    Assert.assertEquals(qHelper.getQueryStatus(h3).getStatus(), QueryStatus.Status.QUEUED);
    Assert.assertTrue(passStatus.contains(qHelper.getQueryStatus(jq1).getStatus()));
    Assert.assertTrue(passStatus.contains(qHelper.getQueryStatus(jq2).getStatus()));

    LensQuery lq1 = qHelper.waitForCompletion(h1);
    LensQuery lq2 = qHelper.waitForCompletion(h2);
    LensQuery lq3 = qHelper.waitForCompletion(h3);

    Assert.assertEquals(lq1.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
    Assert.assertEquals(lq2.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
    Assert.assertEquals(lq3.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
  }

  @Test(enabled = true, groups= "user-cost-ceiling")
  public void runMultipleQueriesCostGreaterThanCeiling() throws Exception {

    int userCeiling = 60;
    Map<QueryHandle, Double> queryhandleCostMap = new HashMap<QueryHandle, Double>();
    QueryPlan qp = null;

    String[] queries = {COST_20, JDBC_QUERY1, COST_95, JDBC_QUERY1, COST_5, COST_10, JDBC_QUERY1,
      COST_30, COST_60, };

    for (int i = 1; i < 3; i++) {
      for(int j = 0; j < queries.length; j++) {
        qp = (QueryPlan) qHelper.explainQuery(queries[j]).getData();
        queryhandleCostMap.put((QueryHandle) qHelper.executeQuery(queries[j]).getData(),
            qp.getQueryCost().getEstimatedResourceUsage());
      }
    }

    List<QueryHandle> running = qHelper.getQueryHandleList(null, "RUNNING", "all");
    List<QueryHandle> queued = qHelper.getQueryHandleList(null, "QUEUED", "all");

    while (running.size() > 0 || queued.size() > 0) {
      if (running.size() > 1) {
        Double costSum = 0.0, maxcost = 0.0;
        for (QueryHandle qH : running) {
          Double currentHandleCost = queryhandleCostMap.get(qH);
          costSum += currentHandleCost;
          if (currentHandleCost > maxcost) {
            maxcost = currentHandleCost;
          }
        }

        logger.info("max-cost : " + maxcost + "   cost-sum : " + costSum);
        Assert.assertTrue((costSum - maxcost) < userCeiling);
      }

      Thread.sleep(1000);
      running = qHelper.getQueryHandleList(null, "RUNNING", "all");
      queued = qHelper.getQueryHandleList(null, "QUEUED", "all");
    }

    for (QueryHandle q : queryhandleCostMap.keySet()) {
      LensQuery lq = qHelper.waitForCompletion(q);
      Assert.assertEquals(lq.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
    }
  }



  @Test(enabled = true, groups= "user-cost-ceiling")
  public void testCostCeilingWithProrityMaxConcurrent() throws Exception {

    String query = String.format(QueryInventory.getQueryFromInventory("HIVE.SLEEP_COST_5"), "5");
    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES, "5",
        DriverConfig.PRIORITY_MAX_CONCURRENT, "HIGH=3");

    List<QueryHandle> handleList = new ArrayList<QueryHandle>();
    for(int i=1; i<=5; i++){
      handleList.add((QueryHandle) qHelper.executeQuery(query).getData());
    }

    QueryStatus s0 = qHelper.getQueryStatus(handleList.get(0));
    QueryStatus s1 = qHelper.getQueryStatus(handleList.get(1));
    QueryStatus s2 = qHelper.getQueryStatus(handleList.get(2));
    QueryStatus s3 = qHelper.getQueryStatus(handleList.get(3));
    QueryStatus s4 = qHelper.getQueryStatus(handleList.get(4));

    Assert.assertEquals(s0.getStatus(), QueryStatus.Status.RUNNING);
    Assert.assertEquals(s1.getStatus(), QueryStatus.Status.RUNNING);
    Assert.assertEquals(s2.getStatus(), QueryStatus.Status.RUNNING);
    Assert.assertEquals(s3.getStatus(), QueryStatus.Status.QUEUED);
    Assert.assertEquals(s4.getStatus(), QueryStatus.Status.QUEUED);

    qHelper.waitForCompletion(handleList.get(0));
    Assert.assertEquals(qHelper.getQueryStatus(handleList.get(3)).getStatus(), QueryStatus.Status.RUNNING);

    qHelper.waitForCompletion(handleList.get(1));
    Assert.assertEquals(qHelper.getQueryStatus(handleList.get(4)).getStatus(), QueryStatus.Status.RUNNING);

    for(QueryHandle q :handleList){
      LensQuery lq = qHelper.waitForCompletion(q);
      Assert.assertEquals(lq.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
    }
  }


  @Test(enabled = true, groups= "user-cost-ceiling")
  public void multipleUserConcurrentPriorityThrottling() throws Exception {

    String query = String.format(QueryInventory.getQueryFromInventory("HIVE.SLEEP_COST_5"), "5");
    long timeToWait= 7 * SECONDS_IN_A_MINUTE; //in seconds
    int sleepTime = 5; //in seconds
    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES, "5",
        DriverConfig.PRIORITY_MAX_CONCURRENT, "HIGH=3");

    String session1 = sHelper.openSession("diff1", "diff1", lens.getCurrentDB());
    String session2 = sHelper.openSession("diff2", "diff2", lens.getCurrentDB());
    sHelper.setAndValidateParam(session1, CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "false");
    sHelper.setAndValidateParam(session2, CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "false");

    List<QueryHandle> handleList = new ArrayList<QueryHandle>();
    for (int i = 1; i <= 3; i++) {
      handleList.add((QueryHandle) qHelper.executeQuery(query).getData());
      handleList.add((QueryHandle) qHelper.executeQuery(query, "", session1).getData());
      handleList.add((QueryHandle) qHelper.executeQuery(query, "", session2).getData());
    }
    Thread.sleep(50);

    List<QueryHandle> running = null, queued = null;
    for (int t = 0; t < timeToWait; t = t + sleepTime) {

      running = qHelper.getQueryHandleList(null, "RUNNING", "all", sessionHandleString, null, null, hiveDriver);
      queued = qHelper.getQueryHandleList(null, "QUEUED", "all", sessionHandleString, null, null, hiveDriver);
      logger.info("Running query count : " + running.size() + "\t Queued query count : " + queued.size());

      if (running.isEmpty() && queued.isEmpty()) {
        break;
      }

      Assert.assertTrue(running.size() <= 5);
      TimeUnit.SECONDS.sleep(sleepTime);
    }

    Assert.assertTrue(running.isEmpty() && queued.isEmpty(), "Queries are taking very long time");

    for (QueryHandle q : handleList) {
      LensQuery lq = qHelper.waitForCompletion(q);
      Assert.assertEquals(lq.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
    }
  }

  //TODO : Add queue level throttling along with user ceiling constraint

  /*
  * LENS-995 : Queue number shouldn't change with in the same prority
  */

  @Test(enabled = true)
  public void queueNumberChangeWithInSamePriority() throws Exception {

    String longRunningQuery = String.format(QueryInventory.getQueryFromInventory("HIVE.SLEEP_COST_95"), "20");
    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES, "1");
    String[] queries = {longRunningQuery, COST_5, COST_5, COST_3, COST_2};

    try {
      Util.changeConfig(map, hiveDriverSitePath);
      lens.restart();

      List<QueryHandle> handleList = new ArrayList<>();
      for(String query : queries){
        handleList.add((QueryHandle) qHelper.executeQuery(query).getData());
      }

      LensQuery lq1 = qHelper.getLensQuery(sessionHandleString, handleList.get(1));
      LensQuery lq2 = qHelper.getLensQuery(sessionHandleString, handleList.get(2));
      LensQuery lq3 = qHelper.getLensQuery(sessionHandleString, handleList.get(3));
      LensQuery lq4 = qHelper.getLensQuery(sessionHandleString, handleList.get(4));

      Assert.assertEquals(lq1.getStatus().getQueueNumber().intValue(), 1);
      Assert.assertEquals(lq2.getStatus().getQueueNumber().intValue(), 2);
      Assert.assertEquals(lq3.getStatus().getQueueNumber().intValue(), 3);
      Assert.assertEquals(lq4.getStatus().getQueueNumber().intValue(), 4);

      LensQuery lq0 = qHelper.waitForCompletion(handleList.get(0));

      lq1 = qHelper.getLensQuery(sessionHandleString, handleList.get(1));
      lq2 = qHelper.getLensQuery(sessionHandleString, handleList.get(2));
      lq3 = qHelper.getLensQuery(sessionHandleString, handleList.get(3));
      lq4 = qHelper.getLensQuery(sessionHandleString, handleList.get(4));

      Assert.assertEquals(lq0.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
      Assert.assertEquals(lq2.getStatus().getQueueNumber().intValue(), 1);
      Assert.assertEquals(lq3.getStatus().getQueueNumber().intValue(), 2);
      Assert.assertEquals(lq4.getStatus().getQueueNumber().intValue(), 3);

      lq1 = qHelper.waitForCompletion(handleList.get(1));

      lq2 = qHelper.getLensQuery(sessionHandleString, handleList.get(2));
      lq3 = qHelper.getLensQuery(sessionHandleString, handleList.get(3));
      lq4 = qHelper.getLensQuery(sessionHandleString, handleList.get(4));

      Assert.assertEquals(lq1.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
      Assert.assertEquals(lq3.getStatus().getQueueNumber().intValue(), 1);
      Assert.assertEquals(lq4.getStatus().getQueueNumber().intValue(), 2);

    }finally {
      Util.changeConfig(hiveDriverSitePath);
      lens.restart();
    }
  }


  @Test(enabled = true)
  public void queueNumberChangeDifferentPriority() throws Exception {

    String longRunningQuery = String.format(QueryInventory.getQueryFromInventory("HIVE.SLEEP_COST_95"), "20");
    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES, "1");
    try {
      Util.changeConfig(map, hiveDriverSitePath);
      lens.restart();

      QueryHandle q0 = (QueryHandle) qHelper.executeQuery(longRunningQuery).getData();
      QueryHandle q1 = (QueryHandle) qHelper.executeQuery(COST_20).getData();
      QueryHandle q2 = (QueryHandle) qHelper.executeQuery(COST_2).getData();

      LensQuery normal1 = qHelper.getLensQuery(sessionHandleString, q1);
      LensQuery high1 = qHelper.getLensQuery(sessionHandleString, q2);

      Assert.assertEquals(normal1.getStatus().getQueueNumber().intValue(), 2);
      Assert.assertEquals(high1.getStatus().getQueueNumber().intValue(), 1);

      QueryHandle q3 = (QueryHandle) qHelper.executeQuery(COST_5).getData();

      LensQuery high2 = qHelper.getLensQuery(sessionHandleString, q3);
      high1 = qHelper.getLensQuery(sessionHandleString, q2);
      normal1 = qHelper.getLensQuery(sessionHandleString, q1);

      Assert.assertEquals(normal1.getStatus().getQueueNumber().intValue(), 3);
      Assert.assertEquals(high1.getStatus().getQueueNumber().intValue(), 1);
      Assert.assertEquals(high2.getStatus().getQueueNumber().intValue(), 2);

      QueryHandle q4 = (QueryHandle) qHelper.executeQuery(COST_20).getData();

      LensQuery normal2 = qHelper.getLensQuery(sessionHandleString, q4);
      normal1 = qHelper.getLensQuery(sessionHandleString, q1);
      high1 = qHelper.getLensQuery(sessionHandleString, q2);
      high2 = qHelper.getLensQuery(sessionHandleString, q3);

      Assert.assertEquals(high1.getStatus().getQueueNumber().intValue(), 1);
      Assert.assertEquals(high2.getStatus().getQueueNumber().intValue(), 2);
      Assert.assertEquals(normal1.getStatus().getQueueNumber().intValue(), 3);
      Assert.assertEquals(normal2.getStatus().getQueueNumber().intValue(), 4);

    }finally {
      Util.changeConfig(hiveDriverSitePath);
      lens.restart();
    }
  }


  @Test(enabled = true)
  public void queueNumberChangeDifferentPriorityWithJdbc() throws Exception {

    String longRunningQuery = String.format(QueryInventory.getQueryFromInventory("HIVE.SLEEP_COST_95"), "20");
    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES, "1");
    List<QueryHandle> handleList = new ArrayList<>();

    try {
      Util.changeConfig(map, hiveDriverSitePath);
      lens.restart();

      String[] queries = {COST_20, COST_2, COST_3, COST_60, COST_5, COST_10, COST_3};
      // Queue order is determined from priority and order in which queries are fired.
      int[] queueNo = {5, 1, 2, 7, 3, 6, 4};

      qHelper.executeQuery(longRunningQuery);
      for(String query : queries){
        handleList.add((QueryHandle) qHelper.executeQuery(query).getData());
        qHelper.executeQuery(JDBC_QUERY1).getData();
      }

      List<LensQuery> lqList = new ArrayList<>();
      for(QueryHandle qh : handleList){
        lqList.add(qHelper.getLensQuery(sessionHandleString, qh));
      }

      for(int i = 0; i < lqList.size(); i++) {
        Assert.assertEquals(lqList.get(i).getStatus().getQueueNumber().intValue(), queueNo[i]);
      }

    }finally {
      Util.changeConfig(hiveDriverSitePath);
      lens.restart();
    }
  }

}


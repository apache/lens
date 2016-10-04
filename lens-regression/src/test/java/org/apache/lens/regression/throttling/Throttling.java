
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

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.WebTarget;

import org.apache.lens.api.Priority;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.cube.parse.CubeQueryConfUtil;
import org.apache.lens.driver.hive.HiveDriver;
import org.apache.lens.regression.core.constants.DriverConfig;
import org.apache.lens.regression.core.constants.QueryInventory;
import org.apache.lens.regression.core.helpers.ServiceManagerHelper;
import org.apache.lens.regression.core.testHelper.BaseTestClass;
import org.apache.lens.regression.util.Util;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.util.LensUtil;

import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.*;

public class Throttling extends BaseTestClass {

  WebTarget servLens;
  String sessionHandleString;

  public static final String SLEEP_QUERY = QueryInventory.getSleepQuery("5");
  public static final String COST_95 = String.format(QueryInventory.getQueryFromInventory("HIVE.SLEEP_COST_95"), "5");
  public static final String COST_60 = String.format(QueryInventory.getQueryFromInventory("HIVE.SLEEP_COST_60"), "5");
  public static final String COST_30 = String.format(QueryInventory.getQueryFromInventory("HIVE.SLEEP_COST_30"), "5");
  public static final String COST_20 = String.format(QueryInventory.getQueryFromInventory("HIVE.SLEEP_COST_20"), "4");
  public static final String COST_10 = String.format(QueryInventory.getQueryFromInventory("HIVE.SLEEP_COST_10"), "4");
  public static final String COST_5 = String.format(QueryInventory.getQueryFromInventory("HIVE.SLEEP_COST_5"), "3");
  public static final String JDBC_QUERY1 = QueryInventory.getQueryFromInventory("JDBC.QUERY1");

  private static String hiveDriver = "hive/hive1";
  private final String hiveDriverConf = lens.getServerDir() + "/conf/drivers/hive/hive1/hivedriver-site.xml";
  private final String backupConfFilePath = lens.getServerDir() + "/conf/drivers/hive/hive1/backup-hivedriver-site.xml";

  private static final long SECONDS_IN_A_MINUTE = 60;
  private String session1 = null, session2 = null;
  //TODO : Read queue names from property file
  private static String queue1 = "dwh", queue2 = "reports";

  private static Logger logger = Logger.getLogger(Throttling.class);

  @BeforeClass(alwaysRun = true)
  public void initialize() throws Exception {
    servLens = ServiceManagerHelper.init();
    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES, "10",
        HiveDriver.HS2_PRIORITY_RANGES, "HIGH,7,NORMAL,30,LOW,90,VERY_LOW");
    Util.changeConfig(map, hiveDriverConf);
    lens.restart();
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp(Method method) throws Exception {
    logger.info("Test Name: " + method.getName());
    Util.runRemoteCommand("cp " + hiveDriverConf + " " + backupConfFilePath);

    sessionHandleString = sHelper.openSession(lens.getCurrentDB());
    session1 = sHelper.openSession("diff1", "diff1", lens.getCurrentDB());
    session2 = sHelper.openSession("diff2", "diff2", lens.getCurrentDB());
    sHelper.setAndValidateParam(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "false");
    sHelper.setAndValidateParam(session1, CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "false");
    sHelper.setAndValidateParam(session2, CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "false");
  }

  @AfterMethod(alwaysRun = true)
  public void afterMethod(Method method) throws Exception {
    logger.info("Test Name: " + method.getName());
    qHelper.killQuery(null, "QUEUED", "all");
    qHelper.killQuery(null, "RUNNING", "all");
    qHelper.killQuery(null, "EXECUTED", "all");
    sHelper.closeSession(session1);
    sHelper.closeSession(session2);
    sHelper.closeSession(sessionHandleString);

    Util.runRemoteCommand("cp " + backupConfFilePath + " " + hiveDriverConf);
  }

  @AfterClass(alwaysRun = false)
  public void closeSession() throws Exception {
    lens.restart();
  }


  @Test(enabled = true)
  public void testHiveThrottling() throws Exception {

    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES, "2");
    Util.changeConfig(map, hiveDriverConf);
    lens.restart();

    List<QueryHandle> handleList = new ArrayList<>();
    handleList.add((QueryHandle) qHelper.executeQuery(SLEEP_QUERY).getData());
    handleList.add((QueryHandle) qHelper.executeQuery(SLEEP_QUERY, null, session1).getData());
    handleList.add((QueryHandle) qHelper.executeQuery(SLEEP_QUERY, null, session2).getData());
    handleList.add((QueryHandle) qHelper.executeQuery(SLEEP_QUERY, null).getData());

    Thread.sleep(1000);

    List<QueryStatus> statusList = new ArrayList<>();
    for(QueryHandle handle : handleList){
      statusList.add(qHelper.getQueryStatus(handle));
    }

    Assert.assertEquals(statusList.get(0).getStatus(), QueryStatus.Status.RUNNING);
    Assert.assertEquals(statusList.get(1).getStatus(), QueryStatus.Status.RUNNING);
    Assert.assertEquals(statusList.get(2).getStatus(), QueryStatus.Status.QUEUED);
    Assert.assertEquals(statusList.get(3).getStatus(), QueryStatus.Status.QUEUED);

    qHelper.waitForCompletion(handleList.get(0));
    Thread.sleep(100);
    Assert.assertEquals(qHelper.getQueryStatus(handleList.get(2)).getStatus(), QueryStatus.Status.RUNNING);
  }


  @Test(enabled = true)
  public void testHiveMaxConcurrentRandomQueryIngestion() throws Exception {

    long timeToWait= 5 * SECONDS_IN_A_MINUTE; // in seconds
    int sleepTime = 3, maxConcurrent = 4;
    List<QueryHandle> handleList = new ArrayList<QueryHandle>();

    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES,
        String.valueOf(maxConcurrent));
    Util.changeConfig(map, hiveDriverConf);
    lens.restart();

    for (int i = 0; i < 5; i++) {
      handleList.add((QueryHandle) qHelper.executeQuery(SLEEP_QUERY).getData());
      handleList.add((QueryHandle) qHelper.executeQuery(JDBC_QUERY1).getData());
      handleList.add((QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_CUBE_QUERY, null, session1).getData());
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
      Assert.assertTrue(running.size() <= maxConcurrent);

      if (t % 30 == 0 && t < 200) {
        handleList.add((QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_DIM_QUERY).getData());
        handleList.add((QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_CUBE_QUERY).getData());
        handleList.add((QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_CUBE_QUERY, null, session1).getData());
      }
      TimeUnit.SECONDS.sleep(sleepTime);
    }

    Assert.assertTrue(running.isEmpty());
    Assert.assertTrue(queued.isEmpty());

    for(QueryHandle q : handleList){
      LensQuery lq = qHelper.waitForCompletion(q);
      Assert.assertEquals(lq.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
    }
  }

  @Test(enabled = true)
  public void testProrityMaxConcurrent() throws Exception {

    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES, "5",
        DriverConfig.PRIORITY_MAX_CONCURRENT, "HIGH=2,VERY_LOW=1", HiveDriver.HS2_PRIORITY_RANGES,
        "HIGH,7,NORMAL,30,LOW,90,VERY_LOW");
    Util.changeConfig(map, hiveDriverConf);
    lens.restart();

    /* First 3 are high priority queries, 2 of them will go to RUNNING, 3rd will be queued as there is a
      threshold of 2 on HIGH priority. cost_95 queries are very_low priority ones, hence 1st will go to RUNNING,
      other 1 is queued. cost_20 and cost_60 goes to running as they are normal and low priority queries and there
      is no limit set for low and normal priority.Last cost_60 query goes to queue as, RUNNING query on this
      driver has reached max concurrent threshold.
    */

    String[] queries = {COST_5, COST_5, COST_5, COST_95, COST_95, COST_20, COST_60, COST_60};
    QueryStatus.Status[] expectedStatus = {QueryStatus.Status.RUNNING, QueryStatus.Status.RUNNING,
      QueryStatus.Status.QUEUED, QueryStatus.Status.RUNNING, QueryStatus.Status.QUEUED,
      QueryStatus.Status.RUNNING, QueryStatus.Status.RUNNING, QueryStatus.Status.QUEUED, };

    List<QueryHandle> handleList = new ArrayList<>();
    for (String query : queries){
      handleList.add((QueryHandle) qHelper.executeQuery(query).getData());
    }

    List<QueryStatus.Status> statusList = new ArrayList<>();
    for (QueryHandle handle : handleList){
      statusList.add(qHelper.getQueryStatus(handle).getStatus());
    }

    for (int i=0; i<statusList.size(); i++){
      Assert.assertEquals(statusList.get(i), expectedStatus[i]);
    }

    qHelper.waitForCompletion(handleList.get(0));
    Assert.assertEquals(qHelper.getQueryStatus(handleList.get(2)).getStatus(), QueryStatus.Status.RUNNING);
  }

  @Test(enabled = true)
  public void prioritySumMoreThanMaxConcurrent() throws Exception {

    long timeToWait= 5 * SECONDS_IN_A_MINUTE; // in seconds
    int sleepTime = 5, maxConcurrent = 5, lowConCurrent = 2, veryLowConcurrent = 1, highConcurrent = 4,
        normalConcurrent = 2;
    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES,
        String.valueOf(maxConcurrent), DriverConfig.PRIORITY_MAX_CONCURRENT,
        "LOW=" + String.valueOf(lowConCurrent) + ",VERY_LOW=" + String.valueOf(veryLowConcurrent)
        + ",NORMAL=" + String.valueOf(normalConcurrent) + ",HIGH=" + String.valueOf(highConcurrent));
    Util.changeConfig(map, hiveDriverConf);
    lens.restart();

    List<QueryHandle> handleList = new ArrayList<QueryHandle>();
    for (int i=1; i<=5; i++){
      handleList.add((QueryHandle) qHelper.executeQuery(COST_95).getData());
      handleList.add((QueryHandle) qHelper.executeQuery(COST_60, null, session1).getData());
      handleList.add((QueryHandle) qHelper.executeQuery(COST_20, null, session2).getData());
      handleList.add((QueryHandle) qHelper.executeQuery(COST_5).getData());
    }

    List<QueryHandle> running=null, queued=null;
    for (int t = 0; t < timeToWait; t = t + sleepTime) {

      running = qHelper.getQueryHandleList(null, "RUNNING", "all", sessionHandleString, null, null, hiveDriver);
      queued = qHelper.getQueryHandleList(null, "QUEUED", "all", sessionHandleString, null, null, hiveDriver);
      logger.info("Running query count : " + running.size() + "\t Queued query count : " + queued.size());

      if (running.isEmpty() && queued.isEmpty()) {
        break;
      }

      Assert.assertTrue(running.size() <= maxConcurrent);

      int low = 0, veryLow = 0, high = 0, normal = 0;
      for (QueryHandle qh : running) {
        Priority p = qHelper.getLensQuery(sessionHandleString, qh).getPriority();
        Assert.assertNotNull(p);
        switch (p) {
        case HIGH:
          high++;
          break;
        case NORMAL:
          normal++;
          break;
        case LOW:
          low++;
          break;
        case VERY_LOW:
          veryLow++;
          break;
        default:
          throw new Exception("Unexpected Priority");
        }
      }

      Assert.assertTrue(low <= lowConCurrent);
      Assert.assertTrue(veryLow <= veryLowConcurrent);
      Assert.assertTrue(high <= highConcurrent);
      Assert.assertTrue(normal <= normalConcurrent);

      TimeUnit.SECONDS.sleep(sleepTime);
    }

    Assert.assertTrue(queued.isEmpty());
    Assert.assertTrue(running.isEmpty());

    for (QueryHandle q: handleList){
      LensQuery lq = qHelper.waitForCompletion(q);
      Assert.assertEquals(lq.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
    }
  }


  @Test(enabled = true)
  public void queueMaxConcurrent() throws Exception {

    int maxConcurrent = 3, queue1Count = 1, queue2Count = 2;
    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES,
        String.valueOf(maxConcurrent), DriverConfig.QUEUE_MAX_CONCURRENT,
        queue1 + "=" + String.valueOf(queue1Count) + "," + queue2 + "=" + String.valueOf(queue2Count));
    List<QueryHandle> handleList = new ArrayList<>();

    Util.changeConfig(map, hiveDriverConf);
    lens.restart();

    sHelper.setAndValidateParam(LensConfConstants.MAPRED_JOB_QUEUE_NAME, queue1);
    handleList.add((QueryHandle) qHelper.executeQuery(COST_60).getData());
    handleList.add((QueryHandle) qHelper.executeQuery(COST_95).getData());

    sHelper.setAndValidateParam(LensConfConstants.MAPRED_JOB_QUEUE_NAME, queue2);
    handleList.add((QueryHandle) qHelper.executeQuery(COST_60).getData());
    handleList.add((QueryHandle) qHelper.executeQuery(COST_95).getData());
    handleList.add((QueryHandle) qHelper.executeQuery(COST_95).getData());

    List<QueryStatus.Status> statusList = new ArrayList<>();
    for (QueryHandle handle : handleList){
      statusList.add(qHelper.getQueryStatus(handle).getStatus());
    }

    Assert.assertEquals(statusList.get(0), QueryStatus.Status.RUNNING);
    Assert.assertEquals(statusList.get(1), QueryStatus.Status.QUEUED);
    Assert.assertEquals(statusList.get(2), QueryStatus.Status.RUNNING);
    Assert.assertEquals(statusList.get(3), QueryStatus.Status.RUNNING);
    Assert.assertEquals(statusList.get(4), QueryStatus.Status.QUEUED);

    qHelper.waitForCompletion(handleList.get(0));
    Assert.assertEquals(qHelper.getQueryStatus(handleList.get(1)).getStatus(), QueryStatus.Status.RUNNING);

    qHelper.waitForCompletion(handleList.get(2));
    Assert.assertEquals(qHelper.getQueryStatus(handleList.get(4)).getStatus(), QueryStatus.Status.RUNNING);
  }

  // LENS-1027
  @Test(enabled = true)
  public void queueDefaultThresholdConstraint() throws Exception {

    int maxConcurrent = 5, queue1Count = 1, queue2Count = 2;
    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES,
        String.valueOf(maxConcurrent), DriverConfig.QUEUE_MAX_CONCURRENT,
        "*=" + String.valueOf(queue1Count) + "," + queue2 + "=" + String.valueOf(queue2Count));
    List<QueryHandle> handleList = new ArrayList<>();

    Util.changeConfig(map, hiveDriverConf);
    lens.restart();

    sHelper.setAndValidateParam(LensConfConstants.MAPRED_JOB_QUEUE_NAME, queue1);
    handleList.add((QueryHandle) qHelper.executeQuery(COST_60).getData());
    handleList.add((QueryHandle) qHelper.executeQuery(COST_95).getData());

    sHelper.setAndValidateParam(LensConfConstants.MAPRED_JOB_QUEUE_NAME, queue2);
    handleList.add((QueryHandle) qHelper.executeQuery(COST_60).getData());
    handleList.add((QueryHandle) qHelper.executeQuery(COST_95).getData());
    handleList.add((QueryHandle) qHelper.executeQuery(COST_95).getData());

    Thread.sleep(2000);

    List<QueryStatus.Status> statusList = new ArrayList<>();
    for (QueryHandle handle : handleList){
      statusList.add(qHelper.getQueryStatus(handle).getStatus());
    }

    Assert.assertEquals(statusList.get(0), QueryStatus.Status.RUNNING);
    Assert.assertEquals(statusList.get(1), QueryStatus.Status.QUEUED);
    Assert.assertEquals(statusList.get(2), QueryStatus.Status.RUNNING);
    Assert.assertEquals(statusList.get(3), QueryStatus.Status.RUNNING);
    Assert.assertEquals(statusList.get(4), QueryStatus.Status.QUEUED);
  }


  @Test(enabled = true)
  public void enableQueueThrottlingWithExistingQueuedQueries() throws Exception {

    long timeToWait= 5 * SECONDS_IN_A_MINUTE; // in seconds
    int sleepTime = 5, maxConcurrent = 4, queue1Concurrent = 1, queue2Concurrent = 2;
    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES,
        String.valueOf(maxConcurrent), DriverConfig.QUEUE_MAX_CONCURRENT,
        queue1 + "=" + String.valueOf(queue1Concurrent) + "," + queue2 + "=" + String.valueOf(queue2Concurrent));

    sHelper.setAndValidateParam(session1, LensConfConstants.MAPRED_JOB_QUEUE_NAME, queue1);
    sHelper.setAndValidateParam(session2, LensConfConstants.MAPRED_JOB_QUEUE_NAME, queue2);

    List<QueryHandle> handleList = new ArrayList<QueryHandle>();
    for (int i = 1; i <= 3; i++) {
      handleList.add((QueryHandle)qHelper.executeQuery(COST_95).getData());
      handleList.add((QueryHandle)qHelper.executeQuery(COST_20, "", session1).getData());
      handleList.add((QueryHandle)qHelper.executeQuery(COST_95, "", session2).getData());
    }

    Util.changeConfig(map, hiveDriverConf);
    lens.restart();

    for (int i = 1; i <= 2; i++) {
      handleList.add((QueryHandle)qHelper.executeQuery(COST_95).getData());
      handleList.add((QueryHandle)qHelper.executeQuery(COST_20, "", session1).getData());
      handleList.add((QueryHandle)qHelper.executeQuery(COST_95, "", session2).getData());
    }

    List<QueryHandle> running = null, queued = null;
    for (int t = 0; t < timeToWait; t = t + sleepTime) {

      running = qHelper.getQueryHandleList(null, "RUNNING", "all", sessionHandleString, null, null, hiveDriver);
      queued = qHelper.getQueryHandleList(null, "QUEUED", "all", sessionHandleString,  null, null, hiveDriver);
      logger.info("Running query count : " + running.size() + "\t Queued query count : " + queued.size());

      if (running.isEmpty() && queued.isEmpty()) {
        break;
      }
      Assert.assertTrue(running.size() <= maxConcurrent);

      int queue1Count = 0, queue2Count = 0;
      for (QueryHandle qh : running) {
        String queue = qHelper.getLensQuery(sessionHandleString, qh).getQueryConf().getProperties()
            .get("mapreduce.job.queuename");
        Assert.assertNotNull(queue);

        if (queue.equals(queue1)) {
          queue1Count++;
        } else if (queue.equals(queue2)) {
          queue2Count++;
        }
      }

      Assert.assertTrue(queue1Count <= queue1Concurrent, "queue1 count : " + queue1Count);
      Assert.assertTrue(queue2Count <= queue2Concurrent, "queue2 count : " + queue2Count);
      TimeUnit.SECONDS.sleep(sleepTime);
    }

    Assert.assertTrue(running.isEmpty());
    Assert.assertTrue(queued.isEmpty());

    for(QueryHandle q: handleList){
      LensQuery lq = qHelper.waitForCompletion(q);
      Assert.assertEquals(lq.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
    }
  }


  @Test(enabled = true)
  public void queueAndPriorityMaxConcurrent() throws Exception {

    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES, "5",
      DriverConfig.PRIORITY_MAX_CONCURRENT, "LOW=2,VERY_LOW=1",
      DriverConfig.QUEUE_MAX_CONCURRENT, queue1 + "=1," + queue2 + "=2");

    Util.changeConfig(map, hiveDriverConf);
    lens.restart();

    sHelper.setAndValidateParam(session1, LensConfConstants.MAPRED_JOB_QUEUE_NAME, queue1);
    sHelper.setAndValidateParam(session2, LensConfConstants.MAPRED_JOB_QUEUE_NAME, queue2);

    QueryStatus.Status[] expectedStatus = {QueryStatus.Status.RUNNING, QueryStatus.Status.QUEUED,
      QueryStatus.Status.QUEUED, QueryStatus.Status.RUNNING, QueryStatus.Status.RUNNING,
      QueryStatus.Status.RUNNING, QueryStatus.Status.QUEUED, QueryStatus.Status.QUEUED, };

    List<QueryHandle> handleList = new ArrayList<>();
    handleList.add((QueryHandle) qHelper.executeQuery(COST_95, null, session1).getData());
    handleList.add((QueryHandle) qHelper.executeQuery(COST_20, null, session1).getData());
    handleList.add((QueryHandle) qHelper.executeQuery(COST_95, null, session2).getData());
    handleList.add((QueryHandle) qHelper.executeQuery(COST_60, null, session2).getData());
    handleList.add((QueryHandle) qHelper.executeQuery(COST_5, null, session2).getData());
    handleList.add((QueryHandle) qHelper.executeQuery(COST_60).getData());
    handleList.add((QueryHandle) qHelper.executeQuery(COST_20, null, session2).getData());
    handleList.add((QueryHandle) qHelper.executeQuery(COST_5, null, session2).getData());

    List<QueryStatus> statusList = new ArrayList<>();
    for(QueryHandle handle: handleList){
      statusList.add(qHelper.getQueryStatus(handle));
    }

    for(int i=0; i<expectedStatus.length; i++){
      Assert.assertEquals(statusList.get(i).getStatus(), expectedStatus[i], "failed : query-" + i);
    }
  }


  @Test(enabled = true)
  public void queueAndPriorityMaxConcurrentMany() throws Exception {

    long timeToWait= 5 * SECONDS_IN_A_MINUTE; // in seconds
    int sleepTime = 5, maxConcurrent = 5, queue1Concurrent = 1, queue2Concurrent = 2, priority1 = 2, priority2 = 1;

    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES,
      String.valueOf(maxConcurrent), DriverConfig.PRIORITY_MAX_CONCURRENT,
      "HIGH=" + String.valueOf(priority1) + ",NORMAL=" + String.valueOf(priority2),
      DriverConfig.QUEUE_MAX_CONCURRENT,
      queue1 + "=" + String.valueOf(queue1Concurrent) + "," + queue2 + "=" + String.valueOf(queue2Concurrent));

    Util.changeConfig(map, hiveDriverConf);
    lens.restart();

    sHelper.setAndValidateParam(session1, LensConfConstants.MAPRED_JOB_QUEUE_NAME, queue1);
    sHelper.setAndValidateParam(session2, LensConfConstants.MAPRED_JOB_QUEUE_NAME, queue2);

    List<QueryHandle> handleList = new ArrayList<QueryHandle>();
    for (int i = 1; i <= 3; i++) {
      handleList.add((QueryHandle) qHelper.executeQuery(COST_5).getData());
      handleList.add((QueryHandle) qHelper.executeQuery(COST_20, "", session1).getData());
      handleList.add((QueryHandle) qHelper.executeQuery(COST_60, "", session2).getData());
      handleList.add((QueryHandle) qHelper.executeQuery(COST_20).getData());
      handleList.add((QueryHandle) qHelper.executeQuery(COST_95, "", session1).getData());
      handleList.add((QueryHandle) qHelper.executeQuery(COST_5, "", session2).getData());
    }

    List<QueryHandle> running = null, queued = null;
    for (int t = 0; t < timeToWait; t = t + sleepTime) {

      running = qHelper.getQueryHandleList(null, "RUNNING", "all", sessionHandleString, null, null, hiveDriver);
      queued = qHelper.getQueryHandleList(null, "QUEUED", "all", sessionHandleString, null, null, hiveDriver);
      logger.info("Running query count : " + running.size() + "\t Queued query count : " + queued.size());

      if (running.isEmpty() && queued.isEmpty()) {
        break;
      }

      Assert.assertTrue(running.size() <= maxConcurrent);

      int pCount1 = 0, pCount2 = 0, queue1Count = 0, queue2Count = 0;
      for (QueryHandle qh : running) {
        Priority priority = qHelper.getLensQuery(sessionHandleString, qh).getPriority();
        String queue = qHelper.getLensQuery(sessionHandleString, qh).getQueryConf().getProperties()
            .get("mapreduce.job.queuename");

        Assert.assertNotNull(priority);
        Assert.assertNotNull(queue);

        if (priority.equals(Priority.LOW)){
          pCount1++;
        } else if (priority.equals(Priority.VERY_LOW)){
          pCount2++;
        }

        if (queue.equals(queue1)){
          queue1Count++;
        } else if (queue.equals(queue2)) {
          queue2Count++;
        }
      }

      Assert.assertTrue(pCount1 <= priority1, "proirty-1 count : " + pCount1);
      Assert.assertTrue(pCount2 <= priority2, "priority-2 count : " + pCount2);
      Assert.assertTrue(queue1Count <= queue1Concurrent, "queue-1 count : " + queue1Count);
      Assert.assertTrue(queue2Count <= queue2Concurrent, "queue-2 count : " + queue2Count);

      TimeUnit.SECONDS.sleep(sleepTime);
    }

    Assert.assertTrue(queued.isEmpty());
    Assert.assertTrue(running.isEmpty());

    for (QueryHandle q : handleList) {
      LensQuery lq = qHelper.waitForCompletion(q);
      Assert.assertEquals(lq.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
    }
  }

  /*
    LENS-973. Scenario is mentioned in jira
  */

  @Test(enabled = true)
  public void queueConstraintFailureOnRestart() throws Exception {

    List<QueryHandle> handleList = new ArrayList<QueryHandle>();

    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES, "3",
        DriverConfig.QUEUE_MAX_CONCURRENT, queue1 + "=1," + queue2 + "=3");
    Util.changeConfig(map, hiveDriverConf);
    lens.restart();

    String newSession = sHelper.openSession("user", "pwd", lens.getCurrentDB());
    sHelper.setAndValidateParam(newSession, LensConfConstants.MAPRED_JOB_QUEUE_NAME, queue2);
    handleList.add((QueryHandle) qHelper.executeQuery(QueryInventory.SLEEP_QUERY, null, newSession).getData());

    for(int i=0; i<2; i++){
      handleList.add((QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_CUBE_QUERY).getData());
    }

    sHelper.closeSession(newSession);
    lens.restart();
    Assert.assertFalse(qHelper.getQueryStatus(handleList.get(0)).finished());

    for(int i=0; i<6; i++){
      handleList.add((QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_DIM_QUERY).getData());
    }

    for(QueryHandle handle: handleList){
      LensQuery lq = qHelper.waitForCompletion(handle);
      Assert.assertEquals(lq.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
    }
  }
}

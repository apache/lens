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

package org.apache.lens.regression.client;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;

import javax.ws.rs.client.WebTarget;

import javax.xml.bind.JAXBException;

import org.apache.lens.api.query.*;
import org.apache.lens.regression.core.constants.QueryInventory;
import org.apache.lens.regression.core.helpers.*;
import org.apache.lens.regression.core.testHelper.BaseTestClass;
import org.apache.lens.server.api.error.LensException;

import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.*;


public class KillQueryTests extends BaseTestClass {

  WebTarget servLens;
  private String sessionHandleString;

  private final String hdfsJarPath = lens.getServerHdfsUrl() + "/tmp";
  private final String localJarPath = new File("").getAbsolutePath() + "/lens-regression/target/testjars/";
  private final String hiveUdfJar = "hiveudftest.jar";
  private final String serverResourcePath = "/tmp/regression/resources";
  String sleepQuery = QueryInventory.getSleepQuery("5");

  private static Logger logger = Logger.getLogger(KillQueryTests.class);

  @BeforeClass(alwaysRun = true)
  public void initialize() throws IOException, JAXBException, LensException, IllegalAccessException,
      InstantiationException {
    servLens = ServiceManagerHelper.init();
    logger.info("Creating a new Session");
    sessionHandleString = sHelper.openSession(lens.getCurrentDB());

    //TODO : Enable when udf registration per driver is fixed
/*  HadoopUtil.uploadJars(localJarPath + "/" + hiveUdfJar, hdfsJarPath);
    logger.info("Adding jar for making query to run for longer period of time");
    sHelper.addResourcesJar(hdfsJarPath + "/" + hiveUdfJar);
    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.SLEEP_FUNCTION).getData();*/
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp(Method method) throws Exception {
    logger.info("Test Name: " + method.getName());
  }


  @AfterClass(alwaysRun = true)
  public void closeSession() throws Exception {
    logger.info("Closing Session");
    sHelper.closeSession();
  }


  @Test(enabled = true)
  public void killQueryByHandle() throws Exception {

    QueryHandle qH = (QueryHandle) qHelper.executeQuery(sleepQuery).getData();
    logger.info("QUERY HANDLE : " + qH);

    QueryStatus queryStatus = qHelper.waitForQueryToRun(qH);
    Assert.assertEquals(queryStatus.getStatus(), QueryStatus.Status.RUNNING, "Query is Not Running");

    qHelper.killQueryByQueryHandle(qH);

    queryStatus = qHelper.getQueryStatus(qH);
    Assert.assertEquals(queryStatus.getStatus(), QueryStatus.Status.CANCELED, "Query is Not Running");
  }


  @Test(enabled = true)
  public void killQueryByUser() throws Exception {

    String diffUser = "diff";
    String diffPass = "diff";

    String newSessionHandleSring = sHelper.openSession(diffUser, diffPass, lens.getCurrentDB());

    logger.info("Adding jar for making query to run for longer period of time");
    sHelper.addResourcesJar(hdfsJarPath + "/" + hiveUdfJar, newSessionHandleSring);

    QueryHandle queryHandle1 = (QueryHandle) qHelper.executeQuery(sleepQuery).getData();
    logger.info("1st QUERY HANDLE : " + queryHandle1);

    QueryHandle queryHandle2 = (QueryHandle) qHelper.executeQuery(sleepQuery).getData();
    logger.info("2nd QUERY HANDLE : " + queryHandle2);

    QueryHandle queryHandle3 = (QueryHandle) qHelper.executeQuery(sleepQuery, null,
        newSessionHandleSring).getData();
    logger.info("3rd QUERY HANDLE : " + queryHandle3);

    QueryStatus queryStatus1 = qHelper.waitForQueryToRun(queryHandle1);
    QueryStatus queryStatus2 = qHelper.waitForQueryToRun(queryHandle2);
    QueryStatus queryStatus3 = qHelper.waitForQueryToRun(queryHandle3, newSessionHandleSring);

    Assert.assertEquals(queryStatus1.getStatus(), QueryStatus.Status.RUNNING, "Query is Not Running");
    Assert.assertEquals(queryStatus2.getStatus(), QueryStatus.Status.RUNNING, "Query is Not Running");
    Assert.assertEquals(queryStatus3.getStatus(), QueryStatus.Status.RUNNING, "Query is Not Running");

    Thread.sleep(1000);

    qHelper.killQuery(null, null, lens.getUserName());
    Thread.sleep(2000);
    queryStatus1 = qHelper.getQueryStatus(queryHandle1);
    queryStatus2 = qHelper.getQueryStatus(queryHandle2);
    queryStatus3 = qHelper.getQueryStatus(queryHandle3);

    Assert.assertEquals(queryStatus1.getStatus(), QueryStatus.Status.CANCELED, "Query is Not Running");
    Assert.assertEquals(queryStatus2.getStatus(), QueryStatus.Status.CANCELED, "Query is Not Running");
    Assert.assertEquals(queryStatus3.getStatus(), QueryStatus.Status.RUNNING, "Query is Not Running");

    qHelper.killQuery(null, null, diffUser);

    queryStatus3 = qHelper.getQueryStatus(queryHandle3);
    Assert.assertEquals(queryStatus3.getStatus(), QueryStatus.Status.CANCELED, "Query is Not Running");

  }

  @Test(enabled = true)
  public void killQueryOfAllUser() throws Exception {

    String diffUser = "diff";
    String diffPass = "diff";
    String newSessionHandleSring = sHelper.openSession(diffUser, diffPass, lens.getCurrentDB());
    sHelper.addResourcesJar(hdfsJarPath + "/" + hiveUdfJar, newSessionHandleSring);

    QueryHandle queryHandle1 = (QueryHandle) qHelper.executeQuery(sleepQuery).getData();
    QueryHandle queryHandle2 = (QueryHandle) qHelper.executeQuery(sleepQuery).getData();
    QueryHandle queryHandle3 = (QueryHandle) qHelper.executeQuery(sleepQuery, null,
        newSessionHandleSring).getData();

    QueryStatus queryStatus1 = qHelper.waitForQueryToRun(queryHandle1);
    QueryStatus queryStatus2 = qHelper.waitForQueryToRun(queryHandle2);
    QueryStatus queryStatus3 = qHelper.waitForQueryToRun(queryHandle3, newSessionHandleSring);

    Assert.assertEquals(queryStatus1.getStatus(), QueryStatus.Status.RUNNING, "Query is Not Running");
    Assert.assertEquals(queryStatus2.getStatus(), QueryStatus.Status.RUNNING, "Query is Not Running");
    Assert.assertEquals(queryStatus3.getStatus(), QueryStatus.Status.RUNNING, "Query is Not Running");

    qHelper.killQuery(null, null, "all");

    queryStatus1 = qHelper.getQueryStatus(queryHandle1);
    queryStatus2 = qHelper.getQueryStatus(queryHandle2);
    queryStatus3 = qHelper.getQueryStatus(queryHandle3);

    Assert.assertEquals(queryStatus1.getStatus(), QueryStatus.Status.CANCELED, "Query is Not Running");
    Assert.assertEquals(queryStatus2.getStatus(), QueryStatus.Status.CANCELED, "Query is Not Running");
    Assert.assertEquals(queryStatus3.getStatus(), QueryStatus.Status.CANCELED, "Query is Not Running");
  }


  @Test(enabled = true)
  public void killAllQueryOfUser() throws Exception {

    QueryHandle queryHandle1 = (QueryHandle) qHelper.executeQuery(sleepQuery).getData();
    QueryHandle queryHandle2 = (QueryHandle) qHelper.executeQuery(sleepQuery).getData();

    QueryStatus queryStatus1 = qHelper.waitForQueryToRun(queryHandle1);
    QueryStatus queryStatus2 = qHelper.waitForQueryToRun(queryHandle2);

    Assert.assertEquals(queryStatus1.getStatus(), QueryStatus.Status.RUNNING, "Query is Not Running");
    Assert.assertEquals(queryStatus2.getStatus(), QueryStatus.Status.RUNNING, "Query is Not Running");

    qHelper.killQuery();

    queryStatus1 = qHelper.getQueryStatus(queryHandle1);
    queryStatus2 = qHelper.getQueryStatus(queryHandle2);

    Assert.assertEquals(queryStatus1.getStatus(), QueryStatus.Status.CANCELED, "Query is Not Running");
    Assert.assertEquals(queryStatus2.getStatus(), QueryStatus.Status.CANCELED, "Query is Not Running");
  }


  @Test(enabled = true)
  public void killQueryByState() throws Exception {

    QueryHandle queryHandle1 = (QueryHandle) qHelper.executeQuery(sleepQuery).getData();
    QueryHandle queryHandle2 = (QueryHandle) qHelper.executeQuery(sleepQuery).getData();

    QueryStatus queryStatus1 = qHelper.waitForQueryToRun(queryHandle1);
    QueryStatus queryStatus2 = qHelper.waitForQueryToRun(queryHandle2);

    Assert.assertEquals(queryStatus1.getStatus(), QueryStatus.Status.RUNNING, "Query is Not Running");
    Assert.assertEquals(queryStatus2.getStatus(), QueryStatus.Status.RUNNING, "Query is Not Running");

    //kill Running queries
    qHelper.killQuery(null, "RUNNING");

    queryStatus1 = qHelper.getQueryStatus(queryHandle1);
    queryStatus2 = qHelper.getQueryStatus(queryHandle2);

    Assert.assertEquals(queryStatus1.getStatus(), QueryStatus.Status.CANCELED, "Query is Not Running");
    Assert.assertEquals(queryStatus2.getStatus(), QueryStatus.Status.CANCELED, "Query is Not Running");

    //kill Canceled query
    qHelper.killQuery(null, "CANCELED");

    queryStatus1 = qHelper.getQueryStatus(queryHandle1);
    queryStatus2 = qHelper.getQueryStatus(queryHandle2);

    Assert.assertEquals(queryStatus1.getStatus(), QueryStatus.Status.CANCELED, "Query is Not Running");
    Assert.assertEquals(queryStatus2.getStatus(), QueryStatus.Status.CANCELED, "Query is Not Running");

    //kill successful query
    QueryHandle queryHandle3 = (QueryHandle) qHelper.executeQuery(QueryInventory.QUERY).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle3);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
    qHelper.killQuery(null, "SUCCESSFUL");
    QueryStatus queryStatus3 = qHelper.getQueryStatus(queryHandle3);
    Assert.assertEquals(queryStatus3.getStatus(), QueryStatus.Status.SUCCESSFUL);
  }

  //TODO: enable when the bug is fixed.

   /* Currently doing kill query by queryName "query" will kill all the query with queryName as "*query*"
    * Raised a JIRA for same
    * When its Fixed Revisit this function */

  @Test(enabled = false)
  public void killQueryByQueryName() throws Exception {

    String queryName1 = "queryNameFirst";
    String queryName2 = "queryNameSecond";
    String queryName3 = "Name";

    QueryHandle queryHandle1 = (QueryHandle) qHelper.executeQuery(sleepQuery, queryName1).getData();
    logger.info("1st QUERY HANDLE : " + queryHandle1);

    QueryHandle queryHandle2 = (QueryHandle) qHelper.executeQuery(sleepQuery, queryName2).getData();
    logger.info("2nd QUERY HANDLE : " + queryHandle2);

    QueryHandle queryHandle3 = (QueryHandle) qHelper.executeQuery(sleepQuery, queryName3).getData();
    logger.info("3rd QUERY HANDLE : " + queryHandle3);

    QueryStatus queryStatus1 = qHelper.waitForQueryToRun(queryHandle1);
    QueryStatus queryStatus2 = qHelper.waitForQueryToRun(queryHandle2);
    QueryStatus queryStatus3 = qHelper.waitForQueryToRun(queryHandle3);

    Assert.assertEquals(queryStatus1.getStatus(), QueryStatus.Status.RUNNING, "Query is Not Running");
    Assert.assertEquals(queryStatus2.getStatus(), QueryStatus.Status.RUNNING, "Query is Not Running");
    Assert.assertEquals(queryStatus3.getStatus(), QueryStatus.Status.RUNNING, "Query is Not Running");

    qHelper.killQuery(queryName3);

    queryStatus1 = qHelper.getQueryStatus(queryHandle1);
    queryStatus2 = qHelper.getQueryStatus(queryHandle2);
    queryStatus3 = qHelper.getQueryStatus(queryHandle3);

    Assert.assertEquals(queryStatus1.getStatus(), QueryStatus.Status.RUNNING, "Query is Not Running");
    Assert.assertEquals(queryStatus2.getStatus(), QueryStatus.Status.RUNNING, "Query is Not Running");
    Assert.assertEquals(queryStatus3.getStatus(), QueryStatus.Status.CANCELED, "Query is Not Cancelled");

    qHelper.killQuery(queryName1);

    queryStatus1 = qHelper.getQueryStatus(queryHandle1);
    queryStatus2 = qHelper.getQueryStatus(queryHandle2);

    Assert.assertEquals(queryStatus1.getStatus(), QueryStatus.Status.CANCELED, "Query is Not CANCELED");
    Assert.assertEquals(queryStatus2.getStatus(), QueryStatus.Status.RUNNING, "Query is Not Running");

    qHelper.killQuery(queryName2);
    queryStatus2 = qHelper.getQueryStatus(queryHandle2);

    Assert.assertEquals(queryStatus2.getStatus(), QueryStatus.Status.CANCELED, "Query is Not CANCELED");
  }

  @Test(enabled = true)
  public void killQueryByTimeRange() throws Exception {

    String startTime1 = String.valueOf(System.currentTimeMillis());
    logger.info("Start Time of 1st Query : " + startTime1);
    Thread.sleep(1000);
    QueryHandle queryHandle1 = (QueryHandle) qHelper.executeQuery(sleepQuery).getData();
    Thread.sleep(1000);
    String endTime1 = String.valueOf(System.currentTimeMillis());
    logger.info("End Time of 1st Query : " + endTime1);

    Thread.sleep(1000);

    String startTime2 = String.valueOf(System.currentTimeMillis());
    logger.info("Start Time of 2nd Query : " + startTime2);
    Thread.sleep(1000);
    QueryHandle queryHandle2 = (QueryHandle) qHelper.executeQuery(sleepQuery).getData();
    Thread.sleep(1000);
    String endTime2 = String.valueOf(System.currentTimeMillis());
    logger.info("End Time of 2nd Query : " + endTime2);

    QueryStatus queryStatus1 = qHelper.waitForQueryToRun(queryHandle1);
    QueryStatus queryStatus2 = qHelper.waitForQueryToRun(queryHandle2);

    Assert.assertEquals(queryStatus1.getStatus(), QueryStatus.Status.RUNNING, "Query is Not Running");
    Assert.assertEquals(queryStatus2.getStatus(), QueryStatus.Status.RUNNING, "Query is Not Running");

    qHelper.killQuery(null, null, null, sessionHandleString, startTime1, endTime1);

    queryStatus1 = qHelper.getQueryStatus(queryHandle1);
    queryStatus2 = qHelper.getQueryStatus(queryHandle2);

    Assert.assertEquals(queryStatus1.getStatus(), QueryStatus.Status.CANCELED, "Query is Not CANCELED");
    Assert.assertEquals(queryStatus2.getStatus(), QueryStatus.Status.RUNNING, "Query is Not CANCELED");

    qHelper.killQuery(null, null, null, sessionHandleString, startTime2, endTime2);

    queryStatus2 = qHelper.getQueryStatus(queryHandle2);
    Assert.assertEquals(queryStatus2.getStatus(), QueryStatus.Status.CANCELED, "Query is Not CANCELED");

  }


  @Test(enabled = true)
  public void killQueryByAllFilter() throws Exception {

    String queryName1 = "TestKill";

    String startTime1 = String.valueOf(System.currentTimeMillis());
    logger.info("Start Time of 1st Query : " + startTime1);
    Thread.sleep(1000);

    QueryHandle queryHandle1 = (QueryHandle) qHelper.executeQuery(sleepQuery, queryName1).getData();
    Thread.sleep(1000);

    String endTime1 = String.valueOf(System.currentTimeMillis());
    logger.info("End Time of 1st Query : " + endTime1);

    QueryStatus queryStatus1 = qHelper.waitForQueryToRun(queryHandle1);
    Assert.assertEquals(queryStatus1.getStatus(), QueryStatus.Status.RUNNING, "Query is Not Running");

    qHelper.killQuery(queryName1, "RUNNING", lens.getUserName(), sessionHandleString, startTime1, endTime1);

    queryStatus1 = qHelper.getQueryStatus(queryHandle1);
    Assert.assertEquals(queryStatus1.getStatus(), QueryStatus.Status.CANCELED, "Query is Not CANCELED");
  }

}


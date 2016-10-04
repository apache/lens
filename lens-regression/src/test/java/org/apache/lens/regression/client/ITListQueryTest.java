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

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;

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


public class ITListQueryTest extends BaseTestClass {

  WebTarget servLens;
  private String sessionHandleString;

  String jdbcDriver = "jdbc/jdbc1", hiveDriver = "hive/hive1";
  String sleepQuery = QueryInventory.getSleepQuery("5");

  private static Logger logger = Logger.getLogger(ITListQueryTest.class);

  @BeforeClass(alwaysRun = true)
  public void initialize() throws IOException, JAXBException, LensException {
    servLens = ServiceManagerHelper.init();
    logger.info("Creating a new Session");
    sessionHandleString = sHelper.openSession(lens.getCurrentDB());
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
  }

  @AfterClass(alwaysRun = true)
  public void closeSession() throws Exception {
    logger.info("Closing Session");
    sHelper.closeSession();
  }

  @DataProvider(name = "query-provider")
  public Object[][] getData() throws Exception {

    String[] queries = new String[]{QueryInventory.HIVE_CUBE_QUERY, QueryInventory.HIVE_DIM_QUERY,
      QueryInventory.JDBC_DIM_QUERY, };
    Object[][] testData = new Object[3][1];

    int i = 0;
    for (String query : queries) {
      testData[i][0] = query;
      i++;
    }
    return testData;
  }


  @Test(enabled = true, dataProvider = "query-provider")
  public void getQueryByQueryNameAllQuery(String queryString) throws Exception {

    String queryName = "queryNameFirst";
    String queryName1 = "queryNameSecond";
    String queryName2 = "Name";

    //Running Query with queryName
    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(queryString, queryName).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");

    //Running Query with queryName1
    QueryHandle queryHandle1 = (QueryHandle) qHelper.executeQuery(queryString, queryName1).getData();
    lensQuery = qHelper.waitForCompletion(queryHandle1);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");

    //Running Query with queryName
    QueryHandle queryHandle2 = (QueryHandle) qHelper.executeQuery(queryString, queryName).getData();
    lensQuery = qHelper.waitForCompletion(queryHandle2);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    //Running Query with queryName2
    QueryHandle queryHandle3 = (QueryHandle) qHelper.executeQuery(queryString, queryName2).getData();
    lensQuery = qHelper.waitForCompletion(queryHandle3);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    //Getting queryList by queryName
    List<QueryHandle> list = qHelper.getQueryHandleList(queryName);
    Assert.assertTrue(list.contains(queryHandle), "List by Query Name failed");
    Assert.assertFalse(list.contains(queryHandle1), "List by Query Name failed");
    Assert.assertTrue(list.contains(queryHandle2), "List by Query Name failed");

    //Getting queryList by queryName1
    List<QueryHandle> list1 = qHelper.getQueryHandleList(queryName1);
    Assert.assertFalse(list1.contains(queryHandle), "List by Query Name failed");
    Assert.assertTrue(list1.contains(queryHandle1), "List by Query Name failed");
    Assert.assertFalse(list1.contains(queryHandle2), "List by Query Name failed");

    //Getting  queryList by queryName2
    List<QueryHandle> list2 = qHelper.getQueryHandleList(queryName2);
    Assert.assertTrue(list2.contains(queryHandle), "List by Query Name failed");
    Assert.assertTrue(list2.contains(queryHandle1), "List by Query Name failed");
    Assert.assertTrue(list2.contains(queryHandle2), "List by Query Name failed");
    Assert.assertTrue(list2.contains(queryHandle3), "List by Query Name failed");

    //Getting all queryList
    List<QueryHandle> list3 = qHelper.getQueryHandleList();
    Assert.assertTrue(list3.contains(queryHandle), "List by Query Name failed");
    Assert.assertTrue(list3.contains(queryHandle1), "List by Query Name failed");
    Assert.assertTrue(list3.contains(queryHandle2), "List by Query Name failed");
    Assert.assertTrue(list3.contains(queryHandle3), "List by Query Name failed");
  }

  @Test
  public void listQueryByTimeRange() throws Exception {

    //Running First Query
    String startTime = String.valueOf(System.currentTimeMillis());
    logger.info("Start Time of 1st Query : " + startTime);
    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.QUERY).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");
    String endTime = String.valueOf(System.currentTimeMillis());
    logger.info("End Time of 1st Query : " + endTime);

    Thread.sleep(1000);

    //Running Second Query
    String startTime1 = String.valueOf(System.currentTimeMillis());
    logger.info("Start Time of 2nd Query : " + startTime1);
    QueryHandle queryHandle1 = (QueryHandle) qHelper.executeQuery(QueryInventory.QUERY).getData();
    lensQuery = qHelper.waitForCompletion(queryHandle1);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");
    String endTime1 = String.valueOf(System.currentTimeMillis());
    logger.info("End Time of 2nd Query : " + endTime1);

    List<QueryHandle> list = qHelper.getQueryHandleList(null, null, null, sessionHandleString, startTime, endTime);
    Assert.assertTrue(list.contains(queryHandle), "QueryList by TimeRange is not correct");
    Assert.assertFalse(list.contains(queryHandle1), "QueryList by TimeRange is not correct");

    List<QueryHandle> list1 = qHelper.getQueryHandleList(null, null, null, sessionHandleString, startTime1, endTime1);
    Assert.assertFalse(list1.contains(queryHandle), "QueryList by TimeRange is not correct");
    Assert.assertTrue(list1.contains(queryHandle1), "QueryList by TimeRange is not correct");

    List<QueryHandle> list2 = qHelper.getQueryHandleList(null, null, null, sessionHandleString, startTime, endTime1);
    Assert.assertTrue(list2.contains(queryHandle), "QueryList by TimeRange is not correct");
    Assert.assertTrue(list2.contains(queryHandle1), "QueryList by TimeRange is not correct");

    List<QueryHandle> list3 = qHelper.getQueryHandleList();
    Assert.assertTrue(list3.contains(queryHandle), "QueryList by TimeRange is not correct");
    Assert.assertTrue(list3.contains(queryHandle1), "QueryList by TimeRange is not correct");
  }


  @Test
  public void listQuerySpecificUserAllUser() throws Exception {
    String diffUser = "diff";
    String diffPass = "diff";
    String diffSessionHandleString = sHelper.openSession(diffUser, diffPass, lens.getCurrentDB());

    //Running Query with user1
    QueryHandle queryHandle1 = (QueryHandle) qHelper.executeQuery(QueryInventory.QUERY).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle1);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");

    //Running Query with user2
    QueryHandle queryHandle2 = (QueryHandle) qHelper.executeQuery(QueryInventory.QUERY, null,
        diffSessionHandleString).getData();
    lensQuery = qHelper.waitForCompletion(diffSessionHandleString, queryHandle2);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL,
        "Query should complete successfully");

    //Getting queryList by user1
    List<QueryHandle> list = qHelper.getQueryHandleList(null, null, lens.getUserName());
    Assert.assertTrue(list.contains(queryHandle1), "List by user failed");
    Assert.assertFalse(list.contains(queryHandle2), "List by user failed");

    //Getting queryList by user2
    list = qHelper.getQueryHandleList(null, null, diffUser);
    Assert.assertFalse(list.contains(queryHandle1), "List by user failed");
    Assert.assertTrue(list.contains(queryHandle2), "List by user failed");

    //Getting queryList by user = all
    list = qHelper.getQueryHandleList(null, null, "all");
    Assert.assertTrue(list.contains(queryHandle1), "List by user failed");
    Assert.assertTrue(list.contains(queryHandle2), "List by user failed");
  }


  @Test
  public void listQueryByTimeRangeQueryName() throws Exception {

    String queryName = "testQueryName";

    String startTime = String.valueOf(System.currentTimeMillis());
    logger.info("Start Time of 1st Query : " + startTime);

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.QUERY, queryName).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");

    String endTime = String.valueOf(System.currentTimeMillis());
    logger.info("End Time of 1st Query : " + endTime);

    List<QueryHandle> list = qHelper.getQueryHandleList(queryName, null, null, sessionHandleString, startTime, endTime);
    Assert.assertTrue(list.contains(queryHandle), "QueryList by TimeRange is not correct");

    List<QueryHandle> list1 = qHelper.getQueryHandleList();
    Assert.assertTrue(list1.contains(queryHandle), "QueryList by TimeRange is not correct");

  }

  @Test
  public void listQueryByState() throws Exception {
    //Successful Query
    QueryHandle queryHandle1 = (QueryHandle) qHelper.executeQuery(QueryInventory.QUERY).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle1);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");

    //Running Query
    QueryHandle queryHandle3 = (QueryHandle) qHelper.executeQuery(sleepQuery).getData();
    qHelper.waitForQueryToRun(queryHandle3);

    //Cancelled Query
    QueryHandle queryHandle4 = (QueryHandle) qHelper.executeQuery(sleepQuery).getData();
    qHelper.waitForQueryToRun(queryHandle4);
    qHelper.killQueryByQueryHandle(queryHandle4);

    //TODO : Add Failed queries

    //List Successful queries
    List<QueryHandle> list = qHelper.getQueryHandleList(null, "SUCCESSFUL");
    Assert.assertTrue(list.contains(queryHandle1), "QueryList by State is not correct");
    Assert.assertFalse(list.contains(queryHandle3), "QueryList by State is not correct");
    Assert.assertFalse(list.contains(queryHandle4), "QueryList by State is not correct");

    //List Failed queries
    list = qHelper.getQueryHandleList(null, "FAILED");
    Assert.assertFalse(list.contains(queryHandle1), "QueryList by State is not correct");
    Assert.assertFalse(list.contains(queryHandle3), "QueryList by State is not correct");
    Assert.assertFalse(list.contains(queryHandle4), "QueryList by State is not correct");

    //List Running queries
    list = qHelper.getQueryHandleList(null, "RUNNING");
    Assert.assertFalse(list.contains(queryHandle1), "QueryList by State is not correct");
    Assert.assertTrue(list.contains(queryHandle3), "QueryList by State is not correct");
    Assert.assertFalse(list.contains(queryHandle4), "QueryList by State is not correct");

    //List Cancelled queries
    list = qHelper.getQueryHandleList(null, "CANCELED");
    Assert.assertFalse(list.contains(queryHandle1), "QueryList by State is not correct");
    Assert.assertFalse(list.contains(queryHandle3), "QueryList by State is not correct");
    Assert.assertTrue(list.contains(queryHandle4), "QueryList by State is not correct");

    //List All queries
    list = qHelper.getQueryHandleList();
    Assert.assertTrue(list.contains(queryHandle1), "QueryList by State is not correct");
    Assert.assertTrue(list.contains(queryHandle3), "QueryList by State is not correct");
    Assert.assertTrue(list.contains(queryHandle4), "QueryList by State is not correct");

  }


  @Test
  public void listQueryByDriver() throws Exception {

    QueryHandle queryHandle1 = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_CUBE_QUERY).getData();
    qHelper.waitForQueryToRun(queryHandle1);

    QueryHandle queryHandle2 = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_DIM_QUERY).getData();
    qHelper.waitForQueryToRun(queryHandle2);

    QueryHandle queryHandle3 = (QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_CUBE_QUERY).getData();
    qHelper.waitForQueryToRun(queryHandle3);

    QueryHandle queryHandle4 = (QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_DIM_QUERY).getData();
    qHelper.waitForQueryToRun(queryHandle4);

    //List jdbc queries
    List<QueryHandle> list = qHelper.getQueryHandleList(null, null, null, sessionHandleString, null, null, jdbcDriver);
    Assert.assertTrue(list.contains(queryHandle1), "QueryList by driver is not correct");
    Assert.assertTrue(list.contains(queryHandle2), "QueryList by driver is not correct");
    Assert.assertFalse(list.contains(queryHandle3), "QueryList by driver is not correct");
    Assert.assertFalse(list.contains(queryHandle4), "QueryList by driver is not correct");

    //List hive queries
    list = qHelper.getQueryHandleList(null, null, null, sessionHandleString, null, null, hiveDriver);
    Assert.assertFalse(list.contains(queryHandle1), "QueryList by driver is not correct");
    Assert.assertFalse(list.contains(queryHandle2), "QueryList by driver is not correct");
    Assert.assertTrue(list.contains(queryHandle3), "QueryList by driver is not correct");
    Assert.assertTrue(list.contains(queryHandle4), "QueryList by driver is not correct");

  }


  @Test
  public void listFinishedQueryByDriver() throws Exception {

    QueryHandle queryHandle1 = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_CUBE_QUERY).getData();
    qHelper.waitForCompletion(queryHandle1);

    QueryHandle queryHandle2 = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_DIM_QUERY).getData();
    qHelper.waitForCompletion(queryHandle2);

    QueryHandle queryHandle3 = (QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_CUBE_QUERY).getData();
    qHelper.waitForCompletion(queryHandle3);

    QueryHandle queryHandle4 = (QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_DIM_QUERY).getData();
    qHelper.waitForCompletion(queryHandle4);

    // TODO : Add purger configuration time

    // sleep for query purger time
    Thread.sleep(30000);

    // This is make sure query is retrived from DB.
    lens.restart();

    //List jdbc queries
    List<QueryHandle> list = qHelper.getQueryHandleList(null, null, null, sessionHandleString, null, null, jdbcDriver);
    Assert.assertTrue(list.contains(queryHandle1), "QueryList by driver is not correct");
    Assert.assertTrue(list.contains(queryHandle2), "QueryList by driver is not correct");
    Assert.assertFalse(list.contains(queryHandle3), "QueryList by driver is not correct");
    Assert.assertFalse(list.contains(queryHandle4), "QueryList by driver is not correct");

    //List hive queries
    list = qHelper.getQueryHandleList(null, null, null, sessionHandleString, null, null, hiveDriver);
    Assert.assertFalse(list.contains(queryHandle1), "QueryList by driver is not correct");
    Assert.assertFalse(list.contains(queryHandle2), "QueryList by driver is not correct");
    Assert.assertTrue(list.contains(queryHandle3), "QueryList by driver is not correct");
    Assert.assertTrue(list.contains(queryHandle4), "QueryList by driver is not correct");
  }


  @Test
  public void listQueryByDriverNUser() throws Exception {

    QueryHandle q1 = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_CUBE_QUERY).getData();
    QueryHandle q2 = (QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_DIM_QUERY).getData();

    //Running Query with diff user
    String diffSession = sHelper.openSession("diff", "diff", lens.getCurrentDB());
    QueryHandle q3 = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_CUBE_QUERY, null, diffSession).getData();
    QueryHandle q4 = (QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_CUBE_QUERY, null, diffSession).getData();

    List<QueryHandle> list = qHelper.getQueryHandleList(null, null, "all", sessionHandleString, null, null, jdbcDriver);
    Assert.assertTrue(list.contains(q1), "QueryList by driver is not correct");
    Assert.assertTrue(list.contains(q3), "QueryList by driver is not correct");

    list = qHelper.getQueryHandleList(null, null, "all", sessionHandleString, null, null, hiveDriver);
    Assert.assertTrue(list.contains(q2), "QueryList by driver is not correct");
    Assert.assertTrue(list.contains(q4), "QueryList by driver is not correct");

    //Check for driver along with user filter
    list = qHelper.getQueryHandleList(null, null, lens.getUserName(), sessionHandleString, null, null, jdbcDriver);
    Assert.assertTrue(list.contains(q1), "QueryList by driver is not correct");
    Assert.assertFalse(list.contains(q3), "QueryList by driver is not correct");

    list = qHelper.getQueryHandleList(null, null, "diff", sessionHandleString, null, null, hiveDriver);
    Assert.assertTrue(list.contains(q4), "QueryList by driver is not correct");
    Assert.assertFalse(list.contains(q2), "QueryList by driver is not correct");
  }


  @Test
  public void listQueryByDriverNStatus() throws Exception {

    String user = "new", pwd = "new";
    String newSession = sHelper.openSession(user, pwd, mHelper.getCurrentDB());

    //Cancelled hive Query
    QueryHandle q1 = (QueryHandle) qHelper.executeQuery(sleepQuery, null, newSession).getData();
    qHelper.waitForQueryToRun(q1, newSession);
    qHelper.killQueryByQueryHandle(q1);

    //Successful hive Query
    QueryHandle q2 = (QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_CUBE_QUERY, null, newSession).getData();
    qHelper.waitForCompletion(newSession, q2);

    //Successful jdbc Query
    QueryHandle q3 = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_CUBE_QUERY, null, newSession).getData();
    qHelper.waitForCompletion(newSession, q3);

    //Running hive Query
    QueryHandle q4 = (QueryHandle) qHelper.executeQuery(sleepQuery, null, newSession).getData();
    qHelper.waitForQueryToRun(q4, newSession);

    List<QueryHandle> cancelledHive = qHelper.getQueryHandleList(null, "CANCELED", user, sessionHandleString, null,
        null, hiveDriver);
    List<QueryHandle> successJdbc = qHelper.getQueryHandleList(null, "SUCCESSFUL", user, sessionHandleString, null,
        null, jdbcDriver);
    List<QueryHandle> successhive = qHelper.getQueryHandleList(null, "SUCCESSFUL", user, sessionHandleString, null,
        null, hiveDriver);

    Assert.assertTrue(cancelledHive.contains(q1), "QueryList by driver is not correct");
    Assert.assertFalse(cancelledHive.contains(q2), "QueryList by driver is not correct");
    Assert.assertFalse(cancelledHive.contains(q3), "QueryList by driver is not correct");
    Assert.assertFalse(cancelledHive.contains(q4), "QueryList by driver is not correct");

    Assert.assertTrue(successhive.contains(q2), "QueryList by driver is not correct");
    Assert.assertFalse(successhive.contains(q1), "QueryList by driver is not correct");
    Assert.assertFalse(successhive.contains(q3), "QueryList by driver is not correct");
    Assert.assertFalse(successhive.contains(q4), "QueryList by driver is not correct");

    Assert.assertTrue(successJdbc.contains(q3), "QueryList by driver is not correct");
    Assert.assertFalse(successJdbc.contains(q1), "QueryList by driver is not correct");
    Assert.assertFalse(successJdbc.contains(q2), "QueryList by driver is not correct");
    Assert.assertFalse(successJdbc.contains(q4), "QueryList by driver is not correct");
  }



  @Test
  public void listQueryByNameStatusUserTimeRange() throws Exception {

    String queryName1 = "first", queryName2 = "second", queryName3 = "third";
    String user1 = "diff", pwd1 = "diff";
    String user2 = "diff1", pwd2 = "diff1";

    String diffSession1 = sHelper.openSession(user1, pwd1, lens.getCurrentDB());
    String diffSession2 = sHelper.openSession(user2, pwd2, lens.getCurrentDB());

    String startTime1 = String.valueOf(System.currentTimeMillis());
    QueryHandle q1 = (QueryHandle) qHelper.executeQuery(QueryInventory.QUERY, queryName1).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(q1);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");
    String endTime1 = String.valueOf(System.currentTimeMillis());

    Thread.sleep(1000);

    String startTime2 = String.valueOf(System.currentTimeMillis());
    Thread.sleep(2000);
    QueryHandle q2 = (QueryHandle) qHelper.executeQuery(sleepQuery, queryName2, diffSession1).getData();
    Thread.sleep(2000);
    String endTime2 = String.valueOf(System.currentTimeMillis());

    Thread.sleep(1000);

    String startTime3 = String.valueOf(System.currentTimeMillis());
    Thread.sleep(2000);
    QueryHandle q3 = (QueryHandle) qHelper.executeQuery(sleepQuery, queryName3, diffSession2).getData();
    qHelper.killQueryByQueryHandle(q3);
    String endTime3 = String.valueOf(System.currentTimeMillis());

    Thread.sleep(1000);

    List<QueryHandle> list1 = qHelper.getQueryHandleList(queryName2, "RUNNING", user1, sessionHandleString, startTime2,
        endTime2);
    List<QueryHandle> list2 = qHelper.getQueryHandleList(queryName3, "CANCELED", user2, sessionHandleString, startTime3,
        endTime3);
    List<QueryHandle> list3 = qHelper.getQueryHandleList(queryName1, "SUCCESSFUL", lens.getUserName(),
        sessionHandleString, startTime1, endTime1);


    Assert.assertTrue(list1.contains(q2), "List by Query Name, Status,Time Range,User Failed");
    Assert.assertFalse(list1.contains(q1), "List by Query Name, Status,Time Range,User Failed");
    Assert.assertFalse(list1.contains(q3), "List by Query Name, Status,Time Range,User Failed");

    Assert.assertTrue(list2.contains(q3), "List by Query Name, Status,Time Range,User Failed");
    Assert.assertFalse(list2.contains(q1), "List by Query Name, Status,Time Range,User Failed");
    Assert.assertFalse(list2.contains(q2), "List by Query Name, Status,Time Range,User Failed");

    Assert.assertTrue(list3.contains(q1), "List by Query Name, Status,Time Range,User Failed");
    Assert.assertFalse(list3.contains(q2), "List by Query Name, Status,Time Range,User Failed");
    Assert.assertFalse(list3.contains(q3), "List by Query Name, Status,Time Range,User Failed");

  }
}

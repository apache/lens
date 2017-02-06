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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.ws.rs.client.WebTarget;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.query.*;
import org.apache.lens.cube.parse.CubeQueryConfUtil;
import org.apache.lens.regression.core.constants.DriverConfig;
import org.apache.lens.regression.core.constants.QueryInventory;
import org.apache.lens.regression.core.helpers.ServiceManagerHelper;
import org.apache.lens.regression.core.testHelper.BaseTestClass;
import org.apache.lens.regression.util.Util;
import org.apache.lens.server.api.util.LensUtil;

import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.*;

public class ITDuplicateQueryTests extends BaseTestClass {

  WebTarget servLens;
  private String sessionHandleString;

  private String hiveDriverSitePath  = lens.getServerDir() + "/conf/drivers/hive/hive1/hivedriver-site.xml";
  private static Logger logger = Logger.getLogger(ITDuplicateQueryTests.class);

  @BeforeClass(alwaysRun = true)
  public void initialize() throws Exception {
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
    sHelper.closeSession();
  }

  /* LENS-1019 : If query is repeated from user - with same query, same name, same conf on the same session
     and earlier is still queued or running, then return the same handle.
  */

  @DataProvider(name = "query_names")
  public Object[][] queryName() {
    String[][] testData = {{"query-name"}, {null}};
    return testData;
  }

  @Test(dataProvider = "query_names", enabled = true)
  public void testRunningSameNameSessionQuery(String queryName) throws Exception {

    String query = QueryInventory.getSleepQuery("10");
    List<QueryHandle> handleList = new ArrayList<>();
    List<PersistentQueryResult> resultList = new ArrayList<>();

    for(int i=0; i<3; i++){
      handleList.add((QueryHandle) qHelper.executeQuery(query, queryName).getData());
    }

    Assert.assertEquals(handleList.get(1).getHandleIdString(), handleList.get(0).getHandleIdString());
    Assert.assertEquals(handleList.get(2).getHandleIdString(), handleList.get(0).getHandleIdString());

    for(QueryHandle handle : handleList){
      LensQuery lq = qHelper.waitForCompletion(handle);
      Assert.assertEquals(lq.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
      resultList.add((PersistentQueryResult) qHelper.getResultSet(handle));
    }

    Assert.assertEquals(resultList.get(1).getPersistedURI(), resultList.get(0).getPersistedURI());
    Assert.assertEquals(resultList.get(2).getPersistedURI(), resultList.get(0).getPersistedURI());
  }

  @Test(enabled = true)
  public void testQueuedSameNameSessionQuery() throws Exception {

    String query = QueryInventory.getSleepQuery("10");
    List<QueryHandle> handleList = new ArrayList<>();
    List<PersistentQueryResult> resultList = new ArrayList<>();
    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES, "1");

    try {
      Util.changeConfig(map, hiveDriverSitePath);
      lens.restart();

      //Fire long running query so that 2nd  query is in queued state
      qHelper.executeQuery(query, "query1").getData();

      for (int i = 0; i < 3; i++) {
        handleList.add((QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_CUBE_QUERY, "query1").getData());
      }

      Assert.assertEquals(handleList.get(1), handleList.get(0));
      Assert.assertEquals(handleList.get(2), handleList.get(0));

      for (QueryHandle handle : handleList) {
        LensQuery lq = qHelper.waitForCompletion(handle);
        Assert.assertEquals(lq.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
        resultList.add((PersistentQueryResult) qHelper.getResultSet(handle));
      }

      Assert.assertEquals(resultList.get(1).getPersistedURI(), resultList.get(0).getPersistedURI());
      Assert.assertEquals(resultList.get(2).getPersistedURI(), resultList.get(0).getPersistedURI());

    } finally {
      Util.changeConfig(hiveDriverSitePath);
      lens.restart();
    }
  }

  @Test(enabled = false)
  public void differentQuerySameNameSession() throws Exception {

    String cost5 = String.format(QueryInventory.getQueryFromInventory("HIVE.SLEEP_COST_5"), "5");
    String cost3 = String.format(QueryInventory.getQueryFromInventory("HIVE.SLEEP_COST_3"), "3");

    QueryHandle handle1 = (QueryHandle) qHelper.executeQuery(cost5, "queryName").getData();
    QueryHandle handle2 = (QueryHandle) qHelper.executeQuery(cost3, "queryName").getData();

    Assert.assertFalse(handle1.getHandleIdString().equals(handle2.getHandleIdString()));
  }

  @Test(enabled = false)
  public void differentSessionSameNameQuery() throws Exception {

    String query = QueryInventory.getSleepQuery("10");
    String session1 = sHelper.openSession("user1", "pwd1", lens.getCurrentDB());
    String session2 = sHelper.openSession("user2", "pwd2", lens.getCurrentDB());
    QueryHandle handle1 = (QueryHandle) qHelper.executeQuery(query, "name", session1).getData();
    QueryHandle handle2 = (QueryHandle) qHelper.executeQuery(query, "name", session2).getData();
    Assert.assertFalse(handle1.getHandleIdString().equals(handle2.getHandleIdString()));
  }

  @Test(enabled = false)
  public void differentNameSameSessionQuery() throws Exception {
    String query = QueryInventory.getSleepQuery("3");
    QueryHandle handle1 = (QueryHandle) qHelper.executeQuery(query, "name1").getData();
    QueryHandle handle2 = (QueryHandle) qHelper.executeQuery(query, "name2").getData();
    Assert.assertFalse(handle1.getHandleIdString().equals(handle2.getHandleIdString()));
  }

  @Test(enabled = false)
  public void differentConfSameNameSessionQuery() throws Exception {

    String query = QueryInventory.getSleepQuery("5");
    LensConf lensConf = new LensConf();

    lensConf.addProperty(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "false");
    QueryHandle qhr1 = (QueryHandle) qHelper.executeQuery(query, "query-name", null, sessionHandleString,
        lensConf).getData();

    lensConf.addProperty(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "true");
    QueryHandle qhr2 = (QueryHandle) qHelper.executeQuery(query, "query-name", null, sessionHandleString,
         lensConf).getData();

    Assert.assertFalse(qhr1.getHandleIdString().equals(qhr2.getHandleIdString()));
  }
}

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
import javax.ws.rs.core.Response;

import javax.xml.bind.JAXBException;

import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.regression.core.constants.DriverConfig;
import org.apache.lens.regression.core.constants.QueryInventory;
import org.apache.lens.regression.core.constants.SessionURL;
import org.apache.lens.regression.core.helpers.*;
import org.apache.lens.regression.core.testHelper.BaseTestClass;
import org.apache.lens.regression.core.type.MapBuilder;
import org.apache.lens.regression.util.Util;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.util.LensUtil;

import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.*;

import com.jcraft.jsch.JSchException;


public class SessionTests extends BaseTestClass {

  private WebTarget servLens;
  private String sessionHandleString;

  private static Logger logger = Logger.getLogger(SessionTests.class);

  @BeforeClass(alwaysRun = true)
  public void initialize() throws IOException, JSchException, JAXBException, LensException {
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


  @Test(enabled = true)
  public void testServerConfExposureInSession()  throws Exception {

    // conf : lens-site.xml
    MapBuilder query1 = new MapBuilder("sessionid", sessionHandleString, "key", LensConfConstants.SERVER_DB_JDBC_PASS);
    Response response1 = lens.sendQuery("get", SessionURL.SESSION_PARAMS_URL, query1);
    Assert.assertEquals(response1.getStatus(), Response.Status.NOT_FOUND.getStatusCode());

    //Driver conf : jdbc-driver.xml
    MapBuilder query2 = new MapBuilder("sessionid", sessionHandleString, "key", "lens.driver.jdbc.db.user");
    Response response2 = lens.sendQuery("get", SessionURL.SESSION_PARAMS_URL, query2);
    Assert.assertEquals(response2.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
  }


  // LENS-760. Check for only running as queued is not fixed.
  @Test(enabled = true)
  public void testRunningQueryContinuationOnSessionClose()  throws Exception {

    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES, "10");
    String hiveDriverConf = lens.getServerDir() + "/conf/drivers/hive/hive1/hivedriver-site.xml";

    try {
      Util.changeConfig(map, hiveDriverConf);
      lens.restart();

      String session = sHelper.openSession("test", "test", lens.getCurrentDB());
      List<QueryHandle> handleList = new ArrayList<QueryHandle>();
      String sleepQuery = QueryInventory.getSleepQuery("5");

      for(int i=1; i<=5; i++){
        handleList.add((QueryHandle) qHelper.executeQuery(sleepQuery, null, session).getData());
      }
      qHelper.waitForQueryToRun(handleList.get(3));

      List<QueryHandle> running = qHelper.getQueryHandleList(null, "RUNNING", "all", sessionHandleString);
      sHelper.closeSession(session);
      Assert.assertTrue(running.size() > 0);
      logger.info("Running query count " + running.size());

      for(QueryHandle handle : running){
        LensQuery lq = qHelper.waitForCompletion(handle);
        Assert.assertEquals(lq.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
      }
    } finally {
      Util.changeConfig(hiveDriverConf);
      lens.restart();
    }
  }

  // Fails. Bug : LENS-904
  // Check for query continuation on session close.
  @Test(enabled = true)
  public void testQueryContinuationOnSessionClose()  throws Exception {

    HashMap<String, String> map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES, "1");
    String hiveDriverConf = lens.getServerDir() + "/conf/drivers/hive/hive1/hivedriver-site.xml";

    try {
      Util.changeConfig(map, hiveDriverConf);
      lens.restart();

      String session = sHelper.openSession("test", "test", lens.getCurrentDB());
      List<QueryHandle> handleList = new ArrayList<QueryHandle>();
      String sleepQuery = QueryInventory.getSleepQuery("3");

      for (int i = 1; i <= 5; i++) {
        handleList.add((QueryHandle) qHelper.executeQuery(sleepQuery, null, session).getData());
      }

      sHelper.closeSession(session);

      for (QueryHandle handle : handleList) {
        LensQuery lq = qHelper.waitForCompletion(handle);
        Assert.assertEquals(lq.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
      }

    } finally {
      Util.changeConfig(hiveDriverConf);
      lens.restart();
    }
  }
}

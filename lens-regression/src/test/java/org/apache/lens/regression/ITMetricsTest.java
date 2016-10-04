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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.HashMap;

import javax.ws.rs.client.WebTarget;

import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.regression.core.constants.QueryInventory;
import org.apache.lens.regression.core.helpers.ServiceManagerHelper;
import org.apache.lens.regression.core.testHelper.BaseTestClass;
import org.apache.lens.server.LensRequestListener;
import org.apache.lens.server.api.query.QueryExecutionService;

import org.apache.log4j.Logger;

import org.json.JSONObject;

import org.testng.Assert;
import org.testng.annotations.*;


public class ITMetricsTest extends BaseTestClass {

  WebTarget servLens;
  String sessionHandleString;

  private static String admin = "/metrics?pretty=true";
  private static Logger logger = Logger.getLogger(ITMetricsTest.class);

  static final String HTTP_REQUESTS_FINISHED = LensRequestListener.HTTP_REQUESTS_FINISHED;
  static final String TOTAL_ACCEPTED_QUERIES = "total-accepted-queries";
  static final String TOTAL_CANCELLED_QUERIES = "total-cancelled-queries";
  static final String TOTAL_FAILED_QUERIES = "total-failed-queries";
  static final String TOTAL_SUCCESS_QUERIES = "total-success-queries";
  static final String TOTAL_FINISHED_QUERIES = "total-finished-queries";

  @BeforeClass(alwaysRun = true)
  public void initialize() throws IOException{
    servLens = ServiceManagerHelper.init();
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp(Method method) throws Exception {
    logger.info("Test Name: " + method.getName());
    logger.info("Creating a new Session for " + method.getName());
    sessionHandleString = sHelper.openSession(lens.getCurrentDB());
  }

  @AfterMethod(alwaysRun = true)
  public void closeSession(Method method) throws Exception {
    logger.info("Closing Session for " + method.getName());
    sHelper.closeSession();
  }


  private HashMap<String, Integer> getMetricsSnapshot() throws  Exception {

    URL lensAdmin = new URL(lens.getAdminUrl() + admin);
    HashMap<String, Integer> metricsMap = new HashMap<String, Integer>();

    BufferedReader br = new BufferedReader(new InputStreamReader(lensAdmin.openStream()));
    StringBuilder sb = new StringBuilder();
    String line;

    try {
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
    } catch (IOException e1) {
      logger.info("Exception " + e1.getMessage());
    }

    JSONObject object = new JSONObject(sb.toString());

    metricsMap.put(HTTP_REQUESTS_FINISHED, object.getJSONObject("counters").getJSONObject(
        LensRequestListener.class.getName() + "." + HTTP_REQUESTS_FINISHED).getInt("count"));

    metricsMap.put(TOTAL_ACCEPTED_QUERIES, object.getJSONObject("counters").getJSONObject(
        QueryExecutionService.class.getName() + "." + TOTAL_ACCEPTED_QUERIES).getInt("count"));

    metricsMap.put(TOTAL_CANCELLED_QUERIES, object.getJSONObject("counters").getJSONObject(
        QueryExecutionService.class.getName()+ "." + TOTAL_CANCELLED_QUERIES).getInt("count"));

    metricsMap.put(TOTAL_FAILED_QUERIES, object.getJSONObject("counters").getJSONObject(
        QueryExecutionService.class.getName()+ "." + TOTAL_FAILED_QUERIES).getInt("count"));

    metricsMap.put(TOTAL_FINISHED_QUERIES, object.getJSONObject("counters").getJSONObject(
        QueryExecutionService.class.getName()+ "." + TOTAL_FINISHED_QUERIES).getInt("count"));

    metricsMap.put(TOTAL_SUCCESS_QUERIES, object.getJSONObject("counters").getJSONObject(
        QueryExecutionService.class.getName()+ "." + TOTAL_SUCCESS_QUERIES).getInt("count"));

    return metricsMap;
  }

  @Test(enabled = true)
  public void testMetrics() throws  Exception {

    HashMap<String, Integer> oldMap = getMetricsSnapshot();
    HashMap<String, Integer> newMap = null;

    String sessionHandleString = sHelper.openSession("diff", "diff");
    newMap = getMetricsSnapshot();
    Assert.assertEquals(newMap.get(HTTP_REQUESTS_FINISHED) - oldMap.get(HTTP_REQUESTS_FINISHED), 1);

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.QUERY).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.getSleepQuery("5")).getData();
    QueryStatus queryStatus = qHelper.waitForQueryToRun(queryHandle);
    qHelper.killQueryByQueryHandle(queryHandle);
    queryStatus = qHelper.getQueryStatus(queryHandle);
    Assert.assertEquals(queryStatus.getStatus(), QueryStatus.Status.CANCELED, "Query is Not Running");

    QueryHandle qh = (QueryHandle) qHelper.executeQuery(QueryInventory.NO_PARTITION_HIVE_CUBE_QUERY).getData();

    newMap = getMetricsSnapshot();
    logger.info("Old map " + oldMap + "\n New map " + newMap);

    Assert.assertEquals(newMap.get(TOTAL_ACCEPTED_QUERIES) - oldMap.get(TOTAL_ACCEPTED_QUERIES), 2);
    Assert.assertEquals(newMap.get(TOTAL_CANCELLED_QUERIES) - oldMap.get(TOTAL_CANCELLED_QUERIES), 1);
    Assert.assertEquals(newMap.get(TOTAL_FINISHED_QUERIES) - oldMap.get(TOTAL_FINISHED_QUERIES), 2);
    Assert.assertEquals(newMap.get(TOTAL_SUCCESS_QUERIES) - oldMap.get(TOTAL_SUCCESS_QUERIES), 1);
    Assert.assertEquals(newMap.get(TOTAL_FAILED_QUERIES) - oldMap.get(TOTAL_FAILED_QUERIES), 0);
  }

}

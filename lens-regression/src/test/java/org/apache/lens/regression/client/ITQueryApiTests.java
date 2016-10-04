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
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.query.*;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.cube.parse.CubeQueryConfUtil;
import org.apache.lens.regression.core.constants.DriverConfig;
import org.apache.lens.regression.core.constants.QueryInventory;
import org.apache.lens.regression.core.constants.QueryURL;
import org.apache.lens.regression.core.helpers.ServiceManagerHelper;
import org.apache.lens.regression.core.testHelper.BaseTestClass;
import org.apache.lens.regression.core.type.FormBuilder;
import org.apache.lens.regression.core.type.MapBuilder;
import org.apache.lens.regression.util.AssertUtil;
import org.apache.lens.regression.util.Util;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.util.LensUtil;

import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.*;


public class ITQueryApiTests extends BaseTestClass {

  WebTarget servLens;
  private String sessionHandleString;

  private String hiveDriverSitePath  = lens.getServerDir() + "/conf/drivers/hive/hive1/hivedriver-site.xml";
  private static Logger logger = Logger.getLogger(ITQueryApiTests.class);

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

  @DataProvider(name = "persistance_values")
  public Object[][] data() {
    String[][] testData = {{"true", "true"}, {"true", "false"}, {"false", "false"}};
    return testData;
  }


  @Test
  public void testQueryServiceStatus() throws Exception {
    Response response = lens.exec("get", QueryURL.QUERYAPI_BASE_URL, servLens, null, null, MediaType.TEXT_PLAIN_TYPE,
        MediaType.TEXT_PLAIN);
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
  }


  @Test
  public void explainQuery() throws Exception {

    //sample_dim2 is on hive only. It should return queryPlan
    QueryPlan queryPlan = (QueryPlan) qHelper.explainQuery(QueryInventory.HIVE_DIM_QUERY).getData();
    Assert.assertNotNull(queryPlan.getPlanString());
    Assert.assertFalse(queryPlan.getPlanString().isEmpty());

    // sample_dim is present in hive and db. Lens should choose the db one
    queryPlan = (QueryPlan) qHelper.explainQuery(QueryInventory.DIM_QUERY).getData();
    Assert.assertNotNull(queryPlan.getPlanString());
    Assert.assertTrue(queryPlan.getPlanString().isEmpty());

    // sample_db_dim is present in db
    queryPlan = (QueryPlan) qHelper.explainQuery(QueryInventory.JDBC_DIM_QUERY).getData();
    Assert.assertNotNull(queryPlan.getPlanString());
    Assert.assertTrue(queryPlan.getPlanString().isEmpty());
  }


  @Test
  public void testExecute() throws Exception {
    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_CUBE_QUERY).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
  }


  @Test
  public void testExecuteWrongQuery() throws Exception {
    QueryHandle qH = (QueryHandle) qHelper.executeQuery(QueryInventory.WRONG_QUERY, "queryName").getData();
    Assert.assertNull(qH);
  }

  /* Bug : LENS-1005
     Fails bcos json output cannot be input for any API.
     Here query handle is in json which cannot be passed to get query status
  */

  @Test(enabled = false)
  public void testExecuteJson() throws Exception {
    QueryHandle handle = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_CUBE_QUERY, null,
        sessionHandleString, null, MediaType.APPLICATION_JSON).getData();
    Assert.assertNotNull(handle);
    LensQuery lensQuery = qHelper.waitForCompletion(sessionHandleString, handle, MediaType.APPLICATION_JSON_TYPE,
        MediaType.APPLICATION_JSON);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
  }

  @Test
  public void passPropertyQueryConf() throws Exception {

    LensConf lensConf = new LensConf();
    lensConf.addProperty(LensConfConstants.QUERY_OUTPUT_SERDE, "org.apache.lens.lib.query.CSVSerde1");
    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.QUERY, null, null, sessionHandleString,
        lensConf).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatusMessage().trim(), "Result formatting failed!",
        "Query did not Fail");

    lensConf.addProperty(LensConfConstants.QUERY_OUTPUT_SERDE, "org.apache.lens.lib.query.CSVSerde");
    queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.QUERY, null, null, sessionHandleString,
        lensConf).getData();
    lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");
  }

  @Test
  public void testExecuteWithTimeout() throws Exception {

    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "false");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");

    QueryHandleWithResultSet qHWithResultSet = (QueryHandleWithResultSet) qHelper.executeQueryTimeout(
        QueryInventory.JDBC_DIM_QUERY, "60000").getData();
    QueryStatus queryStatus = qHelper.getQueryStatus(qHWithResultSet.getQueryHandle());
    Assert.assertEquals(queryStatus.getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");
    InMemoryQueryResult result = (InMemoryQueryResult) qHWithResultSet.getResult();
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getRows().size(), 2);

    qHWithResultSet = (QueryHandleWithResultSet) qHelper.executeQueryTimeout(QueryInventory.getSleepQuery("10"),
        "10000").getData();
    queryStatus = qHelper.getQueryStatus(qHWithResultSet.getQueryHandle());
    Assert.assertEquals(queryStatus.getStatus(), QueryStatus.Status.RUNNING, "Query is Not Running");

    qHelper.killQueryByQueryHandle(qHWithResultSet.getQueryHandle());
    queryStatus = qHelper.getQueryStatus(qHWithResultSet.getQueryHandle());
    Assert.assertEquals(queryStatus.getStatus(), QueryStatus.Status.CANCELED, "Query should be cancelled");
  }

  @Test
  public void testGetInMemoryResultSet() throws Exception {

    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "false");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_MAIL_NOTIFY, "false");

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_DIM_QUERY).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");
    InMemoryQueryResult result = (InMemoryQueryResult) qHelper.getResultSet(queryHandle);
    Assert.assertNotNull(result);
    validateInMemoryResultSet(result);
  }

  //LENS-909
  @Test
  public void testInMemoryResultMailNotify() throws Exception {

    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "false");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_MAIL_NOTIFY, "true");

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_DIM_QUERY).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");
    InMemoryQueryResult result = (InMemoryQueryResult) qHelper.getResultSet(queryHandle);
    Assert.assertNotNull(result);
    validateInMemoryResultSet(result);
    //TODO : add assert on actual mail sent
  }

  @Test
  public void testGetPersistentResultSet() throws Exception {

    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_DIM_QUERY).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");
    PersistentQueryResult result = (PersistentQueryResult) qHelper.getResultSet(queryHandle);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getNumRows().intValue(), 2, "Wrong result");
  }


  private void validateInMemoryResultSet(InMemoryQueryResult result) {

    int size = result.getRows().size();
    for (int i = 0; i < size; i++) {
      logger.info(result.getRows().get(i).getValues().get(0) + " " + result.getRows().get(i).getValues().get(1));
    }
    Assert.assertEquals(result.getRows().size(), 2, "Wrong result");
    Assert.assertEquals(result.getRows().get(0).getValues().get(0), 2, "Wrong result");
    Assert.assertEquals(result.getRows().get(0).getValues().get(1), "second", "Wrong result");
    Assert.assertEquals(result.getRows().get(1).getValues().get(0), 3, "Wrong result");
    Assert.assertEquals(result.getRows().get(1).getValues().get(1), "third", "Wrong result");
  }


  @Test
  public void testQueryResultJsonInmemory() throws Exception {

    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "false");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_DIM_QUERY).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");
    String resultSetJson = qHelper.getInmemoryResultSetJson(queryHandle, "0", "100", sessionHandleString);
    Assert.assertNotNull(resultSetJson);
    Assert.assertFalse(resultSetJson.isEmpty());

    //TODO : Enable this once LENS-1005 is fixed
//    Cannot convert json output to InMemoryQueryResult
//    InMemoryQueryResult ResultSetJson = (InMemoryQueryResult) qHelper.getResultSetJson(queryHandle1, "0", "100");
  }


  @Test
  public void testQueryResultJsonPersistent() throws Exception {

    String parentDir = "file:/tmp/lensreports";
    sHelper.setAndValidateParam(LensConfConstants.RESULT_SET_PARENT_DIR, parentDir);

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_DIM_QUERY, null,
        sessionHandleString).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");
    String resultSetJson = qHelper.getPersistentResultSetJson(queryHandle, "0", "100");
    Assert.assertNotNull(resultSetJson);
    Assert.assertFalse(resultSetJson.isEmpty());

    //TODO : Enable this once LENS-1005 is fixed

//    PersistentQueryResult resultSetJson = qHelper.getPersistentResultSetJson(queryHandle, "0", "100");
//    Assert.assertNotNull(resultSetJson);
//    Assert.assertEquals(resultSetJson.getPersistedURI(), parentDir+"/"+queryHandle.toString()+".zip",
//    "Persistened result url not matching");
  }

  //LENS-928
  @Test
  public void testDeleteOnInMemoryResult() throws Exception {

    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "false");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    sHelper.setAndValidateParam("lens.query.enable.mail.notify", "false");

    MapBuilder query = new MapBuilder("sessionid", sessionHandleString, "fromindex", "0", "fetchsize", "100");

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_DIM_QUERY).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");

    Response response = qHelper.exec("get", QueryURL.QUERY_URL + "/" + queryHandle.toString() + "/resultset", servLens,
        null, query);
    AssertUtil.assertSucceededResponse(response);

    Thread.sleep(20000);

    Response response1 = qHelper.exec("delete", QueryURL.QUERY_URL + "/" + queryHandle.toString() + "/resultset",
        servLens, null, query);
    AssertUtil.assertSucceededResponse(response1);

  }

  @Test
  public void cubeKeywordOptional() throws Exception {
    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.NO_CUBE_KEYWORD_QUERY).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");
  }


  @Test
  public void testQueryWithQuote() throws Exception {
    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.QUOTE_QUERY).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");
  }


  /*
      LENS-909. When query is formatting result (status between executed and successfull), if fetched for
      result it provides the result url as hdfs path. getResult was not checking for status= success,hence it had wrong
      path. With this fix, fetch result will return 404 if query is not complete.
  */

  @Test
  public void testResultPathBeforeQueryCompletion() throws Exception {

    MapBuilder query = new MapBuilder("sessionid", sessionHandleString, "fromindex", "0", "fetchsize", "100");

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.SLEEP_QUERY).getData();
    LensQuery lq = qHelper.getLensQuery(sessionHandleString, queryHandle);

    int i = 0;
    while (lq.getStatus().getStatus() != QueryStatus.Status.SUCCESSFUL) {
      Response response = qHelper.exec("get", QueryURL.QUERY_URL + "/" + queryHandle.toString() + "/resultset",
          servLens, null, query);
      Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
      logger.info("trial : " + i++);
      lq = qHelper.getLensQuery(sessionHandleString, queryHandle);
    }

    PersistentQueryResult result = (PersistentQueryResult) qHelper.getResultSet(queryHandle);
    String resultUrl = result.getHttpResultUrl();
    System.out.println("http url : " + resultUrl);
    Assert.assertTrue(resultUrl.contains("httpresultset"));
  }


  @Test(dataProvider = "persistance_values")
  public void testCancelQueryOnTimeout(String serverPersistent, String driverPersistent) throws Exception {

    sHelper.setAndValidateParam(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "false");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, serverPersistent);
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, driverPersistent);
    String query = QueryInventory.getSleepQuery("5");

    //checking for default value
    QueryHandleWithResultSet qhr1 = (QueryHandleWithResultSet) qHelper.executeQueryTimeout(query, "10",
        null, sessionHandleString).getData();
    LensQuery lq1 = qHelper.waitForCompletion(qhr1.getQueryHandle());
    Assert.assertEquals(lq1.getStatus().getStatus(), QueryStatus.Status.CANCELED);

    //Setting explicitly
    LensConf lensConf = new LensConf();
    lensConf.addProperty(LensConfConstants.CANCEL_QUERY_ON_TIMEOUT, "true");
    QueryHandleWithResultSet qhr2 = (QueryHandleWithResultSet) qHelper.executeQueryTimeout(query, "5",
        null, sessionHandleString, lensConf).getData();
    LensQuery lq2 = qHelper.waitForCompletion(qhr2.getQueryHandle());
    Assert.assertEquals(lq2.getStatus().getStatus(), QueryStatus.Status.CANCELED);
  }


  @Test(dataProvider = "persistance_values")
  public void testDontCancelQueryOnTimeout(String serverPersistent, String driverPersistent) throws Exception {

    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, serverPersistent);
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, driverPersistent);
    String query = QueryInventory.getSleepQuery("5");

    LensConf lensConf = new LensConf();
    lensConf.addProperty(LensConfConstants.CANCEL_QUERY_ON_TIMEOUT, "false");

    QueryHandleWithResultSet qHWithResultSet = (QueryHandleWithResultSet) qHelper.executeQueryTimeout(query, "5",
        null, sessionHandleString, lensConf).getData();

    LensQuery lq = qHelper.waitForCompletion(qHWithResultSet.getQueryHandle());
    Assert.assertEquals(lq.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
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

  // LENS-1186
  @Test
  public void testInvalidOperation() throws Exception {
    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", sessionHandleString);
    formData.add("query", QueryInventory.JDBC_CUBE_QUERY);
    formData.add("operation", "INVALID_EXECUTE");
    String conf = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><conf />";
    formData.add("conf", conf);

    Response response = lens.exec("post", QueryURL.QUERY_URL, servLens, null, null, MediaType.MULTIPART_FORM_DATA_TYPE,
        MediaType.APPLICATION_XML, formData.getForm());
    AssertUtil.assertBadRequest(response);
    LensAPIResult result = response.readEntity(new GenericType<LensAPIResult>(){});
    Assert.assertEquals(result.getErrorCode(), 2003);
  }

  @Test
  public void testInvalidSessionHandle() throws Exception {

    String session = sHelper.openSession("u1", "p1", lens.getCurrentDB());
    QueryHandle q = (QueryHandle) qHelper.executeQuery(QueryInventory.getSleepQuery("10"), null, session).getData();
    qHelper.waitForQueryToRun(q);
    sHelper.closeSession(session);

    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", session);
    formData.add("query", QueryInventory.HIVE_CUBE_QUERY);
    formData.add("operation", "EXECUTE");
    String conf = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><conf />";
    formData.add("conf", conf);

    Response response = lens.exec("post", QueryURL.QUERY_URL, servLens, null, null, MediaType.MULTIPART_FORM_DATA_TYPE,
        MediaType.APPLICATION_XML, formData.getForm());
    AssertUtil.assertBadRequest(response);
    LensAPIResult result = response.readEntity(new GenericType<LensAPIResult>() {
    });
    Assert.assertEquals(result.getErrorCode(), 2005);
  }
}

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

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.query.*;
import org.apache.lens.regression.core.constants.QueryInventory;
import org.apache.lens.regression.core.constants.QueryURL;
import org.apache.lens.regression.core.helpers.LensServerHelper;
import org.apache.lens.regression.core.helpers.MetastoreHelper;
import org.apache.lens.regression.core.helpers.QueryHelper;
import org.apache.lens.regression.core.helpers.ServiceManagerHelper;
import org.apache.lens.regression.core.helpers.SessionHelper;
import org.apache.lens.regression.core.testHelper.BaseTestClass;
import org.apache.lens.regression.core.type.MapBuilder;
import org.apache.lens.regression.util.AssertUtil;
import org.apache.lens.server.api.LensConfConstants;

import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.*;


public class ITQueryApiTests extends BaseTestClass {

  WebTarget servLens;
  private String sessionHandleString;

  LensServerHelper lens = getLensServerHelper();
  MetastoreHelper mHelper = getMetastoreHelper();
  SessionHelper sHelper = getSessionHelper();
  QueryHelper qHelper = getQueryHelper();

  private static Logger logger = Logger.getLogger(ITQueryApiTests.class);


  @BeforeClass(alwaysRun = true)
  public void initialize() throws Exception {
    servLens = ServiceManagerHelper.init();
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp(Method method) throws Exception {
    logger.info("Test Name: " + method.getName());
    logger.info("Creating a new Session");
    sessionHandleString = lens.openSession(lens.getCurrentDB());
  }

  @AfterMethod(alwaysRun = true)
  public void closeSession() throws Exception {
    logger.info("Closing Session");
    lens.closeSession();
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
        QueryInventory.QUERY, "60000").getData();
    QueryStatus queryStatus = qHelper.getQueryStatus(qHWithResultSet.getQueryHandle());
    Assert.assertEquals(queryStatus.getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");
    InMemoryQueryResult result = (InMemoryQueryResult) qHWithResultSet.getResult();
    Assert.assertNotNull(result);
    validateInMemoryResultSet(result);

    qHWithResultSet = (QueryHandleWithResultSet) qHelper.executeQueryTimeout(QueryInventory.SLEEP_QUERY, "10000")
        .getData();
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

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.QUERY).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");
    InMemoryQueryResult result = (InMemoryQueryResult) qHelper.getResultSet(queryHandle);
    Assert.assertNotNull(result);
    validateInMemoryResultSet(result);

  }

  @Test
  public void testGetPersistentResultSet() throws Exception {

    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.QUERY).getData();
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

}

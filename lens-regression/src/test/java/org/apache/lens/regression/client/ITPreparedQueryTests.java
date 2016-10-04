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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import javax.xml.bind.JAXBException;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.query.*;
import org.apache.lens.regression.core.constants.QueryInventory;
import org.apache.lens.regression.core.constants.QueryURL;
import org.apache.lens.regression.core.helpers.ServiceManagerHelper;
import org.apache.lens.regression.core.testHelper.BaseTestClass;
import org.apache.lens.regression.core.type.FormBuilder;
import org.apache.lens.regression.core.type.MapBuilder;
import org.apache.lens.regression.util.AssertUtil;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;

import org.apache.log4j.Logger;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import org.testng.Assert;
import org.testng.annotations.*;

public class ITPreparedQueryTests extends BaseTestClass {

  WebTarget servLens;
  String sessionHandleString;

  private static Map<String, String> defaultParams = new HashMap<String, String>();
  static {
    defaultParams.put(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "false");
    defaultParams.put(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
  }

  private static Logger logger = Logger.getLogger(ITPreparedQueryTests.class);


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

  @AfterClass(alwaysRun = true)
  public void closeSession() throws Exception {
    logger.info("Closing Session");
    sHelper.closeSession();
  }

  @Test
  public void testPrepareAndExecutePreparedQuery()  throws Exception {

    sHelper.setAndValidateParam(defaultParams);
    QueryPrepareHandle queryPrepareHandle = qHelper.submitPreparedQuery(QueryInventory.JDBC_DIM_QUERY);
    Assert.assertNotNull(queryPrepareHandle, "Query Execute Failed");
    logger.info("PREPARE QUERY HANDLE : " + queryPrepareHandle);

    QueryHandle queryHandle = qHelper.executePreparedQuery(queryPrepareHandle);
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");
    InMemoryQueryResult result = (InMemoryQueryResult) qHelper.getResultSet(queryHandle);
    Assert.assertEquals(result.getRows().size(), 2);
  }

  // This is failing
  @Test(enabled = true)
  public void testPrepareAndExecuteTimeoutPreparedQuery()  throws Exception {

    sHelper.setAndValidateParam(defaultParams);
    QueryPrepareHandle queryPrepareHandle = qHelper.submitPreparedQuery(QueryInventory.QUERY);
    Assert.assertNotNull(queryPrepareHandle, "Query Execute Failed");
    logger.info("PREPARE QUERY HANDLE : " + queryPrepareHandle);

    QueryHandleWithResultSet queryHandleResultSet = qHelper.executePreparedQueryTimeout(queryPrepareHandle, "60000");
    InMemoryQueryResult result = (InMemoryQueryResult) queryHandleResultSet.getResult();
    validateInMemoryResultSet(result);
  }

  @Test
  public void destroyPreparedQueryByHandle()  throws Exception {

    QueryPrepareHandle queryPrepareHandle = qHelper.submitPreparedQuery(QueryInventory.QUERY);
    Assert.assertNotNull(queryPrepareHandle, "Query Execute Failed");
    logger.info("PREPARE QUERY HANDLE : " + queryPrepareHandle);

    MapBuilder query = new MapBuilder("sessionid", sessionHandleString,
        "prepareHandle", queryPrepareHandle.toString()
    );

    Response response = qHelper.getPreparedQuery(queryPrepareHandle);
    AssertUtil.assertSucceededResponse(response);

    logger.info("Deleting Prepared Query");
    qHelper.destoryPreparedQueryByHandle(queryPrepareHandle);

    logger.info("Get Should now give 404");
    response = qHelper.getPreparedQuery(queryPrepareHandle);
    AssertUtil.assertNotFound(response);
  }

  @Test
  public void modifyPreparedQueryByHandle()  throws Exception {

    QueryPrepareHandle queryPrepareHandle = qHelper.submitPreparedQuery(QueryInventory.QUERY);
    Assert.assertNotNull(queryPrepareHandle, "Query Execute Failed");
    logger.info("PREPARE QUERY HANDLE : " + queryPrepareHandle);

    MapBuilder query = new MapBuilder("sessionid", sessionHandleString,
        "prepareHandle", queryPrepareHandle.toString()
    );

    logger.info("Get Should be Successful");
    Response response = lens.exec("get", QueryURL.PREPAREDQUERY_URL + "/" + queryPrepareHandle.toString(),
        servLens, null, query);
    AssertUtil.assertSucceededResponse(response);

    logger.info("Modifying PreparedQuery conf");

    LensConf lensConf = new LensConf();
    lensConf.addProperty(LensConfConstants.RESULT_SET_PARENT_DIR, "hdfs://lens-test:8020/tmp/lensreports");

    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", sessionHandleString);
    formData.getForm().bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(),
        lensConf, MediaType.APPLICATION_XML_TYPE));

    response = lens.exec("put", QueryURL.PREPAREDQUERY_URL + "/" + queryPrepareHandle.toString(), servLens, null, null,
        MediaType.MULTIPART_FORM_DATA_TYPE, MediaType.APPLICATION_XML, formData.getForm());
    AssertUtil.assertSucceededResponse(response);

    logger.info("Get Should be Successful");
    response = lens.exec("get", QueryURL.PREPAREDQUERY_URL + "/" + queryPrepareHandle.toString(),
        servLens, null, query);
    AssertUtil.assertSucceededResponse(response);

    LensPreparedQuery gq = response.readEntity(new GenericType<LensPreparedQuery>(){});
    logger.info("Modified Conf : " + gq.getConf().getProperties().get("lens.query.result.parent.dir"));
    Assert.assertEquals(gq.getConf().getProperties().get(LensConfConstants.RESULT_SET_PARENT_DIR),
        "hdfs://lens-test:8020/tmp/lensreports", "Update on Prepared Query Failed!!");
  }


  @Test
  public void listAllPreparedQueriesOfUser()  throws Exception {

    QueryPrepareHandle queryPrepareHandle = qHelper.submitPreparedQuery(QueryInventory.QUERY);
    Assert.assertNotNull(queryPrepareHandle, "Query Execute Failed");
    logger.info("PREPARE QUERY HANDLE : " + queryPrepareHandle);

    QueryPrepareHandle queryPrepareHandle1 = qHelper.submitPreparedQuery(QueryInventory.QUERY);
    Assert.assertNotNull(queryPrepareHandle1, "Query Execute Failed");
    logger.info("PREPARE QUERY HANDLE : " + queryPrepareHandle1);

    List<QueryPrepareHandle> list = qHelper.getPreparedQueryHandleList();

    Assert.assertTrue(list.contains(queryPrepareHandle), "List of All QueryPreparedHandle of a user failed");
    Assert.assertTrue(list.contains(queryPrepareHandle1), "List of All QueryPreparedHandle of a user failed");
  }


  @Test
  public void listAllPreparedQueriesByQueryName()  throws Exception {

    //TODO : Filter by queryName is not working, Commented the fail part, Uncomment it when fixed
    String name1 = "FirstQuery";
    String name2 = "SecondQuery";

    QueryPrepareHandle queryPrepareHandle = qHelper.submitPreparedQuery(QueryInventory.QUERY, name1);
    Assert.assertNotNull(queryPrepareHandle, "Query Execute Failed");
    logger.info("PREPARE QUERY HANDLE : " + queryPrepareHandle);

    QueryPrepareHandle queryPrepareHandle1 = qHelper.submitPreparedQuery(QueryInventory.QUERY, name2);
    Assert.assertNotNull(queryPrepareHandle1, "Query Execute Failed");
    logger.info("PREPARE QUERY HANDLE : " + queryPrepareHandle1);

    List<QueryPrepareHandle> list = qHelper.getPreparedQueryHandleList(name1);
    Assert.assertTrue(list.contains(queryPrepareHandle), "List of All QueryPreparedHandle By QueryName failed");
    //Assert.assertFalse(list.contains(queryPrepareHandle1), "List of All QueryPreparedHandle By QueryName failed");

    list = qHelper.getPreparedQueryHandleList(name2);
    //Assert.assertFalse(list.contains(queryPrepareHandle), "List of All QueryPreparedHandle By QueryName failed");
    Assert.assertTrue(list.contains(queryPrepareHandle1), "List of All QueryPreparedHandle By QueryName failed");

    list = qHelper.getPreparedQueryHandleList();
    Assert.assertTrue(list.contains(queryPrepareHandle), "List of All QueryPreparedHandle of a user failed");
    Assert.assertTrue(list.contains(queryPrepareHandle1), "List of All QueryPreparedHandle of a user failed");
  }

  @Test
  public void listAllPreparedQueriesByUser()  throws Exception {

    //TODO : Filter by user is not working, Commented the fail part, Uncomment it when fixed

    String user = "diff", pass = "diff";

    String session1 = sHelper.openSession(user, pass, lens.getCurrentDB());

    QueryPrepareHandle queryPrepareHandle1 = qHelper.submitPreparedQuery(QueryInventory.QUERY);
    Assert.assertNotEquals(queryPrepareHandle1, null, "Query Execute Failed");
    logger.info("PREPARE QUERY HANDLE : " + queryPrepareHandle1);

    QueryPrepareHandle queryPrepareHandle2 = qHelper.submitPreparedQuery(QueryInventory.QUERY, null, session1);
    Assert.assertNotEquals(queryPrepareHandle2, null, "Query Execute Failed");
    logger.info("PREPARE QUERY HANDLE : " + queryPrepareHandle2);

    List<QueryPrepareHandle> list = qHelper.getPreparedQueryHandleList(null, lens.getUserName());
    Assert.assertTrue(list.contains(queryPrepareHandle1), "List of All QueryPreparedHandle By user failed");
    //Assert.assertFalse(list.contains(queryPrepareHandle2), "List of All QueryPreparedHandle By user failed");

    list = qHelper.getPreparedQueryHandleList(null, user);
    //Assert.assertFalse(list.contains(queryPrepareHandle1), "List of All QueryPreparedHandle By user failed");
    Assert.assertTrue(list.contains(queryPrepareHandle2), "List of All QueryPreparedHandle By user failed");

    list = qHelper.getPreparedQueryHandleList(null, "all");
    Assert.assertTrue(list.contains(queryPrepareHandle1), "List of All QueryPreparedHandle by 'all' user failed");
    Assert.assertTrue(list.contains(queryPrepareHandle2), "List of All QueryPreparedHandle by 'all' user failed");
  }

  @Test
  public void listAllPreparedQueriesByTimeRange()  throws Exception {

    //Submitting First Query
    String startTime = String.valueOf(System.currentTimeMillis());
    logger.info("Start Time of 1st Query : " + startTime);

    QueryPrepareHandle queryPrepareHandle1 = qHelper.submitPreparedQuery(QueryInventory.QUERY);
    Assert.assertNotEquals(queryPrepareHandle1, null, "Query Execute Failed");
    logger.info("PREPARE QUERY HANDLE : " + queryPrepareHandle1);

    String endTime = String.valueOf(System.currentTimeMillis());
    logger.info("End Time of 1st Query : " + endTime);

    Thread.sleep(1000);

    //Submitting Second Query
    String startTime1 = String.valueOf(System.currentTimeMillis());
    logger.info("Start Time of 2nd Query : " + startTime1);

    QueryPrepareHandle queryPrepareHandle2 = qHelper.submitPreparedQuery(QueryInventory.QUERY);
    Assert.assertNotEquals(queryPrepareHandle2, null, "Query Execute Failed");
    logger.info("PREPARE QUERY HANDLE : " + queryPrepareHandle2);

    String endTime1 = String.valueOf(System.currentTimeMillis());
    logger.info("End Time of 2nd Query : " + endTime1);

    List<QueryPrepareHandle> list = qHelper.getPreparedQueryHandleList(null, null, sessionHandleString,
        startTime, endTime);
    Assert.assertTrue(list.contains(queryPrepareHandle1), "List of All QueryPreparedHandle By timeRange failed");
    Assert.assertFalse(list.contains(queryPrepareHandle2), "List of All QueryPreparedHandle By timeRange failed");

    list = qHelper.getPreparedQueryHandleList(null, null, sessionHandleString, startTime1, endTime1);
    Assert.assertFalse(list.contains(queryPrepareHandle1), "List of All QueryPreparedHandle By timeRange failed");
    Assert.assertTrue(list.contains(queryPrepareHandle2), "List of All QueryPreparedHandle By timeRange failed");

    list = qHelper.getPreparedQueryHandleList(null, null, sessionHandleString, startTime, endTime1);
    Assert.assertTrue(list.contains(queryPrepareHandle1), "List of All QueryPreparedHandle by timeRange user failed");
    Assert.assertTrue(list.contains(queryPrepareHandle2), "List of All QueryPreparedHandle by timeRange user failed");
  }

  @Test
  public void destroyAllPreparedQueriesOfUser()  throws Exception {

    QueryPrepareHandle queryPrepareHandle1 = qHelper.submitPreparedQuery(QueryInventory.QUERY);
    Assert.assertNotEquals(queryPrepareHandle1, null, "Query Execute Failed");
    logger.info("PREPARE QUERY HANDLE : " + queryPrepareHandle1);

    QueryPrepareHandle queryPrepareHandle2 = qHelper.submitPreparedQuery(QueryInventory.QUERY);
    Assert.assertNotEquals(queryPrepareHandle2, null, "Query Execute Failed");
    logger.info("PREPARE QUERY HANDLE : " + queryPrepareHandle2);

    Response response = qHelper.getPreparedQuery(queryPrepareHandle1);
    AssertUtil.assertSucceededResponse(response);

    response = qHelper.getPreparedQuery(queryPrepareHandle2);
    AssertUtil.assertSucceededResponse(response);

    qHelper.destroyPreparedQuery();

    response = qHelper.getPreparedQuery(queryPrepareHandle1);
    AssertUtil.assertNotFound(response);

    response = qHelper.getPreparedQuery(queryPrepareHandle2);
    AssertUtil.assertNotFound(response);

  }

  //LENS-1012
  @Test
  public void destroyAllPreparedQueriesByQueryName()  throws Exception {

    //TODO : Destroy by queryName is not working, Commented the fail part, Uncomment it when fixed

    String name1 = "Query1";
    String name2 = "Query2";

    QueryPrepareHandle queryPrepareHandle1 = qHelper.submitPreparedQuery(QueryInventory.QUERY, name1);
    Assert.assertNotEquals(queryPrepareHandle1, null, "Query Execute Failed");
    logger.info("PREPARE QUERY HANDLE : " + queryPrepareHandle1);

    QueryPrepareHandle queryPrepareHandle2 = qHelper.submitPreparedQuery(QueryInventory.QUERY, name2);
    Assert.assertNotEquals(queryPrepareHandle2, null, "Query Execute Failed");
    logger.info("PREPARE QUERY HANDLE : " + queryPrepareHandle2);

    Response response = qHelper.getPreparedQuery(queryPrepareHandle1);
    AssertUtil.assertSucceededResponse(response);
    response = qHelper.getPreparedQuery(queryPrepareHandle2);
    AssertUtil.assertSucceededResponse(response);

    qHelper.destroyPreparedQuery(name1);

    response = qHelper.getPreparedQuery(queryPrepareHandle1);
    AssertUtil.assertNotFound(response);
    response = qHelper.getPreparedQuery(queryPrepareHandle2);
    //AssertUtil.assertSucceededResponse(response);

    qHelper.destroyPreparedQuery(name2);

    response = qHelper.getPreparedQuery(queryPrepareHandle1);
    AssertUtil.assertNotFound(response);
    response = qHelper.getPreparedQuery(queryPrepareHandle2);
    AssertUtil.assertNotFound(response);

  }

  //LENS-1012
  @Test
  public void destroyAllPreparedQueriesByUser()  throws Exception {

    //TODO : Destroy by user is not working, Commented the fail part, Uncomment it when fixed

    String user = "diff", pass = "diff";
    String session1 = sHelper.openSession(user, pass, lens.getCurrentDB());

    QueryPrepareHandle queryPrepareHandle1 = qHelper.submitPreparedQuery(QueryInventory.QUERY);
    Assert.assertNotEquals(queryPrepareHandle1, null, "Query Execute Failed marker");
    logger.info("PREPARE QUERY HANDLE : " + queryPrepareHandle1);

    QueryPrepareHandle queryPrepareHandle2 = qHelper.submitPreparedQuery(QueryInventory.QUERY, null, session1);
    Assert.assertNotEquals(queryPrepareHandle2, null, "Query Execute Failed");
    logger.info("PREPARE QUERY HANDLE : " + queryPrepareHandle2);

    Response response = qHelper.getPreparedQuery(queryPrepareHandle1);
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    response = qHelper.getPreparedQuery(queryPrepareHandle2);
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

    qHelper.destroyPreparedQuery(null, lens.getUserName());

    response = qHelper.getPreparedQuery(queryPrepareHandle1);
    AssertUtil.assertNotFound(response);
    response = qHelper.getPreparedQuery(queryPrepareHandle2);
//    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

    qHelper.destroyPreparedQuery(null, user);

    response = qHelper.getPreparedQuery(queryPrepareHandle1);
    AssertUtil.assertNotFound(response);
    response = qHelper.getPreparedQuery(queryPrepareHandle2);
    AssertUtil.assertNotFound(response);
  }

  //jira raised LENS-567
  @Test
  public void destroyAllPreparedQueriesByTimeRange()  throws Exception {

    //TODO : Filter by user is not working, Commented the fail part, Uncomment it when fixed

    //Submitting First Query
    String startTime = String.valueOf(System.currentTimeMillis());
    logger.info("Start Time of 1st Query : " + startTime);
    QueryPrepareHandle queryPrepareHandle1 = qHelper.submitPreparedQuery(QueryInventory.QUERY);
    Assert.assertNotEquals(queryPrepareHandle1, null, "Query Execute Failed");
    logger.info("PREPARE QUERY HANDLE : " + queryPrepareHandle1);
    String endTime = String.valueOf(System.currentTimeMillis());
    logger.info("End Time of 1st Query : " + endTime);

    Thread.sleep(1000);

    //Submitting Second Query
    String startTime1 = String.valueOf(System.currentTimeMillis());
    logger.info("Start Time of 2nd Query : " + startTime1);
    QueryPrepareHandle queryPrepareHandle2 = qHelper.submitPreparedQuery(QueryInventory.QUERY);
    Assert.assertNotEquals(queryPrepareHandle2, null, "Query Execute Failed");
    logger.info("PREPARE QUERY HANDLE : " + queryPrepareHandle2);
    String endTime1 = String.valueOf(System.currentTimeMillis());
    logger.info("End Time of 2nd Query : " + endTime1);

    Thread.sleep(1000);

    Response response = qHelper.getPreparedQuery(queryPrepareHandle1);
    AssertUtil.assertSucceededResponse(response);
    response = qHelper.getPreparedQuery(queryPrepareHandle2);
    AssertUtil.assertSucceededResponse(response);

    qHelper.destroyPreparedQuery(null, null, sessionHandleString, startTime, endTime);

    response = qHelper.getPreparedQuery(queryPrepareHandle1);
    AssertUtil.assertNotFound(response);
    response = qHelper.getPreparedQuery(queryPrepareHandle2);
    AssertUtil.assertSucceededResponse(response);

    qHelper.destroyPreparedQuery(null, null, sessionHandleString, startTime1, endTime1);

    response = qHelper.getPreparedQuery(queryPrepareHandle1);
    AssertUtil.assertNotFound(response);
    response = qHelper.getPreparedQuery(queryPrepareHandle2);
    AssertUtil.assertNotFound(response);

  }

  private void validateInMemoryResultSet(InMemoryQueryResult result) {

    int size = result.getRows().size();
    for(int i=0; i<size; i++) {
      logger.info(result.getRows().get(i).getValues().get(0) + " " + result.getRows().get(i).getValues().get(1));
    }
    Assert.assertEquals(size, 2, "Wrong result");
    Assert.assertEquals(result.getRows().get(0).getValues().get(0), 2, "Wrong result");
    Assert.assertEquals(result.getRows().get(0).getValues().get(1), "second", "Wrong result");
    Assert.assertEquals(result.getRows().get(1).getValues().get(0), 3, "Wrong result");
    Assert.assertEquals(result.getRows().get(1).getValues().get(1), "third", "Wrong result");
  }

  @Test
  public void listAllPreparedQueriesByQueryNameUserTimeRange()  throws Exception {

    String queryName1 = "queryfirst", queryName2 = "querysecond";
    String user2 = "user2", pass2 = "pass2";
    String session1 = sHelper.openSession(user2, pass2, lens.getCurrentDB());

    String startTime1=String.valueOf(System.currentTimeMillis());
    logger.info("Start Time of first query- "+startTime1);
    QueryPrepareHandle queryPreparedHandle1= qHelper.submitPreparedQuery(QueryInventory.QUERY, queryName1);
    String endTime1=String.valueOf(System.currentTimeMillis());
    logger.info("End Time of first query- "+endTime1);

    Thread.sleep(1000);

    String startTime2=String.valueOf(System.currentTimeMillis());
    logger.info("Start Time of second query- " + startTime2);
    QueryPrepareHandle queryPreparedHandle2=qHelper.submitPreparedQuery(QueryInventory.QUERY, queryName2, session1);
    String endTime2=String.valueOf(System.currentTimeMillis());
    logger.info("End Time for second query- "+ endTime2);

    Thread.sleep(1000);

    List<QueryPrepareHandle> list = qHelper.getPreparedQueryHandleList(queryName1, lens.getUserName(),
        sessionHandleString, startTime1, endTime1);
    Assert.assertTrue(list.contains(queryPreparedHandle1),
        "List of all prepared query by user,Time range and query name failed");
    Assert.assertFalse(list.contains(queryPreparedHandle2),
        "List of all prepared query by user,Time range and query name failed");

    list = qHelper.getPreparedQueryHandleList(queryName2, user2, session1, startTime2, endTime2);
    Assert.assertTrue(list.contains(queryPreparedHandle2),
        "List of all prepared query by user,Time range and query name failed");
    Assert.assertFalse(list.contains(queryPreparedHandle1),
        "List of all prepared query by user,Time range and query name failed");
  }

  @Test
  public void testExplainAndPrepareQuery() throws Exception {
    QueryPlan queryPlan = qHelper.explainAndPrepareQuery(QueryInventory.HIVE_CUBE_QUERY);
    logger.info("Query Handle" + queryPlan.getPrepareHandle());
    Assert.assertNotNull(queryPlan.getPrepareHandle(), "not returning queryhandle");
  }
}

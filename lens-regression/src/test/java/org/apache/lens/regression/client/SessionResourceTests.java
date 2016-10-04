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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import javax.xml.bind.JAXBException;

import org.apache.lens.api.StringList;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.api.session.UserSessionInfo;
import org.apache.lens.regression.core.constants.MetastoreURL;
import org.apache.lens.regression.core.constants.QueryInventory;
import org.apache.lens.regression.core.constants.QueryURL;
import org.apache.lens.regression.core.constants.SessionURL;
import org.apache.lens.regression.core.helpers.ServiceManagerHelper;
import org.apache.lens.regression.core.testHelper.BaseTestClass;
import org.apache.lens.regression.core.type.FormBuilder;
import org.apache.lens.regression.core.type.MapBuilder;
import org.apache.lens.regression.util.AssertUtil;
import org.apache.lens.regression.util.Util;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.util.LensUtil;

import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.*;

import com.jcraft.jsch.JSchException;

public class SessionResourceTests extends BaseTestClass {

  WebTarget servLens;
  private String sessionHandleString;

  private final String hdfsJarPath = lens.getServerHdfsUrl() + "/tmp";
  private final String localJarPath = new File("").getAbsolutePath() + "/lens-regression/target/testjars/";
  private final String hiveUdfJar = "hiveudftest.jar";
  private final String serverResourcePath = "/tmp/regression/resources";

  private static String newParamsKey = "datanucleus.autoCreateSchema";
  private static String newParamsValue = "false";
  private static String createSleepFunction = "CREATE TEMPORARY FUNCTION sleep AS 'SampleUdf'";
  private static Logger logger = Logger.getLogger(SessionResourceTests.class);


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

  private boolean checkSessionParamMap(String sessionHandle) throws Exception {
    MapBuilder query = new MapBuilder("sessionid", sessionHandle, "verbose", "true");
    Response response = lens.sendQuery("get", SessionURL.SESSION_PARAMS_URL, query);
    AssertUtil.assertSucceededResponse(response);
    StringList strList = response.readEntity(new GenericType<StringList>(StringList.class));
    HashMap<String, String> map = Util.stringListToMap(strList);
    if (map == null) {
      return false;
    }
    return true;
  }


  @Test
  public void testSessionGet() throws Exception {
    String newSessionHandle = sHelper.openSession("diff", "diff");
    Assert.assertNotNull(newSessionHandle);
  }


  @Test
  public void testSessionParamsGetVerbose() throws Exception {
    Assert.assertTrue(checkSessionParamMap(sessionHandleString), "Returned Empty Params list");
  }


  @Test
  public void testSessionParamsGetNPut() throws Exception {

    Map<String, String> resource = LensUtil.getHashMap("sessionid", sessionHandleString, "key", newParamsKey,
      "value", newParamsValue);
    FormBuilder formData = new FormBuilder(resource);
    Response response = lens.sendForm("put", SessionURL.SESSION_PARAMS_URL, formData);
    AssertUtil.assertSucceededResult(response);

    String value = sHelper.getSessionParam(newParamsKey);
    Assert.assertEquals(value, newParamsValue, "From Session Params Put");

  }

  //Negative Test Case
  @Test
  public void testSessionGetUndefinedParams() throws Exception {
    String undefinedParamsKey = "test123";
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString, "key", undefinedParamsKey);
    Response response = lens.sendQuery("get", SessionURL.SESSION_PARAMS_URL, query);
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
  }

/*
 * Testing if Session is restored after server restart
 */

  @Test
  public void testSessionRestore() throws Exception {

    Map<String, String> resource = LensUtil.getHashMap("sessionid", sessionHandleString,
      "key", newParamsKey, "value", newParamsValue);
    FormBuilder formData = new FormBuilder(resource);

    Response response = lens.sendForm("put", SessionURL.SESSION_PARAMS_URL, formData);
    AssertUtil.assertSucceededResult(response);

    lens.restart();

    MapBuilder query = new MapBuilder("sessionid", sessionHandleString, "key", newParamsKey);
    response = lens.sendQuery("get", SessionURL.SESSION_PARAMS_URL, query);
    AssertUtil.assertSucceededResponse(response);
    StringList strList = response.readEntity(new GenericType<StringList>(StringList.class));
    HashMap<String, String> map = Util.stringListToMap(strList);

    Assert.assertEquals(map.get(newParamsKey), newParamsValue, "From Session Params Put");
    Assert.assertEquals(map.size(), 1, "Params List contains more than one param");
  }

  @Test(enabled = true)
  public void testSessionHDFSResourcePutNDelete() throws Exception {

    String path = hdfsJarPath + "/" + hiveUdfJar;
    sHelper.addResourcesJar(path);

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(createSleepFunction).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    sHelper.removeResourcesJar(path);

    queryHandle = (QueryHandle) qHelper.executeQuery(createSleepFunction).getData();
    lensQuery = qHelper.waitForCompletion(queryHandle);
    // TODO : Works only when there is single instance for each driver
//    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.FAILED);

  }


  @Test
  public void testSessionLocalResourcePutNDelete() throws Exception {

    String path = serverResourcePath + "/" + hiveUdfJar;
    sHelper.addResourcesJar(path);

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(createSleepFunction).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    sHelper.removeResourcesJar(path);

    queryHandle = (QueryHandle) qHelper.executeQuery(createSleepFunction).getData();
    lensQuery = qHelper.waitForCompletion(queryHandle);
//  TODO : Works only when there is single instance for each driver
//  Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.FAILED);

  }

  @Test
  public void testListResources() throws Exception {

    String path = serverResourcePath + "/" + hiveUdfJar;
    sHelper.addResourcesJar(path);

    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = lens.sendQuery("get", SessionURL.SESSION_LIST_RESOURCE_URL, query);
    AssertUtil.assertSucceededResponse(response);
    StringList responseString = response.readEntity(StringList.class);
    List<String> jars = responseString.getElements();
    for (String t : jars) {
      Assert.assertTrue(t.contains(hiveUdfJar));
    }
  }


  @Test
  public void testSessionGone() throws Exception {

    String newSession = sHelper.openSession("test", "test");
    sHelper.closeSession(newSession);

    MapBuilder query = new MapBuilder("sessionid", newSession);

    // Get Session resources with closed session
    Response response = lens.sendQuery("get", SessionURL.SESSION_LIST_RESOURCE_URL, query);
    AssertUtil.assertGone(response);

    // Get Session params with closd session
    response = lens.sendQuery("get", SessionURL.SESSION_PARAMS_URL, query);
    AssertUtil.assertGone(response);

    //Setting DB with closed session Handle
    response = lens.exec("post", MetastoreURL.METASTORE_DATABASES_URL, servLens,
        null, query, MediaType.APPLICATION_XML_TYPE, null, lens.getCurrentDB());
    AssertUtil.assertGone(response);

    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", newSession);
    formData.add("query", QueryInventory.QUERY);
    formData.add("conf", "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><conf />");

    //Explain Query with closed session Handle
    formData.add("operation", "EXPLAIN");
    response = lens.exec("post", QueryURL.QUERY_URL, servLens, null, null,
        MediaType.MULTIPART_FORM_DATA_TYPE, MediaType.APPLICATION_XML, formData.getForm());
    AssertUtil.assertGone(response);

    //Execute Query with closed session Handle
    formData.add("operation", "EXECUTE");
    response = lens.exec("post", QueryURL.QUERY_URL, servLens, null, null,
        MediaType.MULTIPART_FORM_DATA_TYPE, MediaType.APPLICATION_XML, formData.getForm());
    AssertUtil.assertGone(response);

  }

  @Test
  public void testOpenSessionWithDB() throws Exception {

    String newDb = "opensessionwithdb";
    mHelper.createDatabase(newDb);
    String newSession = sHelper.openSession("test", "test", newDb);
    String curDB = mHelper.getCurrentDatabase(newSession);
    Assert.assertEquals(curDB, newDb, "Could not open session with passed db");
    sHelper.closeSession(newSession);
    //TODO : Enable when drop table is fixed
//    mHelper.dropDatabase(newDb);
  }

  @Test
  public void testOpenSessionDefault() throws Exception {

    String newSession = sHelper.openSession("test", "test");
    String curDB = mHelper.getCurrentDatabase(newSession);
    Assert.assertEquals(curDB, "default", "Could not open session with passed db");
    sHelper.closeSession(newSession);
  }


  @Test
  public void testOpenSessionDBDoesnotExist() throws Exception {

    Response response = sHelper.openSessionReturnResponse("test", "test", "dbdoesnotexist", null);
    AssertUtil.assertNotFound(response);
  }

  @Test
  public void testSessionDBChange() throws Exception {

    String newDb = "opensessionwithdb";
    String newDb1 = "opensessionwithdb1";
    mHelper.createDatabase(newDb);
    mHelper.createDatabase(newDb1);

    String newSession = sHelper.openSession("test", "test", newDb);
    String curDB = mHelper.getCurrentDatabase(newSession);
    Assert.assertEquals(curDB, newDb, "Could not open session with passed db");

    mHelper.setCurrentDatabase(newSession, newDb1);
    curDB = mHelper.getCurrentDatabase(newSession);
    Assert.assertEquals(curDB, newDb1, "Could not open session with passed db");

    sHelper.closeSession(newSession);
    //TODO : Enable when drop table issue is fixed
//    mHelper.dropDatabase(newDb);
//    mHelper.dropDatabase(newDb1);
  }

  //Fails as closeSession cannot take json as input,. (No API can take json as input)
  @Test(enabled = false)
  public void testGetSessionJson() throws Exception {

    String newSessionHandle = sHelper.openSession("diff", "diff", null, MediaType.APPLICATION_JSON);
    Assert.assertNotNull(newSessionHandle);
    Assert.assertFalse(newSessionHandle.isEmpty());
    sHelper.closeSession(newSessionHandle, MediaType.APPLICATION_JSON);
  }

  @Test(enabled = true)
  public void assertSucceededResponse() throws Exception {
    String session = sHelper.openSession("diff", "diff", null, MediaType.APPLICATION_XML);
    Assert.assertNotNull(session);
    Assert.assertFalse(session.isEmpty());

    MapBuilder query = new MapBuilder("sessionid", session);
    Response response = lens.exec("delete", SessionURL.SESSION_BASE_URL, servLens, null, query, null,
        MediaType.APPLICATION_JSON, null);
    AssertUtil.assertSucceededResponse(response);
  }


  @Test(enabled = true)
  public void listSessionTest() throws Exception {

    int origSize = sHelper.getSessionList().size();
    for(int i=1; i<4; i++) {
      sHelper.openSession("u" + i, "p" + i);
    }
    List<UserSessionInfo> sessionList = sHelper.getSessionList();
    Assert.assertEquals(sessionList.size(), origSize+3);
  }

  //TODO : enable when session handle returned is entire xml instead of just public id
  @Test(enabled = false)
  public void listSessionUserSessionInfoVerification() throws Exception {

    List<UserSessionInfo> sessionList = sHelper.getSessionList();
    for(UserSessionInfo u : sessionList){
      System.out.println(u.toString() + "\n");
      sHelper.closeSession(u.getHandle());
    }

    String session1 = sHelper.openSession("u1", "p1", lens.getCurrentDB());
    String session2 = sHelper.openSession("u2", "p2", lens.getCurrentDB());

    QueryHandle qh1 = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_DIM_QUERY, null, session1).getData();
    QueryHandle qh2 = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_DIM_QUERY, null, session2).getData();

    sessionList = sHelper.getSessionList();
    Assert.assertEquals(sessionList.size(), 2);

    UserSessionInfo s1 = sessionList.get(0);
    Assert.assertEquals(s1.getUserName(), "u1");
    List<QueryHandle> queryHandleList = s1.getActiveQueries();
    Assert.assertEquals(queryHandleList.size(), 2);
    Assert.assertTrue(queryHandleList.contains(qh1));
    Assert.assertTrue(queryHandleList.contains(qh2));

    for(UserSessionInfo u : sessionList){
      System.out.println(u.toString() + "\n");
      sHelper.closeSession(u.getHandle());
    }
  }

  //LENS-1199
  @Test(enabled = true)
  public void multipleCloseSessionForActiveQueries() throws Exception {
    String session = sHelper.openSession("diff", "diff", lens.getCurrentDB());
    QueryHandle qh = (QueryHandle) qHelper.executeQuery(QueryInventory.getSleepQuery("3"), null, session).getData();
    sHelper.closeSession(session);
    sHelper.closeSession(session);
  }

  //LENS-1199
  @Test(enabled = true)
  public void multipleCloseSession() throws Exception {
    String session = sHelper.openSession("diff", "diff", lens.getCurrentDB());
    sHelper.closeSession(session);
    MapBuilder query = new MapBuilder("sessionid", session);
    Response response = lens.exec("delete", SessionURL.SESSION_BASE_URL, servLens, null, query);
    AssertUtil.assertGone(response);
  }

}

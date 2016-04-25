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
import org.apache.lens.regression.core.constants.MetastoreURL;
import org.apache.lens.regression.core.constants.QueryInventory;
import org.apache.lens.regression.core.constants.SessionURL;
import org.apache.lens.regression.core.helpers.LensServerHelper;
import org.apache.lens.regression.core.helpers.MetastoreHelper;
import org.apache.lens.regression.core.helpers.QueryHelper;
import org.apache.lens.regression.core.helpers.ServiceManagerHelper;
import org.apache.lens.regression.core.helpers.SessionHelper;
import org.apache.lens.regression.core.testHelper.BaseTestClass;
import org.apache.lens.regression.core.type.FormBuilder;
import org.apache.lens.regression.core.type.MapBuilder;
import org.apache.lens.regression.util.AssertUtil;
import org.apache.lens.regression.util.HadoopUtil;
import org.apache.lens.regression.util.Util;
import org.apache.lens.server.api.error.LensException;

import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.*;

import com.jcraft.jsch.JSchException;

public class SessionResourceTests extends BaseTestClass {

  WebTarget servLens;
  private String sessionHandleString;

  LensServerHelper lens = getLensServerHelper();
  MetastoreHelper mHelper = getMetastoreHelper();
  SessionHelper sHelper = getSessionHelper();
  QueryHelper qHelper = getQueryHelper();

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

    HadoopUtil.uploadJars(localJarPath + "/" + hiveUdfJar, hdfsJarPath);
    logger.info("Creating Local Resource Directory in Server Machine");
    Util.runRemoteCommand("mkdir -p " + serverResourcePath);
    String path = hdfsJarPath + "/" + hiveUdfJar;
    logger.info("Copying " + path + " to Local Resource Directory in Server Machine");
    Util.runRemoteCommand("hadoop fs -get " + path + " " + serverResourcePath);
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

  public boolean checkSessionParamMap(String sessionHandle)  throws Exception {
    MapBuilder query = new MapBuilder("sessionid", sessionHandle, "verbose", "true");
    Response response = lens.sendQuery("get", SessionURL.SESSION_PARAMS_URL, query);
    AssertUtil.assertSucceededResponse(response);
    StringList strList = response.readEntity(new GenericType<StringList>(StringList.class));
    HashMap<String, String> map = Util.stringListToMap(strList);
    if (map == null){
      return false;
    }
    return true;
  }


  @Test
  public void testSessionGet() throws Exception {
    Response response = lens.exec("get", "/session", servLens, null, null);
    AssertUtil.assertSucceededResponse(response);
  }


  @Test
  public void testSessionPostDelete() throws Exception {

    Map<String, String> resource = new HashMap<String, String>();
    resource.put("username", lens.getUserName());
    resource.put("password", lens.getPassword());

    FormBuilder formData = new FormBuilder(resource);
    Response response = lens.sendForm("post", SessionURL.SESSION_BASE_URL, formData);
    AssertUtil.assertSucceededResponse(response);
    String sessionHandle = response.readEntity(String.class);
    Assert.assertTrue(checkSessionParamMap(sessionHandle), "Session is Closed");

    MapBuilder query = new MapBuilder("sessionid", sessionHandle);
    response = lens.exec("delete", SessionURL.SESSION_BASE_URL, servLens, null, query);
    AssertUtil.assertSucceeded(response);
    Assert.assertFalse(checkSessionParamMap(sessionHandle), "Session is Still Open");
  }

  @Test
  public void testSessionParamsGetVerbose()  throws Exception {
    Assert.assertTrue(checkSessionParamMap(sessionHandleString), "Returned Empty Params list");
  }


  @Test
  public void testSessionParamsGetNPut()  throws Exception {

    Map<String, String> resource = new HashMap<String, String>();
    resource.put("sessionid", sessionHandleString);
    resource.put("key", newParamsKey);
    resource.put("value", newParamsValue);

    FormBuilder formData = new FormBuilder(resource);
    Response response = lens.sendForm("put", SessionURL.SESSION_PARAMS_URL, formData);
    AssertUtil.assertSucceeded(response);

    MapBuilder query = new MapBuilder("sessionid", sessionHandleString, "key", "datanucleus.autoCreateSchema");

    response = lens.sendQuery("get", SessionURL.SESSION_PARAMS_URL, query);
    AssertUtil.assertSucceededResponse(response);

    StringList strList = response.readEntity(new GenericType<StringList>(StringList.class));
    HashMap<String, String> map = Util.stringListToMap(strList);

    Assert.assertEquals(map.get("datanucleus.autoCreateSchema"),
        newParamsValue, "From Session Params Put");
    Assert.assertEquals(map.size(), 1, "Params List contains more than one param");
  }

  //Negative Test Case
  @Test
  public void testSessionGetUndefinedParams()  throws Exception {

    String undefinedParamsKey = "test123";
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString, "key", undefinedParamsKey);
    Response response = lens.sendQuery("get", SessionURL.SESSION_PARAMS_URL, query);
    AssertUtil.assertSucceededResponse(response);
    StringList strList = response.readEntity(new GenericType<StringList>(StringList.class));
    HashMap<String, String> map = Util.stringListToMap(strList);
    Assert.assertNull(map, "Get should have returned empty params list, but didnt");
  }

/*
 * Testing if Session is restored after server restart
 */

  @Test
  public void testSessionRestore() throws Exception {
    Map<String, String> resource = new HashMap<String, String>();
    resource.put("sessionid", sessionHandleString);
    resource.put("key", newParamsKey);
    resource.put("value", newParamsValue);

    FormBuilder formData = new FormBuilder(resource);
    Response response = lens.sendForm("put", SessionURL.SESSION_PARAMS_URL, formData);
    AssertUtil.assertSucceeded(response);

    lens.restart();

    MapBuilder query = new MapBuilder("sessionid", sessionHandleString, "key", "datanucleus.autoCreateSchema");
    response = lens.sendQuery("get", SessionURL.SESSION_PARAMS_URL, query);
    AssertUtil.assertSucceededResponse(response);

    StringList strList = response.readEntity(new GenericType<StringList>(StringList.class));
    HashMap<String, String> map = Util.stringListToMap(strList);

    Assert.assertEquals(map.get("datanucleus.autoCreateSchema"),
        newParamsValue, "From Session Params Put");
    Assert.assertEquals(map.size(), 1, "Params List contains more than one param");
  }


  @Test
  public void testSessionHDFSResourcePutNDelete()  throws Exception {

    String path = hdfsJarPath + "/" + hiveUdfJar;
    sHelper.addResourcesJar(path);

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(createSleepFunction).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
    Assert.assertEquals(lensQuery.getStatus().getStatusMessage().trim(),
        "Query is successful!", "Query did not succeed");

    sHelper.removeResourcesJar(path);

    queryHandle = (QueryHandle) qHelper.executeQuery(createSleepFunction).getData();
    lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.FAILED);
    Assert.assertNotEquals(lensQuery.getStatus().getStatusMessage().trim(),
        "Query is successful!", "Query Should have Failed but it Passed");
  }


  @Test
  public void testSessionLocalResourcePutNDelete()  throws Exception {

    String path = serverResourcePath + "/" + hiveUdfJar;
    sHelper.addResourcesJar(path);

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(createSleepFunction).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
    Assert.assertEquals(lensQuery.getStatus().getStatusMessage().trim(),
        "Query is successful!", "Query did not succeed");

    sHelper.removeResourcesJar(path);

    queryHandle = (QueryHandle) qHelper.executeQuery(createSleepFunction).getData();
    lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.FAILED);
    Assert.assertNotEquals(lensQuery.getStatus().getStatusMessage().trim(),
        "Query is successful!", "Query Should have Failed but it Passed");

  }

  @Test
  public void testListResources()  throws Exception {

    String path = serverResourcePath + "/" + hiveUdfJar;
    sHelper.addResourcesJar(path);

    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = lens.sendQuery("get", SessionURL.SESSION_LIST_RESOURCE_URL, query);
    logger.info(response);
    AssertUtil.assertSucceededResponse(response);
    StringList responseString = response.readEntity(StringList.class);
    List<String> jars = responseString.getElements();
    for(String t : jars) {
      Assert.assertTrue(t.contains(hiveUdfJar));
    }
    System.out.println(responseString);
  }


  @Test
  public void testSessionGone()  throws Exception {
    String newSession = sHelper.openNewSession("test", "test");
    sHelper.closeNewSession(newSession);
    MapBuilder query = new MapBuilder("sessionid", newSession);

    // Get Session Params with closed session
    Response response = lens.sendQuery("get", SessionURL.SESSION_LIST_RESOURCE_URL, query);
    logger.info(response);
    AssertUtil.assertGoneResponse(response);

    //Setting DB with closed session Handle
    response = lens.exec("post", MetastoreURL.METASTORE_DATABASES_URL, servLens,
        null, query, MediaType.APPLICATION_XML_TYPE, null, lens.getCurrentDB());
    AssertUtil.assertGoneResponse(response);

    //Explain Query with closed session Handle
    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", newSession);
    formData.add("query", QueryInventory.QUERY);
    formData.add("operation", "EXPLAIN");
    formData.add("conf", "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><conf />");
    response = lens.exec("post", "/queryapi/queries", servLens, null, null,
        MediaType.MULTIPART_FORM_DATA_TYPE, MediaType.APPLICATION_XML, formData.getForm());
    AssertUtil.assertGoneResponse(response);

    //Execute Query with closed session Handle
    formData = new FormBuilder();
    formData.add("sessionid", newSession);
    formData.add("query", QueryInventory.QUERY);
    formData.add("operation", "EXECUTE");
    formData.add("conf", "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><conf />");
    response = lens.exec("post", "/queryapi/queries", servLens, null, null,
        MediaType.MULTIPART_FORM_DATA_TYPE, MediaType.APPLICATION_XML, formData.getForm());
    AssertUtil.assertGoneResponse(response);

  }

  @Test
  public void testOpenSessionWithDB()  throws Exception {

    String newDb = "opensessionwithdb";
    mHelper.createDatabase(newDb);
    String newSession = sHelper.openNewSession("test", "test", newDb);
    String curDB = mHelper.getCurrentDatabase(newSession);
    Assert.assertEquals(curDB, newDb, "Could not open session with passed db");
    sHelper.closeNewSession(newSession);
    mHelper.dropDatabase(newDb);
  }

  @Test
  public void testOpenSessionDefault()  throws Exception {

    String newSession = sHelper.openNewSession("test", "test");
    String curDB = mHelper.getCurrentDatabase(newSession);
    Assert.assertEquals(curDB, "default", "Could not open session with passed db");
    sHelper.closeNewSession(newSession);
  }


  @Test
  public void testOpenSessionDBDoesnotExist()  throws Exception {

    FormBuilder formData = new FormBuilder();
    formData.add("username", "test");
    formData.add("password", "test");
    formData.add("database", "dbdoesnotexist");

    Response response = mHelper.exec("post", "/session", servLens, null, null,
        MediaType.MULTIPART_FORM_DATA_TYPE, MediaType.APPLICATION_XML, formData.getForm());
    AssertUtil.assertFailedResponse(response);

  }

  @Test
  public void testDBCeption()  throws Exception {

    String newDb = "opensessionwithdb";
    String newDb1 = "opensessionwithdb1";
    mHelper.createDatabase(newDb);
    mHelper.createDatabase(newDb1);

    String newSession = sHelper.openNewSession("test", "test", newDb);
    String curDB = mHelper.getCurrentDatabase(newSession);
    Assert.assertEquals(curDB, newDb, "Could not open session with passed db");

    mHelper.setCurrentDatabase(newSession, newDb1);
    curDB = mHelper.getCurrentDatabase(newSession);
    Assert.assertEquals(curDB, newDb1, "Could not open session with passed db");
    sHelper.closeNewSession(newSession);
    mHelper.dropDatabase(newDb);
    mHelper.dropDatabase(newDb1);
  }

}

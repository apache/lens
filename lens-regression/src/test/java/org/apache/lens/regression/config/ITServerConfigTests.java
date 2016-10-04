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

package org.apache.lens.regression.config;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


import org.apache.lens.api.StringList;
import org.apache.lens.api.error.LensHttpStatus;
import org.apache.lens.api.query.InMemoryQueryResult;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
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
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.util.LensUtil;

import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.*;

import com.jcraft.jsch.JSchException;


public class ITServerConfigTests extends BaseTestClass {

  private WebTarget servLens;
  private String sessionHandleString;

  private final String confFilePath = lens.getServerDir() + "/conf/lens-site.xml";
  private final String backupConfFilePath = lens.getServerDir() + "/conf/backup-lens-site.xml";
  private static String lensKillCmd = "ps -ef | grep -i lens | grep -v lens-client | grep -v \"grep\" | "
      + "awk '{print $2}' | xargs -L1 kill -9";

  private static Logger logger = Logger.getLogger(ITServerConfigTests.class);


  @BeforeClass(alwaysRun = true)
  public void initialize() throws IOException, JSchException {
    servLens = ServiceManagerHelper.init();
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp(Method method) throws Exception {
    logger.info("Test Name: " + method.getName());
    Util.runRemoteCommand("cp " + confFilePath + " " + backupConfFilePath);
    sessionHandleString = sHelper.openSession(lens.getCurrentDB());
  }


  @AfterMethod(alwaysRun = true)
  public void restoreConfig() throws JSchException, IOException, LensException, InterruptedException {
    logger.info("Executing after method\n");
    Util.runRemoteCommand("cp " + backupConfFilePath + " " + confFilePath);
    lens.restart();
    if (sessionHandleString != null){
      sHelper.closeSession();
    }
  }


  // Session is not getting timedout,
  // Opened JIRA lens-295

  @Test(enabled = false)
  public void testServerSessionTimeoutSeconds() throws Exception {

    String sessionHandle = null;
    try {
      HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.SESSION_TIMEOUT_SECONDS, "30");
      Util.changeConfig(map, confFilePath);
      lens.restart();

      sessionHandle = sHelper.openSession("user", "pass");
      sHelper.setAndValidateParam(sessionHandle, LensConfConstants.SESSION_CLUSTER_USER, "test");

      // Waiting for session timeout
      Thread.sleep(40000);

      MapBuilder query = new MapBuilder("sessionid", sessionHandle, "key", LensConfConstants.SESSION_CLUSTER_USER);
      Response response = lens.sendQuery("get", SessionURL.SESSION_PARAMS_URL, query);
      Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());

    } finally {
      if (sessionHandle!=null) {
        sHelper.closeSession(sessionHandle);
      }
    }
  }

  /*
   * for "lens.server.restart.enabled" and "lens.server.recover.onrestart"
  */


  @DataProvider(name = "boolean_values")
  public Object[][] data() {
    String[][] testData = {{"true"}, {"false"}};
    return testData;
  }

  @Test(enabled = true, dataProvider = "boolean_values")
  public void testServerRestartEnabled(String restartEnabled) throws Exception {

    HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.SERVER_STATE_PERSISTENCE_ENABLED,
        restartEnabled);
    Util.changeConfig(map, confFilePath);
    lens.restart();

    String session = sHelper.openSession("user", "pass");
    lens.restart();

    MapBuilder query = new MapBuilder("sessionid", session);
    Response response = lens.sendQuery("get", SessionURL.SESSION_PARAMS_URL, query);

    if (restartEnabled.equalsIgnoreCase("true")) {
      Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
      sHelper.closeSession(session);
    } else {
      Assert.assertEquals(response.getStatus(), Response.Status.GONE.getStatusCode());
    }
  }

  /*
   * Test for Property lens.server.snapshot.interval
  */

  @Test(enabled = true)
  public void testSnapshotInterval() throws Exception {

    String sessionHandle = null;
    try {
      HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.SERVER_STATE_PERSISTENCE_INTERVAL_MILLIS,
          "10000");
      Util.changeConfig(map, confFilePath);
      lens.restart();

      sessionHandle = sHelper.openSession("user", "pass");
      sHelper.setAndValidateParam(sessionHandle, LensConfConstants.SESSION_CLUSTER_USER, "test");
      //Waiting for snapshot interval time
      Thread.sleep(11000);
      Util.runRemoteCommand(lensKillCmd);  //killing so that lens shouldn't have gracefull shutdown
      lens.restart();

      String value = sHelper.getSessionParam(sessionHandle, LensConfConstants.SESSION_CLUSTER_USER);
      Assert.assertEquals(value, "test");

    } finally {
      if (sessionHandle != null) {
        sHelper.closeSession(sessionHandle);
      }
    }

  }

  /*
  * Negative Test for Property lens.server.snapshot.interval
  */


  @Test(enabled = true)
  public void negativeTestSnapshotInterval() throws Exception {

    HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.SERVER_STATE_PERSISTENCE_INTERVAL_MILLIS,
        "50000");
    Util.changeConfig(map, confFilePath);
    lens.restart();

    String sessionHandle = sHelper.openSession("user", "pass");
    sHelper.setAndValidateParam(sessionHandle, LensConfConstants.SESSION_CLUSTER_USER, "test");

    //killing so that lens is not stopped gracefully.
    Util.runRemoteCommand(lensKillCmd);
    lens.restart();

    MapBuilder query = new MapBuilder("sessionid", sessionHandle, "key", LensConfConstants.SESSION_CLUSTER_USER);
    Response response = lens.sendQuery("get", SessionURL.SESSION_PARAMS_URL, query);
    Assert.assertEquals(response.getStatus(), Response.Status.GONE.getStatusCode(), "Snapshot interval test failed");
  }


  /*
   * Test for Property lens.server.persist.location
  */

  @DataProvider(name = "location_provider")
  public Object[][] locationProvider() {
    String[][] locations = {{"file:///tmp/lensserver"}, {lens.getServerHdfsUrl() + "/tmp/lensserver"}};
    return locations;
  }

  @Test(enabled = true, dataProvider = "location_provider")
  public void testSessionPersistLocation(String location) throws Exception {

    String sessionHandle = null;
    try {
      HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.SERVER_STATE_PERSIST_LOCATION, location);
      Util.changeConfig(map, confFilePath);
      lens.restart();

      sessionHandle = sHelper.openSession("user", "pass");
      sHelper.setAndValidateParam(sessionHandle, LensConfConstants.SESSION_CLUSTER_USER, "test");

      lens.restart();

      String value = sHelper.getSessionParam(sessionHandle, LensConfConstants.SESSION_CLUSTER_USER);
      Assert.assertEquals(value, "test");

    } finally {
      if (sessionHandle != null) {
        sHelper.closeSession(sessionHandle);
      }
    }
  }


  @DataProvider(name = "query_provider")
  public Object[][] queryProvider() {
    String[][] query = {{QueryInventory.HIVE_CUBE_QUERY}, {QueryInventory.JDBC_CUBE_QUERY}};
    return query;
  }


  /*
   * Test for Property lens.server.mode=READ_ONLY
  */

  //This is failing
  @Test(enabled = false)
  public void testServerModeReadOnly() throws Exception {

    HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.SERVER_MODE, "READ_ONLY");
    Util.changeConfig(map, confFilePath);
    lens.restart();

    sHelper.setAndValidateParam(sessionHandleString, LensConfConstants.SESSION_CLUSTER_USER, "test");

    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", sessionHandleString);
    formData.add("query", QueryInventory.QUERY);
    formData.add("operation", "EXECUTE");
    formData.add("conf", "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><conf />");
    Response response = lens.sendForm("post", QueryURL.QUERY_URL, formData);
    Assert.assertEquals(response.getStatus(), Response.Status.METHOD_NOT_ALLOWED.getStatusCode());

    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    response = lens.sendQuery("get", QueryURL.QUERYAPI_BASE_URL, query);
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

  }

  /*
  *  Test for Property lens.server.mode=METASTORE_READONLY,METASTORE_NODROP,OPEN
  */


  @Test(enabled = true)
  public void testServerMode() throws Exception {

    String newDb = "TestMetastoreService_testDb1";

    HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.SERVER_MODE, "METASTORE_READONLY");
    Util.changeConfig(map, confFilePath);
    lens.restart();

    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = mHelper.exec("post", MetastoreURL.METASTORE_DATABASES_URL, servLens,
        null, query, MediaType.APPLICATION_XML_TYPE, null, newDb);
    Assert.assertEquals(response.getStatus(), Response.Status.METHOD_NOT_ALLOWED.getStatusCode());

    map.put(LensConfConstants.SERVER_MODE, "METASTORE_NODROP");
    Util.changeConfig(map, confFilePath);
    lens.restart();

    response = mHelper.exec("post", MetastoreURL.METASTORE_DATABASES_URL, servLens,
        null, query, MediaType.APPLICATION_XML_TYPE, null, newDb);
    AssertUtil.assertSucceededResponse(response);
    StringList allDb = mHelper.listDatabases();
    Assert.assertTrue(allDb.getElements().contains(newDb.toLowerCase()), "Unable to Create DB");

    query.put("cascade", "true");
    response = mHelper.exec("delete", MetastoreURL.METASTORE_DATABASES_URL + "/" + newDb, servLens,
        null, query, MediaType.APPLICATION_XML_TYPE, null);
    Assert.assertEquals(response.getStatus(), Response.Status.METHOD_NOT_ALLOWED.getStatusCode());

    map.put(LensConfConstants.SERVER_MODE, "OPEN");
    Util.changeConfig(map, confFilePath);
    lens.restart();

    //TODO : Enable this when delete db issue is fixed
/*    response = mHelper.exec("delete", MetastoreURL.METASTORE_DATABASES_URL + "/" + newDb, servLens,
        null, query, MediaType.APPLICATION_XML_TYPE, null);
    AssertUtil.assertSucceededResponse(response);
    allDb = mHelper.listDatabases();
    Assert.assertFalse(allDb.getElements().contains(newDb.toLowerCase()), "Unable to Create DB");*/

  }


  /*
  * Test for Lens statistics related Properties
  */

  //TODO : Need to implement this correctly
  //Rollover value is being ignored
  @Test(enabled = false)
  public void testLensStatistics() throws Exception {

    try {
      HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.STATS_ROLLUP_SCAN_RATE, "60000",
          LensConfConstants.STATISTICS_DATABASE_KEY, "stats",
          LensConfConstants.STATISTICS_WAREHOUSE_KEY, lens.getServerHdfsUrl() + "/tmp/lens/statistics/warehouse");
      Util.changeConfig(map, confFilePath);
      lens.restart();

      FormBuilder formData = new FormBuilder();
      formData.add("sessionid", sessionHandleString);
      formData.add("type", "jar");
      formData.add("path", "file:///usr/local/lens/webapp/lens-server/WEB-INF/lib/lens-query-lib-1.2.3-SNAPSHOT.jar");
      String time = String.valueOf(System.currentTimeMillis());
      Response response = lens.sendForm("put", "/session/resources/add", formData);
      AssertUtil.assertSucceededResponse(response);

      QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.QUERY).getData();
      LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
      Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

      QueryHandle queryHandle1 = (QueryHandle) qHelper.executeQuery(QueryInventory.WRONG_QUERY).getData();
      LensQuery lensQuery1 = qHelper.waitForCompletion(queryHandle1);

      Thread.sleep(120000);
      mHelper.setCurrentDatabase("stats");

      QueryHandle statsQuery = (QueryHandle) qHelper.executeQuery("select handle from stats.queryexecutionstatistics")
          .getData();
      lensQuery = qHelper.waitForCompletion(statsQuery);
      Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

      InMemoryQueryResult resultSet = (InMemoryQueryResult) qHelper.getResultSet(statsQuery, "0", "100");
      for (int i = 0; i < resultSet.getRows().size(); i++) {
        logger.info(resultSet.getRows().get(i).toString());
      }
      Assert.assertTrue(resultSet.getRows().contains(queryHandle), "lensStats are not Saved");
      Assert.assertTrue(resultSet.getRows().contains(queryHandle1), "lensStats are not Saved");

    } finally {
      sHelper.closeSession();
    }
  }

  //TODO : Add for all possible combination of enablePersistentResultSet and enablePersistentResultSetInDriver

  /*
  * Test for Property lens.server.max.finished.queries for persistent result set
  */

  //This is failing
  @Test(enabled = true)
  public void testQueryResultRetention() throws Exception {

    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");

    HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.RESULTSET_PURGE_ENABLED, "true",
        LensConfConstants.RESULTSET_PURGE_INTERVAL_IN_SECONDS, "10",
        LensConfConstants.QUERY_RESULTSET_RETENTION, "20 sec",
        LensConfConstants.HDFS_OUTPUT_RETENTION, "60 min");

    Util.changeConfig(map, confFilePath);
    lens.restart();

    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_CUBE_QUERY).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    Response response = qHelper.exec("get", QueryURL.QUERY_URL + "/" + queryHandle.toString() + "/resultset",
        servLens, null, query);
    AssertUtil.assertSucceededResponse(response);

    Thread.sleep(40000);

    response = qHelper.exec("get", QueryURL.QUERY_URL + "/" + queryHandle.toString() + "/resultset",
        servLens, null, query);
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
  }


  /*
  * InMemoryResultSet should be purged after ttl time is over or its read once
  */

  @Test(enabled = true, dataProvider = "query_provider")
  public void testInMemoryPurger(String query) throws Exception {

    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "false");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_MAIL_NOTIFY, "false");

    HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.INMEMORY_RESULT_SET_TTL_SECS, "20",
        LensConfConstants.PURGE_INTERVAL, "10000"); //in millis
    Util.changeConfig(map, confFilePath);
    lens.restart();
    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(query).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    Response response = qHelper.getResultSetResponse(queryHandle, "0", "100", sessionHandleString);
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

    Thread.sleep(30000); //waiting till query gets purged ( ttl + purge interval time)

    response = qHelper.getResultSetResponse(queryHandle, "0", "100", sessionHandleString);
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
  }

  // Behaviour is not the same for hive query before result is purged
  @Test(enabled = true)
  public void readInmemoryTwiceBeforePurgerTime() throws Exception {

    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "false");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_MAIL_NOTIFY, "false");

    HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.INMEMORY_RESULT_SET_TTL_SECS, "500",
        LensConfConstants.PURGE_INTERVAL, "10000");
    Util.changeConfig(map, confFilePath);
    lens.restart();

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_CUBE_QUERY).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");

    Response response = qHelper.getResultSetResponse(queryHandle, "0", "100", sessionHandleString);
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

    response = qHelper.getResultSetResponse(queryHandle, "0", "100", sessionHandleString);

    //TODO : enable this when LENS-823 is fixed
    //Currently its throwing 500 which needs to be fixed.
//    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
  }


  //LENS-833
  @Test(enabled = true)
  public void testMaxSessionPerUser() throws Exception {

    String user = "test", pwd = "test";
    List<String> sessionList = new ArrayList<String>();
    int maxSession = 3;

    try {
      HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.MAX_SESSIONS_PER_USER,
          Integer.toString(maxSession));
      Util.changeConfig(map, confFilePath);
      lens.restart();

      for (int i = 1; i <= maxSession; i++) {
        sessionList.add(sHelper.openSession(user, pwd, lens.getCurrentDB()));
      }

      Response response = sHelper.openSessionReturnResponse(user, pwd, lens.getCurrentDB(), null);
      Assert.assertEquals(response.getStatus(), LensHttpStatus.TOO_MANY_REQUESTS.getStatusCode());

    } finally {
      for (String session : sessionList) {
        if (session != null) {
          sHelper.closeSession(session);
        }
      }
    }
  }

  @Test(enabled = true)
  public void testMaxSessionPerUserDelete() throws Exception {

    String user1 = "test1", user2 = "test2";
    String pwd1 = "test1", pwd2 = "test2";

    List<String> sessionList1 = new ArrayList<String>();
    List<String> sessionList2 = new ArrayList<String>();
    int maxSession = 5;

    try {

      HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.MAX_SESSIONS_PER_USER,
          Integer.toString(maxSession));
      Util.changeConfig(map, confFilePath);
      lens.restart();

      for (int i = 0; i < maxSession; i++) {
        sessionList1.add(sHelper.openSession(user1, pwd1, lens.getCurrentDB()));
        sessionList2.add(sHelper.openSession(user2, pwd2, lens.getCurrentDB()));
      }

      for (int i = 0; i < maxSession; i++) {

        Response response1 = sHelper.openSessionReturnResponse(user1, pwd1, null, null);
        Response response2 = sHelper.openSessionReturnResponse(user2, pwd2, null, null);

        Assert.assertEquals(response1.getStatus(), LensHttpStatus.TOO_MANY_REQUESTS.getStatusCode());
        Assert.assertEquals(response2.getStatus(), LensHttpStatus.TOO_MANY_REQUESTS.getStatusCode());

        sHelper.closeSession(sessionList1.remove(1));
        sHelper.closeSession(sessionList2.remove(1));

        sessionList1.add(sHelper.openSession(user1, pwd1, lens.getCurrentDB()));
        sessionList2.add(sHelper.openSession(user2, pwd2, lens.getCurrentDB()));
      }

    }finally {
      for (int i = 0; i < sessionList1.size(); i++) {
        sHelper.closeSession(sessionList1.get(i));
        sHelper.closeSession(sessionList2.get(i));
      }
    }
  }



}


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
package org.apache.lens.server;

import static org.apache.lens.server.LensServerTestUtil.createTable;
import static org.apache.lens.server.LensServerTestUtil.loadData;
import static org.apache.lens.server.api.user.MockDriverQueryHook.*;
import static org.apache.lens.server.common.RestAPITestUtil.*;

import static org.testng.Assert.*;

import java.io.*;
import java.util.*;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.lens.api.*;
import org.apache.lens.api.APIResult.Status;
import org.apache.lens.api.query.*;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.driver.hive.TestRemoteHiveDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.api.session.SessionService;
import org.apache.lens.server.api.util.LensUtil;
import org.apache.lens.server.common.LenServerTestException;
import org.apache.lens.server.common.LensServerTestFileUtils;
import org.apache.lens.server.common.TestResourceFile;
import org.apache.lens.server.query.QueryExecutionServiceImpl;
import org.apache.lens.server.query.TestQueryService;
import org.apache.lens.server.session.HiveSessionService;
import org.apache.lens.server.session.LensSessionImpl;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.Service;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.testng.Assert;
import org.testng.annotations.*;

import com.google.common.base.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * The Class TestServerRestart.
 */
@Test(alwaysRun = true, groups = "restart-test", dependsOnGroups = "unit-test")
@Slf4j
public class TestServerRestart extends LensAllApplicationJerseyTest {

  /** The data file. */
  private File dataFile;

  /**
   * No of valid hive drivers that can execute queries in this test class
   */
  private static final int NO_OF_HIVE_DRIVERS = 2;

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#setUp()
   */
  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public Map<String, String> getServerConfOverWrites() {
    return LensUtil.getHashMap("lens.server.state.persistence.interval.millis", "1000");
  }

  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @BeforeClass
  public void restartBeforeClass() throws Exception {
    // restart server with test configuration for tests
    restartLensServer(getServerConf());
  }

  @AfterClass
  public void restart() throws Exception {
    // restart server with normal configuration once the tests are done.
    restartLensServer();
  }
  /** The file created. */
  private boolean fileCreated;

  /** The nrows. */
  public static final int NROWS = 10000;

  /**
   * Creates the restart test data file.
   *
   * @throws FileNotFoundException the file not found exception
   */
  private void createRestartTestDataFile() throws FileNotFoundException {
    if (fileCreated) {
      return;
    }

    dataFile = new File(TestResourceFile.TEST_DATA_FILE.getValue());
    dataFile.deleteOnExit();

    PrintWriter dataFileOut = new PrintWriter(dataFile);
    for (int i = 0; i < NROWS; i++) {
      dataFileOut.println(i);
    }
    dataFileOut.flush();
    dataFileOut.close();
    fileCreated = true;
  }

  /**
   * Test query service.
   *
   * @throws InterruptedException the interrupted exception
   * @throws IOException          Signals that an I/O exception has occurred.
   * @throws LensException        the lens exception
   */
  @Test
  public void testQueryService() throws InterruptedException, IOException, LensException {
    log.info("Server restart test");

    QueryExecutionServiceImpl queryService = LensServices.get().getService(QueryExecutionService.NAME);
    Assert.assertTrue(queryService.getHealthStatus().isHealthy());

    LensSessionHandle lensSessionId = queryService.openSession("foo", "bar", new HashMap<String, String>());
    // Create data file
    createRestartTestDataFile();

    // Create a test table
    createTable("test_server_restart", target(), lensSessionId, defaultMT);
    loadData("test_server_restart", TestResourceFile.TEST_DATA_FILE.getValue(), target(), lensSessionId, defaultMT);
    log.info("Loaded data");

    // test post execute op
    List<QueryHandle> launchedQueries = new ArrayList<>();
    final int NUM_QUERIES = 10;

    boolean isQuerySubmitterPaused = false;
    QueryHandle handleForMockDriverQueryHookTest = null;
    for (int i = 0; i < NUM_QUERIES; i++) {
      if (!isQuerySubmitterPaused && i > NUM_QUERIES / 3) {
        // Kill the query submitter thread to make sure some queries stay in accepted queue
        try {
          queryService.pauseQuerySubmitter(true);
          log.info("Stopped query submitter");
          Assert.assertFalse(queryService.getHealthStatus().isHealthy());
        } catch (Exception exc) {
          log.error("Could not kill query submitter", exc);
        }
        isQuerySubmitterPaused = true;
      }

      final QueryHandle handle = executeAndGetHandle(target(), Optional.of(lensSessionId),
        Optional.of("select COUNT(ID) from test_server_restart"), Optional.<LensConf>absent(), defaultMT);
      LensQuery ctx = getLensQuery(target(), lensSessionId, handle, defaultMT);
      log.info("{} submitted query {} state: {}", i, handle, ctx.getStatus().getStatus());
      launchedQueries.add(handle);
      if (i == (NUM_QUERIES-1)) {
        //checking this only for one of the queued queries. A queued query has all the config information available in
        // server memory. (Some of the information is lost after query is purged)
        testMockDriverQueryHookPostDriverSelection(queryService, handle, false);
        handleForMockDriverQueryHookTest = handle;
        log.info("Testing query {} for MockDriverQueryHook", handleForMockDriverQueryHookTest);
      }
    }

    // Restart the server
    log.info("Restarting lens server!");
    restartLensServer(getServerConf(), true);
    log.info("Restarted lens server!");
    queryService = LensServices.get().getService(QueryExecutionService.NAME);
    Assert.assertFalse(queryService.getHealthStatus().isHealthy());
    testMockDriverQueryHookPostDriverSelection(queryService, handleForMockDriverQueryHookTest, true);
    queryService.pauseQuerySubmitter(false);
    Assert.assertTrue(queryService.getHealthStatus().isHealthy());

    // All queries should complete after server restart
    for (QueryHandle handle : launchedQueries) {
      log.info("Polling query {}", handle);
      try {
        PersistentQueryResult resultset = getLensQueryResult(target(), lensSessionId, handle, defaultMT);
        List<String> rows = TestQueryService.readResultSet(resultset, handle, true);
        assertEquals(rows.size(), 1);
        assertEquals(rows.get(0), "" + NROWS);
        log.info("Completed {}", handle);
      } catch (Exception exc) {
        log.error("Failed query {}", handle, exc);
        Assert.fail(exc.getMessage());
      }
    }
    log.info("End server restart test");
    LensServerTestUtil.dropTable("test_server_restart", target(), lensSessionId, defaultMT);
    queryService.closeSession(lensSessionId);
  }

  /**
   * Tests whether the driver configuration updated by mock query driver hook is
   * 1. updated in LensConf wherever applicable and
   * 2. is persisted and available even after server startup.
   *
   * @param queryService
   * @param handle
   * @param afterRestart
   */
  private void testMockDriverQueryHookPostDriverSelection(QueryExecutionServiceImpl queryService, QueryHandle handle,
    boolean afterRestart){
    QueryContext ctx = queryService.getQueryContext(handle);
    assertNotNull(ctx, "Make sure that the query has not  been purged");
    assertTrue(ctx.getStatus().queued(), "Make sure query is still in QUEUED state");
    LensConf lensQueryConf = queryService.getQueryContext(handle).getLensConf();
    Configuration driverConf = queryService.getQueryContext(handle).getSelectedDriverConf();

    assertEquals(driverConf.get(KEY_POST_SELECT), VALUE_POST_SELECT);
    assertEquals(lensQueryConf.getProperty(KEY_POST_SELECT), VALUE_POST_SELECT);

    if (afterRestart) {
      //This will be unavailable since if was not updated in LensConf by MockDriverQueryHook
      assertNull(driverConf.get(UNSAVED_KEY_POST_SELECT));
    } else {
      assertEquals(driverConf.get(UNSAVED_KEY_POST_SELECT), UNSAVED_VALUE_POST_SELECT);
    }
    assertNull(lensQueryConf.getProperty(UNSAVED_KEY_POST_SELECT));
  }

  /**
   * Test hive server restart.
   *
   * @throws Exception the exception
   */
  @Test
  public void testHiveServerRestart() throws Exception {
    QueryExecutionServiceImpl queryService = LensServices.get().getService(QueryExecutionService.NAME);
    Assert.assertTrue(queryService.getHealthStatus().isHealthy());

    LensSessionHandle lensSessionId = queryService.openSession("foo", "bar", new HashMap<String, String>());

    // set params
    setParams(lensSessionId);

    // Create data file
    createRestartTestDataFile();

    // Add a resource to check if its added after server restart.
    HiveSessionService sessionService = LensServices.get().getService(SessionService.NAME);
    Assert.assertTrue(sessionService.getHealthStatus().isHealthy());

    sessionService.addResource(lensSessionId, "FILE", dataFile.toURI().toString());
    log.info("@@ Added resource {}", dataFile.toURI());

    // Create a test table
    createTable("test_hive_server_restart", target(), lensSessionId, defaultMT);
    loadData("test_hive_server_restart", TestResourceFile.TEST_DATA_FILE.getValue(), target(),
      lensSessionId, defaultMT);
    log.info("Loaded data");

    log.info("Hive Server restart test");
    // test post execute op

    QueryHandle handle = executeAndGetHandle(target(), Optional.of(lensSessionId),
      Optional.of("select COUNT(ID) from test_hive_server_restart"), Optional.<LensConf>absent(), defaultMT);

    // wait for query to move out of QUEUED state
    LensQuery ctx = getLensQuery(target(), lensSessionId, handle, defaultMT);
    while (ctx.getStatus().queued()) {
      ctx = getLensQuery(target(), lensSessionId, handle, defaultMT);
      Thread.sleep(1000);
    }

    List<LensSessionImpl.ResourceEntry> sessionResources = queryService.getSession(lensSessionId)
      .getLensSessionPersistInfo().getResources();
    int[] restoreCounts = new int[sessionResources.size()];
    for (int i = 0; i < sessionResources.size(); i++) {
      restoreCounts[i] = sessionResources.get(i).getRestoreCount();
    }
    log.info("@@ Current counts {}", Arrays.toString(restoreCounts));
    // Restart hive server
    TestRemoteHiveDriver.stopHS2Service();

    // Wait for server to stop
    while (TestRemoteHiveDriver.getServerState() != Service.STATE.STOPPED) {
      log.info("Waiting for HS2 to stop. Current state {}", TestRemoteHiveDriver.getServerState());
      Thread.sleep(1000);
    }

    TestRemoteHiveDriver.createHS2Service();
    // Wait for server to come up
    while (Service.STATE.STARTED != TestRemoteHiveDriver.getServerState()) {
      log.info("Waiting for HS2 to start {}", TestRemoteHiveDriver.getServerState());
      Thread.sleep(1000);
    }
    Thread.sleep(10000);
    log.info("Server restarted");

    // Check params to be set
    verifyParamOnRestart(lensSessionId);

    // Poll for first query, we should not get any exception
    ctx = waitForQueryToFinish(target(), lensSessionId, handle, defaultMT);

    Assert.assertTrue(ctx.getStatus().finished());
    log.info("Previous query status: {}", ctx.getStatus().getStatusMessage());

    // After hive server restart, first few queries fail with Invalid Operation Handle followed by
    // Invalid Session Handle. Ideal behaviour is to fail with Invalid Session Handle immediately.
    // Jira Ticket raised for debugging: https://issues.apache.org/jira/browse/LENS-707

    final String query = "select COUNT(ID) from test_hive_server_restart";
    Response response = null;
    while (true) {
      response = execute(target(), Optional.of(lensSessionId), Optional.of(query), defaultMT);
      if (response != null) {
        LensAPIResult<QueryHandle> result = response.readEntity(new GenericType<LensAPIResult<QueryHandle>>() {});
        handle = result.getData();
        if (handle != null) {
          break;
        }
      }
      Thread.sleep(1000);
    }
    // Poll for second query, this should finish successfully
    ctx = waitForQueryToFinish(target(), lensSessionId, handle, defaultMT);
    log.info("Final status for {}: {}", handle, ctx.getStatus().getStatus());

    // Now we can expect that session resources have been added back exactly once
    for (int i = 0; i < sessionResources.size(); i++) {
      LensSessionImpl.ResourceEntry resourceEntry = sessionResources.get(i);
      //The restore count can vary based on How many Hive Drivers were able to execute the estimate on the query
      //successfully after Hive Server Restart.
      Assert.assertTrue((resourceEntry.getRestoreCount() > restoreCounts[i]
          && resourceEntry.getRestoreCount() <=  restoreCounts[i] + NO_OF_HIVE_DRIVERS),
          "Restore test failed for " + resourceEntry + " pre count=" + restoreCounts[i] + " post count=" + resourceEntry
              .getRestoreCount());
      log.info("@@ Latest count {}={}", resourceEntry, resourceEntry.getRestoreCount());
    }
    // Assert.assertEquals(stat.getStatus(), QueryStatus.Status.SUCCESSFUL,
    // "Expected to be successful " + handle);

    log.info("End hive server restart test");
    LensServerTestUtil.dropTable("test_hive_server_restart", target(), lensSessionId, defaultMT);
    queryService.closeSession(lensSessionId);
  }

  /**
   * Test session restart.
   *
   * @throws Exception the exception
   */
  @Test
  public void testSessionRestart() throws Exception {
    System.out.println("### Test session restart");

    // Create a new session
    WebTarget sessionTarget = target().path("session");
    FormDataMultiPart sessionForm = new FormDataMultiPart();
    sessionForm.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(), "foo"));
    sessionForm.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(), "bar"));
    sessionForm.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionconf").fileName("sessionconf")
      .build(), new LensConf(), defaultMT));

    final LensSessionHandle restartTestSession = sessionTarget.request(defaultMT).post(
      Entity.entity(sessionForm, MediaType.MULTIPART_FORM_DATA_TYPE), LensSessionHandle.class);
    Assert.assertNotNull(restartTestSession);

    // Set a param
    setParams(restartTestSession);
    // Add resource
    // add a resource
    final WebTarget resourcetarget = target().path("session/resources");
    final FormDataMultiPart mp1 = new FormDataMultiPart();
    mp1.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), restartTestSession,
      defaultMT));
    mp1.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("type").build(), "file"));
    mp1.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("path").build(),
      "target/test-classes/lens-site.xml"));
    APIResult result = resourcetarget.path("add").request(defaultMT)
      .put(Entity.entity(mp1, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    assertEquals(result.getStatus(), Status.SUCCEEDED);

    // restart server
    restartLensServer(getServerConf());

    // Check resources added again
    verifyParamOnRestart(restartTestSession);

    HiveSessionService sessionService = LensServices.get().getService(SessionService.NAME);
    Assert.assertTrue(sessionService.getHealthStatus().isHealthy());

    LensSessionImpl session = sessionService.getSession(restartTestSession);
    assertEquals(session.getLensSessionPersistInfo().getResources().size(), 1);
    LensSessionImpl.ResourceEntry resourceEntry = session.getLensSessionPersistInfo().getResources().get(0);
    assertEquals(resourceEntry.getType(), "FILE");
    Assert.assertTrue(resourceEntry.getUri().contains("target/test-classes/lens-site.xml"));
    Assert.assertTrue(resourceEntry.getLocation().contains("target/test-classes/lens-site.xml"));

    // close session
    result = sessionTarget.queryParam("sessionid", restartTestSession).request(defaultMT).delete(APIResult.class);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  private void setParams(LensSessionHandle lensSessionHandle) {
    FormDataMultiPart setpart = new FormDataMultiPart();
    setpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionHandle,
      defaultMT));
    setpart
      .bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("key").build(), "lens.session.testRestartKey"));
    setpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("value").build(), "myvalue"));
    APIResult result = target().path("session").path("params").request(defaultMT)
      .put(Entity.entity(setpart, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  private void verifyParamOnRestart(LensSessionHandle lensSessionHandle) {

    StringList sessionParams = target().path("session").path("params").queryParam("sessionid", lensSessionHandle)
      .queryParam("verbose", true).queryParam("key", "lens.session.testRestartKey").request(defaultMT)
      .get(StringList.class);
    System.out.println("Session params:" + sessionParams.getElements());
    assertEquals(sessionParams.getElements().size(), 1);
    Assert.assertTrue(sessionParams.getElements().contains("lens.session.testRestartKey=myvalue"));
  }

  @Test(dataProvider = "mediaTypeData")
  public void testServerMustRestartOnManualDeletionOfAddedResources(MediaType mt)
    throws IOException, LensException, LenServerTestException {

    /* Begin: Setup */

    /* Add a resource jar to current working directory */
    File jarFile = new File(TestResourceFile.TEST_RESTART_ON_RESOURCE_MOVE_JAR.getValue());
    FileUtils.touch(jarFile);

    /* Add the created resource jar to lens server */
    LensSessionHandle sessionHandle = LensServerTestUtil.openSession(target(), "foo", "bar", new LensConf(), mt);
    LensServerTestUtil.addResource(target(), sessionHandle, "jar", jarFile.getPath(), mt);

    /* Delete resource jar from current working directory */
    LensServerTestFileUtils.deleteFile(jarFile);

    /* End: Setup */

    /* Verification Steps: server should restart without exceptions */
    restartLensServer();
    HiveSessionService service = LensServices.get().getService(SessionService.NAME);
    service.closeSession(sessionHandle);
  }
}

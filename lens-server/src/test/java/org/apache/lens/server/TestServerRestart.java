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
import static org.apache.lens.server.common.RestAPITestUtil.execute;

import static org.testng.Assert.assertEquals;

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
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.api.session.SessionService;
import org.apache.lens.server.common.TestResourceFile;
import org.apache.lens.server.query.QueryExecutionServiceImpl;
import org.apache.lens.server.query.TestQueryService;
import org.apache.lens.server.session.HiveSessionService;
import org.apache.lens.server.session.LensSessionImpl;

import org.apache.hive.service.Service;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

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

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#tearDown()
   */
  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
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
    createTable("test_server_restart", target(), lensSessionId);
    loadData("test_server_restart", TestResourceFile.TEST_DATA_FILE.getValue(), target(), lensSessionId);
    log.info("Loaded data");

    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

    List<QueryHandle> launchedQueries = new ArrayList<QueryHandle>();
    final int NUM_QUERIES = 10;

    boolean killed = false;
    for (int i = 0; i < NUM_QUERIES; i++) {
      if (!killed && i > NUM_QUERIES / 3) {
        // Kill the query submitter thread to make sure some queries stay in accepted queue
        try {
          queryService.pauseQuerySubmitter();
          log.info("Stopped query submitter");
          Assert.assertFalse(queryService.getHealthStatus().isHealthy());
        } catch (Exception exc) {
          log.error("Could not kill query submitter", exc);
        }
        killed = true;
      }

      final FormDataMultiPart mp = new FormDataMultiPart();
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
        MediaType.APPLICATION_XML_TYPE));
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        "select COUNT(ID) from test_server_restart"));
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(),
        new LensConf(), MediaType.APPLICATION_XML_TYPE));
      final QueryHandle handle = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
        new GenericType<LensAPIResult<QueryHandle>>() {}).getData();

      Assert.assertNotNull(handle);
      LensQuery ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request()
        .get(LensQuery.class);
      QueryStatus stat = ctx.getStatus();
      log.info("{} submitted query {} state: {}", i, handle, ctx.getStatus().getStatus());
      launchedQueries.add(handle);
    }

    // Restart the server
    log.info("Restarting lens server!");
    restartLensServer();
    log.info("Restarted lens server!");
    queryService = LensServices.get().getService(QueryExecutionService.NAME);
    Assert.assertTrue(queryService.getHealthStatus().isHealthy());

    // All queries should complete after server restart
    for (QueryHandle handle : launchedQueries) {
      log.info("Polling query {}", handle);
      try {
        LensQuery ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request()
          .get(LensQuery.class);
        QueryStatus stat = ctx.getStatus();
        while (!stat.finished()) {
          log.info("Polling query {} Status:{}", handle, stat);
          ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request().get(LensQuery.class);
          stat = ctx.getStatus();
          Thread.sleep(1000);
        }
        assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Expected to be successful " + handle);
        PersistentQueryResult resultset = target.path(handle.toString()).path("resultset")
          .queryParam("sessionid", lensSessionId).request().get(PersistentQueryResult.class);
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
    LensServerTestUtil.dropTable("test_server_restart", target(), lensSessionId);
    queryService.closeSession(lensSessionId);
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
    queryService.getSession(lensSessionId).addResource("FILE", dataFile.toURI().toString());
    log.info("@@ Added resource {}", dataFile.toURI());

    // Create a test table
    createTable("test_hive_server_restart", target(), lensSessionId);
    loadData("test_hive_server_restart", TestResourceFile.TEST_DATA_FILE.getValue(), target(),
      lensSessionId);
    log.info("Loaded data");

    log.info("Hive Server restart test");
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

    // Submit query, restart HS2, submit another query
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
      "select COUNT(ID) from test_hive_server_restart"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      MediaType.APPLICATION_XML_TYPE));
    QueryHandle handle = target.request()
      .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
        new GenericType<LensAPIResult<QueryHandle>>() {}).getData();

    Assert.assertNotNull(handle);

    // wait for query to move out of QUEUED state
    LensQuery ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request()
        .get(LensQuery.class);
    QueryStatus stat = ctx.getStatus();
    while (stat.queued()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request().get(LensQuery.class);
      stat = ctx.getStatus();
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
    ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request()
      .get(LensQuery.class);
    stat = ctx.getStatus();
    while (!stat.finished()) {
      log.info("Polling query {} Status:{}", handle, stat);
      ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request().get(LensQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }

    Assert.assertTrue(stat.finished());
    log.info("Previous query status: {}", stat.getStatusMessage());

    // After hive server restart, first few queries fail with Invalid Operation Handle followed by
    // Invalid Session Handle. Idle behaviour is to fail with Invalid Session Handle immediately.
    // Jira Ticket raised for debugging: https://issues.apache.org/jira/browse/LENS-707

    final String query = "select COUNT(ID) from test_hive_server_restart";
    Response response = null;
    while (response == null || response.getStatus() == Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
      response = execute(target(), Optional.of(lensSessionId), Optional.of(query));
      Thread.sleep(1000);
    }

    handle = response.readEntity(new GenericType<LensAPIResult<QueryHandle>>() {}).getData();

    // Poll for second query, this should finish successfully
    ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request().get(LensQuery.class);
    stat = ctx.getStatus();
    while (!stat.finished()) {
      log.info("Post restart polling query {} Status:{}", handle, stat);
      ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request().get(LensQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    log.info("Final status for {}: {}", handle, stat.getStatus());

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
    LensServerTestUtil.dropTable("test_hive_server_restart", target(), lensSessionId);
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
      .build(), new LensConf(), MediaType.APPLICATION_XML_TYPE));

    final LensSessionHandle restartTestSession = sessionTarget.request().post(
      Entity.entity(sessionForm, MediaType.MULTIPART_FORM_DATA_TYPE), LensSessionHandle.class);
    Assert.assertNotNull(restartTestSession);

    // Set a param
    setParams(restartTestSession);
    // Add resource
    // add a resource
    final WebTarget resourcetarget = target().path("session/resources");
    final FormDataMultiPart mp1 = new FormDataMultiPart();
    mp1.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), restartTestSession,
      MediaType.APPLICATION_XML_TYPE));
    mp1.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("type").build(), "file"));
    mp1.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("path").build(),
      "target/test-classes/lens-site.xml"));
    APIResult result = resourcetarget.path("add").request()
      .put(Entity.entity(mp1, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    assertEquals(result.getStatus(), Status.SUCCEEDED);

    // restart server
    restartLensServer();

    // Check resources added again
    verifyParamOnRestart(restartTestSession);

    HiveSessionService sessionService = LensServices.get().getService(SessionService.NAME);
    Assert.assertTrue(sessionService.getHealthStatus().isHealthy());

    LensSessionImpl session = sessionService.getSession(restartTestSession);
    assertEquals(session.getLensSessionPersistInfo().getResources().size(), 1);
    LensSessionImpl.ResourceEntry resourceEntry = session.getLensSessionPersistInfo().getResources().get(0);
    assertEquals(resourceEntry.getType(), "file");
    Assert.assertTrue(resourceEntry.getLocation().contains("target/test-classes/lens-site.xml"));

    // close session
    result = sessionTarget.queryParam("sessionid", restartTestSession).request().delete(APIResult.class);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  private void setParams(LensSessionHandle lensSessionHandle) {
    FormDataMultiPart setpart = new FormDataMultiPart();
    setpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionHandle,
      MediaType.APPLICATION_XML_TYPE));
    setpart
      .bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("key").build(), "lens.session.testRestartKey"));
    setpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("value").build(), "myvalue"));
    APIResult result = target().path("session").path("params").request()
      .put(Entity.entity(setpart, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  private void verifyParamOnRestart(LensSessionHandle lensSessionHandle) {

    StringList sessionParams = target().path("session").path("params").queryParam("sessionid", lensSessionHandle)
      .queryParam("verbose", true).queryParam("key", "lens.session.testRestartKey").request().get(StringList.class);
    System.out.println("Session params:" + sessionParams.getElements());
    assertEquals(sessionParams.getElements().size(), 1);
    Assert.assertTrue(sessionParams.getElements().contains("lens.session.testRestartKey=myvalue"));

  }
}

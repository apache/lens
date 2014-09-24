package com.inmobi.grill.server;

/*
 * #%L
 * Grill Server
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hive.service.Service;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.StringList;
import com.inmobi.grill.api.APIResult.Status;
import com.inmobi.grill.api.query.GrillQuery;
import com.inmobi.grill.api.query.PersistentQueryResult;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.driver.hive.TestRemoteHiveDriver;
import com.inmobi.grill.server.query.QueryExecutionServiceImpl;
import com.inmobi.grill.server.query.TestQueryService;
import com.inmobi.grill.server.session.GrillSessionImpl;
import com.inmobi.grill.server.session.HiveSessionService;

@Test(alwaysRun=true, groups="restart-test",dependsOnGroups="unit-test")
public class TestServerRestart extends GrillAllApplicationJerseyTest {

  public static final Log LOG = LogFactory.getLog(TestServerRestart.class);
  private File dataFile;

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }

  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Override
  protected int getTestPort() {
    return 8091;
  }

  private boolean fileCreated;
  final int NROWS = 10000;

  private void createRestartTestDataFile() throws FileNotFoundException {
    if (fileCreated) {
      return;
    }

    dataFile = new File("target/testdata.txt");
    dataFile.deleteOnExit();

    PrintWriter dataFileOut = new PrintWriter(dataFile);
    for (int i = 0; i < NROWS; i++) {
      dataFileOut.println(i);
    }
    dataFileOut.flush();
    dataFileOut.close();
    fileCreated = true;
  }

  @Test
  public void testQueryService() throws InterruptedException, IOException, GrillException {
    LOG.info("Server restart test");

    QueryExecutionServiceImpl queryService = (QueryExecutionServiceImpl)GrillServices.get().getService("query");
    GrillSessionHandle grillSessionId = queryService.openSession("foo", "bar", new HashMap<String, String>());
    // Create data file
    createRestartTestDataFile();

    // Create a test table
    GrillTestUtil.createTable("test_server_restart", target(), grillSessionId);
    GrillTestUtil.loadData("test_server_restart", "target/testdata.txt", target(), grillSessionId);
    LOG.info("Loaded data");

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
          LOG.info("Stopped query submitter");
        } catch (Exception exc) {
          LOG.error("Could not kill query submitter", exc);
        }
        killed = true;
      }

      final FormDataMultiPart mp = new FormDataMultiPart();
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
          grillSessionId, MediaType.APPLICATION_XML_TYPE));
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        "select COUNT(ID) from test_server_restart"));
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name(
         "operation").build(),
        "execute"
      ));
      mp.bodyPart(new FormDataBodyPart(
          FormDataContentDisposition.name("conf").fileName("conf").build(),
          new GrillConf(),
          MediaType.APPLICATION_XML_TYPE));
      final QueryHandle handle = target.request().post(
          Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

      Assert.assertNotNull(handle);
      GrillQuery ctx = target.path(handle.toString())
        .queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
      QueryStatus stat = ctx.getStatus();
      LOG.info(i + " submitted query " + handle + " state: " + ctx.getStatus().getStatus());
      launchedQueries.add(handle);
    }

    // Restart the server
    LOG.info("Restarting grill server!");
    restartGrillServer();
    LOG.info("Restarted grill server!");
    queryService = (QueryExecutionServiceImpl)GrillServices.get().getService("query");

    // All queries should complete after server restart
    for (QueryHandle handle : launchedQueries) {
      LOG.info("Polling query " + handle);
      try {
        GrillQuery ctx = target.path(handle.toString())
          .queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
        QueryStatus stat = ctx.getStatus();
        while (!stat.isFinished()) {
          LOG.info("Polling query " + handle + " Status:" + stat);
          ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
          stat = ctx.getStatus();
          Thread.sleep(1000);
        }
        Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL,
          "Expected to be successful " + handle);
        PersistentQueryResult resultset = target.path(handle.toString()).path(
          "resultset").queryParam("sessionid", grillSessionId).request().get(PersistentQueryResult.class);
        List<String> rows = TestQueryService.readResultSet(resultset, handle, true);
        Assert.assertEquals(rows.size(), 1);
        Assert.assertEquals(rows.get(0), "" + NROWS);
        LOG.info("Completed " + handle);
      } catch (Exception exc) {
        LOG.error("Failed query "  + handle, exc);
        Assert.fail(exc.getMessage());
      }
    }
    LOG.info("End server restart test");
    GrillTestUtil.dropTable("test_server_restart", target(), grillSessionId);
    queryService.closeSession(grillSessionId);
  }

  @Test
  public void testHiveServerRestart() throws Exception {
    QueryExecutionServiceImpl queryService = (QueryExecutionServiceImpl)GrillServices.get().getService("query");
    GrillSessionHandle grillSessionId = queryService.openSession("foo", "bar", new HashMap<String, String>());
    // Create data file
    createRestartTestDataFile();

    // Add a resource to check if its added after server restart.
    queryService.addResource(grillSessionId, "FILE", dataFile.toURI().toString());
    queryService.getSession(grillSessionId).addResource("FILE", dataFile.toURI().toString());
    LOG.info("@@ Added resource " + dataFile.toURI());

    // Create a test table
    GrillTestUtil.createTable("test_hive_server_restart", target(), grillSessionId);
    GrillTestUtil.loadData("test_hive_server_restart", "target/testdata.txt", target(), grillSessionId);
    LOG.info("Loaded data");

    LOG.info("Hive Server restart test");
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

    // Submit query, restart HS2, submit another query
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
      grillSessionId, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
      "select COUNT(ID) from test_hive_server_restart"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name(
      "operation").build(),
      "execute"
    ));
    mp.bodyPart(new FormDataBodyPart(
      FormDataContentDisposition.name("conf").fileName("conf").build(),
      new GrillConf(),
      MediaType.APPLICATION_XML_TYPE));
    QueryHandle handle = target.request().post(
      Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

    Assert.assertNotNull(handle);

    List<GrillSessionImpl.ResourceEntry> sessionResources =
      queryService.getSession(grillSessionId).getGrillSessionPersistInfo().getResources();
    int[] restoreCounts = new int[sessionResources.size()];
    for (int i = 0; i < sessionResources.size(); i++) {
      restoreCounts[i] = sessionResources.get(i).getRestoreCount();
    }
    LOG.info("@@ Current counts " + Arrays.toString(restoreCounts));
    // Restart hive server
    TestRemoteHiveDriver.stopHS2Service();

    // Wait for server to stop
    while (TestRemoteHiveDriver.server.getServiceState() != Service.STATE.STOPPED) {
      LOG.info("Waiting for HS2 to stop. Current state " + TestRemoteHiveDriver.server.getServiceState());
      Thread.sleep(1000);
    }

    TestRemoteHiveDriver.createHS2Service();
    // Wait for server to come up
    while (Service.STATE.STARTED != TestRemoteHiveDriver.server.getServiceState()) {
      LOG.info("Waiting for HS2 to start " + TestRemoteHiveDriver.server.getServiceState());
      Thread.sleep(1000);
    }
    Thread.sleep(10000);
    LOG.info("Server restarted");

    // Poll for first query, we should not get any exception
    GrillQuery ctx = target.path(handle.toString())
      .queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      LOG.info("Polling query " + handle + " Status:" + stat);
      ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }

    Assert.assertTrue(stat.isFinished());
    LOG.info("Previous query status: " + stat.getStatusMessage());

    for (int i = 0; i < 5; i++) {
      // Submit another query, again no exception expected
      mp = new FormDataMultiPart();
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        "select COUNT(ID) from test_hive_server_restart"));
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name(
        "operation").build(),
        "execute"
      ));
      mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        new GrillConf(),
        MediaType.APPLICATION_XML_TYPE));
      handle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);
      Assert.assertNotNull(handle);

      // Poll for second query, this should finish successfully
      ctx = target.path(handle.toString())
        .queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
      stat = ctx.getStatus();
      while (!stat.isFinished()) {
        LOG.info("Post restart polling query " + handle + " Status:" + stat);
        ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
        stat = ctx.getStatus();
        Thread.sleep(1000);
      }
      LOG.info("@@ "+ i + " Final status for " + handle + " " + stat.getStatus());
    }

    // Now we can expect that session resources have been added back exactly once
    for (int i = 0; i < sessionResources.size(); i++) {
      GrillSessionImpl.ResourceEntry resourceEntry = sessionResources.get(i);
      Assert.assertEquals(resourceEntry.getRestoreCount(), 1 + restoreCounts[i],
        "Restore test failed for " + resourceEntry
          + " pre count=" + restoreCounts[i] + " post count=" + resourceEntry.getRestoreCount());
      LOG.info("@@ Latest count " + resourceEntry + " = " + resourceEntry.getRestoreCount());
    }
    //Assert.assertEquals(stat.getStatus(), QueryStatus.Status.SUCCESSFUL,
    //    "Expected to be successful " + handle);

    LOG.info("End hive server restart test");
    GrillTestUtil.dropTable("test_hive_server_restart", target(), grillSessionId);
    queryService.closeSession(grillSessionId);
  }

  @Test
  public void testSessionRestart() throws Exception {
    System.out.println("### Test session restart");

    // Create a new session
    WebTarget sessionTarget = target().path("session");
    FormDataMultiPart sessionForm = new FormDataMultiPart();
    sessionForm.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(),
      "foo"));
    sessionForm.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(),
      "bar"));
    sessionForm.bodyPart(new FormDataBodyPart(
      FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
      new GrillConf(),
      MediaType.APPLICATION_XML_TYPE));

    final GrillSessionHandle restartTestSession = sessionTarget.request().post(
      Entity.entity(sessionForm, MediaType.MULTIPART_FORM_DATA_TYPE), GrillSessionHandle.class);
    Assert.assertNotNull(restartTestSession);

    // Set a param
    WebTarget paramTarget = sessionTarget.path("params");
    FormDataMultiPart setpart = new FormDataMultiPart();
    setpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
      restartTestSession, MediaType.APPLICATION_XML_TYPE));
    setpart.bodyPart(new FormDataBodyPart(
      FormDataContentDisposition.name("key").build(), "grill.session.testRestartKey"));
    setpart.bodyPart(new FormDataBodyPart(
      FormDataContentDisposition.name("value").build(), "myvalue"));
    APIResult result = paramTarget.request().put(
      Entity.entity(setpart, MediaType.MULTIPART_FORM_DATA_TYPE),
      APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    // Add resource
    // add a resource
    final WebTarget resourcetarget = target().path("session/resources");
    final FormDataMultiPart mp1 = new FormDataMultiPart();
    mp1.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
      restartTestSession, MediaType.APPLICATION_XML_TYPE));
    mp1.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("type").build(),
      "file"));
    mp1.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("path").build(),
      "target/test-classes/grill-site.xml"));
    result = resourcetarget.path("add").request().put(
      Entity.entity(mp1, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    Assert.assertEquals(result.getStatus(), Status.SUCCEEDED);


    // restart server
    restartGrillServer();

    // Check session exists in the server

    // Try and get params back
    StringList sessionParams = paramTarget.queryParam("sessionid", restartTestSession)
        .queryParam("verbose", true).request().get(
          StringList.class);
    sessionParams = paramTarget.queryParam("sessionid", restartTestSession)
        .queryParam("key", "grill.session.testRestartKey").request().get(
            StringList.class);
    System.out.println("Session params:" + sessionParams.getElements());
    Assert.assertEquals(sessionParams.getElements().size(), 1);
    Assert.assertTrue(sessionParams.getElements().contains("grill.session.testRestartKey=myvalue"));

    // Check resources added again
    HiveSessionService sessionService = GrillServices.get().getService("session");
    GrillSessionImpl session = sessionService.getSession(restartTestSession);
    Assert.assertEquals(session.getGrillSessionPersistInfo().getResources().size(), 1);
    GrillSessionImpl.ResourceEntry resourceEntry = session.getGrillSessionPersistInfo().getResources().get(0);
    Assert.assertEquals(resourceEntry.getType(), "file");
    Assert.assertEquals(resourceEntry.getLocation(), "target/test-classes/grill-site.xml");

    // close session
    result = sessionTarget.queryParam("sessionid", restartTestSession).request().delete(APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

}

package com.inmobi.grill.server.query;

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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import com.inmobi.grill.api.*;
import com.inmobi.grill.api.query.GrillPreparedQuery;
import com.inmobi.grill.api.query.GrillQuery;
import com.inmobi.grill.api.query.InMemoryQueryResult;
import com.inmobi.grill.api.query.PersistentQueryResult;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryHandleWithResultSet;
import com.inmobi.grill.api.query.QueryPlan;
import com.inmobi.grill.api.query.QueryPrepareHandle;
import com.inmobi.grill.api.query.QueryResultSetMetadata;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.api.query.QueryStatus.Status;
import com.inmobi.grill.driver.hive.HiveDriver;
import com.inmobi.grill.driver.hive.TestHiveDriver.FailHook;
import com.inmobi.grill.server.GrillJerseyTest;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.GrillTestUtil;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.driver.GrillDriver;
import com.inmobi.grill.server.api.metrics.MetricsService;
import com.inmobi.grill.server.query.QueryApp;
import com.inmobi.grill.server.query.QueryExecutionServiceImpl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.subethamail.wiser.Wiser;
import org.subethamail.wiser.WiserMessage;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Test(groups="unit-test")
public class TestQueryService extends GrillJerseyTest {
  public static final Log LOG = LogFactory.getLog(TestQueryService.class);

  QueryExecutionServiceImpl queryService;
  MetricsService metricsSvc;
  GrillSessionHandle grillSessionId;
  final int NROWS = 10000;
  private Wiser wiser;

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
    wiser = new Wiser();
    wiser.setHostname("localhost");
    wiser.setPort(25000);
    queryService = (QueryExecutionServiceImpl)GrillServices.get().getService("query");
    metricsSvc = (MetricsService)GrillServices.get().getService(MetricsService.NAME);
    Map<String, String> sessionconf = new HashMap<String, String>();
    sessionconf.put("test.session.key", "svalue");
    grillSessionId = queryService.openSession("foo@localhost", "bar", sessionconf);
    createTable(testTable);
    loadData(testTable, TEST_DATA_FILE);
  }

  @AfterTest
  public void tearDown() throws Exception {
    dropTable(testTable);
    queryService.closeSession(grillSessionId);
    for (GrillDriver driver : queryService.getDrivers()) {
      if (driver instanceof HiveDriver) {
        assertFalse(((HiveDriver) driver).hasGrillSession(grillSessionId));
      }
    }
    super.tearDown();
  }

  @Override
  protected Application configure() {
    return new QueryApp();
  }

  @Override
  protected void configureClient(ClientConfig config) {
    config.register(MultiPartFeature.class);
  }

  private static String testTable = "TEST_TABLE";
  public static final String TEST_DATA_FILE = "../grill-driver-hive/testdata/testdata2.txt";

  private void createTable(String tblName) throws InterruptedException {
    GrillTestUtil.createTable(tblName, target(), grillSessionId);
  }

  private void loadData(String tblName, final String TEST_DATA_FILE)
      throws InterruptedException {
    GrillTestUtil.loadData(tblName, TEST_DATA_FILE, target(), grillSessionId);
  }
  private void dropTable(String tblName) throws InterruptedException {
    GrillTestUtil.dropTable(tblName, target(), grillSessionId);
  }

  // test get a random query, should return 400
  @Test
  public void testGetRandomQuery() {
    final WebTarget target = target().path("queryapi/queries");

    Response rs = target.path("random").queryParam("sessionid", grillSessionId).request().get();
    Assert.assertEquals(rs.getStatus(), 400);
  }

  @Test
  public void testLaunchFail() throws InterruptedException {
    final WebTarget target = target().path("queryapi/queries");
    long failedQueries = metricsSvc.getTotalFailedQueries();
    System.out.println("%% " + failedQueries);
    GrillConf conf = new GrillConf();
    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query")
        .build(),
        "select ID from non_exist_table"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name(
        "operation").build(),
        "execute"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        conf,
        MediaType.APPLICATION_XML_TYPE));
    final QueryHandle handle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

    Assert.assertNotNull(handle);
    
    GrillQuery ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
      stat = ctx.getStatus();
      System.out.println("%% query " + ctx.getQueryHandle() + " status:" + stat);
      Thread.sleep(1000);
    }
    
    assertTrue(ctx.getSubmissionTime() > 0);
    assertEquals(ctx.getLaunchTime(), 0);
    assertEquals(ctx.getDriverStartTime(), 0);
    assertEquals(ctx.getDriverFinishTime(), 0);
    assertTrue(ctx.getFinishTime() > 0);
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.FAILED);
    System.out.println("%% " + metricsSvc.getTotalFailedQueries());
    Assert.assertEquals(metricsSvc.getTotalFailedQueries(), failedQueries + 1);
  }

  // test with execute async post, get all queries, get query context,
  // get wrong uuid query
  @Test
  public void testQueriesAPI() throws InterruptedException {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");
    GrillConf conf = new GrillConf();
    conf.addProperty("hive.exec.driver.run.hooks", FailHook.class.getCanonicalName());
    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query")
        .build(),
        "select ID from " + testTable));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name(
        "operation").build(),
        "execute"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        conf,
        MediaType.APPLICATION_XML_TYPE));
    
    long queuedQueries = metricsSvc.getQueuedQueries();
    long runningQueries = metricsSvc.getRunningQueries();
    long finishedQueries = metricsSvc.getFinishedQueries();
    
    final QueryHandle handle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

    Assert.assertNotNull(handle);

    // Get all queries
    // XML 
    List<QueryHandle> allQueriesXML = target.queryParam("sessionid", grillSessionId).request(MediaType.APPLICATION_XML)
        .get(new GenericType<List<QueryHandle>>() {
        });
    Assert.assertTrue(allQueriesXML.size() >= 1);

    //JSON
    //  List<QueryHandle> allQueriesJSON = target.request(
    //      MediaType.APPLICATION_JSON).get(new GenericType<List<QueryHandle>>() {
    //  });
    //  Assert.assertEquals(allQueriesJSON.size(), 1);
    //JAXB
    List<QueryHandle> allQueries = (List<QueryHandle>)target.queryParam("sessionid", grillSessionId).request().get(
        new GenericType<List<QueryHandle>>(){});
    Assert.assertTrue(allQueries.size() >= 1);
    Assert.assertTrue(allQueries.contains(handle));

    // Get query
    // Invocation.Builder builderjson = target.path(handle.toString()).request(MediaType.APPLICATION_JSON);
    // String responseJSON = builderjson.get(String.class);
    // System.out.println("query JSON:" + responseJSON);
    String queryXML = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request(MediaType.APPLICATION_XML).get(String.class);
    System.out.println("query XML:" + queryXML);

    Response response = target.path(handle.toString() + "001").queryParam("sessionid", grillSessionId).request().get();
    Assert.assertEquals(response.getStatus(), 404);

    GrillQuery ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
    // Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.QUEUED);

    // wait till the query finishes
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
      stat = ctx.getStatus();
      switch (stat.getStatus()) {
      case RUNNING:
        assertEquals(metricsSvc.getRunningQueries(), runningQueries + 1);
        break;
      case QUEUED:
        assertEquals(metricsSvc.getQueuedQueries(), queuedQueries + 1);
        break;
      default: // nothing
      }
      Thread.sleep(1000);
    }
    assertTrue(ctx.getSubmissionTime() > 0);
    assertTrue(ctx.getLaunchTime() > 0);
    assertTrue(ctx.getDriverStartTime() > 0);
    assertTrue(ctx.getDriverFinishTime() > 0);
    assertTrue(ctx.getFinishTime() > 0);
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.FAILED);

    // Update conf for query
    final FormDataMultiPart confpart = new FormDataMultiPart();
    conf = new GrillConf();
    conf.addProperty("my.property", "myvalue");
    confpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    confpart.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        conf,
        MediaType.APPLICATION_XML_TYPE));
    APIResult updateConf = target.path(handle.toString()).request().put(
        Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE),
        APIResult.class);
    Assert.assertEquals(updateConf.getStatus(), APIResult.Status.FAILED);
  }


  @Test
  public void testExecuteWithoutSessionId() throws Exception {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");
    GrillConf conf = new GrillConf();
    conf.addProperty("hive.exec.driver.run.hooks", FailHook.class.getCanonicalName());
    final FormDataMultiPart mp = new FormDataMultiPart();

    /**
     * We are not passing session id in this test
     */
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query")
      .build(),
      "select ID from " + testTable));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name(
      "operation").build(),
      "execute"));
    mp.bodyPart(new FormDataBodyPart(
      FormDataContentDisposition.name("conf").fileName("conf").build(),
      conf,
      MediaType.APPLICATION_XML_TYPE));

    try {
      final QueryHandle handle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);
      Assert.fail("Should have thrown bad request error");
    } catch (BadRequestException badReqeust) {
      // pass
    }
  }

  // Test explain query
  @Test
  public void testExplainQuery() throws InterruptedException {    
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        "select ID from " + testTable));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(),
        "explain"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        new GrillConf(),
        MediaType.APPLICATION_XML_TYPE));

    final QueryPlan plan = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryPlan.class);
    Assert.assertEquals(plan.getNumSels(), 1);
    Assert.assertEquals(plan.getTablesQueried().size(), 1);
    Assert.assertTrue(plan.getTablesQueried().get(0).equalsIgnoreCase(testTable));
    Assert.assertNull(plan.getPrepareHandle());
  }

  // post to preparedqueries
  // get all prepared queries
  // get a prepared query
  // update a prepared query
  // post to prepared query multiple times
  // delete a prepared query
  @Test
  public void testPrepareQuery() throws InterruptedException {    
    final WebTarget target = target().path("queryapi/preparedqueries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        "select ID from " + testTable));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(),
        "prepare"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("queryName").build(),
      "testQuery1"));

    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        new GrillConf(),
        MediaType.APPLICATION_XML_TYPE));

    final QueryPrepareHandle pHandle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryPrepareHandle.class);

    // Get all prepared queries
    List<QueryPrepareHandle> allQueries = (List<QueryPrepareHandle>)target
        .queryParam("sessionid", grillSessionId)
        .queryParam("queryName", "testQuery1")
      .request().get(new GenericType<List<QueryPrepareHandle>>(){});
    Assert.assertTrue(allQueries.size() >= 1);
    Assert.assertTrue(allQueries.contains(pHandle));

    GrillPreparedQuery ctx = target.path(pHandle.toString()).queryParam("sessionid", grillSessionId).request().get(
        GrillPreparedQuery.class);
    Assert.assertTrue(ctx.getUserQuery().equalsIgnoreCase(
        "select ID from " + testTable));
    Assert.assertTrue(ctx.getDriverQuery().equalsIgnoreCase(
        "select ID from " + testTable));
    Assert.assertEquals(ctx.getSelectedDriverClassName(),
        com.inmobi.grill.driver.hive.HiveDriver.class.getCanonicalName());
    Assert.assertNull(ctx.getConf().getProperties().get("my.property"));

    // Update conf for prepared query
    final FormDataMultiPart confpart = new FormDataMultiPart();
    GrillConf conf = new GrillConf();
    conf.addProperty("my.property", "myvalue");
    confpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    confpart.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        conf,
        MediaType.APPLICATION_XML_TYPE));
    APIResult updateConf = target.path(pHandle.toString()).request().put(
        Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE),
        APIResult.class);
    Assert.assertEquals(updateConf.getStatus(), APIResult.Status.SUCCEEDED);

    ctx = target.path(pHandle.toString()).queryParam("sessionid", grillSessionId).request().get(
        GrillPreparedQuery.class);
    Assert.assertEquals(ctx.getConf().getProperties().get("my.property"),
        "myvalue");

    QueryHandle handle1 = target.path(pHandle.toString()).request().post(
        Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE),
        QueryHandle.class);


    // Override query name
    confpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("queryName").build(),
      "testQueryName2"));
    // do post once again
    QueryHandle handle2 = target.path(pHandle.toString()).request().post(
        Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE),
        QueryHandle.class);
    Assert.assertNotEquals(handle1, handle2);

    GrillQuery ctx1 = target().path("queryapi/queries").path(
        handle1.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
    Assert.assertEquals(ctx1.getQueryName().toLowerCase(), "testquery1");
    // wait till the query finishes
    QueryStatus stat = ctx1.getStatus();
    while (!stat.isFinished()) {
      ctx1 = target().path("queryapi/queries").path(
          handle1.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
      stat = ctx1.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx1.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    GrillQuery ctx2 = target().path("queryapi/queries").path(
        handle2.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
    Assert.assertEquals(ctx2.getQueryName().toLowerCase(), "testqueryname2");
    // wait till the query finishes
    stat = ctx2.getStatus();
    while (!stat.isFinished()) {
      ctx2 = target().path("queryapi/queries").path(
          handle1.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
      stat = ctx2.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx1.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    // destroy prepared
    APIResult result = target.path(pHandle.toString()).queryParam("sessionid", grillSessionId).request().delete(APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    // Post on destroyed query
    Response response = target.path(pHandle.toString()).request().post(
        Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE),
        Response.class);
    Assert.assertEquals(response.getStatus(), 404);
  }

  @Test
  public void testExplainAndPrepareQuery() throws InterruptedException {    
    final WebTarget target = target().path("queryapi/preparedqueries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        "select ID from " + testTable));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(),
        "explain_and_prepare"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        new GrillConf(),
        MediaType.APPLICATION_XML_TYPE));

    final QueryPlan plan = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryPlan.class);
    Assert.assertEquals(plan.getNumSels(), 1);
    Assert.assertEquals(plan.getTablesQueried().size(), 1);
    Assert.assertTrue(plan.getTablesQueried().get(0).equalsIgnoreCase(testTable));
    Assert.assertNotNull(plan.getPrepareHandle());

    GrillPreparedQuery ctx = target.path(plan.getPrepareHandle().toString())
        .queryParam("sessionid", grillSessionId).request().get(GrillPreparedQuery.class);
    Assert.assertTrue(ctx.getUserQuery().equalsIgnoreCase(
        "select ID from " + testTable));
    Assert.assertTrue(ctx.getDriverQuery().equalsIgnoreCase(
        "select ID from " + testTable));
    Assert.assertEquals(ctx.getSelectedDriverClassName(),
        com.inmobi.grill.driver.hive.HiveDriver.class.getCanonicalName());
    Assert.assertNull(ctx.getConf().getProperties().get("my.property"));

    // Update conf for prepared query
    final FormDataMultiPart confpart = new FormDataMultiPart();
    GrillConf conf = new GrillConf();
    conf.addProperty("my.property", "myvalue");
    confpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    confpart.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        conf,
        MediaType.APPLICATION_XML_TYPE));
    APIResult updateConf = target.path(plan.getPrepareHandle().toString()).request().put(
        Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE),
        APIResult.class);
    Assert.assertEquals(updateConf.getStatus(), APIResult.Status.SUCCEEDED);

    ctx = target.path(plan.getPrepareHandle().toString()).queryParam("sessionid", grillSessionId).request().get(
        GrillPreparedQuery.class);
    Assert.assertEquals(ctx.getConf().getProperties().get("my.property"),
        "myvalue");

    QueryHandle handle1 = target.path(plan.getPrepareHandle().toString()).request().post(
        Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE),
        QueryHandle.class);

    // do post once again
    QueryHandle handle2 = target.path(plan.getPrepareHandle().toString()).request().post(
        Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE),
        QueryHandle.class);
    Assert.assertNotEquals(handle1, handle2);

    GrillQuery ctx1 = target().path("queryapi/queries").path(
        handle1.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
    // wait till the query finishes
    QueryStatus stat = ctx1.getStatus();
    while (!stat.isFinished()) {
      ctx1 = target().path("queryapi/queries").path(
          handle1.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
      stat = ctx1.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx1.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    GrillQuery ctx2 = target().path("queryapi/queries").path(
        handle2.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
    // wait till the query finishes
    stat = ctx2.getStatus();
    while (!stat.isFinished()) {
      ctx2 = target().path("queryapi/queries").path(
          handle1.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
      stat = ctx2.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx1.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    // destroy prepared
    APIResult result = target.path(plan.getPrepareHandle().toString())
        .queryParam("sessionid", grillSessionId).request().delete(APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    // Post on destroyed query
    Response response = target.path(plan.getPrepareHandle().toString())
        .request().post(
            Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE),
            Response.class);
    Assert.assertEquals(response.getStatus(), 404);

  }

  // test with execute async post, get query, get results
  // test cancel query
  @Test
  public void testExecuteAsync() throws InterruptedException, IOException {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");
    
    long queuedQueries = metricsSvc.getQueuedQueries();
    long runningQueries = metricsSvc.getRunningQueries();
    
    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        "select ID, IDSTR from " + testTable));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name(
        "operation").build(),
        "execute"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        new GrillConf(),
        MediaType.APPLICATION_XML_TYPE));
    final QueryHandle handle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

    Assert.assertNotNull(handle);

    // Get query
    GrillQuery ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
    Assert.assertTrue(ctx.getStatus().getStatus().equals(Status.QUEUED) ||
        ctx.getStatus().getStatus().equals(Status.LAUNCHED) ||
        ctx.getStatus().getStatus().equals(Status.RUNNING) ||
        ctx.getStatus().getStatus().equals(Status.SUCCESSFUL));

    // wait till the query finishes
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
      stat = ctx.getStatus();
      switch (stat.getStatus()) {
      case RUNNING:
        assertEquals(metricsSvc.getRunningQueries(), runningQueries + 1, "Asserting queries for " + ctx.getQueryHandle());
        break;
      case QUEUED:
        assertEquals(metricsSvc.getQueuedQueries(), queuedQueries + 1);
        break;
      default: // nothing
      }
      Thread.sleep(1000);
    }
    assertTrue(ctx.getSubmissionTime() > 0);
    assertTrue(ctx.getLaunchTime() > 0);
    assertTrue(ctx.getDriverStartTime() > 0);
    assertTrue(ctx.getDriverFinishTime() > 0);
    assertTrue(ctx.getFinishTime() > 0);
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
    
    validatePersistedResult(handle, target(), grillSessionId, true);

    // test cancel query
    final QueryHandle handle2 = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

    Assert.assertNotNull(handle2);
    APIResult result = target.path(handle2.toString())
        .queryParam("sessionid", grillSessionId).request().delete(APIResult.class);
    // cancel would fail query is already successful
    Assert.assertTrue(result.getStatus().equals(APIResult.Status.SUCCEEDED) ||
        result.getStatus().equals(APIResult.Status.FAILED));

    GrillQuery ctx2 = target.path(handle2.toString()).queryParam("sessionid",
        grillSessionId).request().get(GrillQuery.class);
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      Assert.assertTrue(ctx2.getStatus().getStatus() == QueryStatus.Status.SUCCESSFUL);
    } else {
      Assert.assertTrue(ctx2.getStatus().getStatus() == QueryStatus.Status.CANCELED);
    }
  }

  @Test
  public void testNotification() throws IOException {
    wiser.start();
    final WebTarget target = target().path("queryapi/queries");
    final FormDataMultiPart mp2 = new FormDataMultiPart();
    GrillConf conf = new GrillConf();
    conf.addProperty(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    conf.addProperty(GrillConfConstants.GRILL_WHETHER_MAIL_NOTIFY, "true");
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
      grillSessionId, MediaType.APPLICATION_XML_TYPE));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
      "select ID, IDSTR from " + testTable));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name(
      "operation").build(),
      "execute_with_timeout"));
    mp2.bodyPart(new FormDataBodyPart(
      FormDataContentDisposition.name("conf").fileName("conf").build(),
      conf,
      MediaType.APPLICATION_XML_TYPE));

    QueryHandleWithResultSet result = target.request().post(
      Entity.entity(mp2, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandleWithResultSet.class);
    Assert.assertNotNull(result.getQueryHandle());
    Assert.assertNotNull(result.getResult());
    validateInmemoryResult((InMemoryQueryResult) result.getResult());
    List<WiserMessage> messages = wiser.getMessages();
    Assert.assertEquals(messages.size(), 1);
    Assert.assertTrue(messages.get(0).toString().contains(result.getQueryHandle().toString()));
    wiser.stop();
  }

  static void validatePersistedResult(QueryHandle handle, WebTarget parent,
      GrillSessionHandle grillSessionId, boolean isDir) throws IOException {
    final WebTarget target = parent.path("queryapi/queries");
    // fetch results
    validateResultSetMetadata(handle, parent, grillSessionId);

    String presultset = target.path(handle.toString()).path(
        "resultset").queryParam("sessionid", grillSessionId).request().get(String.class);
    System.out.println("PERSISTED RESULT:" + presultset);

    PersistentQueryResult resultset = target.path(handle.toString()).path(
        "resultset").queryParam("sessionid", grillSessionId).request().get(PersistentQueryResult.class);
    validatePersistentResult(resultset, handle, isDir);

    if (isDir) {
      validNotFoundForHttpResult(parent, grillSessionId, handle);
    }
  }

  public static List<String> readResultSet(PersistentQueryResult resultset,
      QueryHandle handle, boolean isDir) throws  IOException {
    Assert.assertTrue(resultset.getPersistedURI().contains(handle.toString()));
    Path actualPath = new Path(resultset.getPersistedURI());
    FileSystem fs = actualPath.getFileSystem(new Configuration());
    List<String> actualRows = new ArrayList<String>();
    if (fs.getFileStatus(actualPath).isDir()) {
      Assert.assertTrue(isDir);
      for (FileStatus fstat : fs.listStatus(actualPath)) {
        addRowsFromFile(actualRows, fs, fstat.getPath());
      }
    } else {
      Assert.assertFalse(isDir);
      addRowsFromFile(actualRows, fs, actualPath);
    }
    return actualRows;
  }

  static void addRowsFromFile(List<String> actualRows,
      FileSystem fs, Path path) throws IOException {
    FSDataInputStream in = fs.open(path);
    BufferedReader br = null;
    try {
      br = new BufferedReader(new InputStreamReader(in));
      String line = "";

      while ((line = br.readLine()) != null) {
        actualRows.add(line);
      }
    } finally {
      if (br != null) {
        br.close();
      }
      if (in != null) {
        in.close();
      }
    }
  }

  static void validatePersistentResult(PersistentQueryResult resultset,
      QueryHandle handle, boolean isDir) throws IOException {
    List<String> actualRows = readResultSet(resultset, handle, isDir);
    Assert.assertEquals(actualRows.get(0), "1one");
    Assert.assertEquals(actualRows.get(1), "\\Ntwo");
    Assert.assertEquals(actualRows.get(2), "3\\N");
    Assert.assertEquals(actualRows.get(3), "\\N\\N");
    Assert.assertEquals(actualRows.get(4), "5");
  }

  static void validateHttpEndPoint(WebTarget parent,
      GrillSessionHandle grillSessionId,
      QueryHandle handle, String redirectUrl) throws IOException {
    Response response = parent.path(
        "queryapi/queries/" +handle.toString() + "/httpresultset")
        .queryParam("sessionid", grillSessionId).request().get();

    Assert.assertTrue(response.getHeaderString("content-disposition").contains(handle.toString()));

    if (redirectUrl == null) {
      InputStream in = (InputStream)response.getEntity();
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      IOUtils.copyBytes(in, bos, new Configuration());
      bos.close();
      in.close();

      String result = new String(bos.toByteArray());
      List<String> actualRows = Arrays.asList(result.split("\n"));
      Assert.assertEquals(actualRows.get(0), "1one");
      Assert.assertEquals(actualRows.get(1), "\\Ntwo");
      Assert.assertEquals(actualRows.get(2), "3\\N");
      Assert.assertEquals(actualRows.get(3), "\\N\\N");
      Assert.assertEquals(actualRows.get(4), "5");
    } else {
      Assert.assertEquals(Response.Status.SEE_OTHER.getStatusCode(), response.getStatus());
      Assert.assertTrue(response.getHeaderString("Location").contains(redirectUrl));
    }
  }

  static void validNotFoundForHttpResult(WebTarget parent, GrillSessionHandle grillSessionId,
      QueryHandle handle) {
    try {
      Response response = parent.path(
          "queryapi/queries/" +handle.toString() + "/httpresultset")
          .queryParam("sessionid", grillSessionId).request().get();
      if (Response.Status.NOT_FOUND.getStatusCode() != response.getStatus()) {
        Assert.fail("Expected not found excepiton, but got:" + response.getStatus());
      }
      Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    } catch (NotFoundException e) {
      // expected
    }

  }
  // test with execute async post, get query, get results
  // test cancel query
  @Test
  public void testExecuteAsyncInMemoryResult() throws InterruptedException, IOException {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    GrillConf conf = new GrillConf();
    conf.addProperty(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        "select ID, IDSTR from " + testTable));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name(
        "operation").build(),
        "execute"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        conf,
        MediaType.APPLICATION_XML_TYPE));
    final QueryHandle handle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

    Assert.assertNotNull(handle);

    // Get query
    GrillQuery ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
    Assert.assertTrue(ctx.getStatus().getStatus().equals(Status.QUEUED) ||
        ctx.getStatus().getStatus().equals(Status.LAUNCHED) ||
        ctx.getStatus().getStatus().equals(Status.RUNNING) ||
        ctx.getStatus().getStatus().equals(Status.SUCCESSFUL));

    // wait till the query finishes
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    // fetch results
    validateResultSetMetadata(handle, target(), grillSessionId);

    InMemoryQueryResult resultset = target.path(handle.toString()).path(
        "resultset").queryParam("sessionid", grillSessionId).request().get(InMemoryQueryResult.class);
    validateInmemoryResult(resultset);

    validNotFoundForHttpResult(target(), grillSessionId, handle);
  }

  @Test
  public void testExecuteAsyncTempTable() throws InterruptedException, IOException {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    GrillConf conf = new GrillConf();
    conf.addProperty(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        "create table temp_output as select ID, IDSTR from " + testTable));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name(
        "operation").build(),
        "execute"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        conf,
        MediaType.APPLICATION_XML_TYPE));
    final QueryHandle handle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

    Assert.assertNotNull(handle);

    // Get query
    GrillQuery ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
    Assert.assertTrue(ctx.getStatus().getStatus().equals(Status.QUEUED) ||
        ctx.getStatus().getStatus().equals(Status.LAUNCHED) ||
        ctx.getStatus().getStatus().equals(Status.RUNNING) ||
        ctx.getStatus().getStatus().equals(Status.SUCCESSFUL));

    // wait till the query finishes
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    String select = "SELECT * FROM temp_output";
    final FormDataMultiPart fetch = new FormDataMultiPart();
    fetch.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    fetch.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        select));
    fetch.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name(
        "operation").build(),
        "execute"));
    fetch.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        conf,
        MediaType.APPLICATION_XML_TYPE));
    final QueryHandle handle2 = target.request().post(
        Entity.entity(fetch, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

    Assert.assertNotNull(handle2);

    // Get query
    ctx = target.path(handle2.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);

    // wait till the query finishes
    stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle2.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    // fetch results
    validateResultSetMetadata(handle2, "temp_output.", target(), grillSessionId);

    InMemoryQueryResult resultset = target.path(handle2.toString()).path(
        "resultset").queryParam("sessionid", grillSessionId).request().get(InMemoryQueryResult.class);
    validateInmemoryResult(resultset);
  }

  static void validateResultSetMetadata(QueryHandle handle, WebTarget parent,
      GrillSessionHandle grillSessionId) {
    validateResultSetMetadata(handle, "", parent, grillSessionId);
  }

  static void validateResultSetMetadata(QueryHandle handle,
      String outputTablePfx, WebTarget parent,
      GrillSessionHandle grillSessionId) {
    final WebTarget target = parent.path("queryapi/queries");

    QueryResultSetMetadata metadata = target.path(handle.toString()).path(
        "resultsetmetadata").queryParam("sessionid", grillSessionId).request().get(QueryResultSetMetadata.class);
    Assert.assertEquals(metadata.getColumns().size(), 2);
    assertTrue(metadata.getColumns().get(0).getName().toLowerCase().equals((outputTablePfx + "ID").toLowerCase()) ||
        metadata.getColumns().get(0).getName().toLowerCase().equals("ID".toLowerCase()));
    assertEquals("INT".toLowerCase(), metadata.getColumns().get(0).getType().name().toLowerCase());
    assertTrue(metadata.getColumns().get(1).getName().toLowerCase().equals((outputTablePfx + "IDSTR").toLowerCase()) ||
        metadata.getColumns().get(0).getName().toLowerCase().equals("IDSTR".toLowerCase()));
    assertEquals("STRING".toLowerCase(), metadata.getColumns().get(1).getType().name().toLowerCase());    
  }

  private void validateInmemoryResult(InMemoryQueryResult resultset) {
    Assert.assertEquals(resultset.getRows().size(), 5);
    Assert.assertEquals(resultset.getRows().get(0).getValues().get(0), 1);
    Assert.assertEquals((String)resultset.getRows().get(0).getValues().get(1), "one");

    Assert.assertNull(resultset.getRows().get(1).getValues().get(0));
    Assert.assertEquals((String)resultset.getRows().get(1).getValues().get(1), "two");

    Assert.assertEquals(resultset.getRows().get(2).getValues().get(0), 3);
    Assert.assertNull(resultset.getRows().get(2).getValues().get(1));

    Assert.assertNull(resultset.getRows().get(3).getValues().get(0));
    Assert.assertNull(resultset.getRows().get(3).getValues().get(1));
    Assert.assertEquals(resultset.getRows().get(4).getValues().get(0), 5);
    Assert.assertEquals(resultset.getRows().get(4).getValues().get(1), "");    
  }

  // test execute with timeout, fetch results
  // cancel the query with execute_with_timeout
  @Test
  public void testExecuteWithTimeoutQuery() throws IOException {
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        "select ID, IDSTR from " + testTable));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(),
        "execute_with_timeout"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        new GrillConf(),
        MediaType.APPLICATION_XML_TYPE));

    QueryHandleWithResultSet result = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandleWithResultSet.class);
    Assert.assertNotNull(result.getQueryHandle());
    Assert.assertNotNull(result.getResult());
    validatePersistentResult((PersistentQueryResult) result.getResult(),
        result.getQueryHandle(), true);
    
    final FormDataMultiPart mp2 = new FormDataMultiPart();
    GrillConf conf = new GrillConf();
    conf.addProperty(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        "select ID, IDSTR from " + testTable));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name(
        "operation").build(),
        "execute_with_timeout"));
    mp2.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        conf,
        MediaType.APPLICATION_XML_TYPE));

    result = target.request().post(
        Entity.entity(mp2, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandleWithResultSet.class);
    Assert.assertNotNull(result.getQueryHandle());
    Assert.assertNotNull(result.getResult());
    validateInmemoryResult((InMemoryQueryResult) result.getResult());

  }

  @Test
  public void testDefaultConfig() throws GrillException {
    GrillConf queryConf = new GrillConf();
    queryConf.addProperty("test.query.conf", "qvalue");
    Configuration conf = queryService.getGrillConf(grillSessionId, queryConf);

    // session specific conf
    Assert.assertEquals(conf.get("test.session.key"), "svalue");
    // query specific conf
    Assert.assertEquals(conf.get("test.query.conf"), "qvalue");
    // grillsession default should be loaded
    Assert.assertNotNull(conf.get("grill.persistent.resultset"));
    // grill site should be loaded
    Assert.assertEquals(conf.get("test.grill.site.key"), "gsvalue");
    // hive default variables should not be set
    Assert.assertNull(conf.get("hive.exec.local.scratchdir"));
    // hive site variables should not be set
    Assert.assertNull(conf.get("hive.metastore.warehouse.dir"));
    // core default should not be loaded
    Assert.assertNull(conf.get("fs.default.name"));

    // Test server config. Hive configs overriden should be set
    Assert.assertFalse(Boolean.parseBoolean(queryService.getHiveConf().get("hive.server2.log.redirection.enabled")));
    Assert.assertEquals(queryService.getHiveConf().get("hive.server2.query.log.dir"), "target/query-logs");
  }

  @Override
  protected int getTestPort() {
    return 8083;
  }
}

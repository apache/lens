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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
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
import com.inmobi.grill.driver.hive.TestHiveDriver.FailHook;
import com.inmobi.grill.server.GrillJerseyTest;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.metrics.MetricsService;
import com.inmobi.grill.server.api.query.QueryEnded;
import com.inmobi.grill.server.query.QueryApp;
import com.inmobi.grill.server.query.QueryExecutionServiceImpl;

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
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestQueryService extends GrillJerseyTest {

  QueryExecutionServiceImpl queryService;
  MetricsService metricsSvc;
  GrillSessionHandle grillSessionId;
  
  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
    queryService = (QueryExecutionServiceImpl)GrillServices.get().getService("query");
    metricsSvc = (MetricsService)GrillServices.get().getService(MetricsService.NAME);
    grillSessionId = queryService.openSession("foo", "bar", new HashMap<String, String>());
  }

  @AfterTest
  public void tearDown() throws Exception {
    queryService.closeSession(grillSessionId);
    super.tearDown();
  }

  @BeforeClass
  public void createTables() throws InterruptedException {
    createTable(testTable);
    loadData(testTable);
  }

  @AfterClass
  public void dropTables() throws InterruptedException {
    dropTable(testTable);
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
    GrillConf conf = new GrillConf();
    conf.addProperty(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, "false");
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    String createTable = "CREATE TABLE IF NOT EXISTS " + tblName  +"(ID INT, IDSTR STRING)";

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        createTable));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(),
        "execute"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        conf,
        MediaType.APPLICATION_XML_TYPE));

    final QueryHandle handle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

    // wait till the query finishes
    GrillQuery ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
  }

  private void loadData(String tblName) throws InterruptedException {
    GrillConf conf = new GrillConf();
    conf.addProperty(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, "false");
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    String dataLoad = "LOAD DATA LOCAL INPATH '"+ TEST_DATA_FILE +
        "' OVERWRITE INTO TABLE " + tblName;

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        dataLoad));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(),
        "execute"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        conf,
        MediaType.APPLICATION_XML_TYPE));

    final QueryHandle handle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

    // wait till the query finishes
    GrillQuery ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

  }

  private void dropTable(String tblName) throws InterruptedException {
    GrillConf conf = new GrillConf();
    conf.addProperty(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, "false");
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    String createTable = "DROP TABLE IF EXISTS " + tblName ;

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        createTable));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(),
        "execute"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        conf,
        MediaType.APPLICATION_XML_TYPE));

    final QueryHandle handle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

    // wait till the query finishes
    GrillQuery ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
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
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.FAILED);
    assertEquals(metricsSvc.getFinishedQueries(), finishedQueries + 1);
    
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
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        new GrillConf(),
        MediaType.APPLICATION_XML_TYPE));

    final QueryPrepareHandle pHandle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryPrepareHandle.class);

    // Get all prepared queries
    List<QueryPrepareHandle> allQueries = (List<QueryPrepareHandle>)target
        .queryParam("sessionid", grillSessionId).request().get(new GenericType<List<QueryPrepareHandle>>(){});
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

    // do post once again
    QueryHandle handle2 = target.path(pHandle.toString()).request().post(
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
        ctx.getStatus().getStatus().equals(Status.RUNNING));

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
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
    
    validatePersistedResult(handle);

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

  private void validatePersistedResult(QueryHandle handle) throws IOException {
    final WebTarget target = target().path("queryapi/queries");
    // fetch results
    validateResultSetMetadata(handle);

    String presultset = target.path(handle.toString()).path(
        "resultset").queryParam("sessionid", grillSessionId).request().get(String.class);
    System.out.println("PERSISTED RESULT:" + presultset);

    PersistentQueryResult resultset = target.path(handle.toString()).path(
        "resultset").queryParam("sessionid", grillSessionId).request().get(PersistentQueryResult.class);
    validatePersistentResult(resultset, handle);
  }

  private void validatePersistentResult(PersistentQueryResult resultset,
      QueryHandle handle) throws IOException {
    Assert.assertTrue(resultset.getPersistedURI().endsWith(handle.toString()));
    Path actualPath = new Path(resultset.getPersistedURI());
    FileSystem fs = actualPath.getFileSystem(new Configuration());
    List<String> actualRows = new ArrayList<String>();
    for (FileStatus fstat : fs.listStatus(actualPath)) {
      FSDataInputStream in = fs.open(fstat.getPath());
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
    Assert.assertEquals(actualRows.get(0), "1one");
    Assert.assertEquals(actualRows.get(1), "\\Ntwo");
    Assert.assertEquals(actualRows.get(2), "3\\N");
    Assert.assertEquals(actualRows.get(3), "\\N\\N");
    Assert.assertEquals(actualRows.get(4), "5");    
  }

  // test with execute async post, get query, get results
  // test cancel query
  @Test
  public void testExecuteAsyncInMemoryResult() throws InterruptedException, IOException {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    GrillConf conf = new GrillConf();
    conf.addProperty(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, "false");
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
        ctx.getStatus().getStatus().equals(Status.RUNNING));

    // wait till the query finishes
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    // fetch results
    validateResultSetMetadata(handle);

    InMemoryQueryResult resultset = target.path(handle.toString()).path(
        "resultset").queryParam("sessionid", grillSessionId).request().get(InMemoryQueryResult.class);
    validateInmemoryResult(resultset);
  }

  private void validateResultSetMetadata(QueryHandle handle) {
    final WebTarget target = target().path("queryapi/queries");

    QueryResultSetMetadata metadata = target.path(handle.toString()).path(
        "resultsetmetadata").queryParam("sessionid", grillSessionId).request().get(QueryResultSetMetadata.class);
    Assert.assertEquals(metadata.getColumns().size(), 2);
    assertEquals("ID".toLowerCase(), metadata.getColumns().get(0).getName().toLowerCase());
    assertEquals("INT".toLowerCase(), metadata.getColumns().get(0).getType().name().toLowerCase());
    assertEquals("IDSTR".toLowerCase(), metadata.getColumns().get(1).getName().toLowerCase());
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
    validatePersistentResult((PersistentQueryResult) result.getResult(), result.getQueryHandle());
    
    final FormDataMultiPart mp2 = new FormDataMultiPart();
    GrillConf conf = new GrillConf();
    conf.addProperty(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, "false");
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
  public void testServerRestart() throws InterruptedException, IOException, GrillException {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

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

    // Restart the server
    queryService.closeSession(grillSessionId);
    restartGrillServer();
    queryService = (QueryExecutionServiceImpl)GrillServices.get().getService("query");
    grillSessionId = queryService.openSession("foo", "bar", new HashMap<String, String>());

    // Get query
    GrillQuery ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.QUEUED);

    // wait till the query finishes
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
    
    validatePersistedResult(handle);
  }
  
  @Override
  protected int getTestPort() {
    return 8083;
  }
}

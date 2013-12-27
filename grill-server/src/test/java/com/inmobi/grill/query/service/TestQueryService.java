package com.inmobi.grill.query.service;


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

import com.inmobi.grill.client.api.APIResult;
import com.inmobi.grill.client.api.InMemoryQueryResult;
import com.inmobi.grill.client.api.PersistentQueryResult;
import com.inmobi.grill.client.api.PreparedQueryContext;
import com.inmobi.grill.client.api.QueryConf;
import com.inmobi.grill.client.api.QueryContext;
import com.inmobi.grill.client.api.QueryPlan;
import com.inmobi.grill.client.api.QueryResultSetMetadata;
import com.inmobi.grill.service.GrillJerseyTest;
import com.inmobi.grill.service.GrillServices;
import com.inmobi.grill.api.GrillConfConstants;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryHandleWithResultSet;
import com.inmobi.grill.api.QueryPrepareHandle;
import com.inmobi.grill.api.QueryStatus;

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
import org.apache.hive.service.cli.SessionHandle;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestQueryService extends GrillJerseyTest {

  QueryExecutionServiceImpl queryService;
  GrillSessionHandle grillSessionId;
  
  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
    queryService = (QueryExecutionServiceImpl)GrillServices.get().getService("query");
    SessionHandle sessionHandle = queryService.openSession("foo", "bar", new HashMap<String, String>());
    grillSessionId = new GrillSessionHandle(sessionHandle);
  }

  @AfterTest
  public void tearDown() throws Exception {
    queryService.closeSession(grillSessionId.getSessionHandle());
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
    QueryConf conf = new QueryConf();
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
    QueryContext ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(QueryContext.class);
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(QueryContext.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
  }

  private void loadData(String tblName) throws InterruptedException {
    QueryConf conf = new QueryConf();
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
    QueryContext ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(QueryContext.class);
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(QueryContext.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

  }

  private void dropTable(String tblName) throws InterruptedException {
    QueryConf conf = new QueryConf();
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
    QueryContext ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(QueryContext.class);
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(QueryContext.class);
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

  // test with execute async post, get all queries, get query context,
  // get wrong uuid query
  @Test
  public void testQueriesAPI() throws InterruptedException {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query")
        .build(),
        "select name from table"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name(
        "operation").build(),
        "execute"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        new QueryConf(),
        MediaType.APPLICATION_XML_TYPE));
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

    QueryContext ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(QueryContext.class);
    // Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.QUEUED);

    // wait till the query finishes
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(QueryContext.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.FAILED);
    // Update conf for query
    final FormDataMultiPart confpart = new FormDataMultiPart();
    QueryConf conf = new QueryConf();
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
        new QueryConf(),
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
        new QueryConf(),
        MediaType.APPLICATION_XML_TYPE));

    final QueryPrepareHandle pHandle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryPrepareHandle.class);

    // Get all prepared queries
    List<QueryPrepareHandle> allQueries = (List<QueryPrepareHandle>)target
        .queryParam("sessionid", grillSessionId).request().get(new GenericType<List<QueryPrepareHandle>>(){});
    Assert.assertTrue(allQueries.size() >= 1);
    Assert.assertTrue(allQueries.contains(pHandle));

    PreparedQueryContext ctx = target.path(pHandle.toString()).queryParam("sessionid", grillSessionId).request().get(
        PreparedQueryContext.class);
    Assert.assertTrue(ctx.getUserQuery().equalsIgnoreCase(
        "select ID from " + testTable));
    Assert.assertTrue(ctx.getDriverQuery().equalsIgnoreCase(
        "select ID from " + testTable));
    Assert.assertEquals(ctx.getSelectedDriverClassName(),
        com.inmobi.grill.driver.hive.HiveDriver.class.getCanonicalName());
    Assert.assertNull(ctx.getConf().getProperties().get("my.property"));

    // Update conf for prepared query
    final FormDataMultiPart confpart = new FormDataMultiPart();
    QueryConf conf = new QueryConf();
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
        PreparedQueryContext.class);
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

    QueryContext ctx1 = target().path("queryapi/queries").path(
        handle1.toString()).queryParam("sessionid", grillSessionId).request().get(QueryContext.class);
    // wait till the query finishes
    QueryStatus stat = ctx1.getStatus();
    while (!stat.isFinished()) {
      ctx1 = target().path("queryapi/queries").path(
          handle1.toString()).queryParam("sessionid", grillSessionId).request().get(QueryContext.class);
      stat = ctx1.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx1.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    QueryContext ctx2 = target().path("queryapi/queries").path(
        handle2.toString()).queryParam("sessionid", grillSessionId).request().get(QueryContext.class);
    // wait till the query finishes
    stat = ctx2.getStatus();
    while (!stat.isFinished()) {
      ctx2 = target().path("queryapi/queries").path(
          handle1.toString()).queryParam("sessionid", grillSessionId).request().get(QueryContext.class);
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
        new QueryConf(),
        MediaType.APPLICATION_XML_TYPE));

    final QueryPlan plan = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryPlan.class);
    Assert.assertEquals(plan.getNumSels(), 1);
    Assert.assertEquals(plan.getTablesQueried().size(), 1);
    Assert.assertTrue(plan.getTablesQueried().get(0).equalsIgnoreCase(testTable));
    Assert.assertNotNull(plan.getPrepareHandle());

    PreparedQueryContext ctx = target.path(plan.getPrepareHandle().toString())
        .queryParam("sessionid", grillSessionId).request().get(PreparedQueryContext.class);
    Assert.assertTrue(ctx.getUserQuery().equalsIgnoreCase(
        "select ID from " + testTable));
    Assert.assertTrue(ctx.getDriverQuery().equalsIgnoreCase(
        "select ID from " + testTable));
    Assert.assertEquals(ctx.getSelectedDriverClassName(),
        com.inmobi.grill.driver.hive.HiveDriver.class.getCanonicalName());
    Assert.assertNull(ctx.getConf().getProperties().get("my.property"));

    // Update conf for prepared query
    final FormDataMultiPart confpart = new FormDataMultiPart();
    QueryConf conf = new QueryConf();
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
        PreparedQueryContext.class);
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

    QueryContext ctx1 = target().path("queryapi/queries").path(
        handle1.toString()).queryParam("sessionid", grillSessionId).request().get(QueryContext.class);
    // wait till the query finishes
    QueryStatus stat = ctx1.getStatus();
    while (!stat.isFinished()) {
      ctx1 = target().path("queryapi/queries").path(
          handle1.toString()).queryParam("sessionid", grillSessionId).request().get(QueryContext.class);
      stat = ctx1.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx1.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    QueryContext ctx2 = target().path("queryapi/queries").path(
        handle2.toString()).queryParam("sessionid", grillSessionId).request().get(QueryContext.class);
    // wait till the query finishes
    stat = ctx2.getStatus();
    while (!stat.isFinished()) {
      ctx2 = target().path("queryapi/queries").path(
          handle1.toString()).queryParam("sessionid", grillSessionId).request().get(QueryContext.class);
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
        new QueryConf(),
        MediaType.APPLICATION_XML_TYPE));
    final QueryHandle handle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

    Assert.assertNotNull(handle);

    // Get query
    QueryContext ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(QueryContext.class);
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.QUEUED);

    // wait till the query finishes
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(QueryContext.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    // fetch results
    QueryResultSetMetadata metadata = target.path(handle.toString()).path(
        "resultsetmetadata").queryParam("sessionid", grillSessionId).request().get(QueryResultSetMetadata.class);
    Assert.assertEquals(metadata.getColumns().size(), 2);
    Assert.assertEquals(metadata.getColumnCount(), 2);
    assertEquals("ID".toLowerCase(), metadata.getColumns().get(0).getName().toLowerCase());
    assertEquals("INT".toLowerCase(), metadata.getColumns().get(0).getType().name().toLowerCase());
    assertEquals("IDSTR".toLowerCase(), metadata.getColumns().get(1).getName().toLowerCase());
    assertEquals("STRING".toLowerCase(), metadata.getColumns().get(1).getType().name().toLowerCase());

    String presultset = target.path(handle.toString()).path(
        "resultset").queryParam("sessionid", grillSessionId).request().get(String.class);
    System.out.println("PERSISTED RESULT:" + presultset);

    PersistentQueryResult resultset = target.path(handle.toString()).path(
        "resultset").queryParam("sessionid", grillSessionId).request().get(PersistentQueryResult.class);
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

    // test cancel query
    final QueryHandle handle2 = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

    Assert.assertNotNull(handle2);
    APIResult result = target.path(handle2.toString()).queryParam("sessionid", grillSessionId).request().delete(APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    QueryContext ctx2 = target.path(handle2.toString()).queryParam("sessionid", grillSessionId).request().get(QueryContext.class);
    Assert.assertEquals(ctx2.getStatus().getStatus(), QueryStatus.Status.CANCELED);
  }

  // test with execute async post, get query, get results
  // test cancel query
  @Test
  public void testExecuteAsyncInMemoryResult() throws InterruptedException, IOException {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    QueryConf conf = new QueryConf();
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
    QueryContext ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(QueryContext.class);
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.QUEUED);

    // wait till the query finishes
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(QueryContext.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    // fetch results
    QueryResultSetMetadata metadata = target.path(handle.toString()).path(
        "resultsetmetadata").queryParam("sessionid", grillSessionId).request().get(QueryResultSetMetadata.class);
    Assert.assertEquals(metadata.getColumns().size(), 2);
    Assert.assertEquals(metadata.getColumnCount(), 2);
    assertEquals("ID".toLowerCase(), metadata.getColumns().get(0).getName().toLowerCase());
    assertEquals("INT".toLowerCase(), metadata.getColumns().get(0).getType().name().toLowerCase());
    assertEquals("IDSTR".toLowerCase(), metadata.getColumns().get(1).getName().toLowerCase());
    assertEquals("STRING".toLowerCase(), metadata.getColumns().get(1).getType().name().toLowerCase());

    //String presultset = target.path(handle.toString()).path(
    //    "resultset").request().get(String.class);
    //System.out.println("InMemory RESULT:" + presultset);

    InMemoryQueryResult resultset = target.path(handle.toString()).path(
        "resultset").queryParam("sessionid", grillSessionId).request().get(InMemoryQueryResult.class);
    Assert.assertEquals(resultset.getRows().size(), 5);
    Assert.assertEquals(resultset.getRows().get(0).getRow().get(0).getValue(), 1);
    Assert.assertEquals((String)resultset.getRows().get(0).getRow().get(1).getValue(), "one");

    Assert.assertNull(resultset.getRows().get(1).getRow().get(0).getValue());
    Assert.assertEquals((String)resultset.getRows().get(1).getRow().get(1).getValue(), "two");

    Assert.assertEquals(resultset.getRows().get(2).getRow().get(0).getValue(), 3);
    Assert.assertNull(resultset.getRows().get(2).getRow().get(1).getValue());

    Assert.assertNull(resultset.getRows().get(3).getRow().get(0).getValue());
    Assert.assertNull(resultset.getRows().get(3).getRow().get(1).getValue());
    Assert.assertEquals(resultset.getRows().get(4).getRow().get(0).getValue(), 5);
    Assert.assertEquals(resultset.getRows().get(4).getRow().get(1).getValue(), "");
  }

  // test execute with timeout, fetch results
  // cancel the query with execute_with_timeout
  @Test
  public void testExecuteWithTimeoutQuery() {
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        "select ID from " + testTable));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(),
        "execute_with_timeout"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        new QueryConf(),
        MediaType.APPLICATION_XML_TYPE));

    final QueryHandleWithResultSet result = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandleWithResultSet.class);
    Assert.assertNotNull(result.getQueryHandle());
  }

  @Override
  protected int getTestPort() {
    return 8083;
  }
}

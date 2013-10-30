package com.inmobi.grill.query.service;


import java.util.List;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import com.inmobi.grill.client.api.APIResult;
import com.inmobi.grill.client.api.PreparedQueryContext;
import com.inmobi.grill.client.api.QueryConf;
import com.inmobi.grill.client.api.QueryContext;
import com.inmobi.grill.client.api.QueryPlan;
import com.inmobi.grill.service.GrillJerseyTest;
import com.inmobi.grill.api.GrillConfConstants;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryPrepareHandle;
import com.inmobi.grill.api.QueryStatus;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.conf.HiveConf;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestQueryService extends GrillJerseyTest {

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
    SessionState.start(new HiveConf(TestQueryService.class));
  }

  @AfterTest
  public void tearDown() throws Exception {
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
  public static final String TEST_DATA_FILE = "../grill-driver-hive/testdata/testdata1.txt";

  private void createTable(String tblName) throws InterruptedException {
    QueryConf conf = new QueryConf();
    conf.addProperty(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, "false");
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    String createTable = "CREATE TABLE IF NOT EXISTS " + tblName  +"(ID STRING)";

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
    QueryContext ctx = target.path(handle.toString()).request().get(QueryContext.class);
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).request().get(QueryContext.class);
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
    QueryContext ctx = target.path(handle.toString()).request().get(QueryContext.class);
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).request().get(QueryContext.class);
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
    QueryContext ctx = target.path(handle.toString()).request().get(QueryContext.class);
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).request().get(QueryContext.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
  }

  @Test
  public void testGetQuery() {
    final WebTarget target = target().path("queryapi/queries");

    Response rs = target.path("random").request().get();
    Assert.assertEquals(rs.getStatus(), 400);
  }

  // test with execute async post, get all queries, get query context,
  // get wrong uuid query
  @Test
  public void testQueriesAPI() throws InterruptedException {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
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
    List<QueryHandle> allQueriesXML = target.request(MediaType.APPLICATION_XML)
        .get(new GenericType<List<QueryHandle>>() {
        });
    Assert.assertTrue(allQueriesXML.size() >= 1);

    //JSON
    //  List<QueryHandle> allQueriesJSON = target.request(
    //      MediaType.APPLICATION_JSON).get(new GenericType<List<QueryHandle>>() {
    //  });
    //  Assert.assertEquals(allQueriesJSON.size(), 1);
    //JAXB
    List<QueryHandle> allQueries = (List<QueryHandle>)target.request().get(
        new GenericType<List<QueryHandle>>(){});
    Assert.assertTrue(allQueries.size() >= 1);

    // Get query
    // Invocation.Builder builderjson = target.path(handle.toString()).request(MediaType.APPLICATION_JSON);
    // String responseJSON = builderjson.get(String.class);
    // System.out.println("query JSON:" + responseJSON);
    String queryXML = target.path(handle.toString()).request(MediaType.APPLICATION_XML).get(String.class);
    System.out.println("query XML:" + queryXML);

    Response response = target.path(handle.toString() + "001").request().get();
    Assert.assertEquals(response.getStatus(), 404);

    QueryContext ctx = target.path(handle.toString()).request().get(QueryContext.class);
   // Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.QUEUED);

    // wait till the query finishes
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).request().get(QueryContext.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.FAILED);
    // Update conf for prepared query
    final FormDataMultiPart confpart = new FormDataMultiPart();
    QueryConf conf = new QueryConf();
    conf.addProperty("my.property", "myvalue");
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
  public void testExplainQuery() throws InterruptedException {    
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
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
  // delete a prepared query
  // post to prepared query - pending
  @Test
  public void testPrepareQuery() throws InterruptedException {    
    final WebTarget target = target().path("queryapi/preparedqueries");

    final FormDataMultiPart mp = new FormDataMultiPart();
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

    PreparedQueryContext ctx = target.path(pHandle.toString()).request().get(
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
    confpart.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        conf,
        MediaType.APPLICATION_XML_TYPE));
    APIResult updateConf = target.path(pHandle.toString()).request().put(
        Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE),
        APIResult.class);
    Assert.assertEquals(updateConf.getStatus(), APIResult.Status.SUCCEEDED);

    ctx = target.path(pHandle.toString()).request().get(
        PreparedQueryContext.class);
    Assert.assertEquals(ctx.getConf().getProperties().get("my.property"),
        "myvalue");

    // destroy prepared
    APIResult result = target.path(pHandle.toString()).request().delete(APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  @Test
  public void testExplainAndPrepareQuery() throws InterruptedException {    
    final WebTarget target = target().path("queryapi/preparedqueries");

    final FormDataMultiPart mp = new FormDataMultiPart();
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
        .request().get(PreparedQueryContext.class);
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
    confpart.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        conf,
        MediaType.APPLICATION_XML_TYPE));
    APIResult updateConf = target.path(plan.getPrepareHandle().toString()).request().put(
        Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE),
        APIResult.class);
    Assert.assertEquals(updateConf.getStatus(), APIResult.Status.SUCCEEDED);

    ctx = target.path(plan.getPrepareHandle().toString()).request().get(
        PreparedQueryContext.class);
    Assert.assertEquals(ctx.getConf().getProperties().get("my.property"),
        "myvalue");

    // destroy prepared
    APIResult result = target.path(plan.getPrepareHandle().toString())
        .request().delete(APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  // test with execute async post, get query, get results
  //@Test
  public void testExecuteAsync() throws InterruptedException {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        "select ID from " + testTable));
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
    QueryContext ctx = target.path(handle.toString()).request().get(QueryContext.class);
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.QUEUED);

    // wait till the query finishes
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).request().get(QueryContext.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
  }

  //@Test
  public void testExecuteWithTimeoutQuery() {
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        "select name from table"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(),
        "execute_with_timeout"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        new QueryConf(),
        MediaType.APPLICATION_XML_TYPE));

    final QueryHandle s = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);
    System.out.println("QueryHandle:" + s.getHandleId());
  }

  @Override
  protected int getTestPort() {
    return 8083;
  }
}

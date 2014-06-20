package com.inmobi.grill.server.query;

import java.io.IOException;
import java.util.HashMap;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.query.GrillQuery;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.api.query.QueryStatus.Status;
import com.inmobi.grill.server.GrillJerseyTest;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.GrillConfConstants;

public class TestResultFormatting extends GrillJerseyTest {

  QueryExecutionServiceImpl queryService;
  GrillSessionHandle grillSessionId;

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
    queryService = (QueryExecutionServiceImpl)GrillServices.get().getService("query");
    grillSessionId = queryService.openSession("foo", "bar", new HashMap<String, String>());
    TestQueryService.createTable(testTable, target(), grillSessionId);
    TestQueryService.loadData(testTable, TestQueryService.TEST_DATA_FILE, target(), grillSessionId);
  }

  @AfterTest
  public void tearDown() throws Exception {
    TestQueryService.dropTable(testTable, target(), grillSessionId);
    queryService.closeSession(grillSessionId);
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

  private static String testTable = "RESULT_TEST_TABLE";

  @Override
  protected int getTestPort() {
    return 8888;
  }

  // test with execute async post with result formatter, get query, get results
  @Test(groups = "unit" )
  public void testResultFormatterInMemoryResult() throws InterruptedException, IOException {
    GrillConf conf = new GrillConf();
    conf.addProperty(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    conf.addProperty(GrillConfConstants.QUERY_OUTPUT_SERDE, LazySimpleSerDe.class.getCanonicalName());
    testResultFormatter(conf, QueryStatus.Status.SUCCESSFUL, false);
  }

  // test with execute async post with result formatter, get query, get results
  @Test(groups = "unit" )
  public void testResultFormatterHDFSpersistentResult() throws InterruptedException, IOException {
    GrillConf conf = new GrillConf();
    conf.addProperty(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "true");
    testResultFormatter(conf, QueryStatus.Status.SUCCESSFUL, false);
  }

  @Test(groups = "unit" )
  public void testPersistentResultWithMaxSize() throws InterruptedException, IOException {
    GrillConf conf = new GrillConf();
    conf.addProperty(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "true");
    conf.addProperty(GrillConfConstants.RESULT_FORMAT_SIZE_THRESHOLD, "1");
    testResultFormatter(conf, QueryStatus.Status.SUCCESSFUL, true);
  }

  @Test(groups = "unit" )
  public void testResultFormatterFailure() throws InterruptedException, IOException {
    GrillConf conf = new GrillConf();
    conf.addProperty(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    conf.addProperty(GrillConfConstants.QUERY_OUTPUT_SERDE, "NonexistentSerde.class");
    testResultFormatter(conf, QueryStatus.Status.FAILED, false);
  }

  // test with execute async post with result formatter, get query, get results
  private void testResultFormatter(GrillConf conf, Status status,
      boolean isDir) throws InterruptedException, IOException {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    conf.addProperty(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, "true");
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
    GrillQuery ctx = target.path(handle.toString()).queryParam("sessionid",
        grillSessionId).request().get(GrillQuery.class);
    // wait till the query finishes
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid",
          grillSessionId).request().get(GrillQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx.getStatus().getStatus(), status);

    if (status.equals(QueryStatus.Status.SUCCESSFUL)) {
      // fetch results
      TestQueryService.validatePersistedResult(handle, target(),
          grillSessionId, isDir);
    }
  }

}

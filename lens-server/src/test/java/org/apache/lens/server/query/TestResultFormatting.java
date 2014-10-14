package org.apache.lens.server.query;

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

import java.io.IOException;
import java.util.HashMap;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.lens.api.GrillConf;
import org.apache.lens.api.GrillSessionHandle;
import org.apache.lens.api.query.GrillQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.api.query.QueryStatus.Status;
import org.apache.lens.server.GrillJerseyTest;
import org.apache.lens.server.GrillServices;
import org.apache.lens.server.GrillTestUtil;
import org.apache.lens.server.api.GrillConfConstants;
import org.apache.lens.server.query.QueryApp;
import org.apache.lens.server.query.QueryExecutionServiceImpl;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


@Test(groups="unit-test")
public class TestResultFormatting extends GrillJerseyTest {

  QueryExecutionServiceImpl queryService;
  GrillSessionHandle lensSessionId;

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
    queryService = (QueryExecutionServiceImpl)GrillServices.get().getService("query");
    lensSessionId = queryService.openSession("foo", "bar", new HashMap<String, String>());
    GrillTestUtil.createTable(testTable, target(), lensSessionId);
    GrillTestUtil.loadData(testTable, TestQueryService.TEST_DATA_FILE, target(), lensSessionId);
  }

  @AfterTest
  public void tearDown() throws Exception {
    GrillTestUtil.dropTable(testTable, target(), lensSessionId);
    queryService.closeSession(lensSessionId);
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
  @Test
  public void testResultFormatterInMemoryResult() throws InterruptedException, IOException {
    GrillConf conf = new GrillConf();
    conf.addProperty(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    conf.addProperty(GrillConfConstants.QUERY_OUTPUT_SERDE, LazySimpleSerDe.class.getCanonicalName());
    testResultFormatter(conf, QueryStatus.Status.SUCCESSFUL, false, null);

    conf.addProperty(GrillConfConstants.RESULT_FS_READ_URL, "filereadurl://");
    testResultFormatter(conf, QueryStatus.Status.SUCCESSFUL, false, "filereadurl://");

  }

  // test with execute async post with result formatter, get query, get results
  @Test
  public void testResultFormatterHDFSpersistentResult() throws InterruptedException, IOException {
    GrillConf conf = new GrillConf();
    conf.addProperty(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "true");
    testResultFormatter(conf, QueryStatus.Status.SUCCESSFUL, false, null);

    conf.addProperty(GrillConfConstants.RESULT_FS_READ_URL, "filereadurl://");
    testResultFormatter(conf, QueryStatus.Status.SUCCESSFUL, false, "filereadurl://");
  }

  @Test
  public void testPersistentResultWithMaxSize() throws InterruptedException, IOException {
    GrillConf conf = new GrillConf();
    conf.addProperty(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "true");
    conf.addProperty(GrillConfConstants.RESULT_FORMAT_SIZE_THRESHOLD, "1");
    testResultFormatter(conf, QueryStatus.Status.SUCCESSFUL, true, null);
  }

  @Test
  public void testResultFormatterFailure() throws InterruptedException, IOException {
    GrillConf conf = new GrillConf();
    conf.addProperty(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    conf.addProperty(GrillConfConstants.QUERY_OUTPUT_SERDE, "NonexistentSerde.class");
    testResultFormatter(conf, QueryStatus.Status.FAILED, false, null);
  }

  // test with execute async post with result formatter, get query, get results
  private void testResultFormatter(GrillConf conf, Status status,
      boolean isDir, String reDirectUrl) throws InterruptedException, IOException {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    conf.addProperty(GrillConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        lensSessionId, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        "select ID, IDSTR from " + testTable));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name(
        "operation").build(),
        "execute"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        conf,
        MediaType.APPLICATION_XML_TYPE));
    QueryHandle handle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

    Assert.assertNotNull(handle);

    // Get query
    GrillQuery ctx = target.path(handle.toString()).queryParam("sessionid",
        lensSessionId).request().get(GrillQuery.class);
    // wait till the query finishes
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid",
          lensSessionId).request().get(GrillQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx.getStatus().getStatus(), status);

    if (status.equals(QueryStatus.Status.SUCCESSFUL)) {
      // fetch results
      TestQueryService.validatePersistedResult(handle, target(),
          lensSessionId, isDir);
      if (!isDir) {
        TestQueryService.validateHttpEndPoint(target(), lensSessionId, handle, reDirectUrl);
      }
    }
  }

}

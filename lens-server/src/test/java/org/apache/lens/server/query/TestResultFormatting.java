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
package org.apache.lens.server.query;

import static org.apache.lens.server.LensServerTestUtil.*;

import static org.testng.Assert.*;

import java.io.IOException;
import java.util.HashMap;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.api.query.QueryStatus.Status;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.query.InMemoryOutputFormatter;
import org.apache.lens.server.api.query.PersistedOutputFormatter;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.common.TestResourceFile;

import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class TestResultFormatting.
 */
@Test(groups = "unit-test")
@Slf4j
public class TestResultFormatting extends LensJerseyTest {

  /** The query service. */
  QueryExecutionServiceImpl queryService;

  /** The lens session id. */
  LensSessionHandle lensSessionId;

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#setUp()
   */
  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
    queryService = LensServices.get().getService(QueryExecutionService.NAME);
    lensSessionId = queryService.openSession("foo", "bar", new HashMap<String, String>());
    createTable(testTable, target(), lensSessionId,
      "(ID INT, IDSTR STRING, IDARR ARRAY<INT>, IDSTRARR ARRAY<STRING>)");
    loadDataFromClasspath(testTable, TestResourceFile.TEST_DATA2_FILE.getValue(), target(), lensSessionId);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#tearDown()
   */
  @AfterTest
  public void tearDown() throws Exception {
    dropTable(testTable, target(), lensSessionId);
    queryService.closeSession(lensSessionId);
    super.tearDown();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#configure()
   */
  @Override
  protected Application configure() {
    return new QueryApp();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#configureClient(org.glassfish.jersey.client.ClientConfig)
   */
  @Override
  protected void configureClient(ClientConfig config) {
    config.register(MultiPartFeature.class);
  }

  /** The test table. */
  private static String testTable = "RESULT_TEST_TABLE";

  // test with execute async post with result formatter, get query, get results

  /**
   * Test result formatter in memory result.
   *
   * @throws InterruptedException the interrupted exception
   * @throws IOException          Signals that an I/O exception has occurred.
   */
  @Test
  public void testResultFormatterInMemoryResult() throws InterruptedException, IOException {
    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    conf.addProperty(LensConfConstants.QUERY_OUTPUT_SERDE, LazySimpleSerDe.class.getCanonicalName());
    testResultFormatter(conf, QueryStatus.Status.SUCCESSFUL, false, null);

    queryService.conf.set(LensConfConstants.RESULT_FS_READ_URL, "filereadurl://");
    testResultFormatter(conf, QueryStatus.Status.SUCCESSFUL, false, "filereadurl://");
    queryService.conf.unset(LensConfConstants.RESULT_FS_READ_URL);
  }

  // test with execute async post with result formatter, get query, get results

  /**
   * Test result formatter hdfs persistent result.
   *
   * @throws InterruptedException the interrupted exception
   * @throws IOException          Signals that an I/O exception has occurred.
   */
  @Test
  public void testResultFormatterHDFSpersistentResult() throws InterruptedException, IOException {
    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "true");
    testResultFormatter(conf, QueryStatus.Status.SUCCESSFUL, false, null);

    queryService.conf.set(LensConfConstants.RESULT_FS_READ_URL, "filereadurl://");
    testResultFormatter(conf, QueryStatus.Status.SUCCESSFUL, false, "filereadurl://");
    queryService.conf.unset(LensConfConstants.RESULT_FS_READ_URL);
  }

  /**
   * Test persistent result with max size.
   *
   * @throws InterruptedException the interrupted exception
   * @throws IOException          Signals that an I/O exception has occurred.
   */
  @Test
  public void testPersistentResultWithMaxSize() throws InterruptedException, IOException {
    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "true");
    conf.addProperty(LensConfConstants.RESULT_FORMAT_SIZE_THRESHOLD, "1");
    testResultFormatter(conf, QueryStatus.Status.SUCCESSFUL, true, null);
  }

  /**
   * Test result formatter failure.
   *
   * @throws InterruptedException the interrupted exception
   * @throws IOException          Signals that an I/O exception has occurred.
   */
  @Test
  public void testResultFormatterFailure() throws InterruptedException, IOException {
    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    conf.addProperty(LensConfConstants.QUERY_OUTPUT_SERDE, "NonexistentSerde.class");
    testResultFormatter(conf, QueryStatus.Status.FAILED, false, null);
  }

  // test with execute async post with result formatter, get query, get results

  /**
   * Test result formatter.
   *
   * @param conf        the conf
   * @param status      the status
   * @param isDir       the is dir
   * @param reDirectUrl the re direct url
   * @throws InterruptedException the interrupted exception
   * @throws IOException          Signals that an I/O exception has occurred.
   */
  private void testResultFormatter(LensConf conf, Status status, boolean isDir, String reDirectUrl)
    throws InterruptedException, IOException {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
      "select ID, IDSTR, IDARR, IDSTRARR from " + testTable));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      MediaType.APPLICATION_XML_TYPE));
    QueryHandle handle = target.request()
      .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
        new GenericType<LensAPIResult<QueryHandle>>() {}).getData();

    assertNotNull(handle);

    // Get query
    LensQuery ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request()
      .get(LensQuery.class);
    // wait till the query finishes
    QueryStatus stat = ctx.getStatus();
    while (!stat.finished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request().get(LensQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }

    assertEquals(ctx.getStatus().getStatus(), status);

    if (status.equals(QueryStatus.Status.SUCCESSFUL)) {
      QueryContext qctx = queryService.getQueryContext(handle);
      if (qctx == null) {
        // This shouldn't occur. It is appearing when query gets purged. So adding extra logs
        // for debugging in the future.
        log.info("successful query's QueryContext is null");
        log.info("query handle: {}", handle);
        log.info("allQueries: {}", queryService.allQueries);
        // not doing formatter validation if qctx is null
      } else if (!isDir) {
        // isDir is true if the formatter is skipped due to result being the max size allowed
        if (qctx.isDriverPersistent()) {
          assertTrue(qctx.getQueryOutputFormatter() instanceof PersistedOutputFormatter);
        } else {
          assertTrue(qctx.getQueryOutputFormatter() instanceof InMemoryOutputFormatter);
        }
      } else {
        assertNull(qctx.getQueryOutputFormatter());
      }
      // fetch results
      TestQueryService.validatePersistedResult(handle, target(), lensSessionId, new String[][]{
        {"ID", "INT"}, {"IDSTR", "STRING"}, {"IDARR", "ARRAY"}, {"IDSTRARR", "ARRAY"},
      }, isDir);
      if (!isDir) {
        TestQueryService.validateHttpEndPoint(target(), lensSessionId, handle, reDirectUrl);
      }
    } else {
      assertTrue(ctx.getSubmissionTime() > 0);
      assertTrue(ctx.getLaunchTime() > 0);
      assertTrue(ctx.getDriverStartTime() > 0);
      assertTrue(ctx.getDriverFinishTime() > 0);
      assertTrue(ctx.getFinishTime() > 0);
      assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.FAILED);
    }
  }

  @AfterTest
  public void waitForPurge() throws InterruptedException {
    waitForPurge(0, queryService.finishedQueries);
  }
}

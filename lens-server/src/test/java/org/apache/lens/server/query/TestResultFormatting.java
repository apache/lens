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

import java.io.IOException;
import java.util.HashMap;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.api.query.QueryStatus.Status;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.LensTestUtil;
import org.apache.lens.server.api.LensConfConstants;
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

/**
 * The Class TestResultFormatting.
 */
@Test(groups = "unit-test")
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
    queryService = (QueryExecutionServiceImpl) LensServices.get().getService("query");
    lensSessionId = queryService.openSession("foo", "bar", new HashMap<String, String>());
    LensTestUtil.createTable(testTable, target(), lensSessionId);
    LensTestUtil.loadData(testTable, TestQueryService.TEST_DATA_FILE, target(), lensSessionId);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.glassfish.jersey.test.JerseyTest#tearDown()
   */
  @AfterTest
  public void tearDown() throws Exception {
    LensTestUtil.dropTable(testTable, target(), lensSessionId);
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

  @Override
  protected int getTestPort() {
    return 8888;
  }

  // test with execute async post with result formatter, get query, get results
  /**
   * Test result formatter in memory result.
   *
   * @throws InterruptedException
   *           the interrupted exception
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @Test
  public void testResultFormatterInMemoryResult() throws InterruptedException, IOException {
    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    conf.addProperty(LensConfConstants.QUERY_OUTPUT_SERDE, LazySimpleSerDe.class.getCanonicalName());
    testResultFormatter(conf, QueryStatus.Status.SUCCESSFUL, false, null);

    conf.addProperty(LensConfConstants.RESULT_FS_READ_URL, "filereadurl://");
    testResultFormatter(conf, QueryStatus.Status.SUCCESSFUL, false, "filereadurl://");

  }

  // test with execute async post with result formatter, get query, get results
  /**
   * Test result formatter hdf spersistent result.
   *
   * @throws InterruptedException
   *           the interrupted exception
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @Test
  public void testResultFormatterHDFSpersistentResult() throws InterruptedException, IOException {
    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "true");
    testResultFormatter(conf, QueryStatus.Status.SUCCESSFUL, false, null);

    conf.addProperty(LensConfConstants.RESULT_FS_READ_URL, "filereadurl://");
    testResultFormatter(conf, QueryStatus.Status.SUCCESSFUL, false, "filereadurl://");
  }

  /**
   * Test persistent result with max size.
   *
   * @throws InterruptedException
   *           the interrupted exception
   * @throws IOException
   *           Signals that an I/O exception has occurred.
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
   * @throws InterruptedException
   *           the interrupted exception
   * @throws IOException
   *           Signals that an I/O exception has occurred.
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
   * @param conf
   *          the conf
   * @param status
   *          the status
   * @param isDir
   *          the is dir
   * @param reDirectUrl
   *          the re direct url
   * @throws InterruptedException
   *           the interrupted exception
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void testResultFormatter(LensConf conf, Status status, boolean isDir, String reDirectUrl)
      throws InterruptedException, IOException {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
        MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID, IDSTR from "
        + testTable));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
        MediaType.APPLICATION_XML_TYPE));
    QueryHandle handle = target.request()
        .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

    Assert.assertNotNull(handle);

    // Get query
    LensQuery ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request()
        .get(LensQuery.class);
    // wait till the query finishes
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request().get(LensQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx.getStatus().getStatus(), status);

    if (status.equals(QueryStatus.Status.SUCCESSFUL)) {
      // fetch results
      TestQueryService.validatePersistedResult(handle, target(), lensSessionId, isDir);
      if (!isDir) {
        TestQueryService.validateHttpEndPoint(target(), lensSessionId, handle, reDirectUrl);
      }
    }
  }

}

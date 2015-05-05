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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.lens.api.response.LensResponse;
import org.apache.lens.api.response.NoErrorPayload;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.LensTestUtil;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.common.TestResourceFile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.subethamail.wiser.Wiser;
import org.subethamail.wiser.WiserMessage;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * The query completion email notifier
 */
@Test(groups = "unit-test")
public class TestQueryEndEmailNotifier extends LensJerseyTest {

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(TestQueryEndEmailNotifier.class);
  private static final int NUM_ITERS = 30;
  /** The query service. */
  QueryExecutionServiceImpl queryService;

  /** The lens session id. */
  LensSessionHandle lensSessionId;

  /** The wiser. */
  private Wiser wiser;

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#setUp()
   */
  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
    wiser = new Wiser();
    wiser.setHostname("localhost");
    wiser.setPort(25000);
    queryService = (QueryExecutionServiceImpl) LensServices.get().getService("query");
    Map<String, String> sessionconf = new HashMap<String, String>();
    sessionconf.put("test.session.key", "svalue");
    sessionconf.put(LensConfConstants.QUERY_MAIL_NOTIFY, "true");
    sessionconf.put(LensConfConstants.QUERY_RESULT_EMAIL_CC, "foo1@localhost,foo2@localhost,foo3@localhost");
    lensSessionId = queryService.openSession("foo@localhost", "bar", sessionconf); // @localhost should be removed
    // automatically
    createTable(TEST_TABLE);
    loadData(TEST_TABLE, TestResourceFile.TEST_DATA2_FILE.getValue());
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#tearDown()
   */
  @AfterTest
  public void tearDown() throws Exception {
    dropTable(TEST_TABLE);
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
  public static final String TEST_TABLE = "EMAIL_NOTIFIER_TEST_TABLE";

  /**
   * Creates the table.
   *
   * @param tblName the tbl name
   * @throws InterruptedException the interrupted exception
   */
  private void createTable(String tblName) throws InterruptedException {
    LensTestUtil.createTable(tblName, target(), lensSessionId);
  }

  /**
   * Load data.
   *
   * @param tblName      the tbl name
   * @param testDataFile the test data file
   * @throws InterruptedException the interrupted exception
   */
  private void loadData(String tblName, final String testDataFile) throws InterruptedException {
    LensTestUtil.loadDataFromClasspath(tblName, testDataFile, target(), lensSessionId);
  }

  /**
   * Drop table.
   *
   * @param tblName the tbl name
   * @throws InterruptedException the interrupted exception
   */
  private void dropTable(String tblName) throws InterruptedException {
    LensTestUtil.dropTable(tblName, target(), lensSessionId);
  }

  private QueryHandle launchAndWaitForQuery(LensConf conf, String query, Status expectedStatus)
    throws InterruptedException {
    final WebTarget target = target().path("queryapi/queries");
    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), query));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      MediaType.APPLICATION_XML_TYPE));
    final QueryHandle handle = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      new GenericType<LensResponse<QueryHandle, NoErrorPayload>>(){}).getData();

    Assert.assertNotNull(handle);
    LensQuery ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request()
      .get(LensQuery.class);
    QueryStatus stat = ctx.getStatus();
    while (!stat.finished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request().get(LensQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx.getStatus().getStatus(), expectedStatus);
    return handle;
  }

  /**
   * Test launch fail.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testEmailNotification() throws InterruptedException {
    wiser.start();
    LensConf conf = new LensConf();
    // launch failure
    QueryHandle handle = launchAndWaitForQuery(conf, "select ID from non_exist_table", QueryStatus.Status.FAILED);
    List<WiserMessage> messages = new ArrayList<WiserMessage>();
    for (int i = 0; i < NUM_ITERS; i++) {
      messages = wiser.getMessages();
      if (messages.size() >= 4) {
        break;
      }
      Thread.sleep(10000);
    }

    Assert.assertEquals(messages.size(), 4);
    Assert.assertTrue(messages.get(0).toString().contains(handle.toString()));
    Assert.assertTrue(messages.get(0).toString().contains("Launching query failed"));
    Assert.assertTrue(messages.get(0).toString().contains("Reason"));

    // rewriter failure
    handle = launchAndWaitForQuery(conf, "cube select ID from nonexist", QueryStatus.Status.FAILED);
    messages = new ArrayList<WiserMessage>();
    for (int i = 0; i < NUM_ITERS; i++) {
      messages = wiser.getMessages();
      if (messages.size() >= 8) {
        break;
      }
      Thread.sleep(10000);
    }

    Assert.assertEquals(messages.size(), 8);
    Assert.assertTrue(messages.get(4).toString().contains(handle.toString()));
    Assert.assertTrue(messages.get(4).toString().contains("Launching query failed"));
    Assert.assertTrue(messages.get(4).toString().contains("Reason"));

    // formatting failure
    conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    conf.addProperty(LensConfConstants.QUERY_OUTPUT_SERDE, "NonexistentSerde.class");
    handle = launchAndWaitForQuery(conf, "select ID, IDSTR from " + TEST_TABLE,
      QueryStatus.Status.FAILED);
    messages = new ArrayList<WiserMessage>();
    for (int i = 0; i < NUM_ITERS; i++) {
      messages = wiser.getMessages();
      if (messages.size() >= 12) {
        break;
      }
      Thread.sleep(10000);
    }

    Assert.assertEquals(messages.size(), 12);
    Assert.assertTrue(messages.get(8).toString().contains(handle.toString()));
    Assert.assertTrue(messages.get(8).toString().contains("Result formatting failed!"));
    Assert.assertTrue(messages.get(8).toString().contains("Reason"));

    // execution failure
    conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "true");
    conf.addProperty(HiveConf.ConfVars.COMPRESSRESULT.name(), "true");
    conf.addProperty("mapred.compress.map.output", "true");
    conf.addProperty("mapred.map.output.compression.codec", "nonexisting");
    handle = launchAndWaitForQuery(conf, "select count(ID) from " + TEST_TABLE, QueryStatus.Status.FAILED);
    messages = new ArrayList<WiserMessage>();
    for (int i = 0; i < NUM_ITERS; i++) {
      messages = wiser.getMessages();
      if (messages.size() >= 16) {
        break;
      }
      Thread.sleep(10000);
    }

    Assert.assertEquals(messages.size(), 16);
    Assert.assertTrue(messages.get(12).toString().contains(handle.toString()));
    Assert.assertTrue(messages.get(12).toString().contains("Query execution failed!"));
    Assert.assertTrue(messages.get(12).toString().contains("Reason"));

    // successful query
    conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "true");
    handle = launchAndWaitForQuery(conf, "select ID, IDSTR from " + TEST_TABLE, QueryStatus.Status.SUCCESSFUL);
    messages = new ArrayList<WiserMessage>();
    for (int i = 0; i < NUM_ITERS; i++) {
      messages = wiser.getMessages();
      if (messages.size() >= 20) {
        break;
      }
      Thread.sleep(10000);
    }
    Assert.assertEquals(messages.size(), 20);
    Assert.assertTrue(messages.get(16).toString().contains(handle.toString()));
    Assert.assertTrue(messages.get(16).toString().contains("Query  SUCCESSFUL"));
    Assert.assertTrue(messages.get(16).toString().contains("Result available at"));
    wiser.stop();
  }
}

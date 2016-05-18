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

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.driver.hive.HiveDriver;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServerTestUtil;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.common.TestResourceFile;
import org.apache.lens.server.query.TestQueryService.QueryServiceTestApp;

import org.apache.hadoop.hive.conf.HiveConf;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.test.TestProperties;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

@Test(groups = "duplicate-query", dependsOnGroups = "two-working-drivers")
@Slf4j
public class TestQueryServiceDuplicate extends LensJerseyTest {
  private HiveConf serverConf;
  /** The query service. */
  QueryExecutionServiceImpl queryService;

  /** The metrics svc. */
  MetricsService metricsSvc;

  /** The lens session id. */
  LensSessionHandle lensSessionId;

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public HiveConf getServerConf() {
    serverConf = new HiveConf(super.getServerConf());
    serverConf.setBoolean(LensConfConstants.SERVER_DUPLICATE_QUERY_ALLOWED, false);
    return serverConf;
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
    for (LensDriver driver : queryService.getDrivers()) {
      if (driver instanceof HiveDriver) {
        assertFalse(((HiveDriver) driver).hasLensSession(lensSessionId));
      }
    }
    super.tearDown();
  }

  @Override
  protected Application configure() {
    enable(TestProperties.LOG_TRAFFIC);
    enable(TestProperties.DUMP_ENTITY);
    return new QueryServiceTestApp();
  }

  /** The test table. */
  public static final String TEST_TABLE = "TEST_TABLE";

  /**
   * Creates the table.
   *
   * @param tblName
   *          the tbl name
   * @throws InterruptedException
   *           the interrupted exception
   */
  private void createTable(String tblName) throws InterruptedException {
    LensServerTestUtil.createTable(tblName, target(), lensSessionId, defaultMT);
  }

  /**
   * Load data.
   *
   * @param tblName
   *          the tbl name
   * @param testDataFile
   *          the test data file
   * @throws InterruptedException
   *           the interrupted exception
   */
  private void loadData(String tblName, final String testDataFile) throws InterruptedException {
    LensServerTestUtil.loadDataFromClasspath(tblName, testDataFile, target(), lensSessionId, defaultMT);
  }

  /**
   * Drop table.
   *
   * @param tblName
   *          the tbl name
   * @throws InterruptedException
   *           the interrupted exception
   */
  private void dropTable(String tblName) throws InterruptedException {
    LensServerTestUtil.dropTable(tblName, target(), lensSessionId, defaultMT);
  }

  /**
   * Checks duplicate query handle. In the starting of test, the lens-server is
   * started with the new configuration.
   *
   * @throws Exception
   */
  public void testExecuteAsyncDuplicate() throws Exception {
    MediaType mt = MediaType.APPLICATION_JSON_TYPE;
    log.info("Restarting lens server!");
    restartLensServer(getServerConf(), false);
    log.info("Restarted lens server!");
    queryService = LensServices.get().getService(QueryExecutionService.NAME);
    metricsSvc = LensServices.get().getService(MetricsService.NAME);
    Map<String, String> sessionconf = new HashMap<>();
    sessionconf.put("test.session.key", "svalue");
    lensSessionId = queryService.openSession("foo", "bar", sessionconf);
    // automatically
    createTable(TEST_TABLE);
    loadData(TEST_TABLE, TestResourceFile.TEST_DATA2_FILE.getValue());
    try {
      final WebTarget target = target().path("queryapi/queries");
      queryService.pauseQuerySubmitter(true);
      final FormDataMultiPart mp = new FormDataMultiPart();
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId, mt));
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID, IDSTR from "
          + TEST_TABLE));
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(),
          new LensConf(), mt));
      // Dummy query
      final QueryHandle handle = target.request(mt)
          .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), new GenericType<LensAPIResult<QueryHandle>>() {
          }).getData();

      target.path(handle.toString()).queryParam("sessionid", lensSessionId).request(mt).delete(APIResult.class);

      final QueryHandle handle1 = target.request(mt)
          .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), new GenericType<LensAPIResult<QueryHandle>>() {
          }).getData();

      final QueryHandle handle2 = target.request(mt)
          .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), new GenericType<LensAPIResult<QueryHandle>>() {
          }).getData();

      assertNotNull(handle1);
      assertNotNull(handle2);
      assertEquals(handle1, handle2);
      // Cancel the query
      target.path(handle1.toString()).queryParam("sessionid", lensSessionId).request(mt).delete(APIResult.class);

      // Create a different query
      final FormDataMultiPart mp1 = new FormDataMultiPart();
      mp1.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId, mt));
      mp1.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID, IDSTR from "
          + TEST_TABLE));
      mp1.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
      mp1.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(),
          new LensConf(), mt));
      final QueryHandle handle3 = target.request(mt)
          .post(Entity.entity(mp1, MediaType.MULTIPART_FORM_DATA_TYPE), new GenericType<LensAPIResult<QueryHandle>>() {
          }).getData();
      assertNotNull(handle3);
      target.path(handle3.toString()).queryParam("sessionid", lensSessionId).request(mt).delete(APIResult.class);

      // After completion If we launch the same query it should return a new
      // handle.
      final QueryHandle handle4 = target.request(mt)
          .post(Entity.entity(mp1, MediaType.MULTIPART_FORM_DATA_TYPE), new GenericType<LensAPIResult<QueryHandle>>() {
          }).getData();
      assertNotEquals(handle4, handle3);

      target.path(handle4.toString()).queryParam("sessionid", lensSessionId).request(mt).delete(APIResult.class);
      // Launch the query in different session should result in the different
      // handle.

      final FormDataMultiPart mp2 = new FormDataMultiPart();
      mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId, mt));
      mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID, IDSTR from "
          + TEST_TABLE));
      mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
      mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(),
          new LensConf(), mt));
      final QueryHandle handle5 = target.request(mt)
          .post(Entity.entity(mp2, MediaType.MULTIPART_FORM_DATA_TYPE), new GenericType<LensAPIResult<QueryHandle>>() {
          }).getData();

      sessionconf = new HashMap<>();
      sessionconf.put("test.session.key", "svalue");

      LensSessionHandle lensSessionId1 = queryService.openSession("foo@localhost", "bar", sessionconf);
      final FormDataMultiPart mp3 = new FormDataMultiPart();
      mp3.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId1, mt));
      mp3.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID, IDSTR from "
          + TEST_TABLE));
      mp3.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
      mp3.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(),
          new LensConf(), mt));
      final QueryHandle handle6 = target.request(mt)
          .post(Entity.entity(mp3, MediaType.MULTIPART_FORM_DATA_TYPE), new GenericType<LensAPIResult<QueryHandle>>() {
          }).getData();

      assertNotNull(handle5);
      assertNotNull(handle6);
      assertNotEquals(handle5, handle6);
      target.path(handle5.toString()).queryParam("sessionid", lensSessionId).request(mt).delete(APIResult.class);
      target.path(handle6.toString()).queryParam("sessionid", lensSessionId1).request(mt).delete(APIResult.class);

      // Diffrent conf should different handle
      LensConf conf = new LensConf();
      final FormDataMultiPart mp4 = new FormDataMultiPart();
      mp4.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId, mt));
      mp4.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID, IDSTR from "
          + TEST_TABLE));
      mp4.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
      mp4.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf, mt));
      final QueryHandle handle7 = target.request(mt)
          .post(Entity.entity(mp4, MediaType.MULTIPART_FORM_DATA_TYPE), new GenericType<LensAPIResult<QueryHandle>>() {
          }).getData();

      // Add a property
      conf.addProperty("test", "test");
      final FormDataMultiPart mp5 = new FormDataMultiPart();
      mp5.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId, mt));
      mp5.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID, IDSTR from "
          + TEST_TABLE));
      mp5.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
      mp5.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf, mt));
      final QueryHandle handle8 = target.request(mt)
          .post(Entity.entity(mp5, MediaType.MULTIPART_FORM_DATA_TYPE), new GenericType<LensAPIResult<QueryHandle>>() {
          }).getData();
      assertNotNull(handle7);
      assertNotNull(handle8);
      assertNotEquals(handle7, handle8);
      target.path(handle7.toString()).queryParam("sessionid", lensSessionId).request(mt).delete(APIResult.class);
      target.path(handle8.toString()).queryParam("sessionid", lensSessionId1).request(mt).delete(APIResult.class);
    } finally {
      queryService.pauseQuerySubmitter(false);
    }
  }
}

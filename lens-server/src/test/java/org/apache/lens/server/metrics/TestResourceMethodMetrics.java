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
package org.apache.lens.server.metrics;

import static org.testng.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotAllowedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.server.LensAllApplicationJerseyTest;
import org.apache.lens.server.LensApplication;
import org.apache.lens.server.LensServerTestUtil;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.metastore.CubeMetastoreService;
import org.apache.lens.server.api.metrics.MethodMetrics;
import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.lens.server.common.TestResourceFile;
import org.apache.lens.server.metastore.CubeMetastoreServiceImpl;
import org.apache.lens.server.query.TestQueryService;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.test.TestProperties;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

@Test(groups = "unit-test")
@Slf4j
public class TestResourceMethodMetrics extends LensAllApplicationJerseyTest {
  private CubeMetastoreServiceImpl metastoreService;
  private MetricsServiceImpl metricsSvc;
  private LensSessionHandle lensSessionId;
  protected String mediaType = MediaType.APPLICATION_XML;
  private Map<String, MethodMetrics> methodMetricsMap;

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#setUp()
   */
  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
    metricsSvc = LensServices.get().getService(MetricsService.NAME);
    metastoreService = LensServices.get().getService(CubeMetastoreService.NAME);
    lensSessionId = metastoreService.openSession("foo", "bar", new HashMap<String, String>());
    methodMetricsMap = metricsSvc.getMethodMetricsFactory().getMethodMetricsMap();
    //reset
  }

  private void createTable(String tblName) throws InterruptedException {
    LensServerTestUtil.createTable(tblName, target(), lensSessionId, defaultMT);
  }

  private void loadData(String tblName, final String testDataFile) throws InterruptedException {
    LensServerTestUtil.loadDataFromClasspath(tblName, testDataFile, target(), lensSessionId, defaultMT);
  }

  @AfterTest
  public void tearDown() throws Exception {
    LensServerTestUtil.dropTable(TestQueryService.TEST_TABLE, target(), lensSessionId, defaultMT);
    metastoreService.closeSession(lensSessionId);
    super.tearDown();
  }

  private void disableResourceMethodMetering() {
    metricsSvc.setEnableResourceMethodMetering(false);
    Assert.assertEquals(methodMetricsMap.size(), 0);
    Assert.assertEquals(metricsSvc.getMetricRegistry().getMeters().size(), 0);
    Assert.assertEquals(metricsSvc.getMetricRegistry().getTimers().size(), 0);
  }

  @Override
  protected Application configure() {
    enable(TestProperties.LOG_TRAFFIC);
    enable(TestProperties.DUMP_ENTITY);
    return new LensApplication();
  }

  @Test
  public void test() throws Exception {
    boolean enabled = metricsSvc.isEnableResourceMethodMetering();
    disableResourceMethodMetering();
    metricsSvc.setEnableResourceMethodMetering(true);
    Assert.assertEquals(methodMetricsMap.size(), 0);
    log.info("database operations");
    databaseOperations();
    Assert.assertEquals(methodMetricsMap.size(), 3);
    log.info("create table");
    createTable(TestQueryService.TEST_TABLE);
    Assert.assertEquals(methodMetricsMap.size(), 5);
    log.info("load data");
    loadData(TestQueryService.TEST_TABLE, TestResourceFile.TEST_DATA2_FILE.getValue());
    Assert.assertEquals(methodMetricsMap.size(), 5);
    log.info("execute async");
    executeAsync();
    verifyValues();
    makeClientError();
    // no change in values
    verifyValues();
    disableResourceMethodMetering();
    metricsSvc.setEnableResourceMethodMetering(enabled);
  }

  private void makeClientError() throws Exception {
    WebTarget dbTarget = target().path("metastore").path("databases/blah");
    try {
      dbTarget.queryParam("sessionid", lensSessionId).request(mediaType).get(APIResult.class);
      fail("Should get 404");
    } catch (NotAllowedException e) {
      // expected
      log.error("Not found excepiton:", e);
    }
  }

  private void verifyValues() throws InterruptedException {
    // wait for values to stabilize
    Thread.sleep(5);
    Assert.assertEquals(methodMetricsMap.size(), 5);
    // 2 for each
    Assert.assertEquals(metricsSvc.getMetricRegistry().getTimers().size(), 10);
    // 1 for each
    Assert.assertEquals(metricsSvc.getMetricRegistry().getMeters().size(), 5);
    Assert.assertFalse(methodMetricsMap.containsKey(
      "org.apache.lens.server.query.QueryServiceResource.query.POST.EXPLAIN"));
    Assert.assertEquals(methodMetricsMap.get("org.apache.lens.server.metastore.MetastoreResource.setDatabase.PUT")
      .getSuccessCount(), 2);
    Assert.assertEquals(methodMetricsMap.get(
      "org.apache.lens.server.metastore.MetastoreResource.setDatabase.PUT").getErrorCount(), 2);
    Assert.assertEquals(methodMetricsMap.get("org.apache.lens.server.metastore.MetastoreResource.getDatabase.GET")
      .getSuccessCount(), 2);
    Assert.assertEquals(methodMetricsMap.get(
      "org.apache.lens.server.metastore.MetastoreResource.getDatabase.GET").getErrorCount(), 0);
    Assert.assertTrue(methodMetricsMap.get("org.apache.lens.server.query.QueryServiceResource.getStatus.GET")
      .getSuccessCount() >= 0);
    Assert.assertEquals(methodMetricsMap.get(
      "org.apache.lens.server.query.QueryServiceResource.getStatus.GET").getErrorCount(), 0);
    Assert.assertEquals(methodMetricsMap.get(
      "org.apache.lens.server.metastore.MetastoreResource.createDatabase.POST").getSuccessCount(), 1);
    Assert.assertEquals(methodMetricsMap.get(
      "org.apache.lens.server.metastore.MetastoreResource.createDatabase.POST").getErrorCount(), 0);
    Assert.assertEquals(methodMetricsMap.get(
      "org.apache.lens.server.query.QueryServiceResource.query.POST.EXECUTE").getSuccessCount(), 3);
    Assert.assertEquals(methodMetricsMap.get(
      "org.apache.lens.server.query.QueryServiceResource.query.POST.EXECUTE").getErrorCount(), 0);

    for (String key : methodMetricsMap.keySet()) {
      long count = methodMetricsMap.get(key).getCount();
      long success = methodMetricsMap.get(key).getSuccessCount();
      long errors = methodMetricsMap.get(key).getErrorCount();
      Assert.assertEquals(count, success + errors, "Total:" + count + ", success: " + success
        + ", errors: " + errors);
    }
  }

  private void databaseOperations() throws Exception {
    String prevDb = getCurrentDatabase();
    WebTarget dbTarget = target().path("metastore").path("databases/current");
    String dbName = "random_" + UUID.randomUUID().toString().substring(0, 6);
    try {
      dbTarget.queryParam("sessionid", lensSessionId).request(mediaType).put(Entity.xml(dbName), APIResult.class);
      fail("Should get 404");
    } catch (NotFoundException e) {
      // expected
    }

    // create
    APIResult result = target().path("metastore").path("databases")
      .queryParam("sessionid", lensSessionId).request(mediaType).post(Entity.xml(dbName), APIResult.class);
    assertNotNull(result);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    // set
    result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType)
      .put(Entity.xml(dbName), APIResult.class);
    assertNotNull(result);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    // set without session id, we should get bad request
    try {
      result = dbTarget.request(mediaType).put(Entity.xml(dbName), APIResult.class);
      fail("Should have thrown bad request exception");
    } catch (BadRequestException badReq) {
      // expected
    }

    String current = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType).get(String.class);
    assertEquals(current, dbName);
    setCurrentDatabase(prevDb);
  }

  private void executeAsync() {
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID, IDSTR from "
      + TestQueryService.TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      MediaType.APPLICATION_XML_TYPE));
    final QueryHandle handle = target.request(mediaType).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
        new GenericType<LensAPIResult<QueryHandle>>() {}).getData();

    Assert.assertNotNull(handle);
  }

  private String getCurrentDatabase() throws Exception {
    WebTarget dbTarget = target().path("metastore").path("databases/current");
    Invocation.Builder builder = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType);
    String response = builder.get(String.class);
    return response;
  }

  private void setCurrentDatabase(String dbName) throws Exception {
    WebTarget dbTarget = target().path("metastore").path("databases/current");
    APIResult result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType).put(Entity.xml(dbName),
      APIResult.class);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

}

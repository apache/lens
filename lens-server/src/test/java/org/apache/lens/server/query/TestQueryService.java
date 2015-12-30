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

import static javax.ws.rs.core.Response.Status.*;

import static org.apache.lens.server.LensServerTestUtil.DB_WITH_JARS;
import static org.apache.lens.server.LensServerTestUtil.DB_WITH_JARS_2;
import static org.apache.lens.server.api.LensServerAPITestUtil.getLensConf;
import static org.apache.lens.server.common.RestAPITestUtil.*;

import static org.testng.Assert.*;

import java.io.*;
import java.util.*;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.jaxb.LensJAXBContextResolver;
import org.apache.lens.api.query.*;
import org.apache.lens.api.query.QueryStatus.Status;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.api.result.LensErrorTO;
import org.apache.lens.api.result.QueryCostTO;
import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.driver.hive.HiveDriver;
import org.apache.lens.driver.hive.LensHiveErrorCode;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServerTestUtil;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.metrics.LensMetricsRegistry;
import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.api.session.SessionService;
import org.apache.lens.server.common.ErrorResponseExpectedData;
import org.apache.lens.server.common.TestDataUtils;
import org.apache.lens.server.common.TestResourceFile;
import org.apache.lens.server.error.LensExceptionMapper;
import org.apache.lens.server.session.HiveSessionService;
import org.apache.lens.server.session.LensSessionImpl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.test.TestProperties;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * The Class TestQueryService.
 */
@Slf4j
@Test(groups = "unit-test")
public class TestQueryService extends LensJerseyTest {

  /** The query service. */
  QueryExecutionServiceImpl queryService;

  /** The metrics svc. */
  MetricsService metricsSvc;

  /** The lens session id. */
  LensSessionHandle lensSessionId;

  public static class QueryServiceTestApp extends QueryApp {

    @Override
    public Set<Class<?>> getClasses() {
      final Set<Class<?>> classes = super.getClasses();
      classes.add(LensExceptionMapper.class);
      classes.add(LensJAXBContextResolver.class);
      return classes;
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#setUp()
   */
  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
    queryService = LensServices.get().getService(QueryExecutionService.NAME);
    metricsSvc = LensServices.get().getService(MetricsService.NAME);
    Map<String, String> sessionconf = new HashMap<String, String>();
    sessionconf.put("test.session.key", "svalue");
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
    for (LensDriver driver : queryService.getDrivers()) {
      if (driver instanceof HiveDriver) {
        assertFalse(((HiveDriver) driver).hasLensSession(lensSessionId));
      }
    }
    super.tearDown();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#configure()
   */
  @Override
  protected Application configure() {
    enable(TestProperties.LOG_TRAFFIC);
    enable(TestProperties.DUMP_ENTITY);
    return new QueryServiceTestApp();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#configureClient(org.glassfish.jersey.client.ClientConfig)
   */
  @Override
  protected void configureClient(ClientConfig config) {
    config.register(MultiPartFeature.class);
    config.register(LensJAXBContextResolver.class);
  }

  /** The test table. */
  public static final String TEST_TABLE = "TEST_TABLE";

  /**
   * Creates the table.
   *
   * @param tblName the tbl name
   * @throws InterruptedException the interrupted exception
   */
  private void createTable(String tblName) throws InterruptedException {
    LensServerTestUtil.createTable(tblName, target(), lensSessionId);
  }

  /**
   * Load data.
   *
   * @param tblName      the tbl name
   * @param testDataFile the test data file
   * @throws InterruptedException the interrupted exception
   */
  private void loadData(String tblName, final String testDataFile) throws InterruptedException {
    LensServerTestUtil.loadDataFromClasspath(tblName, testDataFile, target(), lensSessionId);
  }

  /**
   * Drop table.
   *
   * @param tblName the tbl name
   * @throws InterruptedException the interrupted exception
   */
  private void dropTable(String tblName) throws InterruptedException {
    LensServerTestUtil.dropTable(tblName, target(), lensSessionId);
  }

  // test get a random query, should return 400

  /**
   * Test get random query.
   */
  @Test
  public void testGetRandomQuery() {
    final WebTarget target = target().path("queryapi/queries");

    Response rs = target.path("random").queryParam("sessionid", lensSessionId).request().get();
    assertEquals(rs.getStatus(), 400);
  }

  @Test
  public void testLoadingMultipleDrivers() {
    Collection<LensDriver> drivers = queryService.getDrivers();
    assertEquals(drivers.size(), 4);
    Set<String> driverNames = new HashSet<String>(drivers.size());
    for(LensDriver driver : drivers){
      assertEquals(driver.getConf().get("lens.driver.test.drivername"), driver.getFullyQualifiedName());
      driverNames.add(driver.getFullyQualifiedName());
    }
    assertTrue(driverNames.containsAll(Arrays.asList("hive/hive1", "hive/hive2", "jdbc/jdbc1", "mock/fail1")));
  }

  /**
   * Test rewrite failure in execute operation.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testRewriteFailureInExecute() throws InterruptedException {
    final WebTarget target = target().path("queryapi/queries");
    LensConf conf = new LensConf();
    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(
      new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID from non_exist_table"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      MediaType.APPLICATION_XML_TYPE));
    final Response response = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));
    assertEquals(response.getStatus(), BAD_REQUEST.getStatusCode());
  }

  /**
   * Test launch failure in execute operation.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testLaunchFail() throws InterruptedException {
    LensQuery lensQuery = executeAndWaitForQueryToFinish(target(), lensSessionId, "select fail from non_exist",
      Optional.<LensConf>absent(), Optional.of(Status.FAILED));
    assertTrue(lensQuery.getSubmissionTime() > 0);
    assertEquals(lensQuery.getLaunchTime(), 0);
    assertEquals(lensQuery.getDriverStartTime(), 0);
    assertEquals(lensQuery.getDriverFinishTime(), 0);
    assertTrue(lensQuery.getFinishTime() > 0);
  }

  // test with execute async post, get all queries, get query context,
  // get wrong uuid query

  /**
   * Test queries api.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testQueriesAPI() throws InterruptedException {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

    long queuedQueries = metricsSvc.getQueuedQueries();
    long runningQueries = metricsSvc.getRunningQueries();
    long finishedQueries = metricsSvc.getFinishedQueries();

    QueryHandle handle = executeAndGetHandle(target(), Optional.of(lensSessionId), Optional.of("select ID from "
      + TEST_TABLE), Optional.<LensConf>absent());

    // Get all queries
    // XML
    List<QueryHandle> allQueriesXML = target.queryParam("sessionid", lensSessionId).request(MediaType.APPLICATION_XML)
      .get(new GenericType<List<QueryHandle>>() {});
    assertTrue(allQueriesXML.size() >= 1);

    List<QueryHandle> allQueries = target.queryParam("sessionid", lensSessionId).request()
      .get(new GenericType<List<QueryHandle>>() {});
    assertTrue(allQueries.size() >= 1);
    assertTrue(allQueries.contains(handle));

    String queryXML = target.path(handle.toString()).queryParam("sessionid", lensSessionId)
      .request(MediaType.APPLICATION_XML).get(String.class);
    log.debug("query XML:{}", queryXML);

    Response response = target.path(handle.toString() + "001").queryParam("sessionid", lensSessionId).request().get();
    assertEquals(response.getStatus(), 404);

    LensQuery ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request()
      .get(LensQuery.class);

    // wait till the query finishes
    QueryStatus stat = ctx.getStatus();
    while (!stat.finished()) {
      Thread.sleep(1000);
      ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request().get(LensQuery.class);
      stat = ctx.getStatus();
      /*
      Commented due to same issue as: https://issues.apache.org/jira/browse/LENS-683
      switch (stat.getStatus()) {
      case RUNNING:
        assertEquals(metricsSvc.getRunningQueries(), runningQueries + 1);
        break;
      case QUEUED:
        assertEquals(metricsSvc.getQueuedQueries(), queuedQueries + 1);
        break;
      default: // nothing
      }*/
    }

    assertTrue(ctx.getSubmissionTime() > 0);
    assertTrue(ctx.getFinishTime() > 0);
    assertEquals(ctx.getStatus().getStatus(), Status.SUCCESSFUL);

    // Update conf for query
    final FormDataMultiPart confpart = new FormDataMultiPart();
    LensConf conf = new LensConf();
    conf.addProperty("my.property", "myvalue");
    confpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    confpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      MediaType.APPLICATION_XML_TYPE));
    APIResult updateConf = target.path(handle.toString()).request()
      .put(Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    assertEquals(updateConf.getStatus(), APIResult.Status.FAILED);
  }

  // Test explain query

  /**
   * Test explain query.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testExplainQuery() throws InterruptedException {
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID from " + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "explain"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      MediaType.APPLICATION_XML_TYPE));

    final QueryPlan plan = target.request()
      .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
        new GenericType<LensAPIResult<QueryPlan>>() {}).getData();
    assertEquals(plan.getTablesQueried().size(), 1);
    assertTrue(plan.getTablesQueried().get(0).endsWith(TEST_TABLE.toLowerCase()));
    assertNull(plan.getPrepareHandle());

    // Test explain and prepare
    final WebTarget ptarget = target().path("queryapi/preparedqueries");

    final FormDataMultiPart mp2 = new FormDataMultiPart();
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
      "select ID from " + TEST_TABLE));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "explain_and_prepare"));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      MediaType.APPLICATION_XML_TYPE));

    final QueryPlan plan2 = ptarget.request().post(Entity.entity(mp2, MediaType.MULTIPART_FORM_DATA_TYPE),
      new GenericType<LensAPIResult<QueryPlan>>() {}).getData();
    assertEquals(plan2.getTablesQueried().size(), 1);
    assertTrue(plan2.getTablesQueried().get(0).endsWith(TEST_TABLE.toLowerCase()));
    assertNotNull(plan2.getPrepareHandle());
  }

  // Test explain failure

  /**
   * Test explain failure.
   *
   * @throws InterruptedException the interrupted exception
   * @throws UnsupportedEncodingException
   */
  @Test
  public void testExplainFailure() throws InterruptedException, UnsupportedEncodingException {
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select NO_ID from "
      + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "explain"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      MediaType.APPLICATION_XML_TYPE));

    final Response responseExplain = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));

    assertEquals(responseExplain.getStatus(), BAD_REQUEST.getStatusCode());

    // Test explain and prepare
    final WebTarget ptarget = target().path("queryapi/preparedqueries");

    final FormDataMultiPart mp2 = new FormDataMultiPart();
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select NO_ID from "
      + TEST_TABLE));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "explain_and_prepare"));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      MediaType.APPLICATION_XML_TYPE));

    final Response responseExplainAndPrepare = target.request().post(
      Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));

    assertEquals(responseExplainAndPrepare.getStatus(), BAD_REQUEST.getStatusCode());
  }

  /**
   * Test semantic error for hive query on non-existent table.
   *
   * @throws IOException          Signals that an I/O exception has occurred.
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testHiveSemanticFailure() throws InterruptedException, IOException {
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), " select ID from NOT_EXISTS"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      MediaType.APPLICATION_XML_TYPE));

    Response response = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));
    LensAPIResult result = response.readEntity(LensAPIResult.class);
    List<LensErrorTO> childErrors = result.getLensErrorTO().getChildErrors();
    boolean hiveSemanticErrorExists=false;
    for (LensErrorTO error : childErrors) {
      if (error.getCode() == LensHiveErrorCode.SEMANTIC_ERROR.getLensErrorInfo().getErrorCode()) {
        hiveSemanticErrorExists = true;
        break;
      }
    }
    assertTrue(hiveSemanticErrorExists);
  }

  // post to preparedqueries
  // get all prepared queries
  // get a prepared query
  // update a prepared query
  // post to prepared query multiple times
  // delete a prepared query

  /**
   * Test prepare query.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testPrepareQuery() throws InterruptedException {
    final WebTarget target = target().path("queryapi/preparedqueries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID from " + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "prepare"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("queryName").build(), "testQuery1"));

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      MediaType.APPLICATION_XML_TYPE));

    final QueryPrepareHandle pHandle = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      new GenericType<LensAPIResult<QueryPrepareHandle>>() {}).getData();

    // Get all prepared queries
    List<QueryPrepareHandle> allQueries = (List<QueryPrepareHandle>) target.queryParam("sessionid", lensSessionId)
      .queryParam("queryName", "testQuery1").request().get(new GenericType<List<QueryPrepareHandle>>() {
      });
    assertTrue(allQueries.size() >= 1);
    assertTrue(allQueries.contains(pHandle));

    LensPreparedQuery ctx = target.path(pHandle.toString()).queryParam("sessionid", lensSessionId).request()
      .get(LensPreparedQuery.class);
    assertTrue(ctx.getUserQuery().equalsIgnoreCase("select ID from " + TEST_TABLE));
    assertTrue(ctx.getDriverQuery().equalsIgnoreCase("select ID from " + TEST_TABLE));
    //both drivers hive/hive1 and hive/hive2 are capable of handling the query as they point to the same hive server
    assertTrue(ctx.getSelectedDriverName().equals("hive/hive1") || ctx.getSelectedDriverName().equals("hive/hive2"));
    assertNull(ctx.getConf().getProperties().get("my.property"));

    // Update conf for prepared query
    final FormDataMultiPart confpart = new FormDataMultiPart();
    LensConf conf = new LensConf();
    conf.addProperty("my.property", "myvalue");
    confpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    confpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      MediaType.APPLICATION_XML_TYPE));
    APIResult updateConf = target.path(pHandle.toString()).request()
      .put(Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    assertEquals(updateConf.getStatus(), APIResult.Status.SUCCEEDED);

    ctx = target.path(pHandle.toString()).queryParam("sessionid", lensSessionId).request().get(LensPreparedQuery.class);
    assertEquals(ctx.getConf().getProperties().get("my.property"), "myvalue");

    QueryHandle handle1 = target.path(pHandle.toString()).request()
      .post(Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

    // Override query name
    confpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("queryName").build(), "testQueryName2"));
    // do post once again
    QueryHandle handle2 = target.path(pHandle.toString()).request()
      .post(Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);
    assertNotEquals(handle1, handle2);

    LensQuery ctx1 = waitForQueryToFinish(target(), lensSessionId, handle1, Status.SUCCESSFUL);
    assertEquals(ctx1.getQueryName().toLowerCase(), "testquery1");

    LensQuery ctx2 = waitForQueryToFinish(target(), lensSessionId, handle2, Status.SUCCESSFUL);
    assertEquals(ctx2.getQueryName().toLowerCase(), "testqueryname2");

    // destroy prepared
    APIResult result = target.path(pHandle.toString()).queryParam("sessionid", lensSessionId).request()
      .delete(APIResult.class);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    // Post on destroyed query
    Response response = target.path(pHandle.toString()).request()
      .post(Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE), Response.class);
    assertEquals(response.getStatus(), 404);
  }

  /**
   * Test explain and prepare query.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testExplainAndPrepareQuery() throws InterruptedException {
    final WebTarget target = target().path("queryapi/preparedqueries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID from " + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "explain_and_prepare"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      MediaType.APPLICATION_XML_TYPE));

    final QueryPlan plan = target.request()
      .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
        new GenericType<LensAPIResult<QueryPlan>>() {}).getData();

    assertEquals(plan.getTablesQueried().size(), 1);
    assertTrue(plan.getTablesQueried().get(0).endsWith(TEST_TABLE.toLowerCase()));
    assertNotNull(plan.getPrepareHandle());

    LensPreparedQuery ctx = target.path(plan.getPrepareHandle().toString()).queryParam("sessionid", lensSessionId)
      .request().get(LensPreparedQuery.class);
    assertTrue(ctx.getUserQuery().equalsIgnoreCase("select ID from " + TEST_TABLE));
    assertTrue(ctx.getDriverQuery().equalsIgnoreCase("select ID from " + TEST_TABLE));
    //both drivers hive/hive1 and hive/hive2 are capable of handling the query as they point to the same hive server
    assertTrue(ctx.getSelectedDriverName().equals("hive/hive1") || ctx.getSelectedDriverName().equals("hive/hive2"));
    assertNull(ctx.getConf().getProperties().get("my.property"));

    // Update conf for prepared query
    final FormDataMultiPart confpart = new FormDataMultiPart();
    LensConf conf = new LensConf();
    conf.addProperty("my.property", "myvalue");
    confpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    confpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      MediaType.APPLICATION_XML_TYPE));
    APIResult updateConf = target.path(plan.getPrepareHandle().toString()).request()
      .put(Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    assertEquals(updateConf.getStatus(), APIResult.Status.SUCCEEDED);

    ctx = target.path(plan.getPrepareHandle().toString()).queryParam("sessionid", lensSessionId).request()
      .get(LensPreparedQuery.class);
    assertEquals(ctx.getConf().getProperties().get("my.property"), "myvalue");

    QueryHandle handle1 = target.path(plan.getPrepareHandle().toString()).request()
      .post(Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

    // do post once again
    QueryHandle handle2 = target.path(plan.getPrepareHandle().toString()).request()
      .post(Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);
    assertNotEquals(handle1, handle2);

    waitForQueryToFinish(target(), lensSessionId, handle1, Status.SUCCESSFUL);
    waitForQueryToFinish(target(), lensSessionId, handle2, Status.SUCCESSFUL);

    // destroy prepared
    APIResult result = target.path(plan.getPrepareHandle().toString()).queryParam("sessionid", lensSessionId).request()
      .delete(APIResult.class);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    // Post on destroyed query
    Response response = target.path(plan.getPrepareHandle().toString()).request()
      .post(Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE), Response.class);
    assertEquals(response.getStatus(), 404);

  }

  // test with execute async post, get query, get results
  // test cancel query

  /**
   * Test execute async.
   *
   * @throws InterruptedException the interrupted exception
   * @throws IOException          Signals that an I/O exception has occurred.
   */
  @Test
  public void testExecuteAsync() throws InterruptedException, IOException, LensException {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

    long queuedQueries = metricsSvc.getQueuedQueries();
    long runningQueries = metricsSvc.getRunningQueries();

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID, IDSTR from "
      + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      MediaType.APPLICATION_XML_TYPE));
    final QueryHandle handle = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      new GenericType<LensAPIResult<QueryHandle>>() {}).getData();

    assertNotNull(handle);

    // Get query
    LensQuery lensQuery = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request()
      .get(LensQuery.class);
    assertTrue(lensQuery.getStatus().getStatus().equals(Status.QUEUED)
      || lensQuery.getStatus().getStatus().equals(Status.LAUNCHED)
      || lensQuery.getStatus().getStatus().equals(Status.RUNNING)
      || lensQuery.getStatus().getStatus().equals(Status.SUCCESSFUL), lensQuery.getStatus().toString());

    // wait till the query finishes
    QueryStatus stat = lensQuery.getStatus();
    while (!stat.finished()) {
      lensQuery = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request().get(LensQuery.class);
      stat = lensQuery.getStatus();
      /* Commented and jira ticket raised for correction: https://issues.apache.org/jira/browse/LENS-683
      switch (stat.getStatus()) {
      case RUNNING:
        assertEquals(metricsSvc.getRunningQueries(), runningQueries + 1,
            "Asserting queries for " + ctx.getQueryHandle());
        break;
      case QUEUED:
        assertEquals(metricsSvc.getQueuedQueries(), queuedQueries + 1);
        break;
      default: // nothing
      }*/
      Thread.sleep(1000);
    }
    assertTrue(lensQuery.getSubmissionTime() > 0);
    assertTrue(lensQuery.getLaunchTime() > 0);
    assertTrue(lensQuery.getDriverStartTime() > 0);
    assertTrue(lensQuery.getDriverFinishTime() > 0);
    assertTrue(lensQuery.getFinishTime() > 0);
    QueryContext ctx = queryService.getQueryContext(lensSessionId, lensQuery.getQueryHandle());
    assertNotNull(ctx.getPhase1RewrittenQuery());
    assertEquals(ctx.getPhase1RewrittenQuery(), ctx.getUserQuery()); //Since there is no rewriter in this test
    assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    validatePersistedResult(handle, target(), lensSessionId, new String[][]{{"ID", "INT"}, {"IDSTR", "STRING"}}, true);

    // test cancel query
    final QueryHandle handle2 = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      new GenericType<LensAPIResult<QueryHandle>>() {}).getData();

    assertNotNull(handle2);
    APIResult result = target.path(handle2.toString()).queryParam("sessionid", lensSessionId).request()
      .delete(APIResult.class);
    // cancel would fail query is already successful
    LensQuery ctx2 = target.path(handle2.toString()).queryParam("sessionid", lensSessionId).request()
      .get(LensQuery.class);
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      assertEquals(ctx2.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL,
        "cancel failed without query having been succeeded");
    } else if (result.getStatus().equals(APIResult.Status.SUCCEEDED)) {
      assertEquals(ctx2.getStatus().getStatus(), QueryStatus.Status.CANCELED,
        "cancel succeeded but query wasn't cancelled");
    } else {
      fail("unexpected cancel status: " + result.getStatus());
    }

    // Test http download end point
    log.info("Starting httpendpoint test");
    final FormDataMultiPart mp3 = new FormDataMultiPart();
    mp3.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp3.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID, IDSTR from "
      + TEST_TABLE));
    mp3.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");

    mp3.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      MediaType.APPLICATION_XML_TYPE));
    final QueryHandle handle3 = target.request().post(Entity.entity(mp3, MediaType.MULTIPART_FORM_DATA_TYPE),
      new GenericType<LensAPIResult<QueryHandle>>() {}).getData();

    // Get query
    waitForQueryToFinish(target(), lensSessionId, handle3, Status.SUCCESSFUL);
    validateHttpEndPoint(target(), null, handle3, null);
  }

  /**
   * Validate persisted result.
   *
   * @param handle        the handle
   * @param parent        the parent
   * @param lensSessionId the lens session id
   * @param isDir         the is dir
   * @throws IOException Signals that an I/O exception has occurred.
   */
  static void validatePersistedResult(QueryHandle handle, WebTarget parent, LensSessionHandle lensSessionId,
    String[][] schema, boolean isDir) throws IOException {
    final WebTarget target = parent.path("queryapi/queries");
    // fetch results
    validateResultSetMetadata(handle, "",
      schema,
      parent, lensSessionId);

    String presultset = target.path(handle.toString()).path("resultset").queryParam("sessionid", lensSessionId)
      .request().get(String.class);
    System.out.println("PERSISTED RESULT:" + presultset);

    PersistentQueryResult resultset = target.path(handle.toString()).path("resultset")
      .queryParam("sessionid", lensSessionId).request().get(PersistentQueryResult.class);
    validatePersistentResult(resultset, handle, isDir);

    if (isDir) {
      validNotFoundForHttpResult(parent, lensSessionId, handle);
    }
  }

  /**
   * Read result set.
   *
   * @param resultset the resultset
   * @param handle    the handle
   * @param isDir     the is dir
   * @return the list
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static List<String> readResultSet(PersistentQueryResult resultset, QueryHandle handle, boolean isDir)
    throws IOException {
    assertTrue(resultset.getPersistedURI().contains(handle.toString()));
    Path actualPath = new Path(resultset.getPersistedURI());
    FileSystem fs = actualPath.getFileSystem(new Configuration());
    List<String> actualRows = new ArrayList<String>();
    if (fs.getFileStatus(actualPath).isDir()) {
      assertTrue(isDir);
      for (FileStatus fstat : fs.listStatus(actualPath)) {
        addRowsFromFile(actualRows, fs, fstat.getPath());
      }
    } else {
      assertFalse(isDir);
      addRowsFromFile(actualRows, fs, actualPath);
    }
    return actualRows;
  }

  /**
   * Returns the size of result set file when result path is a file, null otherwise
   *
   * @param resultset
   * @param handle
   * @param isDir
   * @return
   * @throws IOException
   */
  public static Long readResultFileSize(PersistentQueryResult resultset, QueryHandle handle, boolean isDir)
    throws IOException {
    assertTrue(resultset.getPersistedURI().contains(handle.toString()));
    Path actualPath = new Path(resultset.getPersistedURI());
    FileSystem fs = actualPath.getFileSystem(new Configuration());
    FileStatus fileStatus = fs.getFileStatus(actualPath);
    if (fileStatus.isDir()) {
      assertTrue(isDir);
      return null;
    } else {
      assertFalse(isDir);
      return fileStatus.getLen();
    }
  }


  /**
   * Adds the rows from file.
   *
   * @param actualRows the actual rows
   * @param fs         the fs
   * @param path       the path
   * @throws IOException Signals that an I/O exception has occurred.
   */
  static void addRowsFromFile(List<String> actualRows, FileSystem fs, Path path) throws IOException {
    FSDataInputStream in = fs.open(path);
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

  /**
   * Validate persistent result.
   *
   * @param resultset the resultset
   * @param handle    the handle
   * @param isDir     the is dir
   * @throws IOException Signals that an I/O exception has occurred.
   */
  static void validatePersistentResult(PersistentQueryResult resultset, QueryHandle handle, boolean isDir)
    throws IOException {
    List<String> actualRows = readResultSet(resultset, handle, isDir);
    validatePersistentResult(actualRows);
    if (!isDir) {
      assertEquals(resultset.getNumRows().intValue(), actualRows.size());
    }
    Long fileSize = readResultFileSize(resultset, handle, isDir);
    assertEquals(resultset.getFileSize(), fileSize);
  }

  static void validatePersistentResult(List<String> actualRows) {
    String[] expected1 = new String[]{
      "1one",
      "\\Ntwo123item1item2",
      "3\\Nitem1item2",
      "\\N\\N",
      "5nothing",
    };
    String[] expected2 = new String[]{
      "1one[][]",
      "\\Ntwo[1,2,3][\"item1\",\"item2\"]",
      "3\\N[][\"item1\",\"item2\"]",
      "\\N\\N[][]",
      "5[][\"nothing\"]",
    };
    for (int i = 0; i < actualRows.size(); i++) {
      assertEquals(
        expected1[i].indexOf(actualRows.get(i)) == 0 || expected2[i].indexOf(actualRows.get(i)) == 0, true);
    }
  }

  /**
   * Validate http end point.
   *
   * @param parent        the parent
   * @param lensSessionId the lens session id
   * @param handle        the handle
   * @param redirectUrl   the redirect url
   * @throws IOException Signals that an I/O exception has occurred.
   */
  static void validateHttpEndPoint(WebTarget parent, LensSessionHandle lensSessionId, QueryHandle handle,
    String redirectUrl) throws IOException {
    log.info("@@@ validateHttpEndPoint sessionid " + lensSessionId);
    Response response = parent.path("queryapi/queries/" + handle.toString() + "/httpresultset")
      .queryParam("sessionid", lensSessionId).request().get();

    assertTrue(response.getHeaderString("content-disposition").contains(handle.toString()));

    if (redirectUrl == null) {
      InputStream in = (InputStream) response.getEntity();
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      IOUtils.copyBytes(in, bos, new Configuration());
      bos.close();
      in.close();

      String result = new String(bos.toByteArray());
      List<String> actualRows = Arrays.asList(result.split("\n"));
      validatePersistentResult(actualRows);
    } else {
      assertEquals(SEE_OTHER.getStatusCode(), response.getStatus());
      assertTrue(response.getHeaderString("Location").contains(redirectUrl));
    }
  }

  /**
   * Valid not found for http result.
   *
   * @param parent        the parent
   * @param lensSessionId the lens session id
   * @param handle        the handle
   */
  static void validNotFoundForHttpResult(WebTarget parent, LensSessionHandle lensSessionId, QueryHandle handle) {
    try {
      Response response = parent.path("queryapi/queries/" + handle.toString() + "/httpresultset")
        .queryParam("sessionid", lensSessionId).request().get();
      if (NOT_FOUND.getStatusCode() != response.getStatus()) {
        fail("Expected not found excepiton, but got:" + response.getStatus());
      }
      assertEquals(response.getStatus(), NOT_FOUND.getStatusCode());
    } catch (NotFoundException e) {
      // expected
      log.error("Resource not found.", e);
    }

  }

  // test with execute async post, get query, get results
  // test cancel query

  /**
   * Test execute async in memory result.
   *
   * @throws InterruptedException the interrupted exception
   * @throws IOException          Signals that an I/O exception has occurred.
   */
  @Test
  public void testExecuteAsyncInMemoryResult() throws InterruptedException, IOException {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID, IDSTR from "
      + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      MediaType.APPLICATION_XML_TYPE));
    final QueryHandle handle = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      new GenericType<LensAPIResult<QueryHandle>>() {}).getData();

    assertNotNull(handle);

    // Get query
    waitForQueryToFinish(target(), lensSessionId, handle, Status.SUCCESSFUL);

    // fetch results
    validateResultSetMetadata(handle, "",
      new String[][]{{"ID", "INT"}, {"IDSTR", "STRING"}},
      target(), lensSessionId);

    InMemoryQueryResult resultset = target.path(handle.toString()).path("resultset")
      .queryParam("sessionid", lensSessionId).request().get(InMemoryQueryResult.class);
    validateInmemoryResult(resultset);

    validNotFoundForHttpResult(target(), lensSessionId, handle);
  }

  /**
   * Test execute async temp table.
   *
   * @throws InterruptedException the interrupted exception
   * @throws IOException          Signals that an I/O exception has occurred.
   */
  @Test
  public void testExecuteAsyncTempTable() throws InterruptedException, IOException {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart drop = new FormDataMultiPart();
    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    drop.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    drop.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
      "drop table if exists temp_output"));
    drop.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    drop.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      MediaType.APPLICATION_XML_TYPE));
    final QueryHandle dropHandle = target.request().post(Entity.entity(drop, MediaType.MULTIPART_FORM_DATA_TYPE),
      new GenericType<LensAPIResult<QueryHandle>>() {}).getData();

    assertNotNull(dropHandle);

    // Get query
    waitForQueryToFinish(target(), lensSessionId, dropHandle, Status.SUCCESSFUL);

    final FormDataMultiPart mp = new FormDataMultiPart();
    conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
      "create table temp_output as select ID, IDSTR from " + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      MediaType.APPLICATION_XML_TYPE));
    final QueryHandle handle = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      new GenericType<LensAPIResult<QueryHandle>>() {}).getData();

    assertNotNull(handle);

    // Get query
    waitForQueryToFinish(target(), lensSessionId, handle, Status.SUCCESSFUL);

    String select = "SELECT * FROM temp_output";
    final FormDataMultiPart fetch = new FormDataMultiPart();
    fetch.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    fetch.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), select));
    fetch.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    fetch.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      MediaType.APPLICATION_XML_TYPE));
    final QueryHandle handle2 = target.request().post(Entity.entity(fetch, MediaType.MULTIPART_FORM_DATA_TYPE),
      new GenericType<LensAPIResult<QueryHandle>>() {}).getData();

    assertNotNull(handle2);

    // Get query
    waitForQueryToFinish(target(), lensSessionId, handle2, Status.SUCCESSFUL);

    // fetch results
    validateResultSetMetadata(handle2, "temp_output.", new String[][]{{"ID", "INT"}, {"IDSTR", "STRING"}},
      target(), lensSessionId);

    InMemoryQueryResult resultset = target.path(handle2.toString()).path("resultset")
      .queryParam("sessionid", lensSessionId).request().get(InMemoryQueryResult.class);
    validateInmemoryResult(resultset);
  }

  /**
   * Validate result set metadata.
   *
   * @param handle        the handle
   * @param parent        the parent
   * @param lensSessionId the lens session id
   */
  static void validateResultSetMetadata(QueryHandle handle, WebTarget parent, LensSessionHandle lensSessionId) {
    validateResultSetMetadata(handle, "",
      new String[][]{{"ID", "INT"}, {"IDSTR", "STRING"}, {"IDARR", "ARRAY"}, {"IDSTRARR", "ARRAY"}},
      parent, lensSessionId);
  }

  /**
   * Validate result set metadata.
   *
   * @param handle         the handle
   * @param outputTablePfx the output table pfx
   * @param parent         the parent
   * @param lensSessionId  the lens session id
   */
  static void validateResultSetMetadata(QueryHandle handle, String outputTablePfx, String[][] columns, WebTarget parent,
    LensSessionHandle lensSessionId) {
    final WebTarget target = parent.path("queryapi/queries");

    QueryResultSetMetadata metadata = target.path(handle.toString()).path("resultsetmetadata")
      .queryParam("sessionid", lensSessionId).request().get(QueryResultSetMetadata.class);
    assertEquals(metadata.getColumns().size(), columns.length);
    for (int i = 0; i < columns.length; i++) {
      assertTrue(
        metadata.getColumns().get(i).getName().toLowerCase().equals(outputTablePfx + columns[i][0].toLowerCase())
          || metadata.getColumns().get(i).getName().toLowerCase().equals(columns[i][0].toLowerCase())
      );
      assertEquals(columns[i][1].toLowerCase(), metadata.getColumns().get(i).getType().name().toLowerCase());
    }
  }

  /**
   * Validate inmemory result.
   *
   * @param resultset the resultset
   */
  private void validateInmemoryResult(InMemoryQueryResult resultset) {
    assertEquals(resultset.getRows().size(), 5);
    assertEquals(resultset.getRows().get(0).getValues().get(0), 1);
    assertEquals((String) resultset.getRows().get(0).getValues().get(1), "one");

    assertNull(resultset.getRows().get(1).getValues().get(0));
    assertEquals((String) resultset.getRows().get(1).getValues().get(1), "two");

    assertEquals(resultset.getRows().get(2).getValues().get(0), 3);
    assertNull(resultset.getRows().get(2).getValues().get(1));

    assertNull(resultset.getRows().get(3).getValues().get(0));
    assertNull(resultset.getRows().get(3).getValues().get(1));
    assertEquals(resultset.getRows().get(4).getValues().get(0), 5);
    assertEquals(resultset.getRows().get(4).getValues().get(1), "");
  }

  // test execute with timeout, fetch results
  // cancel the query with execute_with_timeout

  /**
   * Test execute with timeout query.
   *
   * @throws IOException          Signals that an I/O exception has occurred.
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testExecuteWithTimeoutQuery() throws IOException, InterruptedException {
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID, IDSTR from "
      + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute_with_timeout"));
    // set a timeout value enough for tests
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("timeoutmillis").build(), "300000"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      MediaType.APPLICATION_XML_TYPE));

    QueryHandleWithResultSet result = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      new GenericType<LensAPIResult<QueryHandleWithResultSet>>() {}).getData();
    assertNotNull(result.getQueryHandle());
    assertNotNull(result.getResult());
    validatePersistentResult((PersistentQueryResult) result.getResult(), result.getQueryHandle(), true);

    final FormDataMultiPart mp2 = new FormDataMultiPart();
    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID, IDSTR from "
      + TEST_TABLE));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute_with_timeout"));
    // set a timeout value enough for tests
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("timeoutmillis").build(), "300000"));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      MediaType.APPLICATION_XML_TYPE));

    result = target.request().post(Entity.entity(mp2, MediaType.MULTIPART_FORM_DATA_TYPE),
      new GenericType<LensAPIResult<QueryHandleWithResultSet>>() {}).getData();
    assertNotNull(result.getQueryHandle());
    assertNotNull(result.getResult());
    validateInmemoryResult((InMemoryQueryResult) result.getResult());
  }

  /**
   * Test execute with timeout query.
   *
   * @throws IOException          Signals that an I/O exception has occurred.
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testExecuteWithTimeoutFailingQuery() throws IOException, InterruptedException {
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID from nonexist"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute_with_timeout"));
    // set a timeout value enough for tests
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("timeoutmillis").build(), "300000"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      MediaType.APPLICATION_XML_TYPE));

    Response response = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));
    assertEquals(response.getStatus(), BAD_REQUEST.getStatusCode());
  }

  /**
   * Test default config.
   *
   * @throws LensException the lens exception
   */
  @Test
  public void testDefaultConfig() throws LensException {
    LensConf queryConf = new LensConf();
    queryConf.addProperty("test.query.conf", "qvalue");
    Configuration conf = queryService.getLensConf(lensSessionId, queryConf);

    // session specific conf
    assertEquals(conf.get("test.session.key"), "svalue");
    // query specific conf
    assertEquals(conf.get("test.query.conf"), "qvalue");
    // lenssession default should be loaded
    assertNotNull(conf.get("lens.query.enable.persistent.resultset"));
    // lens site should be loaded
    assertEquals(conf.get("test.lens.site.key"), "gsvalue");
    // hive default variables should not be set
    assertNull(conf.get("hive.exec.local.scratchdir"));
    // hive site variables should not be set
    assertNull(conf.get("hive.metastore.warehouse.dir"));
    // core default should not be loaded
    assertNull(conf.get("fs.default.name"));
    // server configuration should not set
    assertNull(conf.get("lens.server.persist.location"));

    // Test server config. Hive configs overriden should be set
    assertFalse(Boolean.parseBoolean(queryService.getHiveConf().get("hive.server2.log.redirection.enabled")));
    assertEquals(queryService.getHiveConf().get("hive.server2.query.log.dir"), "target/query-logs");

    final String query = "select ID from " + TEST_TABLE;
    QueryContext ctx = new QueryContext(query, null, queryConf, conf, queryService.getDrivers());
    Map<LensDriver, String> driverQueries = new HashMap<LensDriver, String>();
    for (LensDriver driver : queryService.getDrivers()) {
      driverQueries.put(driver, query);
    }
    ctx.setDriverQueries(driverQueries);

    // This still holds since current database is default
    assertEquals(queryService.getSession(lensSessionId).getCurrentDatabase(), "default");
    assertEquals(queryService.getSession(lensSessionId).getHiveConf().getClassLoader(), ctx.getConf()
      .getClassLoader());
    assertEquals(queryService.getSession(lensSessionId).getHiveConf().getClassLoader(),
      ctx.getDriverContext().getDriverConf(queryService.getDrivers().iterator().next()).getClassLoader());
    assertTrue(ctx.isDriverQueryExplicitlySet());
    for (LensDriver driver : queryService.getDrivers()) {
      Configuration dconf = ctx.getDriverConf(driver);
      assertEquals(dconf.get("test.session.key"), "svalue");
      // query specific conf
      assertEquals(dconf.get("test.query.conf"), "qvalue");
      // lenssession default should be loaded
      assertNotNull(dconf.get("lens.query.enable.persistent.resultset"));
      // lens site should be loaded
      assertEquals(dconf.get("test.lens.site.key"), "gsvalue");
      // hive default variables should not be set
      assertNull(conf.get("hive.exec.local.scratchdir"));
      // driver site should be loaded
      assertEquals(dconf.get("lens.driver.test.key"), "set");
      // core default should not be loaded
      assertNull(dconf.get("fs.default.name"));
      // server configuration should not set
      assertNull(dconf.get("lens.server.persist.location"));
    }
  }

  /**
   * Test estimate native query.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testEstimateNativeQuery() throws InterruptedException {
    final WebTarget target = target().path("queryapi/queries");

    // estimate native query
    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID from " + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "estimate"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      MediaType.APPLICATION_XML_TYPE));

    final QueryCostTO result = target.request()
      .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
        new GenericType<LensAPIResult<QueryCostTO>>() {}).getData();
    assertNotNull(result);
    assertEquals(result.getEstimatedExecTimeMillis(), null);
    assertEquals(result.getEstimatedResourceUsage(), Double.MAX_VALUE);
  }


  /**
   * Check if DB static jars get passed to Hive driver
   * @throws Exception
   */
  @Test
  public void testHiveDriverGetsDBJars() throws Exception {
    // Set DB to a db with static jars
    HiveSessionService sessionService = LensServices.get().getService(SessionService.NAME);

    // Open session with a DB which has static jars
    LensSessionHandle sessionHandle =
      sessionService.openSession("foo@localhost", "bar", DB_WITH_JARS, new HashMap<String, String>());

    // Add a jar in the session
    File testJarFile = new File("target/testjars/test2.jar");
    sessionService.addResourceToAllServices(sessionHandle, "jar", "file://" + testJarFile.getAbsolutePath());

    log.info("@@@ Opened session " + sessionHandle.getPublicId() + " with database " + DB_WITH_JARS);
    LensSessionImpl session = sessionService.getSession(sessionHandle);

    // Jars should be pending until query is run
    assertEquals(session.getPendingSessionResourcesForDatabase(DB_WITH_JARS).size(), 1);
    assertEquals(session.getPendingSessionResourcesForDatabase(DB_WITH_JARS_2).size(), 1);

    final String tableInDBWithJars = "testHiveDriverGetsDBJars";
    try {
      // First execute query on the session with db should load jars from DB
      LensServerTestUtil.createTable(tableInDBWithJars, target(), sessionHandle, "(ID INT, IDSTR STRING) "
        + "ROW FORMAT SERDE \"DatabaseJarSerde\"");

      boolean addedToHiveDriver = false;

      for (LensDriver driver : queryService.getDrivers()) {
        if (driver instanceof HiveDriver) {
          addedToHiveDriver =
            ((HiveDriver) driver).areDBResourcesAddedForSession(sessionHandle.getPublicId().toString(), DB_WITH_JARS);
          if (addedToHiveDriver){
            break; //There are two Hive drivers now both pointing to same hive server. So break after first success
          }
        }
      }
      assertTrue(addedToHiveDriver);

      // Switch database
      log.info("@@@# database switch test");
      session.setCurrentDatabase(DB_WITH_JARS_2);
      LensServerTestUtil.createTable(tableInDBWithJars + "_2", target(), sessionHandle, "(ID INT, IDSTR STRING) "
        + "ROW FORMAT SERDE \"DatabaseJarSerde\"");

      // All db jars should have been added
      assertTrue(session.getDBResources(DB_WITH_JARS_2).isEmpty());
      assertTrue(session.getDBResources(DB_WITH_JARS).isEmpty());

      // All session resources must have been added to both DBs
      assertFalse(session.getLensSessionPersistInfo().getResources().isEmpty());
      for (LensSessionImpl.ResourceEntry resource : session.getLensSessionPersistInfo().getResources()) {
        assertTrue(resource.isAddedToDatabase(DB_WITH_JARS_2));
        assertTrue(resource.isAddedToDatabase(DB_WITH_JARS));
      }

      assertTrue(session.getPendingSessionResourcesForDatabase(DB_WITH_JARS).isEmpty());
      assertTrue(session.getPendingSessionResourcesForDatabase(DB_WITH_JARS_2).isEmpty());

    } finally {
      log.info("@@@ TEST_OVER");
      try {
        LensServerTestUtil.dropTable(tableInDBWithJars, target(), sessionHandle);
        LensServerTestUtil.dropTable(tableInDBWithJars + "_2", target(), sessionHandle);
      } catch (Throwable th) {
        log.error("Exception while dropping table.", th);
      }
      sessionService.closeSession(sessionHandle);
    }
  }

  @Test
  public void testRewriteFailure() {
    final WebTarget target = target().path("queryapi/queries");

    // estimate cube query which fails semantic analysis
    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "cube select ID from nonexist"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "estimate"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      MediaType.APPLICATION_XML_TYPE));

    final Response response = target.request()
      .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));


    LensErrorTO expectedLensErrorTO = LensErrorTO.composedOf(
      LensCubeErrorCode.NEITHER_CUBE_NOR_DIMENSION.getLensErrorInfo().getErrorCode(),
      "Neither cube nor dimensions accessed in the query", TestDataUtils.MOCK_STACK_TRACE);
    ErrorResponseExpectedData expectedData = new ErrorResponseExpectedData(BAD_REQUEST, expectedLensErrorTO);

    expectedData.verify(response);
  }

  @Test
  public void testDriverEstimateSkippingForRewritefailure() throws LensException {
    Configuration conf = queryService.getLensConf(lensSessionId, new LensConf());
    QueryContext ctx = new QueryContext("cube select ID from nonexist", "user", new LensConf(), conf,
      queryService.getDrivers());
    for (LensDriver driver : queryService.getDrivers()) {
      ctx.setDriverRewriteError(driver, new LensException());
    }

    // All estimates should be skipped.
    Map<LensDriver, AbstractQueryContext.DriverEstimateRunnable> estimateRunnables = ctx.getDriverEstimateRunnables();
    for (LensDriver driver : estimateRunnables.keySet()) {
      estimateRunnables.get(driver).run();
      assertFalse(estimateRunnables.get(driver).isSucceeded(), driver + " estimate should have been skipped");
    }

    for (LensDriver driver : queryService.getDrivers()) {
      assertNull(ctx.getDriverQueryCost(driver));
    }
  }

  @Test
  public void testNonSelectQueriesWithPersistResult() throws InterruptedException {
    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "true");
    String tblName = "testNonSelectQueriesWithPersistResult";
    LensServerTestUtil.dropTableWithConf(tblName, target(), lensSessionId, conf);
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    LensServerTestUtil.dropTableWithConf(tblName, target(), lensSessionId, conf);
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    LensServerTestUtil.dropTableWithConf(tblName, target(), lensSessionId, conf);
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "false");
    LensServerTestUtil.dropTableWithConf(tblName, target(), lensSessionId, conf);
  }

  @Test
  public void testEstimateGauges() {
    final WebTarget target = target().path("queryapi/queries");

    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_METRIC_UNIQUE_ID_CONF_KEY, "TestQueryService-testEstimateGauges");
    // estimate native query
    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID from " + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "estimate"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      MediaType.APPLICATION_XML_TYPE));

    final QueryCostTO queryCostTO = target.request()
      .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
        new GenericType<LensAPIResult<QueryCostTO>>() {
        }).getData();
    assertNotNull(queryCostTO);

    MetricRegistry reg = LensMetricsRegistry.getStaticRegistry();

    assertTrue(reg.getGauges().keySet().containsAll(Arrays.asList(
        "lens.MethodMetricGauge.TestQueryService-testEstimateGauges-DRIVER_SELECTION",
        "lens.MethodMetricGauge.TestQueryService-testEstimateGauges-hive/hive1-CUBE_REWRITE",
        "lens.MethodMetricGauge.TestQueryService-testEstimateGauges-hive/hive1-DRIVER_ESTIMATE",
        "lens.MethodMetricGauge.TestQueryService-testEstimateGauges-hive/hive1-RewriteUtil-rewriteQuery",
        "lens.MethodMetricGauge.TestQueryService-testEstimateGauges-hive/hive2-CUBE_REWRITE",
        "lens.MethodMetricGauge.TestQueryService-testEstimateGauges-hive/hive2-DRIVER_ESTIMATE",
        "lens.MethodMetricGauge.TestQueryService-testEstimateGauges-hive/hive2-RewriteUtil-rewriteQuery",
        "lens.MethodMetricGauge.TestQueryService-testEstimateGauges-jdbc/jdbc1-CUBE_REWRITE",
        "lens.MethodMetricGauge.TestQueryService-testEstimateGauges-jdbc/jdbc1-DRIVER_ESTIMATE",
        "lens.MethodMetricGauge.TestQueryService-testEstimateGauges-jdbc/jdbc1-RewriteUtil-rewriteQuery",
        "lens.MethodMetricGauge.TestQueryService-testEstimateGauges-PARALLEL_ESTIMATE")),
      reg.getGauges().keySet().toString());
  }

  @Test
  public void testQueryRejection() throws InterruptedException, IOException {
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "blah select ID from "
      + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      MediaType.APPLICATION_XML_TYPE));

    Response response = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));
    assertEquals(response.getStatus(), 400);
  }

  /**
   * Test query purger
   *
   * @throws InterruptedException the interrupted exception
   * @throws IOException          Signals that an I/O exception has occurred.
   */
  @Test
  public void testQueryPurger() throws InterruptedException, IOException {
    waitForPurge();
    LensConf conf = getLensConf(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    // test post execute op
    LensQuery ctx1 = executeAndWaitForQueryToFinish(target(), lensSessionId,
      "select ID, IDSTR from " + TEST_TABLE,
      Optional.of(conf), Optional.of(Status.SUCCESSFUL));
    LensQuery ctx2 = executeAndWaitForQueryToFinish(target(), lensSessionId,
      "select ID, IDSTR from " + TEST_TABLE,
      Optional.of(conf), Optional.of(Status.SUCCESSFUL));
    LensQuery ctx3 = executeAndWaitForQueryToFinish(target(), lensSessionId,
      "select ID, IDSTR from " + TEST_TABLE,
      Optional.of(conf), Optional.of(Status.SUCCESSFUL));
    waitForPurge(3, queryService.finishedQueries);
    assertEquals(queryService.finishedQueries.size(), 3);
    getLensQueryResult(target(), lensSessionId, ctx3.getQueryHandle());
    waitForPurge(2, queryService.finishedQueries);
    assertTrue(queryService.finishedQueries.size() == 2);
    getLensQueryResult(target(), lensSessionId, ctx2.getQueryHandle());
    waitForPurge(1, queryService.finishedQueries);
    assertTrue(queryService.finishedQueries.size() == 1);
    getLensQueryResult(target(), lensSessionId, ctx1.getQueryHandle());
  }

  /**
   * Test session close when a query is active on the session
   *
   * @throws Exception
   */
  @Test
  public void testSessionClose() throws Exception {
    // Query with group by, will run long enough to close the session before finish
    String query = "select ID, IDSTR, count(*) from " + TEST_TABLE + " group by ID, IDSTR";
    SessionService sessionService = LensServices.get().getService(HiveSessionService.NAME);
    Map<String, String> sessionconf = new HashMap<String, String>();
    LensSessionHandle sessionHandle = sessionService.openSession("foo", "bar", "default", sessionconf);
    LensConf conf = getLensConf(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "true");
    QueryHandle qHandle =
      executeAndGetHandle(target(), Optional.of(sessionHandle), Optional.of(query), Optional.of(conf));
    sessionService.closeSession(sessionHandle);
    sessionHandle = sessionService.openSession("foo", "bar", "default", sessionconf);
    waitForQueryToFinish(target(), sessionHandle, qHandle, Status.SUCCESSFUL);
  }

  @AfterMethod
  private void waitForPurge() throws InterruptedException {
    waitForPurge(0, queryService.finishedQueries);
  }
}

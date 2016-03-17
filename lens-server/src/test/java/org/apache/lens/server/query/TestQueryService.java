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
import javax.ws.rs.core.*;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.Priority;
import org.apache.lens.api.jaxb.LensJAXBContextResolver;
import org.apache.lens.api.query.*;
import org.apache.lens.api.query.QueryStatus.Status;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.api.result.LensErrorTO;
import org.apache.lens.api.result.QueryCostTO;
import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.driver.hive.HiveDriver;
import org.apache.lens.lib.query.FilePersistentFormatter;
import org.apache.lens.lib.query.FileSerdeFormatter;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServerTestUtil;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.*;
import org.apache.lens.server.api.error.LensDriverErrorCode;
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

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.test.TestProperties;
import org.testng.annotations.*;

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
    Map<String, String> sessionconf = new HashMap<>();
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

  /** The test table. */
  public static final String TEST_TABLE = "TEST_TABLE";

  /**
   * Creates the table.
   *
   * @param tblName the tbl name
   * @throws InterruptedException the interrupted exception
   */
  private void createTable(String tblName) throws InterruptedException {
    LensServerTestUtil.createTable(tblName, target(), lensSessionId, defaultMT);
  }

  /**
   * Load data.
   *
   * @param tblName      the tbl name
   * @param testDataFile the test data file
   * @throws InterruptedException the interrupted exception
   */
  private void loadData(String tblName, final String testDataFile) throws InterruptedException {
    LensServerTestUtil.loadDataFromClasspath(tblName, testDataFile, target(), lensSessionId, defaultMT);
  }

  /**
   * Drop table.
   *
   * @param tblName the tbl name
   * @throws InterruptedException the interrupted exception
   */
  private void dropTable(String tblName) throws InterruptedException {
    LensServerTestUtil.dropTable(tblName, target(), lensSessionId, defaultMT);
  }

  /**
   * Test get random query. should return 400
   */
  @Test(dataProvider = "mediaTypeData")
  public void testGetRandomQuery(MediaType mt) {
    final WebTarget target = target().path("queryapi/queries");

    Response rs = target.path("random").queryParam("sessionid", lensSessionId).request(mt).get();
    assertEquals(rs.getStatus(), 400);
  }

  @Test
  public void testLoadingMultipleDrivers() {
    Collection<LensDriver> drivers = queryService.getDrivers();
    assertEquals(drivers.size(), 4);
    Set<String> driverNames = new HashSet<>(drivers.size());
    for (LensDriver driver : drivers) {
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
  @Test(dataProvider = "mediaTypeData")
  public void testRewriteFailureInExecute(MediaType mt) throws InterruptedException {
    final WebTarget target = target().path("queryapi/queries");
    LensConf conf = new LensConf();
    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId, mt));
    mp.bodyPart(
      new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID from non_exist_table"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf, mt));
    final Response response = target.request(mt).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));
    assertEquals(response.getStatus(), BAD_REQUEST.getStatusCode());
  }

  /**
   * Test launch failure in execute operation.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test(dataProvider = "mediaTypeData")
  public void testLaunchFail(MediaType mt) throws InterruptedException {
    LensQuery lensQuery = executeAndWaitForQueryToFinish(target(), lensSessionId, "select fail from non_exist",
      Optional.<LensConf>absent(), Optional.of(Status.FAILED), mt);
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
  @Test(dataProvider = "mediaTypeData")
  public void testQueriesAPI(MediaType mt) throws InterruptedException {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

    long queuedQueries = metricsSvc.getQueuedQueries();
    long runningQueries = metricsSvc.getRunningQueries();
    long finishedQueries = metricsSvc.getFinishedQueries();

    int noOfQueriesBeforeExecution = queryService.allQueries.size();
    QueryHandle theHandle = executeAndGetHandle(target(), Optional.of(lensSessionId), Optional.of("select ID from "
      + TEST_TABLE), Optional.<LensConf>absent(), mt);

    // Get all queries
    // XML
    List<QueryHandle> allQueriesXML = target.queryParam("sessionid", lensSessionId).request(MediaType.APPLICATION_XML)
      .get(new GenericType<List<QueryHandle>>() {});
    assertTrue(allQueriesXML.size() >= 1);

    List<QueryHandle> allQueries = target.queryParam("sessionid", lensSessionId).request(mt)
      .get(new GenericType<List<QueryHandle>>() {});
    assertTrue(allQueries.size() >= 1);
    assertTrue(allQueries.contains(theHandle));

    String queryXML = target.path(theHandle.toString()).queryParam("sessionid", lensSessionId)
      .request(MediaType.APPLICATION_XML).get(String.class);
    log.debug("query XML:{}", queryXML);

    Response response =
        target.path(theHandle.toString() + "001").queryParam("sessionid", lensSessionId).request(mt).get();
    assertEquals(response.getStatus(), 404);

    LensQuery query = target.path(theHandle.toString()).queryParam("sessionid", lensSessionId).request(mt)
      .get(LensQuery.class);

    // wait till the query finishes
    QueryStatus stat = query.getStatus();
    while (!stat.finished()) {
      Thread.sleep(1000);
      query = target.path(theHandle.toString()).queryParam("sessionid", lensSessionId).request(mt).get(LensQuery.class);
      stat = query.getStatus();
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

    assertTrue(query.getSubmissionTime() > 0);
    assertTrue(query.getFinishTime() > 0);
    assertEquals(query.getStatus().getStatus(), Status.SUCCESSFUL);

    assertEquals(query.getPriority(), Priority.LOW);
    //Check Query Priority can be read even after query is purged i,e query details are read from DB.
    boolean isPurged = false;
    while (!isPurged) {
      isPurged = true;
      for (QueryHandle aHandle : queryService.allQueries.keySet()) {
        if (aHandle.equals(theHandle)) {
          isPurged = false;  //current query is still not purged
          Thread.sleep(1000);
          break;
        }
      }
    }
    assertEquals(query.getPriority(), Priority.LOW);

    // Update conf for query
    final FormDataMultiPart confpart = new FormDataMultiPart();
    LensConf conf = new LensConf();
    conf.addProperty("my.property", "myvalue");
    confpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      mt));
    confpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      mt));
    APIResult updateConf = target.path(theHandle.toString()).request(mt)
      .put(Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    assertEquals(updateConf.getStatus(), APIResult.Status.FAILED);
  }

  // Test explain query

  /**
   * Test explain query.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test(dataProvider = "mediaTypeData")
  public void testExplainQuery(MediaType mt) throws InterruptedException {
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      mt));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID from " + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "explain"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      mt));

    final QueryPlan plan = target.request(mt)
      .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
        new GenericType<LensAPIResult<QueryPlan>>() {}).getData();
    assertEquals(plan.getTablesQueried().size(), 1);
    assertTrue(plan.getTablesQueried().get(0).endsWith(TEST_TABLE.toLowerCase()));
    assertNull(plan.getPrepareHandle());

    // Test explain and prepare
    final WebTarget ptarget = target().path("queryapi/preparedqueries");

    final FormDataMultiPart mp2 = new FormDataMultiPart();
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      mt));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
      "select ID from " + TEST_TABLE));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "explain_and_prepare"));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      mt));

    final QueryPlan plan2 = ptarget.request(mt).post(Entity.entity(mp2, MediaType.MULTIPART_FORM_DATA_TYPE),
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
  @Test(dataProvider = "mediaTypeData")
  public void testExplainFailure(MediaType mt) throws InterruptedException, UnsupportedEncodingException {
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      mt));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select NO_ID from "
      + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "explain"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      mt));

    final Response responseExplain = target.request(mt).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));

    assertEquals(responseExplain.getStatus(), BAD_REQUEST.getStatusCode());

    // Test explain and prepare
    final WebTarget ptarget = target().path("queryapi/preparedqueries");

    final FormDataMultiPart mp2 = new FormDataMultiPart();
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      mt));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select NO_ID from "
      + TEST_TABLE));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "explain_and_prepare"));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      mt));

    final Response responseExplainAndPrepare = ptarget.request(mt).post(
      Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));

    assertEquals(responseExplainAndPrepare.getStatus(), BAD_REQUEST.getStatusCode());
  }

  /**
   * Test semantic error for hive query on non-existent table.
   *
   * @throws IOException          Signals that an I/O exception has occurred.
   * @throws InterruptedException the interrupted exception
   */
  @Test(dataProvider = "mediaTypeData")
  public void testHiveSemanticFailure(MediaType mt) throws InterruptedException, IOException {
    final WebTarget target = target().path("queryapi/queries");
    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId, mt));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), " select ID from NOT_EXISTS"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      mt));

    Response response = target.request(mt).post(Entity.entity(mp, MediaType
      .MULTIPART_FORM_DATA_TYPE));
    LensAPIResult result = response.readEntity(LensAPIResult.class);
    List<LensErrorTO> childErrors = result.getLensErrorTO().getChildErrors();
    boolean hiveSemanticErrorExists = false;
    for (LensErrorTO error : childErrors) {
      if (error.getCode() == LensDriverErrorCode.SEMANTIC_ERROR.getLensErrorInfo().getErrorCode()) {
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
  @Test(dataProvider = "mediaTypeData")
  public void testPrepareQuery(MediaType mt) throws InterruptedException {
    final WebTarget target = target().path("queryapi/preparedqueries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      mt));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID from " + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "prepare"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("queryName").build(), "testQuery1"));

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      mt));

    final QueryPrepareHandle pHandle = target.request(mt).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      new GenericType<LensAPIResult<QueryPrepareHandle>>() {}).getData();

    // Get all prepared queries
    List<QueryPrepareHandle> allQueries = target.queryParam("sessionid", lensSessionId)
      .queryParam("queryName", "testQuery1").request(mt).get(new GenericType<List<QueryPrepareHandle>>() {
      });
    assertTrue(allQueries.size() >= 1);
    assertTrue(allQueries.contains(pHandle));

    LensPreparedQuery ctx = target.path(pHandle.toString()).queryParam("sessionid", lensSessionId).request(mt)
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
      mt));
    confpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      mt));
    APIResult updateConf = target.path(pHandle.toString()).request(mt)
      .put(Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    assertEquals(updateConf.getStatus(), APIResult.Status.SUCCEEDED);

    ctx = target.path(pHandle.toString()).queryParam("sessionid", lensSessionId).request(mt).get(LensPreparedQuery
      .class);
    assertEquals(ctx.getConf().getProperties().get("my.property"), "myvalue");

    QueryHandle handle1 = target.path(pHandle.toString()).request(mt)
      .post(Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

    // Override query name
    confpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("queryName").build(), "testQueryName2"));
    // do post once again
    QueryHandle handle2 = target.path(pHandle.toString()).request(mt)
      .post(Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);
    assertNotEquals(handle1, handle2);

    LensQuery ctx1 = waitForQueryToFinish(target(), lensSessionId, handle1, Status.SUCCESSFUL, mt);
    assertEquals(ctx1.getQueryName().toLowerCase(), "testquery1");

    LensQuery ctx2 = waitForQueryToFinish(target(), lensSessionId, handle2, Status.SUCCESSFUL, mt);
    assertEquals(ctx2.getQueryName().toLowerCase(), "testqueryname2");

    // destroy prepared
    APIResult result = target.path(pHandle.toString()).queryParam("sessionid", lensSessionId).request(mt)
      .delete(APIResult.class);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    // Post on destroyed query
    Response response = target.path(pHandle.toString()).request(mt)
      .post(Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE), Response.class);
    assertEquals(response.getStatus(), 404);
  }

  /**
   * Test explain and prepare query.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test(dataProvider = "mediaTypeData")
  public void testExplainAndPrepareQuery(MediaType mt) throws InterruptedException {
    final WebTarget target = target().path("queryapi/preparedqueries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      mt));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID from " + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "explain_and_prepare"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      mt));

    final QueryPlan plan = target.request(mt)
      .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
        new GenericType<LensAPIResult<QueryPlan>>() {}).getData();

    assertEquals(plan.getTablesQueried().size(), 1);
    assertTrue(plan.getTablesQueried().get(0).endsWith(TEST_TABLE.toLowerCase()));
    assertNotNull(plan.getPrepareHandle());

    LensPreparedQuery ctx = target.path(plan.getPrepareHandle().toString()).queryParam("sessionid", lensSessionId)
      .request(mt).get(LensPreparedQuery.class);
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
      mt));
    confpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      mt));
    APIResult updateConf = target.path(plan.getPrepareHandle().toString()).request(mt)
      .put(Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    assertEquals(updateConf.getStatus(), APIResult.Status.SUCCEEDED);

    ctx = target.path(plan.getPrepareHandle().toString()).queryParam("sessionid", lensSessionId).request(mt)
      .get(LensPreparedQuery.class);
    assertEquals(ctx.getConf().getProperties().get("my.property"), "myvalue");

    QueryHandle handle1 = target.path(plan.getPrepareHandle().toString()).request(mt)
      .post(Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

    // do post once again
    QueryHandle handle2 = target.path(plan.getPrepareHandle().toString()).request(mt)
      .post(Entity.entity(confpart, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);
    assertNotEquals(handle1, handle2);

    waitForQueryToFinish(target(), lensSessionId, handle1, Status.SUCCESSFUL, mt);
    waitForQueryToFinish(target(), lensSessionId, handle2, Status.SUCCESSFUL, mt);

    // destroy prepared
    APIResult result = target.path(plan.getPrepareHandle().toString()).queryParam("sessionid", lensSessionId)
      .request(mt).delete(APIResult.class);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    // Post on destroyed query
    Response response = target.path(plan.getPrepareHandle().toString()).request(mt)
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
  @Test(dataProvider = "mediaTypeData")
  public void testExecuteAsync(MediaType mt) throws InterruptedException, IOException, LensException {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

    long queuedQueries = metricsSvc.getQueuedQueries();
    long runningQueries = metricsSvc.getRunningQueries();

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      mt));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID, IDSTR from "
      + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      mt));
    final QueryHandle handle = target.request(mt).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      new GenericType<LensAPIResult<QueryHandle>>() {}).getData();

    assertNotNull(handle);

    // Get query
    LensQuery lensQuery = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request(mt)
      .get(LensQuery.class);
    assertTrue(lensQuery.getStatus().getStatus().equals(Status.QUEUED)
      || lensQuery.getStatus().getStatus().equals(Status.LAUNCHED)
      || lensQuery.getStatus().getStatus().equals(Status.RUNNING)
      || lensQuery.getStatus().getStatus().equals(Status.SUCCESSFUL), lensQuery.getStatus().toString());

    // wait till the query finishes
    QueryStatus stat = lensQuery.getStatus();
    while (!stat.finished()) {
      lensQuery = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request(mt).get(LensQuery
        .class);
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
    QueryContext ctx = queryService.getUpdatedQueryContext(lensSessionId, lensQuery.getQueryHandle());
    assertNotNull(ctx.getPhase1RewrittenQuery());
    assertEquals(ctx.getPhase1RewrittenQuery(), ctx.getUserQuery()); //Since there is no rewriter in this test
    assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    validatePersistedResult(handle, target(), lensSessionId, new String[][]{{"ID", "INT"}, {"IDSTR", "STRING"}}, true,
        false, mt);

    // test cancel query
    final QueryHandle handle2 = target.request(mt).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      new GenericType<LensAPIResult<QueryHandle>>() {}).getData();

    assertNotNull(handle2);
    APIResult result = target.path(handle2.toString()).queryParam("sessionid", lensSessionId).request(mt)
      .delete(APIResult.class);
    // cancel would fail query is already successful
    LensQuery ctx2 = target.path(handle2.toString()).queryParam("sessionid", lensSessionId).request(mt)
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

    // 1. Test http download end point and result path should be correct (when both driver and server persist)
    // 2. Test Fetch result should fail before query is marked successful
    log.info("Starting httpendpoint test");
    final FormDataMultiPart mp3 = new FormDataMultiPart();
    mp3.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      mt));
    mp3.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID, IDSTR from "
      + TEST_TABLE));
    mp3.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "true");
    conf.addProperty(LensConfConstants.QUERY_OUTPUT_FORMATTER, DeferredPersistentResultFormatter.class.getName());
    conf.addProperty("deferPersistenceByMillis", 5000); // defer persistence for 5 secs

    mp3.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      mt));
    final QueryHandle handle3 = target.request(mt).post(Entity.entity(mp3, MediaType.MULTIPART_FORM_DATA_TYPE),
      new GenericType<LensAPIResult<QueryHandle>>() {}).getData();

    QueryContext ctx3 = queryService.getQueryContext(handle3);
    assertFalse(ctx3.finished()); //Formatting is deferred so query will take time to finish
    try {
      queryService.fetchResultSet(lensSessionId, handle3, 0, 100);
      fail("client should not be allowed to fetch result before query finishes successfully");
    } catch (NotFoundException e) {
      // Expected. Ignore
    }
    waitForQueryToFinish(target(), lensSessionId, handle3, Status.SUCCESSFUL, mt);
    LensResultSet rs = queryService.getResultset(handle3);
    //check persisted result path
    String expectedPath =
        ctx3.getConf().get(LensConfConstants.RESULT_SET_PARENT_DIR) + "/" + handle3.getHandleIdString()
            + ctx3.getConf().get(LensConfConstants.QUERY_OUTPUT_FILE_EXTN);
    assertTrue(((PersistentResultSet) rs).getOutputPath().endsWith(expectedPath));

    validateHttpEndPoint(target(), null, handle3, null);
  }

  /**
   * Validate persisted result.
   *
   * @param handle        the handle
   * @param parent        the parent
   * @param lensSessionId the lens session id
   * @param isDir         the is dir
   * @param isCSVFormat   the result format is csv.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  static void validatePersistedResult(QueryHandle handle, WebTarget parent, LensSessionHandle lensSessionId,
    String[][] schema, boolean isDir, boolean isCSVFormat, MediaType mt) throws IOException {
    final WebTarget target = parent.path("queryapi/queries");
    // fetch results
    validateResultSetMetadata(handle, "", schema, parent, lensSessionId, mt);

    String presultset = target.path(handle.toString()).path("resultset").queryParam("sessionid", lensSessionId)
      .request(mt).get(String.class);
    System.out.println("PERSISTED RESULT:" + presultset);

    PersistentQueryResult resultset = target.path(handle.toString()).path("resultset")
      .queryParam("sessionid", lensSessionId).request().get(PersistentQueryResult.class);
    validatePersistentResult(resultset, handle, isDir, isCSVFormat);

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
    List<String> actualRows = new ArrayList<>();
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
      String line;

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
   * @param isCSVFormat   the result format is csv.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  static void validatePersistentResult(PersistentQueryResult resultset, QueryHandle handle, boolean isDir,
      boolean isCSVFormat)throws IOException {
    List<String> actualRows = readResultSet(resultset, handle, isDir);
    validatePersistentResult(actualRows, isCSVFormat);
    if (!isDir) {
      assertEquals(resultset.getNumRows().intValue(), actualRows.size());
    }
    Long fileSize = readResultFileSize(resultset, handle, isDir);
    assertEquals(resultset.getFileSize(), fileSize);
  }

  static void validatePersistentResult(List<String> actualRows,  boolean isCSVFormat) {
    String[] expected1 = null;
    String[] expected2 = null;
    if (isCSVFormat) {
      //This case will be hit when the result is persisted by the server (CSV result)
      expected1 = new String[]{
        "\"1\",\"one\"",
        "\"NULL\",\"two\"",
        "\"3\",\"NULL\"",
        "\"NULL\",\"NULL\"",
        "\"5\",\"\"",
      };
    } else {
      //This is case of hive driver persistence
      expected1 = new String[] {
        "1one",
        "\\Ntwo123item1item2",
        "3\\Nitem1item2",
        "\\N\\N",
        "5nothing",
      };
      expected2 = new String[] {
        "1one[][]",
        "\\Ntwo[1,2,3][\"item1\",\"item2\"]",
        "3\\N[][\"item1\",\"item2\"]",
        "\\N\\N[][]",
        "5[][\"nothing\"]",
      };
    }

    for (int i = 0; i < actualRows.size(); i++) {
      assertEquals(expected1[i].indexOf(actualRows.get(i)) == 0
          || (expected2 != null && expected2[i].indexOf(actualRows.get(i)) == 0), true);
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
      validatePersistentResult(actualRows, false);
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
  @Test(dataProvider = "mediaTypeData")
  public void testExecuteAsyncInMemoryResult(MediaType mt) throws InterruptedException, IOException {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      mt));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID, IDSTR from "
      + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      mt));
    final QueryHandle handle = target.request(mt).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      new GenericType<LensAPIResult<QueryHandle>>() {}).getData();

    assertNotNull(handle);

    // Get query
    waitForQueryToFinish(target(), lensSessionId, handle, Status.SUCCESSFUL, mt);

    // fetch results
    validateResultSetMetadata(handle, "",
      new String[][]{{"ID", "INT"}, {"IDSTR", "STRING"}},
      target(), lensSessionId, mt);

    validateInmemoryResult(target, handle, mt);

    validNotFoundForHttpResult(target(), lensSessionId, handle);
    waitForPurge(0, queryService.finishedQueries);
    APIResult result=target.path(handle.toString()).path("resultset")
      .queryParam("sessionid", lensSessionId).request().delete(APIResult.class);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  @Test
  public void testTTLForInMemoryResult() throws InterruptedException, IOException, LensException {
    long inMemoryresultsetTTLMillisBackup = queryService.getInMemoryResultsetTTLMillis();
    queryService.setInMemoryResultsetTTLMillis(5000); // 5 secs
    try {
      // test post execute op
      final WebTarget target = target().path("queryapi/queries");

      final FormDataMultiPart mp = new FormDataMultiPart();
      LensConf conf = new LensConf();
      conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
      conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "false");
      conf.addProperty(LensConfConstants.QUERY_MAIL_NOTIFY, "false");
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
          defaultMT));
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID, IDSTR from "
          + TEST_TABLE));
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
        defaultMT));

      final QueryHandle handle =
          target
              .request()
              .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
                  new GenericType<LensAPIResult<QueryHandle>>() {
                  }).getData();
      assertNotNull(handle);

      waitForQueryToFinish(target(), lensSessionId, handle, Status.SUCCESSFUL, defaultMT);

      // Check TTL
      QueryContext ctx = queryService.getUpdatedQueryContext(lensSessionId, handle);
      long softExpiryTime = ctx.getDriverStatus().getDriverFinishTime()
          + queryService.getInMemoryResultsetTTLMillis() - 1000; //Keeping buffer of 1 secs
      int checkCount = 0;
      while (System.currentTimeMillis() < softExpiryTime) {
        assertEquals(queryService.getFinishedQueriesCount(), 1);
        assertEquals(queryService.finishedQueries.peek().canBePurged(), false);
        assertEquals(((InMemoryResultSet) queryService.getResultset(handle)).canBePurged(), false);
        checkCount++;
        Thread.sleep(1000); // sleep for 1 secs and then check again
      }
      assertTrue(checkCount >= 2, "CheckCount = " + checkCount); // TTl check at least twice

      Thread.sleep(3000); // should be past TTL after this sleep . purge thread runs every 1 secs for Tests
      assertEquals(queryService.getFinishedQueriesCount(), 0);
    } finally {
      queryService.setInMemoryResultsetTTLMillis(inMemoryresultsetTTLMillisBackup);
    }
  }

  /**
   * Test execute async temp table.
   *
   * @throws InterruptedException the interrupted exception
   * @throws IOException          Signals that an I/O exception has occurred.
   */
  @Test(dataProvider = "mediaTypeData")
  public void testExecuteAsyncTempTable(MediaType mt) throws InterruptedException, IOException {
    // test post execute op
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart drop = new FormDataMultiPart();
    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    drop.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      mt));
    drop.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
      "drop table if exists temp_output"));
    drop.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    drop.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      mt));
    final QueryHandle dropHandle = target.request(mt).post(Entity.entity(drop, MediaType.MULTIPART_FORM_DATA_TYPE),
      new GenericType<LensAPIResult<QueryHandle>>() {}).getData();

    assertNotNull(dropHandle);

    // Get query
    waitForQueryToFinish(target(), lensSessionId, dropHandle, Status.SUCCESSFUL, mt);

    final FormDataMultiPart mp = new FormDataMultiPart();
    conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      mt));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
      "create table temp_output as select ID, IDSTR from " + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      mt));
    final QueryHandle handle = target.request(mt).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      new GenericType<LensAPIResult<QueryHandle>>() {}).getData();

    assertNotNull(handle);

    // Get query
    waitForQueryToFinish(target(), lensSessionId, handle, Status.SUCCESSFUL, mt);

    String select = "SELECT * FROM temp_output";
    final FormDataMultiPart fetch = new FormDataMultiPart();
    fetch.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      mt));
    fetch.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), select));
    fetch.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    fetch.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      mt));
    final QueryHandle handle2 = target.request(mt).post(Entity.entity(fetch, MediaType.MULTIPART_FORM_DATA_TYPE),
      new GenericType<LensAPIResult<QueryHandle>>() {}).getData();

    assertNotNull(handle2);

    // Get query
    waitForQueryToFinish(target(), lensSessionId, handle2, Status.SUCCESSFUL, mt);

    // fetch results
    validateResultSetMetadata(handle2, "temp_output.", new String[][]{{"ID", "INT"}, {"IDSTR", "STRING"}},
      target(), lensSessionId, mt);

    validateInmemoryResult(target, handle2, mt);
  }

  /**
   * Validate result set metadata.
   *
   * @param handle        the handle
   * @param parent        the parent
   * @param lensSessionId the lens session id
   */
  static void validateResultSetMetadata(QueryHandle handle, WebTarget parent, LensSessionHandle lensSessionId,
    MediaType mt) {
    validateResultSetMetadata(handle, "",
      new String[][]{{"ID", "INT"}, {"IDSTR", "STRING"}, {"IDARR", "ARRAY"}, {"IDSTRARR", "ARRAY"}},
      parent, lensSessionId, mt);
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
    LensSessionHandle lensSessionId, MediaType mt) {
    final WebTarget target = parent.path("queryapi/queries");

    QueryResultSetMetadata metadata = target.path(handle.toString()).path("resultsetmetadata")
      .queryParam("sessionid", lensSessionId).request(mt).get(QueryResultSetMetadata.class);
    assertEquals(metadata.getColumns().size(), columns.length);
    for (int i = 0; i < columns.length; i++) {
      assertTrue(
        metadata.getColumns().get(i).getName().toLowerCase().equals(outputTablePfx + columns[i][0].toLowerCase())
          || metadata.getColumns().get(i).getName().toLowerCase().equals(columns[i][0].toLowerCase())
      );
      assertEquals(columns[i][1].toLowerCase(), metadata.getColumns().get(i).getType().name().toLowerCase());
    }
  }
  private void validateInmemoryResult(WebTarget target, QueryHandle handle, MediaType mt) throws IOException {
    if (mt.equals(MediaType.APPLICATION_JSON_TYPE)) {
      String resultSet = target.path(handle.toString()).path("resultset")
        .queryParam("sessionid", lensSessionId).request(mt).get(String.class);
      // this is being done because json unmarshalling does not work to construct java Objects back
      assertEquals(resultSet.replaceAll("\\W", ""), expectedJsonResult().replaceAll("\\W", ""));
    } else {
      InMemoryQueryResult resultSet = target.path(handle.toString()).path("resultset")
        .queryParam("sessionid", lensSessionId).request(mt).get(InMemoryQueryResult.class);
      validateInmemoryResult(resultSet);
    }
  }
  private String expectedJsonResult() {
    StringBuilder expectedJson = new StringBuilder();
    expectedJson.append("{\"inMemoryQueryResult\" : {\"rows\" : [ ")
      .append("{\"values\" : [ {\n\"type\" : \"int\",\n\"value\" : 1}, {\"type\" : \"string\",\"value\" : \"one\"} ]},")
      .append("{\"values\" : [ null, {\"type\" : \"string\",\"value\" : \"two\"} ]},")
      .append("{\"values\" : [ {\"type\" : \"int\",\"value\" : 3}, null ]},")
      .append("{\"values\" : [ null, null ]},")
      .append("{\"values\" : [ {\"type\" : \"int\",\"value\" : 5}, {\"type\" : \"string\",\"value\" : \"\"} ]} ]}}");
    return expectedJson.toString();
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
  @Test(dataProvider = "mediaTypeData")
  public void testExecuteWithTimeoutQuery(MediaType mt) throws IOException, InterruptedException {
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      mt));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID, IDSTR from "
      + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute_with_timeout"));
    // set a timeout value enough for tests
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("timeoutmillis").build(), "300000"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      mt));

    QueryHandleWithResultSet result = target.request(mt).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      new GenericType<LensAPIResult<QueryHandleWithResultSet>>() {}).getData();
    assertNotNull(result.getQueryHandle());
    assertNotNull(result.getResult());
    validatePersistentResult((PersistentQueryResult) result.getResult(), result.getQueryHandle(), true, false);

    final FormDataMultiPart mp2 = new FormDataMultiPart();
    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      mt));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID, IDSTR from "
      + TEST_TABLE));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute_with_timeout"));
    // set a timeout value enough for tests
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("timeoutmillis").build(), "300000"));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      mt));

    validateInmemoryResultForTimeoutQuery(target, mp2, mt);
  }

  private void validateInmemoryResultForTimeoutQuery(WebTarget target, FormDataMultiPart mp, MediaType mt) {
    if (mt.equals(MediaType.APPLICATION_JSON_TYPE)) {
      String result = target.request(mt).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), String.class);
      assertTrue(result.contains("\"type\" : \"queryHandleWithResultSet\""));
      assertTrue(result.contains("\"status\" : \"SUCCESSFUL\""));
      assertTrue(result.contains("\"isResultSetAvailable\" : true"));
      assertTrue(result.replaceAll("\\W", "").contains(expectedJsonResult().replaceAll("\\W", "")));
    } else {
      QueryHandleWithResultSet result = target.request(mt).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
        new GenericType<LensAPIResult<QueryHandleWithResultSet>>() {
        }).getData();
      assertNotNull(result.getQueryHandle());
      assertNotNull(result.getResult());
      validateInmemoryResult((InMemoryQueryResult) result.getResult());
    }
  }
  /**
   * Data provider for test case {@link #testExecuteWithTimeoutAndPreFetechAndServerPersistence()}
   * @return
   */
  @DataProvider
  public Object[][] executeWithTimeoutAndPreFetechAndServerPersistenceDP() {
    //Columns: timeOutMillis, preFetchRows, isStreamingResultAvailable, deferPersistenceByMillis
    return new Object[][] {
      {30000, 5, true, 0}, //result has 5 rows & all 5 rows are requested to be pre-fetched
      {30000, 10, true, 6000}, //result has 5 rows & 10 rows are requested to be pre-fetched.
      {30000, 2, false, 4000}, //result has 5 rows & 2 rows are requested to be pre-fetched. Will not stream
      {10, 5, false, 0}, //result has 5 rows & 5 rows requested. Timeout is less (10ms). Will not stream
    };
  }

  /**
   * @param timeOutMillis : wait time for execute with timeout api
   * @param preFetchRows : number of rows to pre-fetch in case of InMemoryResultSet
   * @param isStreamingResultAvailable : whether the execute call is expected to return InMemoryQueryResult
   * @param ttlMillis : The time window for which pre-fetched InMemoryResultSet will be available for sure.
   * @param deferPersistenceByMillis : The time in millis by which Result formatter will be deferred by.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test(dataProvider = "executeWithTimeoutAndPreFetechAndServerPersistenceDP")
  public void testExecuteWithTimeoutAndPreFetechAndServerPersistence(long timeOutMillis, int preFetchRows,
      boolean isStreamingResultAvailable, long deferPersistenceByMillis) throws Exception {
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
        MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID, IDSTR from "
        + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute_with_timeout"));
    // Set a timeout value enough for tests
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("timeoutmillis").build(), timeOutMillis + ""));
    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    conf.addProperty(LensConfConstants.PREFETCH_INMEMORY_RESULTSET, "true");
    conf.addProperty(LensConfConstants.PREFETCH_INMEMORY_RESULTSET_ROWS, preFetchRows);
    conf.addProperty(LensConfConstants.QUERY_OUTPUT_FORMATTER, DeferredInMemoryResultFormatter.class.getName());
    conf.addProperty("deferPersistenceByMillis", deferPersistenceByMillis); // property used for test only
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
        MediaType.APPLICATION_XML_TYPE));
    QueryHandleWithResultSet result =target.request(MediaType.APPLICATION_XML_TYPE)
            .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
                new GenericType<LensAPIResult<QueryHandleWithResultSet>>() {}).getData();
    QueryHandle handle = result.getQueryHandle();
    assertNotNull(handle);
    assertNotEquals(result.getStatus().getStatus(), QueryStatus.Status.FAILED);

    if (isStreamingResultAvailable) {
      // TEST streamed result
      assertTrue(result.getStatus().getStatus() == QueryStatus.Status.EXECUTED
          || result.getStatus().getStatus() == QueryStatus.Status.SUCCESSFUL,
          "Check if timeoutmillis need to be increased based on query status " + result.getStatus());
      assertEquals(result.getResultMetadata().getColumns().size(), 2);
      assertNotNull(result.getResult());
      validateInmemoryResult((InMemoryQueryResult) result.getResult());
    } else if (timeOutMillis > 20000) { // timeout is sufficient for query to finish
      assertTrue(result.getResult() instanceof PersistentQueryResult);
    } else {
      assertNull(result.getResult()); // Query execution not finished yet
    }

    waitForQueryToFinish(target(), lensSessionId, handle, Status.SUCCESSFUL, MediaType.APPLICATION_XML_TYPE);

    // Test Persistent Result
    validatePersistedResult(handle, target(), lensSessionId, new String[][] { { "ID", "INT" }, { "IDSTR", "STRING" } },
        false, true, MediaType.APPLICATION_XML_TYPE);
  }

  private static class DeferredInMemoryResultFormatter extends FileSerdeFormatter {
    /**
     * Defer init so that this output formatter takes significant time.
     */
    @Override
    public void init(QueryContext ctx, LensResultSetMetadata metadata) throws IOException {
      super.init(ctx, metadata);
      deferFormattingIfApplicable(ctx);
    }
  }

  private static class DeferredPersistentResultFormatter extends FilePersistentFormatter {
    /**
     * Defer init so that this output formatter takes significant time.
     */
    @Override
    public void init(QueryContext ctx, LensResultSetMetadata metadata) throws IOException {
      super.init(ctx, metadata);
      deferFormattingIfApplicable(ctx);
    }
  }

  private static void deferFormattingIfApplicable(QueryContext ctx) {
    long deferPersistenceByMillis = ctx.getConf().getLong("deferPersistenceByMillis", 0);
    if (deferPersistenceByMillis > 0) {
      try {
        log.info("Deferring result formatting by {} millis", deferPersistenceByMillis);
        Thread.sleep(deferPersistenceByMillis);
      } catch (InterruptedException e) {
        // Ignore
      }
    }
  }

  /**
   * Test execute with timeout query.
   *
   * @throws IOException          Signals that an I/O exception has occurred.
   * @throws InterruptedException the interrupted exception
   */
  @Test(dataProvider = "mediaTypeData")
  public void testExecuteWithTimeoutFailingQuery(MediaType mt) throws IOException, InterruptedException {
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      mt));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID from nonexist"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute_with_timeout"));
    // set a timeout value enough for tests
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("timeoutmillis").build(), "300000"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      mt));

    Response response = target.request(mt).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));
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
    Map<LensDriver, String> driverQueries = new HashMap<>();
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

    checkDefaultConfigConsistency();
  }

  public void checkDefaultConfigConsistency() {
    Configuration conf = LensSessionImpl.createDefaultConf();
    assertNotNull(conf.get("lens.query.enable.persistent.resultset"));
    boolean isDriverPersistent = conf.getBoolean("lens.query.enable.persistent.resultset", false);
    conf.setBoolean("lens.query.enable.persistent.resultset", isDriverPersistent ? false : true);
    conf.set("new_random_property", "new_random_property");

    // Get the default conf again and verify its not modified by previous operations
    conf = LensSessionImpl.createDefaultConf();
    boolean isDriverPersistentNow = conf.getBoolean("lens.query.enable.persistent.resultset", false);
    assertEquals(isDriverPersistentNow, isDriverPersistent);
    assertNull(conf.get("new_random_property"));
  }

  /**
   * Test estimate native query.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test(dataProvider = "mediaTypeData")
  public void testEstimateNativeQuery(MediaType mt) throws InterruptedException {
    final WebTarget target = target().path("queryapi/queries");

    // estimate native query
    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      mt));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID from " + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "estimate"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      mt));

    final QueryCostTO result = target.request(mt)
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
  @Test(dataProvider = "mediaTypeData")
  public void testHiveDriverGetsDBJars(MediaType mt) throws Exception {
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
        + "ROW FORMAT SERDE \"DatabaseJarSerde\"", mt);

      boolean addedToHiveDriver = false;

      for (LensDriver driver : queryService.getDrivers()) {
        if (driver instanceof HiveDriver) {
          addedToHiveDriver =
            ((HiveDriver) driver).areDBResourcesAddedForSession(sessionHandle.getPublicId().toString(), DB_WITH_JARS);
          if (addedToHiveDriver) {
            break; //There are two Hive drivers now both pointing to same hive server. So break after first success
          }
        }
      }
      assertTrue(addedToHiveDriver);

      // Switch database
      log.info("@@@# database switch test");
      session.setCurrentDatabase(DB_WITH_JARS_2);
      LensServerTestUtil.createTable(tableInDBWithJars + "_2", target(), sessionHandle, "(ID INT, IDSTR STRING) "
        + "ROW FORMAT SERDE \"DatabaseJarSerde\"", mt);

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
        LensServerTestUtil.dropTable(tableInDBWithJars, target(), sessionHandle, mt);
        LensServerTestUtil.dropTable(tableInDBWithJars + "_2", target(), sessionHandle, mt);
      } catch (Throwable th) {
        log.error("Exception while dropping table.", th);
      }
      sessionService.closeSession(sessionHandle);
    }
  }

  @Test(dataProvider = "mediaTypeData")
  public void testRewriteFailure(MediaType mt) {
    final WebTarget target = target().path("queryapi/queries");

    // estimate cube query which fails semantic analysis
    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      mt));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
      "cube sdfelect ID from cube_nonexist"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "estimate"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      mt));

    final Response response = target.request(mt)
      .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));


    LensErrorTO expectedLensErrorTO = LensErrorTO.composedOf(
      LensCubeErrorCode.SYNTAX_ERROR.getLensErrorInfo().getErrorCode(),
      "Syntax Error: line 1:5 cannot recognize input near 'sdfelect' 'ID' 'from' in select clause",
      TestDataUtils.MOCK_STACK_TRACE);
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

  @Test(dataProvider = "mediaTypeData")
  public void testNonSelectQueriesWithPersistResult(MediaType mt) throws InterruptedException {
    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "true");
    String tblName = "testNonSelectQueriesWithPersistResult";
    LensServerTestUtil.dropTableWithConf(tblName, target(), lensSessionId, conf, mt);
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    LensServerTestUtil.dropTableWithConf(tblName, target(), lensSessionId, conf, mt);
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    LensServerTestUtil.dropTableWithConf(tblName, target(), lensSessionId, conf, mt);
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "false");
    LensServerTestUtil.dropTableWithConf(tblName, target(), lensSessionId, conf, mt);
  }

  @Test(dataProvider = "mediaTypeData")
  public void testEstimateGauges(MediaType mt) {
    final WebTarget target = target().path("queryapi/queries");

    LensConf conf = new LensConf();
    String gaugeKey = "TestQueryService-testEstimateGauges" + mt.getSubtype();
    conf.addProperty(LensConfConstants.QUERY_METRIC_UNIQUE_ID_CONF_KEY, gaugeKey);
    // estimate native query
    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      mt));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select ID from " + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "estimate"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      mt));

    final QueryCostTO queryCostTO = target.request(mt)
      .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
        new GenericType<LensAPIResult<QueryCostTO>>() {
        }).getData();
    assertNotNull(queryCostTO);

    MetricRegistry reg = LensMetricsRegistry.getStaticRegistry();

    assertTrue(reg.getGauges().keySet().containsAll(Arrays.asList(
      "lens.MethodMetricGauge." + gaugeKey + "-DRIVER_SELECTION",
      "lens.MethodMetricGauge." + gaugeKey + "-hive/hive1-CUBE_REWRITE",
      "lens.MethodMetricGauge." + gaugeKey + "-hive/hive1-DRIVER_ESTIMATE",
      "lens.MethodMetricGauge." + gaugeKey + "-hive/hive1-RewriteUtil-rewriteQuery",
      "lens.MethodMetricGauge." + gaugeKey + "-hive/hive2-CUBE_REWRITE",
      "lens.MethodMetricGauge." + gaugeKey + "-hive/hive2-DRIVER_ESTIMATE",
      "lens.MethodMetricGauge." + gaugeKey + "-hive/hive2-RewriteUtil-rewriteQuery",
      "lens.MethodMetricGauge." + gaugeKey + "-jdbc/jdbc1-CUBE_REWRITE",
      "lens.MethodMetricGauge." + gaugeKey + "-jdbc/jdbc1-DRIVER_ESTIMATE",
      "lens.MethodMetricGauge." + gaugeKey + "-jdbc/jdbc1-RewriteUtil-rewriteQuery",
      "lens.MethodMetricGauge." + gaugeKey + "-PARALLEL_ESTIMATE")),
      reg.getGauges().keySet().toString());
  }

  @Test(dataProvider = "mediaTypeData")
  public void testQueryRejection(MediaType mt) throws InterruptedException, IOException {
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      mt));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "blah select ID from "
      + TEST_TABLE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), new LensConf(),
      mt));

    Response response = target.request(mt).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));
    assertEquals(response.getStatus(), 400);
  }

  /**
   * Test query purger
   *
   * @throws InterruptedException the interrupted exception
   * @throws IOException          Signals that an I/O exception has occurred.
   */
  @Test(dataProvider = "mediaTypeData")
  public void testQueryPurger(MediaType mt) throws InterruptedException, IOException {
    waitForPurge();
    LensConf conf = getLensConf(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    // test post execute op
    LensQuery ctx1 = executeAndWaitForQueryToFinish(target(), lensSessionId,
      "select ID, IDSTR from " + TEST_TABLE,
      Optional.of(conf), Optional.of(Status.SUCCESSFUL), mt);
    LensQuery ctx2 = executeAndWaitForQueryToFinish(target(), lensSessionId,
      "select ID, IDSTR from " + TEST_TABLE,
      Optional.of(conf), Optional.of(Status.SUCCESSFUL), mt);
    LensQuery ctx3 = executeAndWaitForQueryToFinish(target(), lensSessionId,
      "select ID, IDSTR from " + TEST_TABLE,
      Optional.of(conf), Optional.of(Status.SUCCESSFUL), mt);
    waitForPurge(3, queryService.finishedQueries);
    assertEquals(queryService.finishedQueries.size(), 3);
    getLensQueryResultAsString(target(), lensSessionId, ctx3.getQueryHandle(), mt);
    waitForPurge(2, queryService.finishedQueries);
    assertTrue(queryService.finishedQueries.size() == 2);
    getLensQueryResultAsString(target(), lensSessionId, ctx2.getQueryHandle(), mt);
    waitForPurge(1, queryService.finishedQueries);
    assertTrue(queryService.finishedQueries.size() == 1);
    getLensQueryResultAsString(target(), lensSessionId, ctx1.getQueryHandle(), mt);
  }

  /**
   * Test session close when a query is active on the session
   *
   * @throws Exception
   */
  @Test(dataProvider = "mediaTypeData")
  public void testSessionClose(MediaType mt) throws Exception {
    // Query with group by, will run long enough to close the session before finish
    String query = "select ID, IDSTR, count(*) from " + TEST_TABLE + " group by ID, IDSTR";
    SessionService sessionService = LensServices.get().getService(HiveSessionService.NAME);
    Map<String, String> sessionconf = new HashMap<>();
    LensSessionHandle sessionHandle = sessionService.openSession("foo", "bar", "default", sessionconf);
    LensConf conf = getLensConf(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "true");
    QueryHandle qHandle =
      executeAndGetHandle(target(), Optional.of(sessionHandle), Optional.of(query), Optional.of(conf), mt);
    sessionService.closeSession(sessionHandle);
    sessionHandle = sessionService.openSession("foo", "bar", "default", sessionconf);
    waitForQueryToFinish(target(), sessionHandle, qHandle, Status.SUCCESSFUL, mt);
  }

  @AfterMethod
  private void waitForPurge() throws InterruptedException {
    waitForPurge(0, queryService.finishedQueries);
  }
}

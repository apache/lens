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

import static org.testng.Assert.*;

import java.util.Map;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.driver.hive.HiveDriver;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServerTestUtil;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.api.util.LensUtil;
import org.apache.lens.server.common.RestAPITestUtil;
import org.apache.lens.server.common.TestResourceFile;
import org.apache.lens.server.error.LensServerErrorCode;

import org.apache.hadoop.hive.conf.HiveConf;

import org.glassfish.jersey.test.TestProperties;
import org.testng.annotations.*;

import com.google.common.base.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * The Class TestQueryService.
 */
@Slf4j
@Test(groups = "unit-test")
public class TestQueryIndependenceFromSession extends LensJerseyTest {
  /** The query service. */
  QueryExecutionServiceImpl queryService;

  /** The lens session id. */
  LensSessionHandle lensSessionId;
  private LensConf conf;

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#setUp()
   */
  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
    queryService = LensServices.get().getService(QueryExecutionService.NAME);
    lensSessionId = getSession();
    createTable(TEST_TABLE);
    loadData(TEST_TABLE, TestResourceFile.TEST_DATA2_FILE.getValue());
    conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "true");
    conf.addProperty(LensConfConstants.QUERY_OUTPUT_FORMATTER,
      TestQueryService.DeferredPersistentResultFormatter.class.getName());
    conf.addProperty("deferPersistenceByMillis", 5000); // defer persistence for 5 secs
  }

  @Override
  public Map<String, String> getServerConfOverWrites() {
    return LensUtil.getHashMap("lens.server.total.query.cost.ceiling.per.user", "1");
  }

  private LensSessionHandle getSession() throws LensException {
    return queryService.openSession("foo", "bar", null);
  }

  private void closeSession(LensSessionHandle session) throws LensException {
    queryService.closeSession(session);
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
  protected void restartLensServer() {
    queryService = null;
    super.restartLensServer();
    queryService = LensServices.get().getService(QueryExecutionService.NAME);
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
    return new TestQueryService.QueryServiceTestApp();
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

  @DataProvider
  public Object[][] restartDataProvider() {
    return new Object[][]{
      {true, true},
      {true, false},
      {false, true},
      {false, false},
    };
  }

  @Test(dataProvider = "restartDataProvider")
  public void testQueryAliveOnSessionClose(boolean restartBeforeFinish, boolean restartAfterFinish)
    throws LensException, InterruptedException {
    MediaType mt = MediaType.APPLICATION_XML_TYPE;
    restartLensServer();
    LensSessionHandle sesssionHandle = getSession();
    QueryHandle queryHandle1 = RestAPITestUtil.executeAndGetHandle(target(),
      Optional.of(sesssionHandle), Optional.of("select * from " + TEST_TABLE), Optional.of(conf), mt);
    QueryHandle queryHandle2 = RestAPITestUtil.executeAndGetHandle(target(),
      Optional.of(sesssionHandle), Optional.of("select * from " + TEST_TABLE), Optional.of(conf), mt);
    assertEquals(queryService.getQueryContext(queryHandle2).getStatus().getStatus(), QueryStatus.Status.QUEUED);
    closeSession(sesssionHandle);
    // Session not 'truly' closed
    assertNotNull(queryService.getSession(sesssionHandle));
    if (restartBeforeFinish) {
      restartLensServer();
    }
    assertNotNull(queryService.getSession(sesssionHandle));
    assertTrue(queryService.getSession(sesssionHandle).isActive());
    LensQuery lensQuery = RestAPITestUtil.waitForQueryToFinish(target(), lensSessionId, queryHandle2,
      QueryStatus.Status.SUCCESSFUL, mt);
    RestAPITestUtil.waitForQueryToFinish(target(), lensSessionId, queryHandle1,
      QueryStatus.Status.SUCCESSFUL, mt);
    if (restartAfterFinish) {
      restartLensServer();
    }
    // Now, session is not active anymore
    assertFalse(queryService.getSession(sesssionHandle).isActive());
    // It should not be possible to submit queries now
    Response response = RestAPITestUtil.postQuery(target(), Optional.of(sesssionHandle),
      Optional.of("select * from " + TEST_TABLE), Optional.of("execute"), Optional.of(conf), mt);
    assertEquals(response.getStatus(), 410);
    LensAPIResult apiResult = response.readEntity(LensAPIResult.class);
    assertEquals(apiResult.getErrorCode(), LensServerErrorCode.SESSION_CLOSED.getLensErrorInfo().getErrorCode());
  }

//  @AfterMethod
//  private void waitForPurge() throws InterruptedException {
//    waitForPurge(0, queryService.finishedQueries);
//  }
}

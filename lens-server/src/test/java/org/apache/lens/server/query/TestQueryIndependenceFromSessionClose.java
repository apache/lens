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

import static org.apache.lens.server.api.LensConfConstants.*;

import static org.testng.Assert.*;

import java.util.*;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.api.session.UserSessionInfo;
import org.apache.lens.driver.hive.HiveDriver;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServerTestUtil;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.LensServerAPITestUtil;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.api.session.SessionService;
import org.apache.lens.server.api.util.LensUtil;
import org.apache.lens.server.common.RestAPITestUtil;
import org.apache.lens.server.common.TestResourceFile;
import org.apache.lens.server.error.LensServerErrorCode;
import org.apache.lens.server.session.HiveSessionService;
import org.apache.lens.server.session.LensSessionImpl;

import org.apache.hadoop.hive.conf.HiveConf;

import org.glassfish.jersey.test.TestProperties;
import org.testng.annotations.*;

import com.google.common.base.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * The Class TestQueryService.
 */
@Slf4j
@Test(groups = "post-restart", dependsOnGroups = "restart-test")
public class TestQueryIndependenceFromSessionClose extends LensJerseyTest {
  /** The query service. */
  QueryExecutionServiceImpl queryService;
  HiveSessionService sessionService;

  /** The lens session id. */
  LensSessionHandle lensSessionId;
  private LensConf conf;

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#tearDown()
   */
  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
  }

  private QueryExecutionServiceImpl getQueryService() {
    return queryService = LensServices.get().getService(QueryExecutionService.NAME);
  }

  private SessionService getSessionService() {
    return sessionService = LensServices.get().getService(SessionService.NAME);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#setUp()
   */
  @BeforeClass
  public void setUpClass() throws Exception {
    restartLensServer(getServerConf());
    lensSessionId = getSession();
    createTable(TEST_TABLE);
    loadData(TEST_TABLE, TestResourceFile.TEST_DATA2_FILE.getValue());
    conf = LensServerAPITestUtil.getLensConf("deferPersistenceByMillis", 5000,
      QUERY_PERSISTENT_RESULT_SET, true,
      QUERY_PERSISTENT_RESULT_INDRIVER, true,
      QUERY_OUTPUT_FORMATTER, TestQueryService.DeferredPersistentResultFormatter.class.getName());
  }

  @Override
  public Map<String, String> getServerConfOverWrites() {
    return LensUtil.getHashMap("lens.server.total.query.cost.ceiling.per.user", "1", "lens.server.drivers",
      "hive:org.apache.lens.driver.hive.HiveDriver", MAX_SESSIONS_PER_USER, "1");
  }

  private LensSessionHandle getSession() throws LensException {
    return getSessionService().openSession("foo", "bar", null, null);
  }

  private void closeSession(LensSessionHandle session) throws LensException {
    getSessionService().closeSession(session);
  }

  /*
     * (non-Javadoc)
     *
     * @see org.glassfish.jersey.test.JerseyTest#tearDown()
     */
  @AfterClass
  public void tearDownClass() throws Exception {
    dropTable(TEST_TABLE);
    getSessionService().closeSession(lensSessionId);
    for (LensDriver driver : getQueryService().getDrivers()) {
      if (driver instanceof HiveDriver) {
        assertFalse(((HiveDriver) driver).hasLensSession(lensSessionId));
      }
    }
    // bring it back with normal configuration
    restartLensServer();
  }

  private void customRestartLensServer() {
    queryService = null;
    super.restartLensServer(getServerConf());
    getQueryService();
  }

  private void restartLensServerWithLowerExpiry() {
    sessionService = null;
    HiveConf hconf = new HiveConf(getServerConf());
    hconf.setLong(LensConfConstants.SESSION_TIMEOUT_SECONDS, 1L);
    super.restartLensServer(hconf);
    getSessionService();
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
  public static final String TEST_TABLE = "TEST_TABLE_INDEPENDENCE";

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
    int numSessions = getSessionsOfFoo().size();
    MediaType mt = MediaType.APPLICATION_XML_TYPE;
    LensSessionHandle sessionHandle = getSession();
    QueryHandle queryHandle1 = RestAPITestUtil.executeAndGetHandle(target(),
      Optional.of(sessionHandle), Optional.of("select * from " + TEST_TABLE), Optional.of(conf), mt);
    QueryHandle queryHandle2 = RestAPITestUtil.executeAndGetHandle(target(),
      Optional.of(sessionHandle), Optional.of("select *  from " + TEST_TABLE), Optional.of(conf), mt);
    assertNotEquals(queryHandle1, queryHandle2);
    // Second query should be queued
    assertEquals(getQueryService().getQueryContext(queryHandle2).getStatus().getStatus(), QueryStatus.Status.QUEUED);
    closeSession(sessionHandle);
    // Session not 'truly' closed
    assertNotNull(getQueryService().getSession(sessionHandle));
    // Just 'marked' for closing
    assertTrue(getQueryService().getSession(sessionHandle).getLensSessionPersistInfo().isMarkedForClose());
    // Try submitting another query in this so called "inactive" session
    Response response = RestAPITestUtil.postQuery(target(),
      Optional.of(sessionHandle), Optional.of("select * from " + TEST_TABLE), Optional.of("execute"), mt);
    assertEquals(response.getStatus(), 410);
    LensAPIResult apiResult = response.readEntity(LensAPIResult.class);
    assertEquals(apiResult.getErrorCode(), 2005);
    // Should be able to open another session, since max session per user is 1 and this session is closed
    LensSessionHandle sessionHandle1 = getSession();
    assertNotNull(sessionHandle1);
    if (restartBeforeFinish) {
      customRestartLensServer();
    }
    assertTrue(getQueryService().getSession(sessionHandle).getLensSessionPersistInfo().isMarkedForClose());
    assertTrue(getQueryService().getSession(sessionHandle).isActive());
    for (QueryHandle handle : Arrays.asList(queryHandle2, queryHandle1)) {
      RestAPITestUtil.waitForQueryToFinish(target(), lensSessionId, handle, QueryStatus.Status.SUCCESSFUL, mt);
    }
    // Session should not be active
    assertFalse(getQueryService().getSession(sessionHandle).isActive());
    if (restartAfterFinish) {
      customRestartLensServer();
    }
    assertTrue(getQueryService().getSession(sessionHandle).getLensSessionPersistInfo().isMarkedForClose());
    // Now, session is not active anymore
    assertFalse(getQueryService().getSession(sessionHandle).isActive());
    // It should not be possible to submit queries now
    response = RestAPITestUtil.postQuery(target(), Optional.of(sessionHandle),
      Optional.of("select * from " + TEST_TABLE), Optional.of("execute"), Optional.of(conf), mt);
    assertEquals(response.getStatus(), 410);
    apiResult = response.readEntity(LensAPIResult.class);
    assertEquals(apiResult.getErrorCode(), LensServerErrorCode.SESSION_CLOSED.getLensErrorInfo().getErrorCode());
    getSessionService().cleanupIdleSessions();

    assertTrue(getSessionsOfFoo().size() - numSessions <= 2);
  }
  private List<UserSessionInfo> getSessionsOfFoo() {
    List<UserSessionInfo> sessions = getSessionService().getSessionInfo();
    Iterator<UserSessionInfo> iter = sessions.iterator();
    while (iter.hasNext()) {
      UserSessionInfo session = iter.next();
      assertNotEquals(session.getHandle(), lensSessionId.getPublicId(),
        "session not cleaned up even after queries finished");
      if (!session.getUserName().equals("foo")) {
        iter.remove();
      }
    }
    return sessions;
  }

  @Test
  public void testSessionExpiryWithActiveOperation() throws Exception {
    LensSessionHandle oldSession = getSession();
    assertTrue(sessionService.getSession(oldSession).isActive());
    restartLensServerWithLowerExpiry();
    assertFalse(sessionService.getSession(oldSession).isActive());
    // create a new session and launch a query
    LensSessionHandle sessionHandle = getSession();
    LensSessionImpl session = sessionService.getSession(sessionHandle);
    QueryHandle handle = RestAPITestUtil.executeAndGetHandle(target(),
      Optional.of(sessionHandle), Optional.of("select * from " + TEST_TABLE), Optional.of(conf), defaultMT);
    assertTrue(session.isActive());
    session.setLastAccessTime(
      session.getLastAccessTime() - 2000 * getServerConf().getLong(LensConfConstants.SESSION_TIMEOUT_SECONDS,
        LensConfConstants.SESSION_TIMEOUT_SECONDS_DEFAULT));
    assertTrue(session.isActive());
    assertFalse(session.isMarkedForClose());

    LensSessionHandle sessionHandle2 = getSession();
    LensQuery ctx = RestAPITestUtil.getLensQuery(target(), sessionHandle2, handle, defaultMT);
    while (!ctx.getStatus().finished()) {
      ctx = RestAPITestUtil.getLensQuery(target(), sessionHandle2, handle, defaultMT);
      Thread.sleep(1000);
      sessionHandle2 = getSession();
    }
    assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, String.valueOf(ctx));
    assertFalse(session.isActive());
    assertFalse(session.isMarkedForClose());

    // run the expiry thread
    sessionService.getSessionExpiryRunnable().run();
    try {
      sessionService.getSession(sessionHandle);
      // should throw exception since session should be expired by now
      fail("Expected get session to fail for session " + sessionHandle.getPublicId());
    } catch (Exception e) {
      // pass
    }
    try {
      sessionService.getSession(oldSession);
      // should throw exception since session should be expired by now
      fail("Expected get session to fail for session " + oldSession.getPublicId());
    } catch (Exception e) {
      // pass
    }
    restartLensServer();
    lensSessionId = getSession();
  }
  @AfterMethod
  private void waitForPurge() throws InterruptedException {
    waitForPurge(0, getQueryService().finishedQueries);
  }
}

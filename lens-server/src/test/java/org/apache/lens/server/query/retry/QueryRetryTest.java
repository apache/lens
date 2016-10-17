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
package org.apache.lens.server.query.retry;


import static org.apache.lens.server.api.LensConfConstants.DRIVER_TYPES_AND_CLASSES;
import static org.apache.lens.server.api.LensConfConstants.QUERY_RETRY_POLICY_CLASSES;
import static org.apache.lens.server.api.LensServerAPITestUtil.getLensConf;
import static org.apache.lens.server.api.util.LensUtil.getHashMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Map;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.FailedAttempt;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.api.session.SessionService;
import org.apache.lens.server.common.RestAPITestUtil;
import org.apache.lens.server.query.QueryExecutionServiceImpl;
import org.apache.lens.server.query.TestQueryService;

import org.glassfish.jersey.test.TestProperties;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Test(groups = "post-restart", dependsOnGroups = "restart-test")
public class QueryRetryTest extends LensJerseyTest {

  private QueryExecutionServiceImpl queryService;
  private SessionService sessionService;
  private LensSessionHandle session;

  private QueryExecutionServiceImpl getQueryService() {
    return queryService = LensServices.get().getService(QueryExecutionService.NAME);
  }

  @Override
  protected Application configure() {
    enable(TestProperties.LOG_TRAFFIC);
    enable(TestProperties.DUMP_ENTITY);
    return new TestQueryService.QueryServiceTestApp();
  }

  private SessionService getSessionService() {
    return sessionService = LensServices.get().getService(SessionService.NAME);
  }

  private LensSessionHandle getSession() throws LensException {
    return getSessionService().openSession("foo", "bar", null, null);
  }

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }
  @BeforeClass
  public void setUpClass() throws Exception {
    restartLensServer(getServerConf(), false);
    session = getSession();
  }
  @AfterClass
  public void cleanupClass() throws Exception {
    getSessionService().closeSession(session);
    restartLensServer();
  }

  @Override
  public Map<String, String> getServerConfOverWrites() {
    return getHashMap(DRIVER_TYPES_AND_CLASSES, "retry:org.apache.lens.server.query.retry.MockDriverForRetries",
      QUERY_RETRY_POLICY_CLASSES, TestServerRetryPolicyDecider.class.getName());
  }

  @Test
  public void testSingleRetrySameDriver() throws LensException, InterruptedException {
    QueryHandle handle = getQueryService().executeAsync(session, "select 1",
      getLensConf("driver.retry/single_failure.cost", "1", "driver.retry/double_failure.cost", "2"),
      "random query");
    QueryContext ctx = getQueryService().getQueryContext(handle);
    while (!ctx.getStatus().finished()) {
      ctx = getQueryService().getQueryContext(handle);
      Thread.sleep(1000);
    }
    assertEquals(ctx.getFailedAttempts().size(), 1);
    FailedAttempt failedAttempt = ctx.getFailedAttempts().get(0);
    assertEquals(failedAttempt.getDriverName(), "retry/single_failure");
    assertEquals(ctx.getSelectedDriver().getFullyQualifiedName(), "retry/single_failure");
    assertTrue(failedAttempt.getDriverFinishTime() > failedAttempt.getDriverStartTime());
    assertTrue(ctx.getDriverStatus().getDriverStartTime() > failedAttempt.getDriverFinishTime());

  }

  @Test
  public void testRetryOnDifferentDriver() throws LensException, InterruptedException {
    QueryHandle handle = getQueryService().executeAsync(session, "select 1",
      getLensConf("driver.retry/single_failure.cost", "2", "driver.retry/double_failure.cost", "1"),
      "random query");
    QueryContext ctx = getQueryService().getQueryContext(handle);
    while (!ctx.getStatus().finished()) {
      ctx = getQueryService().getQueryContext(handle);
      Thread.sleep(1000);
    }
    assertEquals(ctx.getFailedAttempts().size(), 2);
    FailedAttempt attempt1 = ctx.getFailedAttempts().get(0);
    FailedAttempt attempt2 = ctx.getFailedAttempts().get(1);
    // two retries on double_failure
    assertEquals(attempt1.getDriverName(), "retry/double_failure");
    assertEquals(attempt2.getDriverName(), "retry/double_failure");
    // first retry failed on single_failure since the driver checks total retries before failing.
    // If there weren't any attempts before this one, then this attempt would fail, but since there
    // have already been 2 attempts and 2 > 1, this attempt passed.
    assertEquals(ctx.getSelectedDriver().getFullyQualifiedName(), "retry/single_failure");
    assertTrue(attempt2.getDriverStartTime() > attempt1.getDriverFinishTime());
    assertTrue(ctx.getDriverStatus().getDriverStartTime() > attempt2.getDriverFinishTime());

    // test rest api
    LensQuery lensQuery = RestAPITestUtil.getLensQuery(target(), session, handle, MediaType.APPLICATION_XML_TYPE);
    assertEquals(lensQuery.getFailedAttempts(), ctx.getFailedAttempts());
  }

  @Test
  public void testFailureAfterRetry() throws LensException, InterruptedException {
    QueryHandle handle = getQueryService().executeAsync(session, "select 1",
      getLensConf("driver.retry/double_failure.cost", "1"),
      "random query");
    QueryContext ctx = getQueryService().getQueryContext(handle);
    while (!ctx.getStatus().finished()) {
      ctx = getQueryService().getQueryContext(handle);
      Thread.sleep(1000);
    }
    assertTrue(ctx.getStatus().failed());
    assertEquals(ctx.getFailedAttempts().size(), 1);
    FailedAttempt attempt1 = ctx.getFailedAttempts().get(0);
    assertEquals(attempt1.getDriverName(), "retry/double_failure");
    assertEquals(ctx.getSelectedDriver().getFullyQualifiedName(), "retry/double_failure");
    assertTrue(ctx.getStatus().failed());
  }

  @Test
  public void testDelayedLaunch() throws LensException, InterruptedException {
    QueryHandle handle = getQueryService().executeAsync(session, "select 1",
      getLensConf("driver.retry/double_failure.cost", "1",
        "driver.retry/double_failure.error.message", "fibonacci.500"),
      "random query");
    QueryContext ctx = getQueryService().getQueryContext(handle);

    while (!ctx.getStatus().finished()) {
      ctx = getQueryService().getQueryContext(handle);
      Thread.sleep(1000);
    }
    assertTrue(ctx.getStatus().successful());
    assertEquals(ctx.getFailedAttempts().size(), 2);
    FailedAttempt attempt1 = ctx.getFailedAttempts().get(0);
    FailedAttempt attempt2 = ctx.getFailedAttempts().get(1);
    assertTrue(attempt2.getDriverStartTime() - attempt1.getDriverFinishTime() >= 500);
    assertTrue(ctx.getDriverStatus().getDriverStartTime() - attempt2.getDriverFinishTime() >= 1000);
  }

  @Test
  public void testRestartWhileRetry() throws LensException, InterruptedException {
    QueryHandle handle = getQueryService().executeAsync(session, "select 1",
      getLensConf("driver.retry/double_failure.cost", "1",
        "driver.retry/double_failure.error.message", "fibonacci.5000"),
      "random query");
    QueryContext ctx = getQueryService().getQueryContext(handle);

    while (ctx.getFailedAttempts().size() == 0) {
      ctx = getQueryService().getQueryContext(handle);
      Thread.sleep(1000);
    }
    restartLensServer(getServerConf(), false);
    ctx = getQueryService().getQueryContext(handle);
    while (!ctx.getStatus().finished()) {
      ctx = getQueryService().getQueryContext(handle);
      Thread.sleep(1000);
    }
    assertTrue(ctx.getStatus().successful());
    assertEquals(ctx.getFailedAttempts().size(), 2);
    FailedAttempt attempt1 = ctx.getFailedAttempts().get(0);
    FailedAttempt attempt2 = ctx.getFailedAttempts().get(1);
    assertTrue(attempt2.getDriverStartTime() - attempt1.getDriverFinishTime() >= 5000);
    assertTrue(ctx.getDriverStatus().getDriverStartTime() - attempt2.getDriverFinishTime() >= 10000);
  }
}

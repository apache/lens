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

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
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

import org.apache.hadoop.hive.conf.HiveConf;

import org.glassfish.jersey.test.TestProperties;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * The Class TestQueryService.
 */
@Slf4j
@Test(groups = "unit-test")
public class TestQueryIndependenceFromSession extends LensJerseyTest {
  private HiveConf serverConf;

  /** The query service. */
  QueryExecutionServiceImpl queryService;

  /** The metrics svc. */
  MetricsService metricsSvc;

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
    lensSessionId = getSession();
    createTable(TEST_TABLE);
    loadData(TEST_TABLE, TestResourceFile.TEST_DATA2_FILE.getValue());
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

  @Test(dataProvider = "mediaTypeData")
  public void testQueryAliveOnSessionClose(MediaType mt) throws LensException, InterruptedException {
    LensSessionHandle sesssionHandle = getSession();
    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "true");
    conf.addProperty(LensConfConstants.QUERY_OUTPUT_FORMATTER,
      TestQueryService.DeferredPersistentResultFormatter.class.getName());
    conf.addProperty("deferPersistenceByMillis", 5000); // defer persistence for 5 secs
    QueryHandle queryHandle1 = RestAPITestUtil.executeAndGetHandle(target(),
      Optional.of(sesssionHandle), Optional.of("select * from " + TEST_TABLE), Optional.of(conf), mt);
    QueryHandle queryHandle2 = RestAPITestUtil.executeAndGetHandle(target(),
      Optional.of(sesssionHandle), Optional.of("select * from " + TEST_TABLE), Optional.of(conf), mt);
    assertEquals(queryService.getQueryContext(queryHandle2).getStatus().getStatus(), QueryStatus.Status.QUEUED);
    closeSession(sesssionHandle);
    // Session not 'truly' closed
    assertNotNull(queryService.getSession(sesssionHandle));
    assertTrue(queryService.getSession(sesssionHandle).isActive());
    LensQuery lensQuery = RestAPITestUtil.waitForQueryToFinish(target(), lensSessionId, queryHandle2,
      QueryStatus.Status.SUCCESSFUL, mt);
    // Now, session is not active anymore
    assertFalse(queryService.getSession(sesssionHandle).isActive());
    System.out.println(lensQuery);
  }

  @AfterMethod
  private void waitForPurge() throws InterruptedException {
    waitForPurge(0, queryService.finishedQueries);
  }
}

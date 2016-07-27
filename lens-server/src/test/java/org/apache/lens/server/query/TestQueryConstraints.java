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

import static org.apache.lens.server.api.LensConfConstants.QUERY_METRIC_UNIQUE_ID_CONF_KEY;

import static org.testng.Assert.*;

import java.util.*;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.driver.hive.HiveDriver;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServerTestUtil;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.LensServerAPITestUtil;
import org.apache.lens.server.api.driver.DriverSelector;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.api.util.LensUtil;
import org.apache.lens.server.common.RestAPITestUtil;
import org.apache.lens.server.common.TestResourceFile;

import org.apache.hadoop.conf.Configuration;

import org.glassfish.jersey.test.TestProperties;

import org.testng.annotations.*;

import com.beust.jcommander.internal.Lists;

import com.google.common.base.Optional;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class TestQueryService.
 */
@Slf4j
@Test(groups = "two-working-drivers", dependsOnGroups = "filter-test")
public class TestQueryConstraints extends LensJerseyTest {

  public static class RoundRobinSelector implements DriverSelector {
    int counter = 0;

    @Override
    public LensDriver select(AbstractQueryContext ctx, Configuration conf) {
      final Collection<LensDriver> drivers = ctx.getDriverContext().getDriversWithValidQueryCost();
      LensDriver driver = drivers.toArray(new LensDriver[drivers.size()])[counter];
      counter = (counter + 1) % 2;
      return driver;
    }
  }

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

  }

  @BeforeClass
  public void setupTest() throws Exception {
    // restart with overwritten conf
    restartLensServer(getServerConf());
    queryService = LensServices.get().getService(QueryExecutionService.NAME);
    metricsSvc = LensServices.get().getService(MetricsService.NAME);
    Map<String, String> sessionConf = new HashMap<>();
    sessionConf.put("test.session.key", "svalue");
    lensSessionId = queryService.openSession("foo@localhost", "bar", sessionConf); // @localhost should be removed
    // automatically
    createTable(TEST_TABLE);
    loadData(TEST_TABLE, TestResourceFile.TEST_DATA2_FILE.getValue());
  }

  @AfterClass
  public void afterTest() throws Exception {
    dropTable(TEST_TABLE);
    queryService.closeSession(lensSessionId);
    // bring it back without overwritten conf
    restartLensServer();
  }

  @Override
  public Map<String, String> getServerConfOverWrites() {
    return LensUtil.getHashMap(LensConfConstants.DRIVER_TYPES_AND_CLASSES, "mockHive:" + HiveDriver.class.getName(),
      LensConfConstants.DRIVER_SELECTOR_CLASS, RoundRobinSelector.class.getName());
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
  public static final String TEST_TABLE = "QUERY_CONSTRAINTS_TEST_TABLE";

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
  public void testThrottling(MediaType mt) throws InterruptedException {
    List<QueryHandle> handles = Lists.newArrayList();
    for (int j = 0; j < 5; j++) {
      for (int i = 0; i < 10; i++) {
        handles.add(launchQuery(mt));
        assertValidity();
      }
      // No harm in sleeping, the queries will anyway take time.
      Thread.sleep(1000);
    }
    for (QueryHandle handle : handles) {
      RestAPITestUtil.waitForQueryToFinish(target(), lensSessionId, handle, mt);
      assertValidity();
    }
    for (QueryHandle handle : handles) {
      RestAPITestUtil.getLensQueryResultAsString(target(), lensSessionId, handle, mt);
      assertValidity();
    }
  }

  @Test(dataProvider = "mediaTypeData")
  public void testInmemoryQueryThrottling(MediaType mt) throws InterruptedException, LensException {
    List<QueryHandle> handles = Lists.newArrayList();
    for (int j = 0; j < 5; j++) {
      for (int i = 0; i < 10; i++) {
        handles.add(launchInmemoryQuery(mt));
        assertValidity();
      }
    }
    for (QueryHandle handle : handles) {
      RestAPITestUtil.waitForQueryToFinish(target(), lensSessionId, handle, mt);
      queryService.fetchResultSet(lensSessionId, handle, 0, 100);
      assertValidity();
    }
  }

  private void assertValidity() {
    QueryExecutionServiceImpl.QueryCount count = queryService.getQueryCountSnapshot();
    assertTrue(count.running <= 4, System.currentTimeMillis() + " " + count.running + " running queries: "
      + queryService.getLaunchedQueries());
  }

  private QueryHandle launchQuery(MediaType mt) {
    return RestAPITestUtil.executeAndGetHandle(target(), Optional.of(lensSessionId),
      Optional.of("select ID from " + TEST_TABLE),
      Optional.of(LensServerAPITestUtil.getLensConf(QUERY_METRIC_UNIQUE_ID_CONF_KEY, UUID.randomUUID())), mt);
  }

  private QueryHandle launchInmemoryQuery(MediaType mt) {
    LensConf conf = LensServerAPITestUtil.getLensConf(QUERY_METRIC_UNIQUE_ID_CONF_KEY, UUID.randomUUID());
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "false");
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    conf.addProperty(LensConfConstants.INMEMORY_RESULT_SET_TTL_SECS, 600);
    return RestAPITestUtil.executeAndGetHandle(target(), Optional.of(lensSessionId),
        Optional.of("select ID from " + TEST_TABLE),
        Optional.of(conf), mt);
  }

  @AfterMethod
  private void waitForPurge() throws InterruptedException {
    waitForPurge(0, queryService.finishedQueries);
  }
}

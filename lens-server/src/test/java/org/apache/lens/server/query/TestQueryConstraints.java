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

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.jaxb.LensJAXBContextResolver;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.driver.hive.HiveDriver;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.LensServerTestUtil;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.LensServerAPITestUtil;
import org.apache.lens.server.api.driver.DriverSelector;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.common.RestAPITestUtil;
import org.apache.lens.server.common.TestResourceFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.test.TestProperties;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.beust.jcommander.internal.Lists;

import com.google.common.base.Optional;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class TestQueryService.
 */
@Slf4j
@Test(groups = "two-working-drivers", dependsOnGroups = "filter-test")
public class TestQueryConstraints extends LensJerseyTest {
  private HiveConf serverConf;

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
    queryService = LensServices.get().getService(QueryExecutionService.NAME);
    metricsSvc = LensServices.get().getService(MetricsService.NAME);
    Map<String, String> sessionConf = new HashMap<>();
    sessionConf.put("test.session.key", "svalue");
    lensSessionId = queryService.openSession("foo@localhost", "bar", sessionConf); // @localhost should be removed
    // automatically
    createTable(TEST_TABLE);
    loadData(TEST_TABLE, TestResourceFile.TEST_DATA2_FILE.getValue());
  }

  @Override
  public HiveConf getServerConf() {
    if (serverConf == null) {
      serverConf = new HiveConf(super.getServerConf());
      // Lets test only mockHive. updating lens server conf for same
      serverConf.set(LensConfConstants.DRIVER_TYPES_AND_CLASSES, "mockHive:" + HiveDriver.class.getName());
      serverConf.set("lens.server.driver.selector.class", RoundRobinSelector.class.getName());
      LensServerConf.getConfForDrivers().addResource(serverConf);
    }
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

  @Test
  public void testThrottling() throws InterruptedException {
    List<QueryHandle> handles = Lists.newArrayList();
    for (int j = 0; j < 5; j++) {
      for (int i = 0; i < 10; i++) {
        handles.add(launchQuery());
        assertValidity();
      }
      // No harm in sleeping, the queries will anyway take time.
      Thread.sleep(1000);
    }
    for (QueryHandle handle : handles) {
      RestAPITestUtil.waitForQueryToFinish(target(), lensSessionId, handle);
      assertValidity();
    }
    for (QueryHandle handle : handles) {
      RestAPITestUtil.getLensQueryResult(target(), lensSessionId, handle);
      assertValidity();
    }
  }

  private void assertValidity() {
    QueryExecutionServiceImpl.QueryCount count = queryService.getQueryCountSnapshot();
    assertTrue(count.running <= 4, System.currentTimeMillis() + " " + count.running + " running queries: "
      + queryService.getLaunchedQueries());
  }

  private QueryHandle launchQuery() {
    return RestAPITestUtil.executeAndGetHandle(target(), Optional.of(lensSessionId),
      Optional.of("select ID from " + TEST_TABLE),
      Optional.of(LensServerAPITestUtil.getLensConf(QUERY_METRIC_UNIQUE_ID_CONF_KEY, UUID.randomUUID())));
  }

  @AfterMethod
  private void waitForPurge() throws InterruptedException {
    waitForPurge(0, queryService.finishedQueries);
  }
}

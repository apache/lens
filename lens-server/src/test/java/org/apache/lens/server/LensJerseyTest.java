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
package org.apache.lens.server;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.net.URI;

import javax.ws.rs.core.UriBuilder;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.Service;
import org.apache.hive.service.Service.STATE;
import org.apache.lens.driver.hive.TestRemoteHiveDriver;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.metrics.MetricsService;
import org.glassfish.jersey.test.JerseyTest;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

/**
 * Extend this class for unit testing Lens Jersey resources
 */
public abstract class LensJerseyTest extends JerseyTest {

  protected URI getUri() {
    return UriBuilder.fromUri("http://localhost/").port(getTestPort()).build();
  }

  protected abstract int getTestPort();

  @Override
  protected URI getBaseUri() {
    return UriBuilder.fromUri(getUri()).path("lens-server").build();
  }

  /**
   * Start all.
   *
   * @throws Exception
   *           the exception
   */
  @BeforeSuite
  public void startAll() throws Exception {
    TestRemoteHiveDriver.createHS2Service();
    System.out.println("Remote hive server started!");
    HiveConf hiveConf = new HiveConf();
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_ASYNC_EXEC_THREADS, 5);
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_CONNECTION_RETRY_LIMIT, 3);
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_RETRY_LIMIT, 3);
    LensServices.get().init(LensServerConf.get());
    LensServices.get().start();

    // Check if mock service is started
    Service mockSvc = LensServices.get().getService(MockNonLensService.NAME);
    assertNotNull(mockSvc);
    assertTrue(mockSvc instanceof MockNonLensService, mockSvc.getClass().getName());
    assertEquals(mockSvc.getServiceState(), STATE.STARTED);
    System.out.println("Lens services started!");
  }

  /**
   * Stop all.
   *
   * @throws Exception
   *           the exception
   */
  @AfterSuite
  public void stopAll() throws Exception {
    verifyMetrics();
    LensServices.get().stop();
    System.out.println("Lens services stopped!");
    TestRemoteHiveDriver.stopHS2Service();
    System.out.println("Remote hive server stopped!");
  }

  /**
   * Verify metrics.
   */
  protected void verifyMetrics() {
    // print final metrics
    System.out.println("Final report");
    MetricsService metrics = ((MetricsService) LensServices.get().getService(MetricsService.NAME));
    metrics.publishReport();

    // validate http error count
    long httpClientErrors = metrics.getCounter(LensRequestListener.class, LensRequestListener.HTTP_CLIENT_ERROR);
    long httpServerErrors = metrics.getCounter(LensRequestListener.class, LensRequestListener.HTTP_SERVER_ERROR);
    long httpOtherErrors = metrics.getCounter(LensRequestListener.class, LensRequestListener.HTTP_UNKOWN_ERROR);
    long httpErrors = metrics.getCounter(LensRequestListener.class, LensRequestListener.HTTP_ERROR);
    assertEquals(httpClientErrors + httpServerErrors + httpOtherErrors, httpErrors,
        "Server + Client error should equal total errors");

    // validate http metrics
    long httpReqStarted = metrics.getCounter(LensRequestListener.class, LensRequestListener.HTTP_REQUESTS_STARTED);
    long httpReqFinished = metrics.getCounter(LensRequestListener.class, LensRequestListener.HTTP_REQUESTS_FINISHED);
    assertEquals(httpReqStarted, httpReqFinished, "Total requests started should equal total requests finished");

    // validate queries in the final state
    long queriesSuccessful = metrics.getTotalSuccessfulQueries();
    long queriesFailed = metrics.getTotalFailedQueries();
    long queriesCancelled = metrics.getTotalCancelledQueries();
    long queriesFinished = metrics.getTotalFinishedQueries();

    assertEquals(queriesFinished, queriesSuccessful + queriesFailed + queriesCancelled,
        "Total finished queries should be sum of successful, failed and cancelled queries");

  }

  /**
   * Restart lens server.
   */
  public void restartLensServer() {
    HiveConf h = LensServerConf.get();
    h.set(LensConfConstants.MAX_NUMBER_OF_FINISHED_QUERY, "0");
    restartLensServer(h);
  }

  /**
   * Restart lens server.
   *
   * @param conf
   *          the conf
   */
  public void restartLensServer(HiveConf conf) {
    LensServices.get().stop();
    System.out.println("Lens services stopped!");
    LensServices.setInstance(new LensServices(LensServices.LENS_SERVICES_NAME));
    LensServices.get().init(conf);
    LensServices.get().start();
    System.out.println("Lens services restarted!");
  }
}

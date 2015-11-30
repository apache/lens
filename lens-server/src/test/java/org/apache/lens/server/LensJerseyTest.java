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

import static org.apache.lens.server.LensServerTestUtil.DB_WITH_JARS;
import static org.apache.lens.server.LensServerTestUtil.DB_WITH_JARS_2;
import static org.apache.lens.server.LensServerTestUtil.createTestDatabaseResources;

import static org.testng.Assert.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.ws.rs.core.UriBuilder;

import org.apache.lens.driver.hive.TestRemoteHiveDriver;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.metrics.LensMetricsUtil;
import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.lens.server.model.LogSegregationContext;
import org.apache.lens.server.model.MappedDiagnosticLogSegregationContext;
import org.apache.lens.server.query.QueryExecutionServiceImpl;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.Service;
import org.apache.hive.service.Service.STATE;

import org.glassfish.jersey.test.JerseyTest;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

/**
 * Extend this class for unit testing Lens Jersey resources
 */
@Slf4j
public abstract class LensJerseyTest extends JerseyTest {

  private int port = -1;

  private final LogSegregationContext logSegregationContext = new MappedDiagnosticLogSegregationContext();

  protected URI getUri() {
    return UriBuilder.fromUri("http://localhost/").port(getTestPort()).build();
  }

  private boolean isPortAlreadyFound() {
    return port != -1;
  }

  public void setUp() throws Exception {
    log.info("setUp in class: {}", this.getClass().getCanonicalName());
    super.setUp();
  }
  public void tearDown() throws Exception {
    log.info("tearDown in class: {}", this.getClass().getCanonicalName());
    super.tearDown();
  }
  protected int getTestPort() {
    if (!isPortAlreadyFound()) {
      return port;
    }
    ServerSocket socket = null;
    try {
      socket = new ServerSocket(0);
      setPort(socket.getLocalPort());
    } catch (IOException e) {
      log.info("Exception occured while creating socket. Use a default port number {}", port);
    } finally {
      try {
        if (socket != null) {
          socket.close();
        }
      } catch (IOException e) {
        log.info("Exception occured while closing the socket", e);
      }
    }
    return port;
  }

  public void setPort(int localPort) {
    port = localPort;
  }

  @Override
  protected URI getBaseUri() {
    return UriBuilder.fromUri(getUri()).path("lens-server").build();
  }

  public HiveConf getServerConf() {
    return LensServerConf.getHiveConf();
  }

  /**
   * Start all.
   *
   * @throws Exception the exception
   */
  @BeforeSuite
  public void startAll() throws Exception {
    log.info("Before suite");
    System.setProperty("lens.log.dir", "target/");
    System.setProperty(LensConfConstants.CONFIG_LOCATION, "target/test-classes/");
    TestRemoteHiveDriver.createHS2Service();
    System.out.println("Remote hive server started!");
    HiveConf hiveConf = new HiveConf();
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_ASYNC_EXEC_THREADS, 5);
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_CONNECTION_RETRY_LIMIT, 3);
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_RETRY_LIMIT, 3);

    createTestDatabaseResources(new String[]{DB_WITH_JARS, DB_WITH_JARS_2},
      hiveConf);

    LensServices.get().init(getServerConf());
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
   * @throws Exception the exception
   */
  @AfterSuite
  public void stopAll() throws Exception {
    log.info("After suite");
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
    MetricsService metrics = LensServices.get().getService(MetricsService.NAME);
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
    HiveConf h = getServerConf();
    restartLensServer(h);
  }

  /**
   * Restart lens server.
   *
   * @param conf the conf
   */
  public void restartLensServer(HiveConf conf) {
    LensServices.get().stop();
    LensMetricsUtil.clearRegistry();
    System.out.println("Lens services stopped!");
    LensServices.setInstance(new LensServices(LensServices.LENS_SERVICES_NAME, this.logSegregationContext));
    LensServices.get().init(conf);
    LensServices.get().start();
    System.out.println("Lens services restarted!");
  }
  public static void waitForPurge(int allowUnpurgable,
    ConcurrentLinkedQueue<QueryExecutionServiceImpl.FinishedQuery> finishedQueries) throws InterruptedException {
    List<QueryExecutionServiceImpl.FinishedQuery> unPurgable = Lists.newArrayList();
    for (QueryExecutionServiceImpl.FinishedQuery finishedQuery : finishedQueries) {
      if (!finishedQuery.canBePurged()) {
        unPurgable.add(finishedQuery);
      }
    }
    if (unPurgable.size() > allowUnpurgable) {
      throw new RuntimeException("finished queries can't be purged: " + unPurgable);
    }
    while (finishedQueries.size() > allowUnpurgable) {
      Thread.sleep(5000);
    }
  }
}

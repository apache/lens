package com.inmobi.grill.server;

/*
 * #%L
 * Grill Server
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import static org.testng.Assert.*;

import java.net.URI;

import javax.ws.rs.core.UriBuilder;

import org.apache.hadoop.hive.conf.HiveConf;
import org.glassfish.jersey.test.JerseyTest;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import com.inmobi.grill.driver.hive.TestRemoteHiveDriver;
import com.inmobi.grill.server.api.metrics.MetricsService;

public abstract class GrillJerseyTest extends JerseyTest {

  protected URI getUri() {
    return UriBuilder.fromUri("http://localhost/").port(getTestPort()).build();
  }

  protected abstract int getTestPort();

  @Override
  protected URI getBaseUri() {
    return UriBuilder.fromUri(getUri()).path("grill-server").build();
  }

  @BeforeSuite
  public void startAll() throws Exception {
    TestRemoteHiveDriver.createHS2Service();
    GrillServices.get().init(new HiveConf());
    GrillServices.get().start();
  }

  @AfterSuite
  public void stopAll() throws Exception {
    verifyMetrics();
    GrillServices.get().stop();
    TestRemoteHiveDriver.stopHS2Service();
  }

<<<<<<< HEAD
  protected void verifyMetrics() {
    // print final metrics
    System.out.println("Final report");
    MetricsService metrics = ((MetricsService) GrillServices.get().getService(MetricsService.NAME));
    metrics.publishReport();
    
    // validate http error count
    long httpClientErrors = metrics.getCounter(GrillRequestListener.class,
        GrillRequestListener.HTTP_CLIENT_ERROR);
    long httpServerErrors = metrics.getCounter(GrillRequestListener.class,
        GrillRequestListener.HTTP_SERVER_ERROR);
    long httpOtherErrors = metrics.getCounter(GrillRequestListener.class,
        GrillRequestListener.HTTP_UNKOWN_ERROR);
    long httpErrors = metrics.getCounter(GrillRequestListener.class, 
        GrillRequestListener.HTTP_ERROR);
    assertEquals(httpClientErrors + httpServerErrors + httpOtherErrors, httpErrors, 
        "Server + Client error should equal total errors");
    
    // validate http metrics
    long httpReqStarted = metrics.getCounter(GrillRequestListener.class, 
        GrillRequestListener.HTTP_REQUESTS_STARTED);
    long httpReqFinished = metrics.getCounter(GrillRequestListener.class,
        GrillRequestListener.HTTP_REQUESTS_FINISHED);
    assertEquals(httpReqStarted, httpReqFinished, 
        "Total requests started should equal total requests finished");
    
    // validate queries in the final state
    long queriesSuccessful = metrics.getTotalSuccessfulQueries();
    long queriesFailed = metrics.getTotalFailedQueries();
    long queriesCancelled = metrics.getTotalCancelledQueries();
    long queriesFinished = metrics.getTotalFinishedQueries();
    
    assertEquals(queriesFinished, queriesSuccessful + queriesFailed + queriesCancelled,
        "Total finished queries should be sum of successful, failed and cancelled queries");
    
  }

=======
  public void restartGrillServer() {
    GrillServices.get().stop();
    GrillServices.setInstance(new GrillServices(GrillServices.GRILL_SERVICES_NAME));
    GrillServices.get().init(new HiveConf());
    GrillServices.get().start();
  }
>>>>>>> 572cd17... Add persistence to query service
}

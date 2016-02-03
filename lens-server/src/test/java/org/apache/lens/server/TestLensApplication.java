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

import java.util.List;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.lens.server.metrics.MetricsServiceImpl;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.codahale.metrics.ScheduledReporter;

/**
 * The Class TestLensApplication.
 */
@Test(alwaysRun = true, groups = "unit-test")
public class TestLensApplication extends LensAllApplicationJerseyTest {

  /**
   * Setup.
   *
   * @throws Exception the exception
   */
  @BeforeTest
  public void setup() throws Exception {
    super.setUp();
  }

  /**
   * Test ws resources loaded.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testWSResourcesLoaded() throws InterruptedException {
    final WebTarget target = target().path("test");
    final Response response = target.request().get();
    Assert.assertEquals(response.getStatus(), 200);
    Assert.assertEquals(response.readEntity(String.class), "OK");
  }

  /**
   * Test log resources loaded.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testLogResourceLoaded() throws InterruptedException {
    final WebTarget target = target().path("logs");
    final Response response = target.request().get();
    Assert.assertEquals(response.getStatus(), 200);
    Assert.assertEquals(response.readEntity(String.class), "Logs resource is up!");
  }

  @Test
  public void testMetricService() {
    MetricsService metrics = LensServices.get().getService(MetricsService.NAME);
    List<ScheduledReporter> reporters = ((MetricsServiceImpl) metrics).getReporters();

    assertEquals(reporters.size(), 1, "mismatch in the number of reporters");
  }
}

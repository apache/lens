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
package org.apache.lens.server.healthcheck;

import static org.testng.Assert.*;

import org.apache.lens.server.EventServiceImpl;
import org.apache.lens.server.LensAllApplicationJerseyTest;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensService;
import org.apache.lens.server.api.health.HealthStatus;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.metastore.CubeMetastoreServiceImpl;
import org.apache.lens.server.metrics.MetricsServiceImpl;
import org.apache.lens.server.quota.QuotaServiceImpl;
import org.apache.lens.server.scheduler.SchedulerServiceImpl;
import org.apache.lens.server.session.HiveSessionService;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.codahale.metrics.health.HealthCheck;

@Test(groups = "unit-test")
public class TestHealthChecks extends LensAllApplicationJerseyTest {
  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }

  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testCubeMetastoreServiceHealth() throws Exception {
    checkHealth(CubeMetastoreServiceImpl.NAME);
  }

  @Test
  public void testEventServiceHealth() throws Exception {
    checkHealth(EventServiceImpl.NAME);
  }

  @Test
  public void testHiveSessionServiceHealth() throws Exception {
    checkHealth(HiveSessionService.NAME);
  }

  @Test
  public void testMetricsServiceHealth() throws Exception {
    checkHealth(MetricsServiceImpl.NAME);
  }

  @Test
  public void testQueryExecutionServiceHealth() throws Exception {
    checkHealth(QueryExecutionService.NAME);
  }

  @Test
  public void testQuerySchedulerServiceHealth() throws Exception {
    checkHealth(SchedulerServiceImpl.NAME);
  }

  @Test
  public void testQuotaServiceHealth() throws Exception {
    checkHealth(QuotaServiceImpl.NAME);
  }

  /**
   * Utility method to check health for provided service name.
   *
   * @param serviceName
   * @throws Exception
   */
  private void checkHealth(String serviceName) throws Exception {
    /** Test via LensServiceHealthCheck **/
    new LensServiceHealthCheck(serviceName).check().equals(HealthCheck.Result.healthy());

    /** Also check directly via service **/
    HealthStatus status;
    LensService service = LensServices.get().getService(serviceName);
    status = service.getHealthStatus();
    assertTrue(status.isHealthy(), serviceName + " Service should had been healthy.");
  }
}

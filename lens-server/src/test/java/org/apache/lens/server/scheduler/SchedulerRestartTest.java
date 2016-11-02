/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.scheduler;

import static org.apache.lens.server.scheduler.util.SchedulerTestUtils.getTestJob;
import static org.apache.lens.server.scheduler.util.SchedulerTestUtils.setupQueryService;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

import java.util.ArrayList;
import java.util.List;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.api.scheduler.*;
import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.metrics.LensMetricsUtil;
import org.apache.lens.server.api.scheduler.SchedulerService;
import org.apache.lens.server.model.LogSegregationContext;
import org.apache.lens.server.model.MappedDiagnosticLogSegregationContext;
import org.apache.lens.server.util.UtilityMethods;

import org.joda.time.DateTime;
import org.powermock.api.mockito.PowerMockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "scheduler-restart", dependsOnGroups = "duplicate-query")
public class SchedulerRestartTest {

  SchedulerServiceImpl scheduler;

  @BeforeClass
  public void setUp() throws Exception {
    System.setProperty(LensConfConstants.CONFIG_LOCATION, "target/test-classes/");
    LensServices.get().init(LensServerConf.getHiveConf());
    scheduler = LensServices.get().getService(SchedulerService.NAME);
    setupQueryService(scheduler);
    LensServices.get().start();
  }

  @AfterClass
  public void tearDown() throws Exception {
  }

  @Test
  public void testRestart() throws Exception {
    long start = new DateTime().getMillis() - 3600000 * 24;
    // One day
    long end = start + 2 * 3600000 * 24;
    XJob testJob = getTestJob("0 0 0 * * ?", "test query", start, end);
    long currentTime = System.currentTimeMillis();
    LensSessionHandle sessionHandle = scheduler.openSessionAsUser("admin");
    SchedulerJobHandle jobHandle = scheduler.submitAndScheduleJob(sessionHandle, testJob);
    Thread.sleep(5000);
    List<SchedulerJobInstanceInfo> instanceInfoList = scheduler.getJobInstances(jobHandle, 10L);
    Assert.assertEquals(instanceInfoList.size(), 1);

    PowerMockito.when(
      scheduler.getQueryService().estimate(anyString(), any(LensSessionHandle.class), anyString(), any(LensConf.class)))
      .thenThrow(new LensException(LensCubeErrorCode.NO_CANDIDATE_FACT_AVAILABLE.getLensErrorInfo(),
        "some name " + " does not have any facts"));
    // Store new instance
    SchedulerJobInstanceHandle instanceHandle = UtilityMethods.generateSchedulerJobInstanceHandle();
    SchedulerJobInstanceInfo instance = new SchedulerJobInstanceInfo(instanceHandle, jobHandle, currentTime,
      new ArrayList<SchedulerJobInstanceRun>());
    SchedulerDAO store = scheduler.getSchedulerDAO();
    // Manually Store instance
    store.storeJobInstance(instance);
    SchedulerJobInstanceRun run = new SchedulerJobInstanceRun(instanceHandle, instance.getInstanceRunList().size() + 1,
      null, currentTime, 0, "N/A", null, SchedulerJobInstanceState.WAITING);
    instance.getInstanceRunList().add(run);
    store.storeJobInstanceRun(run);

    // Restart Lens Services
    LensServices.get().stop();
    LensMetricsUtil.clearRegistry();
    LogSegregationContext logSegregationContext = new MappedDiagnosticLogSegregationContext();
    LensServices.setInstance(new LensServices(LensServices.LENS_SERVICES_NAME, logSegregationContext));
    LensServices.get().init(LensServerConf.getHiveConf());
    scheduler = LensServices.get().getService(SchedulerService.NAME);
    setupQueryService(scheduler);
    PowerMockito.when(
      scheduler.getQueryService().estimate(anyString(), any(LensSessionHandle.class), anyString(), any(LensConf.class)))
      .thenThrow(new LensException(LensCubeErrorCode.NO_CANDIDATE_FACT_AVAILABLE.getLensErrorInfo(),
        "some name " + " does not have any facts"));
    LensQuery mockedQuery = PowerMockito.mock(LensQuery.class);
    QueryStatus mockStatus = PowerMockito.mock(QueryStatus.class);
    PowerMockito.when(mockStatus.getStatus()).thenReturn(QueryStatus.Status.SUCCESSFUL);
    PowerMockito.when(mockedQuery.getStatus()).thenReturn(mockStatus);
    PowerMockito.when(mockedQuery.getStatus().getStatus()).thenReturn(QueryStatus.Status.SUCCESSFUL);
    PowerMockito.when(mockedQuery.getResultSetPath()).thenReturn("/tmp/path");
    PowerMockito.when(scheduler.getQueryService().getQuery(any(LensSessionHandle.class), any(QueryHandle.class)))
      .thenReturn(mockedQuery);
    LensServices.get().start();

    // Sleep for some time to let the event get processed
    Thread.sleep(5000);
    // This should have 2 instance Run
    SchedulerJobInstanceInfo storedInfo = scheduler.getInstanceDetails(instanceHandle);
    Assert.assertEquals(storedInfo.getInstanceRunList().size(), 2);
    // The first instance  will be killed state after restart.
    SchedulerJobInstanceInfo previousInstanceInfo = scheduler.getInstanceDetails(instanceInfoList.get(0).getId());
    //Because we mocked the query, It should not rerun.
    Assert.assertEquals(previousInstanceInfo.getInstanceRunList().size(), 1);
    Assert.assertEquals(previousInstanceInfo.getInstanceRunList().get(0).getResultPath(), "/tmp/path");
    Assert.assertEquals(previousInstanceInfo.getInstanceRunList().get(0).getInstanceState(),
      SchedulerJobInstanceState.SUCCEEDED);
    scheduler.expireJob(sessionHandle, jobHandle);
    scheduler.closeSession(sessionHandle);
  }
}

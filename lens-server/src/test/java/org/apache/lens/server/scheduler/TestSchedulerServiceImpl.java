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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

import java.util.List;
import java.util.UUID;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.api.scheduler.*;
import org.apache.lens.server.EventServiceImpl;
import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.QueryEnded;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.api.scheduler.SchedulerService;

import org.apache.hadoop.conf.Configuration;

import org.powermock.api.mockito.PowerMockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Test(groups = "unit-test")
public class TestSchedulerServiceImpl {

  SchedulerServiceImpl scheduler;
  EventServiceImpl eventService;
  String user = "someuser";
  String queryString = "select ID from test_table";

  @BeforeMethod
  public void setup() throws Exception {
    System.setProperty(LensConfConstants.CONFIG_LOCATION, "target/test-classes/");
  }

  private void setupQueryService() throws Exception {
    QueryExecutionService queryExecutionService = PowerMockito.mock(QueryExecutionService.class);
    scheduler.setQueryService(queryExecutionService);
    PowerMockito.when(
      scheduler.getQueryService().estimate(anyString(), any(LensSessionHandle.class), anyString(), any(LensConf.class)))
      .thenReturn(null);
    PowerMockito.when(scheduler.getQueryService()
      .executeAsync(any(LensSessionHandle.class), anyString(), any(LensConf.class), anyString()))
      .thenReturn(new QueryHandle(UUID.randomUUID()));
    PowerMockito.when(scheduler.getQueryService().cancelQuery(any(LensSessionHandle.class), any(QueryHandle.class)))
      .thenReturn(true);
    scheduler.getSchedulerEventListener().setQueryService(queryExecutionService);
  }

  private QueryEnded mockQueryEnded(SchedulerJobInstanceHandle instanceHandle, QueryStatus.Status status) {
    QueryContext mockContext = PowerMockito.mock(QueryContext.class);
    PowerMockito.when(mockContext.getResultSetPath()).thenReturn("/tmp/query1/result");
    Configuration conf = new Configuration();
    // set the instance handle
    conf.set("job_instance_key", instanceHandle.getHandleIdString());
    PowerMockito.when(mockContext.getConf()).thenReturn(conf);
    // Get the queryHandle.
    PowerMockito.when(mockContext.getQueryHandle()).thenReturn(new QueryHandle(UUID.randomUUID()));
    QueryEnded queryEnded = PowerMockito.mock(QueryEnded.class);
    PowerMockito.when(queryEnded.getQueryContext()).thenReturn(mockContext);
    PowerMockito.when(queryEnded.getCurrentValue()).thenReturn(status);
    return queryEnded;
  }

  @Test(priority = 1)
  public void testScheduler() throws Exception {
    LensServices.get().init(LensServerConf.getHiveConf());
    LensServices.get().start();
    scheduler = LensServices.get().getService(SchedulerService.NAME);
    eventService = LensServices.get().getService(EventServiceImpl.NAME);
    setupQueryService();
    LensSessionHandle sessionHandle = scheduler.openSessionAsUser(user);
    long currentTime = System.currentTimeMillis();
    XJob job = getTestJob("0/5 * * * * ?", queryString, currentTime, currentTime + 15000);
    SchedulerJobHandle jobHandle = scheduler.submitAndScheduleJob(sessionHandle, job);
    Assert.assertNotNull(jobHandle);
    Assert.assertEquals(scheduler.getSchedulerDAO().getJobState(jobHandle), SchedulerJobState.SCHEDULED);
    // Wait for job to finish
    Thread.sleep(30000);
    List<SchedulerJobInstanceInfo> instanceHandleList = scheduler.getSchedulerDAO().getJobInstances(jobHandle);
    Assert.assertEquals(instanceHandleList.size() >= 3, true);
    Assert.assertEquals(scheduler.getSchedulerDAO().getJobState(jobHandle), SchedulerJobState.EXPIRED);
    // SuccessFul query
    eventService.notifyEvent(mockQueryEnded(instanceHandleList.get(0).getId(), QueryStatus.Status.SUCCESSFUL));
    // Wait, for event to get processed
    Thread.sleep(2000);
    // Check the instance value
    SchedulerJobInstanceInfo info = scheduler.getSchedulerDAO()
      .getSchedulerJobInstanceInfo(instanceHandleList.get(0).getId());
    Assert.assertEquals(info.getInstanceRunList().size(), 1);
    Assert.assertEquals(info.getInstanceRunList().get(0).getResultPath(), "/tmp/query1/result");
    Assert.assertEquals(info.getInstanceRunList().get(0).getInstanceState(), SchedulerJobInstanceState.SUCCEEDED);
    scheduler.closeSession(sessionHandle);
  }

  @Test(priority = 2)
  public void testSuspendResume() throws Exception {
    long currentTime = System.currentTimeMillis();
    XJob job = getTestJob("0/10 * * * * ?", queryString, currentTime, currentTime + 180000);
    LensSessionHandle sessionHandle = scheduler.openSessionAsUser(user);
    SchedulerJobHandle jobHandle = scheduler.submitAndScheduleJob(sessionHandle, job);
    Assert.assertNotNull(jobHandle);
    scheduler.suspendJob(sessionHandle, jobHandle);
    Assert.assertEquals(scheduler.getSchedulerDAO().getJobState(jobHandle), SchedulerJobState.SUSPENDED);
    scheduler.resumeJob(sessionHandle, jobHandle);
    Assert.assertEquals(scheduler.getSchedulerDAO().getJobState(jobHandle), SchedulerJobState.SCHEDULED);
    Thread.sleep(10000);
    scheduler.expireJob(sessionHandle, jobHandle);
    Assert.assertEquals(scheduler.getSchedulerDAO().getJobState(jobHandle), SchedulerJobState.EXPIRED);
    scheduler.closeSession(sessionHandle);
  }

  @Test(priority = 2)
  public void testRerunInstance() throws Exception {
    long currentTime = System.currentTimeMillis();

    XJob job = getTestJob("0/10 * * * * ?", queryString, currentTime, currentTime + 180000);
    LensSessionHandle sessionHandle = scheduler.openSessionAsUser(user);
    SchedulerJobHandle jobHandle = scheduler.submitAndScheduleJob(sessionHandle, job);
    // Wait for some instances.
    Thread.sleep(15000);
    List<SchedulerJobInstanceInfo> instanceHandleList = scheduler.getSchedulerDAO().getJobInstances(jobHandle);
    // Mark fail
    eventService.notifyEvent(mockQueryEnded(instanceHandleList.get(0).getId(), QueryStatus.Status.FAILED));
    Thread.sleep(1000);
    SchedulerJobInstanceInfo info = scheduler.getSchedulerDAO()
      .getSchedulerJobInstanceInfo(instanceHandleList.get(0).getId());
    // First run
    Assert.assertEquals(info.getInstanceRunList().size(), 1);
    Assert.assertEquals(info.getInstanceRunList().get(0).getInstanceState(), SchedulerJobInstanceState.FAILED);

    // Rerun
    scheduler.rerunInstance(sessionHandle, instanceHandleList.get(0).getId());
    Thread.sleep(5000);
    eventService.notifyEvent(mockQueryEnded(instanceHandleList.get(0).getId(), QueryStatus.Status.SUCCESSFUL));
    Thread.sleep(1000);
    info = scheduler.getSchedulerDAO().getSchedulerJobInstanceInfo(instanceHandleList.get(0).getId());
    // There should be 2 reruns.
    Assert.assertEquals(info.getInstanceRunList().size(), 2);
    Assert.assertEquals(info.getInstanceRunList().get(1).getResultPath(), "/tmp/query1/result");
    Assert.assertEquals(info.getInstanceRunList().get(1).getInstanceState(), SchedulerJobInstanceState.SUCCEEDED);
    scheduler.expireJob(sessionHandle, jobHandle);
    Assert.assertEquals(scheduler.getSchedulerDAO().getJobState(jobHandle), SchedulerJobState.EXPIRED);
    scheduler.closeSession(sessionHandle);
  }

  @Test(priority = 2)
  public void testKillRunningInstance() throws Exception {
    long currentTime = System.currentTimeMillis();
    XJob job = getTestJob("0/5 * * * * ?", queryString, currentTime, currentTime + 180000);
    LensSessionHandle sessionHandle = scheduler.openSessionAsUser(user);
    SchedulerJobHandle jobHandle = scheduler.submitAndScheduleJob(sessionHandle, job);
    // Let it run
    Thread.sleep(6000);
    List<SchedulerJobInstanceInfo> instanceHandleList = scheduler.getSchedulerDAO().getJobInstances(jobHandle);
    Assert.assertTrue(scheduler.killInstance(sessionHandle, instanceHandleList.get(0).getId()));
    Thread.sleep(2000);
    SchedulerJobInstanceInfo info = scheduler.getSchedulerDAO()
      .getSchedulerJobInstanceInfo(instanceHandleList.get(0).getId());
    Assert.assertEquals(info.getInstanceRunList().size(), 1);
    Assert.assertEquals(info.getInstanceRunList().get(0).getInstanceState(), SchedulerJobInstanceState.RUNNING);
    // Query End event
    eventService.notifyEvent(mockQueryEnded(instanceHandleList.get(0).getId(), QueryStatus.Status.CANCELED));
    Thread.sleep(2000);
    info = scheduler.getSchedulerDAO().getSchedulerJobInstanceInfo(instanceHandleList.get(0).getId());
    Assert.assertEquals(info.getInstanceRunList().get(0).getInstanceState(), SchedulerJobInstanceState.KILLED);
    scheduler.expireJob(sessionHandle, jobHandle);
    Assert.assertEquals(scheduler.getSchedulerDAO().getJobState(jobHandle), SchedulerJobState.EXPIRED);
    scheduler.closeSession(sessionHandle);
  }
}

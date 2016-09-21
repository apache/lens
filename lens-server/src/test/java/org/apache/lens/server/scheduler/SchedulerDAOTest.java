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

import java.util.*;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.scheduler.*;
import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.util.UtilityMethods;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.hadoop.conf.Configuration;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Test(groups = "unit-test")
public class SchedulerDAOTest {

  SchedulerDAO schedulerDAO;
  Map<SchedulerJobInstanceHandle, SchedulerJobInstanceInfo> instances = new HashMap<>();
  SchedulerJobHandle jobHandle = null;

  @BeforeClass
  public void setup() throws Exception {
    System.setProperty(LensConfConstants.CONFIG_LOCATION, "target/test-classes/");
    Configuration conf = LensServerConf.getHiveConf();
    QueryRunner runner = new QueryRunner(UtilityMethods.getDataSourceFromConfForScheduler(conf));
    // Cleanup all tables
    runner.update("DROP TABLE IF EXISTS job_table");
    runner.update("DROP TABLE IF EXISTS job_instance_table");
    this.schedulerDAO = new SchedulerDAO(conf);
  }

  private XTrigger getTestTrigger() {
    XTrigger trigger = new XTrigger();
    XFrequency frequency = new XFrequency();
    frequency.setCronExpression("0 0 12 * * ?");
    frequency.setTimezone("UTC");
    trigger.setFrequency(frequency);
    return trigger;
  }

  private XExecution getTestExecution() {
    XExecution execution = new XExecution();
    XJobQuery query = new XJobQuery();
    query.setQuery("select * from test_table");
    execution.setQuery(query);
    XSessionType sessionType = new XSessionType();
    sessionType.setDb("test");
    execution.setSession(sessionType);
    return execution;
  }

  private XJob getTestJob() throws DatatypeConfigurationException {
    XJob job = new XJob();
    job.setTrigger(getTestTrigger());
    job.setName("Test lens Job");
    GregorianCalendar startTime = new GregorianCalendar();
    startTime.setTimeInMillis(System.currentTimeMillis());
    XMLGregorianCalendar start = DatatypeFactory.newInstance().newXMLGregorianCalendar(startTime);

    GregorianCalendar endTime = new GregorianCalendar();
    endTime.setTimeInMillis(System.currentTimeMillis());
    XMLGregorianCalendar end = DatatypeFactory.newInstance().newXMLGregorianCalendar(endTime);

    job.setStartTime(start);
    job.setEndTime(end);
    job.setExecution(getTestExecution());
    return job;
  }

  @Test(priority = 1)
  public void testStoreJob() throws Exception {
    XJob job = getTestJob();
    long currentTime = System.currentTimeMillis();
    jobHandle = new SchedulerJobHandle(UUID.randomUUID());
    SchedulerJobInfo info = new SchedulerJobInfo(jobHandle, job, "lens", SchedulerJobState.NEW, currentTime,
      currentTime);
    // Store the job
    schedulerDAO.storeJob(info);
    // Retrive the stored job
    XJob outJob = schedulerDAO.getJob(info.getId());
    Assert.assertEquals(job, outJob);
  }

  @Test(priority = 2)
  public void testStoreInstance() throws Exception {
    long currentTime = System.currentTimeMillis();
    SchedulerJobInstanceHandle instanceHandle = new SchedulerJobInstanceHandle(UUID.randomUUID());
    SchedulerJobInstanceInfo firstInstance = new SchedulerJobInstanceInfo(instanceHandle, jobHandle, currentTime,
      new ArrayList<SchedulerJobInstanceRun>());
    SchedulerJobInstanceRun run1 = new SchedulerJobInstanceRun(instanceHandle, 1,
      new LensSessionHandle(UUID.randomUUID(), UUID.randomUUID()), currentTime, currentTime, "/tmp/",
      QueryHandle.fromString(UUID.randomUUID().toString()), SchedulerJobInstanceState.WAITING);
    instances.put(firstInstance.getId(), firstInstance);
    schedulerDAO.storeJobInstance(firstInstance);
    schedulerDAO.storeJobInstanceRun(run1);
    // Put run in the instance
    firstInstance.getInstanceRunList().add(run1);

    currentTime = System.currentTimeMillis();
    instanceHandle = new SchedulerJobInstanceHandle(UUID.randomUUID());
    SchedulerJobInstanceInfo secondInstance = new SchedulerJobInstanceInfo(instanceHandle, jobHandle, currentTime,
      new ArrayList<SchedulerJobInstanceRun>());
    SchedulerJobInstanceRun run2 = new SchedulerJobInstanceRun(instanceHandle, 1,
      new LensSessionHandle(UUID.randomUUID(), UUID.randomUUID()), currentTime, currentTime, "/tmp/",
      QueryHandle.fromString(UUID.randomUUID().toString()), SchedulerJobInstanceState.WAITING);
    instances.put(secondInstance.getId(), secondInstance);
    schedulerDAO.storeJobInstance(secondInstance);
    schedulerDAO.storeJobInstanceRun(run2);
    secondInstance.getInstanceRunList().add(run2);

    List<SchedulerJobInstanceInfo> handleList = schedulerDAO.getJobInstances(jobHandle);
    // Size should be 2
    Assert.assertEquals(handleList.size(), 2);
    // Get the definition of instance from the store.
    SchedulerJobInstanceInfo instance1 = handleList.get(0);
    Assert.assertEquals(instances.get(handleList.get(0).getId()), instance1);

    SchedulerJobInstanceInfo instance2 = handleList.get(1);
    Assert.assertEquals(instances.get(handleList.get(1).getId()), instance2);
  }

  @Test(priority = 2)
  public void testUpdateJob() throws Exception {
    // Get all the stored jobs.
    // update one and check if it successful.
    SchedulerJobInfo jobInfo = schedulerDAO.getSchedulerJobInfo(jobHandle);
    XJob newJob = getTestJob();
    jobInfo.setJob(newJob);
    schedulerDAO.updateJob(jobInfo);

    XJob storedJob = schedulerDAO.getJob(jobInfo.getId());
    Assert.assertEquals(storedJob, newJob);

    // Change SchedulerJobInstanceState
    jobInfo.setJobState(jobInfo.getJobState().nextTransition(SchedulerJobEvent.ON_SCHEDULE));
    schedulerDAO.updateJobStatus(jobInfo);
    Assert.assertEquals(schedulerDAO.getJobState(jobInfo.getId()), SchedulerJobState.SCHEDULED);
  }

  @Test(priority = 3)
  public void testUpdateJobInstance() {
    SchedulerJobInstanceHandle handle = instances.keySet().iterator().next();
    SchedulerJobInstanceInfo info = instances.get(handle);
    SchedulerJobInstanceRun run = info.getInstanceRunList().get(0);
    run.setInstanceState(SchedulerJobInstanceState.LAUNCHED);
    schedulerDAO.updateJobInstanceRun(run);
    // Get the instance
    Assert.assertEquals(schedulerDAO.getSchedulerJobInstanceInfo(handle), info);
  }

  @Test(priority = 3)
  public void testSearchStoreJob() throws Exception {
    // Store more jobs with the one user and search
    XJob job = getTestJob();
    long currentTime = System.currentTimeMillis();
    SchedulerJobInfo info = new SchedulerJobInfo(SchedulerJobHandle.fromString(UUID.randomUUID().toString()), job,
      "lens", SchedulerJobState.NEW, currentTime, currentTime);
    // Store the job
    schedulerDAO.storeJob(info);
    info = new SchedulerJobInfo(SchedulerJobHandle.fromString(UUID.randomUUID().toString()), job, "lens",
      SchedulerJobState.NEW, currentTime, currentTime);
    schedulerDAO.storeJob(info);
    // There should be 3 jobs till now.
    Assert.assertEquals(schedulerDAO.getJobs("lens", null, null, SchedulerJobState.values()).size(), 3);
    Assert.assertEquals(schedulerDAO.getJobs("lens", 1L, System.currentTimeMillis(), SchedulerJobState.NEW).size(), 2);
    Assert.assertEquals(schedulerDAO.getJobs("Alice", null, null, SchedulerJobState.NEW).size(), 0);
  }
}

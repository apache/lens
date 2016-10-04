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

package org.apache.lens.regression.client;

import java.lang.reflect.Method;
import java.util.Calendar;
import java.util.List;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.scheduler.*;
import org.apache.lens.regression.core.constants.QueryInventory;
import org.apache.lens.regression.core.helpers.ServiceManagerHelper;
import org.apache.lens.regression.core.testHelper.BaseTestClass;
import org.apache.lens.regression.util.Util;

import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;



public class ITScheduleQueryTests extends BaseTestClass {

  WebTarget servLens;
  private String sessionHandleString;

  private static Logger logger = Logger.getLogger(ITScheduleQueryTests.class);
  private static String format = "yyyy-MM-dd HH:mm:ss";
  private static String currentDate = Util.getCurrentDate(format);

  @BeforeClass(alwaysRun = true)
  public void initialize() throws Exception {
    servLens = ServiceManagerHelper.init();
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp(Method method) throws Exception {
    logger.info("Test Name: " + method.getName());
    logger.info("Creating a new Session");
    sessionHandleString = sHelper.openSession(lens.getCurrentDB());
  }

  @AfterMethod(alwaysRun = true)
  public void closeSession() throws Exception {
    logger.info("Closing Session");
    if (sessionHandleString != null){
      sHelper.closeSession();
    }
  }


  @Test
  public void submitJob() throws Exception {
    String endDate = Util.modifyDate(currentDate, format, Calendar.DATE, 4);
    XJob xJob = scheduleHelper.getXJob("job-submit", QueryInventory.QUERY, null, currentDate, endDate,
        XFrequencyEnum.DAILY);
    String jobHandle = scheduleHelper.submitJob(xJob, sessionHandleString);
    Assert.assertNotNull(jobHandle);
    Assert.assertEquals(scheduleHelper.getJobStatus(jobHandle), SchedulerJobState.NEW);
  }

  @Test
  public void submitNScheduleQuery() throws Exception {

    String startDate = Util.modifyDate(currentDate, format, Calendar.DATE, -1);
    String endDate = Util.modifyDate(currentDate, format, Calendar.DATE, 3);
    XJob xJob = scheduleHelper.getXJob("job-submit-schedule", QueryInventory.JDBC_CUBE_QUERY, null, startDate,
        endDate, XFrequencyEnum.DAILY);
    String jobHandle = scheduleHelper.submitNScheduleJob(xJob, sessionHandleString);
    Assert.assertNotNull(jobHandle);
    Assert.assertEquals(scheduleHelper.getJobStatus(jobHandle), SchedulerJobState.SCHEDULED);

    SchedulerJobInfo jobInfo = scheduleHelper.getJobDetails(jobHandle, sessionHandleString);
    Assert.assertNotNull(jobInfo);
    Assert.assertEquals(jobInfo.getJob().getName(), "job-submit-schedule");
  }

  //submit and schedule and also get job definition
  @Test
  public void submitNScheduleQueryCronExp() throws Exception {

    String endDate = Util.modifyDate(currentDate, format, Calendar.DATE, 1);
    XJob xJob = scheduleHelper.getXJob("job-submit-schedule-cronExp", QueryInventory.QUERY, null, currentDate,
        endDate, "0/30 * * * * ?");
    String jobHandle = scheduleHelper.submitNScheduleJob(xJob, sessionHandleString);
    Assert.assertNotNull(jobHandle);

    XJob job = scheduleHelper.getJobDefinition(jobHandle, sessionHandleString, MediaType.APPLICATION_XML_TYPE,
        MediaType.APPLICATION_XML);
    Assert.assertNotNull(job);
    Assert.assertEquals(job.getName(), "job-submit-schedule-cronExp");
  }


  @Test
  public void testDeleteJob() throws Exception {

    String endDate = Util.modifyDate(currentDate, format, Calendar.DATE, 1);
    XJob xJob = scheduleHelper.getXJob("job-delete", QueryInventory.QUERY, null, currentDate, endDate,
         "0/30 * * * * ?");

    //delete in submit state
    String jobHandle = scheduleHelper.submitJob(xJob, sessionHandleString);
    APIResult res = scheduleHelper.deleteJob(jobHandle, sessionHandleString);
//    Assert.assertEquals(res.getStatus(), APIResult.Status.SUCCEEDED);
    Assert.assertEquals(scheduleHelper.getJobStatus(jobHandle), SchedulerJobState.DELETED);

    //delete in scheduled state
    jobHandle = scheduleHelper.submitNScheduleJob(xJob, sessionHandleString);
    res = scheduleHelper.deleteJob(jobHandle, sessionHandleString);
//    Assert.assertEquals(res.getStatus(), APIResult.Status.SUCCEEDED);
    Assert.assertEquals(scheduleHelper.getJobStatus(jobHandle), SchedulerJobState.DELETED);

    //delete in suspended state
    jobHandle = scheduleHelper.submitNScheduleJob(xJob, sessionHandleString);
    scheduleHelper.updateJob(jobHandle, "SUSPEND", sessionHandleString);
    Assert.assertEquals(scheduleHelper.getJobStatus(jobHandle), SchedulerJobState.SUSPENDED);
    res = scheduleHelper.deleteJob(jobHandle, sessionHandleString);
//    Assert.assertEquals(res.getStatus(), APIResult.Status.SUCCEEDED);
    Assert.assertEquals(scheduleHelper.getJobStatus(jobHandle), SchedulerJobState.DELETED);

    //delete in expired state
    jobHandle = scheduleHelper.submitNScheduleJob(xJob, sessionHandleString);
    scheduleHelper.updateJob(jobHandle, "EXPIRE", sessionHandleString);
    Assert.assertEquals(scheduleHelper.getJobStatus(jobHandle), SchedulerJobState.EXPIRED);
    res = scheduleHelper.deleteJob(jobHandle, sessionHandleString);
//    Assert.assertEquals(res.getStatus(), APIResult.Status.SUCCEEDED);
    Assert.assertEquals(scheduleHelper.getJobStatus(jobHandle), SchedulerJobState.DELETED);
  }

  @Test
  public void testUpdateJob() throws Exception {

    String endDate = Util.modifyDate(currentDate, format, Calendar.DATE, 4);
    XJob job = scheduleHelper.getXJob("job-update", QueryInventory.QUERY, null, currentDate, endDate,
        XFrequencyEnum.WEEKLY);
    String jobHandle = scheduleHelper.submitJob(job, sessionHandleString);

    XJob tmp = scheduleHelper.getJobDefinition(jobHandle, sessionHandleString);
    tmp.setName("modified-name");
    endDate = Util.modifyDate(currentDate, format, Calendar.DATE, 6);
    tmp.setEndTime(Util.getGregorianCalendar(endDate));
    APIResult res = scheduleHelper.updateJob(tmp, jobHandle, sessionHandleString);
    Assert.assertEquals(res.getStatus(), APIResult.Status.SUCCEEDED);

    XJob modifiedJob = scheduleHelper.getJobDefinition(jobHandle, sessionHandleString);
    Assert.assertEquals(modifiedJob.getName(), "modified-name");
    String modifiedEndTime = Util.getDateStringFromGregorainCalender(modifiedJob.getEndTime(), format);
    Assert.assertEquals(modifiedEndTime, endDate);
  }

  @Test
  public void testUpdateJobAction() throws Exception {

    String endDate = Util.modifyDate(currentDate, format, Calendar.DATE, 1);
    XJob job = scheduleHelper.getXJob("job-update-action", QueryInventory.QUERY, null, currentDate, endDate,
        "0/20 * * * * ?");
    String jobHandle = scheduleHelper.submitJob(job, sessionHandleString);
    Assert.assertEquals(scheduleHelper.getJobStatus(jobHandle), SchedulerJobState.NEW);

    scheduleHelper.updateJob(jobHandle, "SCHEDULE", sessionHandleString);
    Assert.assertEquals(scheduleHelper.getJobStatus(jobHandle), SchedulerJobState.SCHEDULED);

    scheduleHelper.updateJob(jobHandle, "SUSPEND", sessionHandleString);
    Assert.assertEquals(scheduleHelper.getJobStatus(jobHandle), SchedulerJobState.SUSPENDED);

    scheduleHelper.updateJob(jobHandle, "RESUME", sessionHandleString);
    Assert.assertEquals(scheduleHelper.getJobStatus(jobHandle), SchedulerJobState.SCHEDULED);

    scheduleHelper.updateJob(jobHandle, "EXPIRE", sessionHandleString);
    Assert.assertEquals(scheduleHelper.getJobStatus(jobHandle), SchedulerJobState.EXPIRED);
  }

  @Test
  public void testGetAllInstancesOfAJob() throws Exception {

    String startDate = Util.modifyDate(Util.getCurrentDate(format), format, Calendar.SECOND, 5);
    String endDate = Util.modifyDate(startDate, format, Calendar.MINUTE, 4);
    XJob xJob = scheduleHelper.getXJob("job-update-action", QueryInventory.QUERY, null, startDate, endDate,
        "0/20 * * * * ?");
    String jobHandle = scheduleHelper.submitNScheduleJob(xJob, sessionHandleString);

    Thread.sleep(60000);

    List<SchedulerJobInstanceInfo> instanceList = scheduleHelper.getAllInstancesOfJob(jobHandle, "10",
        sessionHandleString);
    Assert.assertEquals(instanceList.size(), 3);
  }


  @Test
  public void updateInstance() throws Exception {

    String startDate = Util.modifyDate(Util.getCurrentDate(format), format, Calendar.SECOND, 10);
    String endDate = Util.modifyDate(startDate, format, Calendar.MINUTE, 3);
    XJob xJob = scheduleHelper.getXJob("job-update-action", QueryInventory.JDBC_DIM_QUERY, null, startDate, endDate,
        "0/20 * * * * ?");
    String jobHandle = scheduleHelper.submitNScheduleJob(xJob, sessionHandleString);

    Thread.sleep(20000);

    List<SchedulerJobInstanceInfo> instanceList = scheduleHelper.getAllInstancesOfJob(jobHandle, "10",
        sessionHandleString);

    Thread.sleep(10000);

    APIResult res = scheduleHelper.updateInstance(instanceList.get(0).getId().getHandleIdString(),
        "RERUN", sessionHandleString);
    Assert.assertEquals(res.getStatus(), APIResult.Status.SUCCEEDED);

    SchedulerJobInstanceInfo instanceInfo = scheduleHelper.getInstanceDetails(instanceList.get(0).getId()
        .getHandleIdString(), sessionHandleString);
    List<SchedulerJobInstanceRun> runList = instanceInfo.getInstanceRunList();
    Assert.assertEquals(runList.size(), 2);
    Assert.assertEquals(runList.get(1).getRunId(), 2);
  }


  @Test(enabled = true)
  public void restart() throws Exception {

    String startDate = Util.modifyDate(Util.getCurrentDate(format), format, Calendar.SECOND, 5);
    String endDate = Util.modifyDate(startDate, format, Calendar.MINUTE, 2);
    XJob xJob = scheduleHelper.getXJob("job-restart", QueryInventory.QUERY, null, startDate, endDate, "0/20 * * * * ?");

    String jobHandle = scheduleHelper.submitNScheduleJob(xJob, sessionHandleString);
    Assert.assertNotNull(jobHandle);

    Thread.sleep(20000);
    lens.stop();
    Thread.sleep(20000);
    lens.start();
    Thread.sleep(60000);

    List<SchedulerJobInstanceInfo> instanceList = scheduleHelper.getAllInstancesOfJob(jobHandle, "50",
        sessionHandleString);
    Assert.assertEquals(instanceList.size(), 6);
  }

  //LENS
  @Test
  public void testMisfiredEvents() throws Exception {

    String startDate = Util.modifyDate(currentDate, format, Calendar.DATE, -2);
    String endDate = Util.modifyDate(currentDate, format, Calendar.DATE, 3);
    XJob xJob = scheduleHelper.getXJob("job-misfire", QueryInventory.JDBC_CUBE_QUERY, null, startDate,
        endDate, XFrequencyEnum.DAILY);
    String jobHandle = scheduleHelper.submitNScheduleJob(xJob, sessionHandleString);
    Assert.assertNotNull(jobHandle);
    Assert.assertEquals(scheduleHelper.getJobStatus(jobHandle), SchedulerJobState.SCHEDULED);

    List<SchedulerJobInstanceInfo> instanceList = scheduleHelper.getAllInstancesOfJob(jobHandle, "10",
        sessionHandleString);
    Assert.assertEquals(instanceList.size(), 3);

    if (jobHandle!=null){
      scheduleHelper.updateJob(jobHandle, "EXPIRE", sessionHandleString);
    }
  }
}

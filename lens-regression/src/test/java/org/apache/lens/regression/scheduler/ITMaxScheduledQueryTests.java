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

package org.apache.lens.regression.scheduler;

import java.lang.reflect.Method;
import java.util.Calendar;
import java.util.HashMap;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.apache.lens.api.scheduler.*;
import org.apache.lens.regression.core.constants.QueryInventory;
import org.apache.lens.regression.core.helpers.ServiceManagerHelper;
import org.apache.lens.regression.core.testHelper.BaseTestClass;
import org.apache.lens.regression.util.AssertUtil;
import org.apache.lens.regression.util.Util;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.util.LensUtil;

import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.*;


public class ITMaxScheduledQueryTests extends BaseTestClass {

  WebTarget servLens;
  private String sessionHandleString;

  private static Logger logger = Logger.getLogger(ITMaxScheduledQueryTests.class);
  private static String format = "yyyy-MM-dd HH:mm:ss";
  private static String currentDate = Util.getCurrentDate(format);
  String lensSiteConf = lens.getServerDir() + "/conf/lens-site.xml";

  @BeforeClass(alwaysRun = true)
  public void initialize() throws Exception {
    servLens = ServiceManagerHelper.init();
    HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.MAX_SCHEDULED_JOB_PER_USER, "2");
    Util.changeConfig(map, lensSiteConf);
    lens.restart();
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

  @AfterClass(alwaysRun = true)
  public void afterClass() throws Exception {
    Util.changeConfig(lensSiteConf);
    lens.restart();
  }

  //LENS-1320

  @Test(groups = "max_scheduled_job_per_user")
  public void testJobsInNewState() throws Exception {

    String endDate = Util.modifyDate(currentDate, format, Calendar.DATE, 4);
    String session = sHelper.openSession("max1", "pwd", lens.getCurrentDB());
    XJob xJob = scheduleHelper.getXJob("job-submit", QueryInventory.QUERY, null, currentDate, endDate,
        XFrequencyEnum.DAILY);

    String j1 = scheduleHelper.submitJob(xJob, session);
    String j2 = scheduleHelper.submitJob(xJob, session);
    Response response = scheduleHelper.submitJobReturnResponse("submit", xJob, session);
    AssertUtil.assertBadRequest(response);
  }

  @Test(groups = "max_scheduled_job_per_user")
  public void testJobsInScheduledState() throws Exception {

    String endDate = Util.modifyDate(currentDate, format, Calendar.DATE, 4);
    String session = sHelper.openSession("max2", "pwd", lens.getCurrentDB());
    XJob xJob = scheduleHelper.getXJob("job-submit", QueryInventory.QUERY, null, currentDate, endDate,
        XFrequencyEnum.DAILY);

    String j1 = scheduleHelper.submitJob(xJob, session);
    String j2 = scheduleHelper.submitNScheduleJob(xJob, session);
    Response response = scheduleHelper.submitJobReturnResponse("submit", xJob, session);
    AssertUtil.assertBadRequest(response);
  }

  @Test(groups = "max_scheduled_job_per_user")
  public void testJobsInSuspendedState() throws Exception {

    String endDate = Util.modifyDate(currentDate, format, Calendar.DATE, 4);
    String session = sHelper.openSession("max3", "pwd", lens.getCurrentDB());
    XJob xJob = scheduleHelper.getXJob("job-submit", QueryInventory.QUERY, null, currentDate, endDate,
        XFrequencyEnum.DAILY);

    String j1 = scheduleHelper.submitJob(xJob, session);
    String j2 = scheduleHelper.submitNScheduleJob(xJob, session);
    scheduleHelper.updateJob(j2, "SUSPEND", session);
    Assert.assertEquals(scheduleHelper.getJobStatus(j2), SchedulerJobState.SUSPENDED);
    Response response = scheduleHelper.submitJobReturnResponse("submit", xJob, session);
    AssertUtil.assertBadRequest(response);
  }

  @Test(groups = "max_scheduled_job_per_user")
  public void testJobsInDeletedState() throws Exception {

    String endDate = Util.modifyDate(currentDate, format, Calendar.DATE, 4);
    String session = sHelper.openSession("max4", "pwd", lens.getCurrentDB());
    XJob xJob = scheduleHelper.getXJob("job-submit", QueryInventory.QUERY, null, currentDate, endDate,
        XFrequencyEnum.DAILY);

    String j1 = scheduleHelper.submitJob(xJob, session);
    scheduleHelper.deleteJob(j1, session);
    Assert.assertEquals(scheduleHelper.getJobStatus(j1), SchedulerJobState.DELETED);

    String j2 = scheduleHelper.submitJob(xJob, session);
    String j3 = scheduleHelper.submitJob(xJob, session);
  }

  @Test(groups = "max_scheduled_job_per_user")
  public void testJobsInExpiredState() throws Exception {

    String endDate = Util.modifyDate(currentDate, format, Calendar.DATE, 4);
    String session = sHelper.openSession("max5", "pwd", lens.getCurrentDB());
    XJob xJob = scheduleHelper.getXJob("job-submit", QueryInventory.QUERY, null, currentDate, endDate,
        XFrequencyEnum.DAILY);

    String j1 = scheduleHelper.submitJob(xJob, session);
    scheduleHelper.updateJob(j1, "EXPIRE", session);
    Assert.assertEquals(scheduleHelper.getJobStatus(j1), SchedulerJobState.EXPIRED);
    String j2 = scheduleHelper.submitJob(xJob, session);
    String j3 = scheduleHelper.submitJob(xJob, session);
  }

}

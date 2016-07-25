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

import java.util.Collection;
import java.util.List;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.error.InvalidStateTransitionException;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.scheduler.*;
import org.apache.lens.cube.parse.CubeQueryConfUtil;
import org.apache.lens.server.BaseLensService;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.SchedulerAlarmEvent;
import org.apache.lens.server.api.health.HealthStatus;
import org.apache.lens.server.api.query.QueryEnded;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.api.scheduler.SchedulerService;
import org.apache.lens.server.session.LensSessionImpl;
import org.apache.lens.server.util.UtilityMethods;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;

import org.joda.time.DateTime;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * This class handles all the scheduler operations.
 */
@Slf4j
public class SchedulerServiceImpl extends BaseLensService implements SchedulerService {

  @Getter
  @Setter
  @VisibleForTesting
  protected QueryExecutionService queryService;
  @Getter
  @VisibleForTesting
  protected SchedulerEventListener schedulerEventListener;
  @Getter
  @VisibleForTesting
  protected SchedulerQueryEventListener schedulerQueryEventListener;
  @Getter
  @VisibleForTesting
  protected SchedulerDAO schedulerDAO;
  private AlarmService alarmService;

  /**
   * Instantiates a new scheduler service.
   *
   * @param cliService the cli service
   */
  public SchedulerServiceImpl(CLIService cliService) throws LensException {
    super(NAME, cliService);
  }

  public synchronized void init(HiveConf hiveConf) {
    super.init(hiveConf);
    try {
      schedulerDAO = new SchedulerDAO(hiveConf);
      alarmService = LensServices.get().getService(AlarmService.NAME);
      queryService = LensServices.get().getService(QueryExecutionService.NAME);
      // Get the listeners' classes from the configuration.
      this.schedulerEventListener = new SchedulerEventListener(schedulerDAO);
      this.schedulerQueryEventListener = new SchedulerQueryEventListener(schedulerDAO);
      getEventService().addListenerForType(schedulerEventListener, SchedulerAlarmEvent.class);
      getEventService().addListenerForType(schedulerQueryEventListener, QueryEnded.class);
    } catch (LensException e) {
      log.error("Error Initialising Scheduler-service", e);
    }
  }

  @Override
  public synchronized void start() {
    super.start();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HealthStatus getHealthStatus() {
    return this.getServiceState().equals(STATE.STARTED)
           ? new HealthStatus(true, "Scheduler service is healthy.")
           : new HealthStatus(false, "Scheduler service is down.");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SchedulerJobHandle submitJob(LensSessionHandle sessionHandle, XJob job) throws LensException {
    LensSessionImpl session = getSession(sessionHandle);
    // Validate XJob
    validateJob(job);
    SchedulerJobHandle handle = UtilityMethods.generateSchedulerJobHandle();
    long createdOn = System.currentTimeMillis();
    long modifiedOn = createdOn;
    SchedulerJobInfo info = new SchedulerJobInfo(handle, job, session.getLoggedInUser(), SchedulerJobState.NEW,
        createdOn, modifiedOn);
    if (schedulerDAO.storeJob(info) == 1) {
      return handle;
    } else {
      throw new LensException("Could not Submit the job");
    }
  }

  private void validateJob(XJob job) throws LensException {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean scheduleJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException {
    SchedulerJobInfo jobInfo = schedulerDAO.getSchedulerJobInfo(jobHandle);
    XJob job = jobInfo.getJob();
    DateTime start = new DateTime(job.getStartTime().toGregorianCalendar().getTime());
    DateTime end = new DateTime(job.getEndTime().toGregorianCalendar().getTime());
    XFrequency frequency = job.getTrigger().getFrequency();
    // check query
    checkQuery(sessionHandle, job);
    alarmService.schedule(start, end, frequency, jobHandle.getHandleIdString());
    return setStateOfJob(jobHandle, SchedulerJobEvent.ON_SCHEDULE) == 1;
  }

  private void checkQuery(LensSessionHandle sessionHandle, XJob job) throws LensException {
    List<MapType> jobConf = job.getExecution().getQuery().getConf();
    LensConf queryConf = new LensConf();
    for (MapType element : jobConf) {
      queryConf.addProperty(element.getKey(), element.getValue());
    }
    queryConf.addProperty(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
    queryService.estimate(LensServices.get().getLogSegregationContext().getLogSegragationId(), sessionHandle,
        job.getExecution().getQuery().getQuery(), queryConf);
    return;
  }

  @Override
  public SchedulerJobHandle submitAndScheduleJob(LensSessionHandle sessionHandle, XJob job) throws LensException {
    SchedulerJobHandle handle = submitJob(sessionHandle, job);
    scheduleJob(sessionHandle, handle);
    return handle;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public XJob getJobDefinition(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException {
    return schedulerDAO.getJob(jobHandle);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SchedulerJobInfo getJobDetails(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle)
    throws LensException {
    return schedulerDAO.getSchedulerJobInfo(jobHandle);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean updateJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle, XJob newJobDefinition)
    throws LensException {
    SchedulerJobInfo jobInfo = schedulerDAO.getSchedulerJobInfo(jobHandle);
    // This will allow only the job definition and configuration change.
    // TODO: fix start and end time changes
    jobInfo.setJob(newJobDefinition);
    jobInfo.setModifiedOn(System.currentTimeMillis());
    int updated = schedulerDAO.updateJob(jobInfo);
    return updated > 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean expireJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException {
    if (alarmService.checkExists(jobHandle)) {
      alarmService.unSchedule(jobHandle);
    }
    return setStateOfJob(jobHandle, SchedulerJobEvent.ON_EXPIRE) == 1;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean suspendJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException {
    alarmService.pauseJob(jobHandle);
    return setStateOfJob(jobHandle, SchedulerJobEvent.ON_SUSPEND) == 1;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean resumeJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException {
    alarmService.resumeJob(jobHandle);
    return setStateOfJob(jobHandle, SchedulerJobEvent.ON_RESUME) == 1;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean deleteJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException {
    if (alarmService.checkExists(jobHandle)) {
      alarmService.unSchedule(jobHandle);
    }
    return setStateOfJob(jobHandle, SchedulerJobEvent.ON_DELETE) == 1;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<SchedulerJobStats> getAllJobStats(LensSessionHandle sessionHandle, String state, String user,
      String jobName, long startTime, long endTime) throws LensException {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SchedulerJobStats getJobStats(LensSessionHandle sessionHandle, SchedulerJobHandle handle, String state,
      long startTime, long endTime) throws LensException {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean rerunInstance(LensSessionHandle sessionHandle, SchedulerJobInstanceHandle instanceHandle)
    throws LensException {
    SchedulerJobInstanceInfo instanceInfo = schedulerDAO.getSchedulerJobInstanceInfo(instanceHandle);
    if (schedulerDAO.getJobState(instanceInfo.getJobId()) != SchedulerJobState.SCHEDULED) {
      throw new LensException("Job with handle " + instanceInfo.getJobId() + " is not scheduled");
    }
    // Get the latest run.
    List<SchedulerJobInstanceRun> runList = instanceInfo.getInstanceRunList();
    if (runList.size() == 0) {
      throw new LensException("Job instance " + instanceHandle + " is not yet run");
    }
    SchedulerJobInstanceRun latestRun = runList.get(runList.size() - 1);
    // This call is for the test that it can be re run.
    try {
      latestRun.getInstanceState().nextTransition(SchedulerJobInstanceEvent.ON_RERUN);
      getEventService().notifyEvent(
          new SchedulerAlarmEvent(instanceInfo.getJobId(), new DateTime(instanceInfo.getScheduleTime()),
              SchedulerAlarmEvent.EventType.SCHEDULE, instanceHandle));
    } catch (InvalidStateTransitionException e) {
      throw new LensException("Invalid State Transition ", e);
    }
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<SchedulerJobInstanceInfo> getJobInstances(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle,
      Long numResults) throws LensException {
    return schedulerDAO.getJobInstances(jobHandle);
  }

  @Override
  public boolean killInstance(LensSessionHandle sessionHandle, SchedulerJobInstanceHandle instanceHandle)
    throws LensException {
    /**
     * Get the query handle from the latest run.
     */
    SchedulerJobInstanceInfo instanceInfo = schedulerDAO.getSchedulerJobInstanceInfo(instanceHandle);
    List<SchedulerJobInstanceRun> runList = instanceInfo.getInstanceRunList();
    if (runList.size() == 0) {
      throw new LensException("Job instance " + instanceHandle + " is not yet run");
    }
    SchedulerJobInstanceRun latestRun = runList.get(runList.size() - 1);
    QueryHandle handle = latestRun.getQueryHandle();
    if (handle.getHandleIdString().isEmpty()) {
      return false;
    }
    // This will cause the QueryEnd event which will set the status of the instance to KILLED.
    return queryService.cancelQuery(sessionHandle, handle);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SchedulerJobInstanceInfo getInstanceDetails(LensSessionHandle sessionHandle,
      SchedulerJobInstanceHandle instanceHandle) throws LensException {
    return schedulerDAO.getSchedulerJobInstanceInfo(instanceHandle);
  }

  private int setStateOfJob(SchedulerJobHandle handle, SchedulerJobEvent event) throws LensException {
    try {
      SchedulerJobInfo info = schedulerDAO.getSchedulerJobInfo(handle);
      SchedulerJobState currentState = info.getJobState();
      SchedulerJobState nextState = currentState.nextTransition(event);
      info.setJobState(nextState);
      info.setModifiedOn(System.currentTimeMillis());
      return schedulerDAO.updateJobStatus(info);
    } catch (InvalidStateTransitionException e) {
      throw new LensException("Invalid state ", e);
    }
  }
}

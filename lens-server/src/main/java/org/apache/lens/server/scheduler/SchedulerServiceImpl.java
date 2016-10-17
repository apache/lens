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

import static org.apache.lens.api.scheduler.SchedulerJobInstanceEvent.ON_KILL;
import static org.apache.lens.api.scheduler.SchedulerJobInstanceEvent.ON_RERUN;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.error.InvalidStateTransitionException;
import org.apache.lens.api.error.LensCommonErrorCode;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.api.scheduler.*;
import org.apache.lens.cube.parse.CubeQueryConfUtil;
import org.apache.lens.server.BaseLensService;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.LensErrorInfo;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.SchedulerAlarmEvent;
import org.apache.lens.server.api.health.HealthStatus;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.api.query.events.QueryEnded;
import org.apache.lens.server.api.scheduler.SchedulerService;
import org.apache.lens.server.error.LensSchedulerErrorCode;
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
  @Getter
  private AlarmService alarmService;

  private int maxJobsPerUser = LensConfConstants.DEFAULT_MAX_SCHEDULED_JOB_PER_USER;

  /**
   * Instantiates a new scheduler service.
   *
   * @param cliService the cli service
   */
  public SchedulerServiceImpl(CLIService cliService) throws LensException {
    super(NAME, cliService);
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    super.init(hiveConf);
    maxJobsPerUser = hiveConf.getInt(LensConfConstants.MAX_SCHEDULED_JOB_PER_USER, maxJobsPerUser);
    try {
      schedulerDAO = new SchedulerDAO(hiveConf);
      alarmService = LensServices.get().getService(AlarmService.NAME);
      queryService = LensServices.get().getService(QueryExecutionService.NAME);
      this.schedulerEventListener = new SchedulerEventListener(schedulerDAO);
      this.schedulerQueryEventListener = new SchedulerQueryEventListener(schedulerDAO);
      getEventService().addListenerForType(schedulerEventListener, SchedulerAlarmEvent.class);
      getEventService().addListenerForType(schedulerQueryEventListener, QueryEnded.class);
    } catch (LensException e) {
      log.error("Error Initialising Scheduler-service", e);
    }
  }

  private void doesSessionBelongToUser(LensSessionHandle sessionHandle, String user) throws LensException {
    LensSessionImpl session = getSession(sessionHandle);
    if (!session.getLoggedInUser().equals(user)) {
      log.warn("Session User {} is not equal to Job owner {}", session.getLoggedInUser(), user);
      throw new LensException(LensSchedulerErrorCode.CURRENT_USER_IS_NOT_SAME_AS_OWNER.getLensErrorInfo(), null,
        session.getLoggedInUser(), sessionHandle.getPublicId().toString(), user);
    }
  }

  /**
   * How the restarts are handled?
   * Get all the instances with state Running or New.
   * If They are running then check the query status. If Query is finished, take query parameters and update the
   * instance. If query is still running then do nothing.
   * If the state is New then Kill the instance and rerun it.
   */
  @Override
  public synchronized void start() {
    super.start();
    List<SchedulerJobInstanceRun> instanceRuns = schedulerDAO
      .getInstanceRuns(SchedulerJobInstanceState.WAITING, SchedulerJobInstanceState.LAUNCHED,
        SchedulerJobInstanceState.RUNNING);
    for (SchedulerJobInstanceRun run : instanceRuns) {
      LensSessionHandle sessionHandle = null;
      try {
        SchedulerJobInstanceInfo instanceInfo = schedulerDAO.getSchedulerJobInstanceInfo(run.getHandle());
        log.info("Recovering instance {} of job {} ", instanceInfo.getId(), instanceInfo.getJobId());
        switch (run.getInstanceState()) {
        case WAITING:
        case LAUNCHED:
          // Kill and rerun
          if (updateInstanceRun(run, SchedulerJobInstanceState.KILLED)) {
            notifyRerun(instanceInfo);
            log.info("Re-running instance {} of job {}", instanceInfo.getId(), instanceInfo.getJobId());
          } else {
            log.error("Not able to recover instance {} of job {}", instanceInfo.getId(), instanceInfo.getJobId());
          }
          break;
        case RUNNING:
          sessionHandle = openSessionAsUser(schedulerDAO.getUser(instanceInfo.getJobId()));
          if (!checkQueryState(sessionHandle, run)) {
            log.info("Re-running instance {} of job {}", instanceInfo.getId(), instanceInfo.getJobId());
            notifyRerun(instanceInfo);
          }
          break;
        }
      } catch (LensException e) {
        log.error("Not able to recover instance {} ", run.getHandle().getHandleIdString(), e);
      } finally {
        try {
          if (sessionHandle != null) {
            closeSession(sessionHandle);
          }
        } catch (Exception e) {
          log.error("Error closing session ", e);
        }
      }
    }
  }

  /**
   * If query is not found of is invalid then rerun again else get the status and update correspondingly.
   *
   * @param sessionHandle
   * @param run
   * @return
   * @throws LensException
   */
  private boolean checkQueryState(LensSessionHandle sessionHandle, SchedulerJobInstanceRun run) throws LensException {
    QueryHandle queryHandle = run.getQueryHandle();
    LensQuery query = null;
    try {
      query = this.queryService.getQuery(sessionHandle, queryHandle);
    } catch (Exception e) {
      updateInstanceRun(run, SchedulerJobInstanceState.KILLED);
      return false;
    }
    if (query == null) {
      // This means we have no idea what happened to query
      // Mark it as Killed.
      updateInstanceRun(run, SchedulerJobInstanceState.KILLED);
      return false;
    }
    QueryStatus.Status status = query.getStatus().getStatus();
    SchedulerJobInstanceState state = run.getInstanceState();
    switch (status) {
    case NEW:
    case QUEUED:
    case LAUNCHED:
    case RUNNING:
    case EXECUTED:
      break;
    case CANCELED:
      state = SchedulerJobInstanceState.KILLED;
      break;
    case SUCCESSFUL:
      state = SchedulerJobInstanceState.SUCCEEDED;
      break;
    case FAILED:
      state = SchedulerJobInstanceState.FAILED;
      break;
    default:
      // This should not happen
      log.warn("Unexpected status {} for the query id {}", status, queryHandle);
      state = SchedulerJobInstanceState.KILLED;
    }
    run.setResultPath(query.getResultSetPath());
    updateInstanceRun(run, state);
    return true;
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
  public LensSessionHandle openSessionAsUser(String user) throws LensException {
    // Open session with no auth
    return openSession(user, "Mimbulus Mimbletonia", new HashMap<String, String>(), false);
  }

  @Override
  public List<SchedulerJobHandle> getAllJobs(String user, SchedulerJobState state, Long start, Long end)
    throws LensException {
    return this.schedulerDAO.getJobs(user, start, end, state);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SchedulerJobHandle submitJob(LensSessionHandle sessionHandle, XJob job) throws LensException {
    LensSessionImpl session = getSession(sessionHandle);
    // Validate XJob
    validateJob(session, job);
    SchedulerJobHandle handle = UtilityMethods.generateSchedulerJobHandle();
    long createdOn = System.currentTimeMillis();
    SchedulerJobInfo info = new SchedulerJobInfo(handle, job, session.getLoggedInUser(), SchedulerJobState.NEW,
      createdOn, createdOn);
    if (schedulerDAO.storeJob(info) == 1) {
      log.info("Successfully submitted job with handle {}", handle);
      return handle;
    } else {
      throw new LensException(LensSchedulerErrorCode.CANT_SUBMIT_JOB.getLensErrorInfo(), null, job.getName());
    }
  }

  private void validateJob(LensSessionImpl session, XJob job) throws LensException {
    // Check if the number of scheduled jobs are not exceeding the configured global value.
    if (maxJobsPerUser > 0) {
      int currentJobs = schedulerDAO
        .getJobs(session.getLoggedInUser(), null, null, SchedulerJobState.NEW, SchedulerJobState.SCHEDULED,
          SchedulerJobState.SUSPENDED).size();
      if (currentJobs >= maxJobsPerUser) {
        throw new LensException(LensSchedulerErrorCode.MAX_SCHEDULED_JOB_EXCEEDED.getLensErrorInfo(), null,
          currentJobs);
      }

    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void scheduleJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException {
    SchedulerJobInfo jobInfo = checkAndGetSchedulerJobInfo(jobHandle);
    doesSessionBelongToUser(sessionHandle, jobInfo.getUserName());
    XJob job = jobInfo.getJob();
    DateTime start = new DateTime(job.getStartTime().toGregorianCalendar().getTime());
    DateTime end = new DateTime(job.getEndTime().toGregorianCalendar().getTime());
    XFrequency frequency = job.getTrigger().getFrequency();
    // check query
    checkQuery(sessionHandle, job);
    alarmService.schedule(start, end, frequency, jobHandle.getHandleIdString());
    log.info("Successfully scheduled job with handle {} in AlarmService", jobHandle);
    setStateOfJob(jobInfo, SchedulerJobEvent.ON_SCHEDULE);
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
  public XJob getJobDefinition(SchedulerJobHandle jobHandle) throws LensException {
    return schedulerDAO.getJob(jobHandle);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SchedulerJobInfo getJobDetails(SchedulerJobHandle jobHandle) throws LensException {
    return checkAndGetSchedulerJobInfo(jobHandle);
  }

  private SchedulerJobInfo checkAndGetSchedulerJobInfo(SchedulerJobHandle jobHandle) throws LensException {
    SchedulerJobInfo jobInfo = schedulerDAO.getSchedulerJobInfo(jobHandle);
    if (jobInfo == null) {
      throw new LensException(
        new LensErrorInfo(LensCommonErrorCode.RESOURCE_NOT_FOUND.getValue(), 0, "Job handle not found"), null, "job",
        jobHandle.getHandleIdString());
    }
    return jobInfo;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle, XJob newJobDefinition)
    throws LensException {
    SchedulerJobInfo jobInfo = checkAndGetSchedulerJobInfo(jobHandle);
    doesSessionBelongToUser(sessionHandle, jobInfo.getUserName());
    // This will allow only the job definition and configuration change.
    jobInfo.setJob(newJobDefinition);
    jobInfo.setModifiedOn(System.currentTimeMillis());
    int updated = schedulerDAO.updateJob(jobInfo);
    if (updated > 0) {
      return;
    }
    throw new LensException(LensSchedulerErrorCode.CANT_UPDATE_RESOURCE_WITH_HANDLE.getLensErrorInfo(), null, "job",
      jobHandle.getHandleIdString());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void expireJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException {
    SchedulerJobInfo info = checkAndGetSchedulerJobInfo(jobHandle);
    doesSessionBelongToUser(sessionHandle, info.getUserName());
    if (alarmService.checkExists(jobHandle)) {
      alarmService.unSchedule(jobHandle);
      log.info("Successfully unscheduled the job with handle {} in AlarmService ", jobHandle);
    }
    setStateOfJob(info, SchedulerJobEvent.ON_EXPIRE);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void suspendJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException {
    SchedulerJobInfo info = checkAndGetSchedulerJobInfo(jobHandle);
    doesSessionBelongToUser(sessionHandle, info.getUserName());
    alarmService.pauseJob(jobHandle);
    setStateOfJob(info, SchedulerJobEvent.ON_SUSPEND);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void resumeJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException {
    SchedulerJobInfo info = checkAndGetSchedulerJobInfo(jobHandle);
    doesSessionBelongToUser(sessionHandle, info.getUserName());
    alarmService.resumeJob(jobHandle);
    setStateOfJob(info, SchedulerJobEvent.ON_RESUME);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException {
    SchedulerJobInfo info = checkAndGetSchedulerJobInfo(jobHandle);
    doesSessionBelongToUser(sessionHandle, info.getUserName());
    if (alarmService.checkExists(jobHandle)) {
      alarmService.unSchedule(jobHandle);
      log.info("Successfully unscheduled the job with handle {} ", jobHandle);
    }
    setStateOfJob(info, SchedulerJobEvent.ON_DELETE);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<SchedulerJobStats> getAllJobStats(String state, String user, String jobName, long startTime,
    long endTime) throws LensException {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SchedulerJobStats getJobStats(SchedulerJobHandle handle, String state, long startTime, long endTime)
    throws LensException {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void rerunInstance(LensSessionHandle sessionHandle, SchedulerJobInstanceHandle instanceHandle)
    throws LensException {
    SchedulerJobInstanceInfo instanceInfo = schedulerDAO.getSchedulerJobInstanceInfo(instanceHandle);
    doesSessionBelongToUser(sessionHandle, schedulerDAO.getUser(instanceInfo.getJobId()));
    SchedulerJobState currentState = schedulerDAO.getJobState(instanceInfo.getJobId());
    if (currentState != SchedulerJobState.SCHEDULED) {
      throw new LensException(LensSchedulerErrorCode.JOB_IS_NOT_SCHEDULED.getLensErrorInfo(), null,
        instanceInfo.getJobId().getHandleIdString(), currentState);
    }
    // Get the latest run.
    List<SchedulerJobInstanceRun> runList = instanceInfo.getInstanceRunList();
    if (runList.size() == 0) {
      throw new LensException(LensSchedulerErrorCode.JOB_INSTANCE_IS_NOT_YET_RUN.getLensErrorInfo(), null,
        instanceHandle.getHandleIdString(), instanceInfo.getJobId().getHandleIdString());
    }
    SchedulerJobInstanceRun latestRun = runList.get(runList.size() - 1);
    try {
      latestRun.getInstanceState().nextTransition(ON_RERUN);
      notifyRerun(instanceInfo);
      log.info("Rerunning the instance with {} for job {} ", instanceHandle, instanceInfo.getJobId());
    } catch (InvalidStateTransitionException e) {
      throw new LensException(LensSchedulerErrorCode.INVALID_EVENT_FOR_JOB_INSTANCE.getLensErrorInfo(), e,
        ON_RERUN.name(), latestRun.getInstanceState().name(), instanceInfo.getId().getHandleIdString(),
        instanceInfo.getJobId().getHandleIdString());
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<SchedulerJobInstanceInfo> getJobInstances(SchedulerJobHandle jobHandle, Long numResults)
    throws LensException {
    return schedulerDAO.getJobInstances(jobHandle);
  }

  @Override
  public boolean killInstance(LensSessionHandle sessionHandle, SchedulerJobInstanceHandle instanceHandle)
    throws LensException {
    /**
     * Get the query handle from the latest run.
     */
    SchedulerJobInstanceInfo instanceInfo = schedulerDAO.getSchedulerJobInstanceInfo(instanceHandle);
    doesSessionBelongToUser(sessionHandle, schedulerDAO.getUser(instanceInfo.getJobId()));
    List<SchedulerJobInstanceRun> runList = instanceInfo.getInstanceRunList();
    if (runList.size() == 0) {
      throw new LensException(LensSchedulerErrorCode.JOB_INSTANCE_IS_NOT_YET_RUN.getLensErrorInfo(), null,
        instanceHandle.getHandleIdString(), instanceInfo.getJobId().getHandleIdString());
    }
    SchedulerJobInstanceRun latestRun = runList.get(runList.size() - 1);
    SchedulerJobInstanceState state = latestRun.getInstanceState();
    try {
      state = state.nextTransition(ON_KILL);
    } catch (InvalidStateTransitionException e) {
      throw new LensException(LensSchedulerErrorCode.INVALID_EVENT_FOR_JOB_INSTANCE.getLensErrorInfo(), e,
        ON_KILL.name(), latestRun.getInstanceState().name(), instanceInfo.getId().getHandleIdString(),
        instanceInfo.getJobId().getHandleIdString());
    }
    QueryHandle handle = latestRun.getQueryHandle();
    if (handle == null || handle.getHandleIdString().isEmpty()) {
      log.info("Killing instance {} for job {} ", instanceInfo.getId(), instanceInfo.getJobId());
      return updateInstanceRun(latestRun, state);
    } else {
      log.info("Killing instance {} for job {} with query handle {} ", instanceInfo.getId(), instanceInfo.getJobId(),
        handle);
      // This will cause the QueryEnd event which will set the status of the instance to KILLED.
      return queryService.cancelQuery(sessionHandle, handle);
    }

  }

  private boolean updateInstanceRun(SchedulerJobInstanceRun latestRun, SchedulerJobInstanceState state)
    throws LensException {
    latestRun.setEndTime(System.currentTimeMillis());
    latestRun.setInstanceState(state);
    return schedulerDAO.updateJobInstanceRun(latestRun) == 1;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SchedulerJobInstanceInfo getInstanceDetails(SchedulerJobInstanceHandle instanceHandle) throws LensException {
    return schedulerDAO.getSchedulerJobInstanceInfo(instanceHandle);
  }

  private void setStateOfJob(SchedulerJobInfo info, SchedulerJobEvent event) throws LensException {
    SchedulerJobState currentState = info.getJobState();
    try {
      SchedulerJobState nextState = currentState.nextTransition(event);
      info.setJobState(nextState);
      info.setModifiedOn(System.currentTimeMillis());
      int ret = schedulerDAO.updateJobStatus(info);
      if (ret == 1) {
        log.info("Successfully changed the status of job with handle {} from {} to {}", info.getId(), currentState,
          nextState);
      } else {
        throw new LensException(LensSchedulerErrorCode.CANT_UPDATE_RESOURCE_WITH_HANDLE.getLensErrorInfo(), null, "job",
          info.getId().getHandleIdString());
      }
    } catch (InvalidStateTransitionException e) {
      throw new LensException(LensSchedulerErrorCode.INVALID_EVENT_FOR_JOB.getLensErrorInfo(), e, event.name(),
        currentState.name(), info.getId().getHandleIdString());
    }
  }

  private void notifyRerun(SchedulerJobInstanceInfo instanceInfo) throws LensException {
    getEventService().notifyEvent(
      new SchedulerAlarmEvent(instanceInfo.getJobId(), new DateTime(instanceInfo.getScheduleTime()),
        SchedulerAlarmEvent.EventType.SCHEDULE, instanceInfo.getId()));
  }
}

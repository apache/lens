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

import java.util.ArrayList;
import java.util.List;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.error.InvalidStateTransitionException;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.scheduler.*;
import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.AsyncEventListener;
import org.apache.lens.server.api.events.SchedulerAlarmEvent;
import org.apache.lens.server.api.metastore.CubeMetastoreService;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.api.scheduler.SchedulerService;
import org.apache.lens.server.api.session.SessionService;
import org.apache.lens.server.util.UtilityMethods;

import org.joda.time.DateTime;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SchedulerEventListener extends AsyncEventListener<SchedulerAlarmEvent> {
  private static final int CORE_POOL_SIZE = 10;
  private static final String JOB_INSTANCE_ID_KEY = "job_instance_key";
  @Getter
  @Setter
  @VisibleForTesting
  protected QueryExecutionService queryService;
  private SchedulerDAO schedulerDAO;
  private SchedulerService schedulerService;
  private SessionService sessionService;
  private CubeMetastoreService cubeMetastoreService;

  public SchedulerEventListener(SchedulerDAO schedulerDAO) {
    super(CORE_POOL_SIZE);
    this.queryService = LensServices.get().getService(QueryExecutionService.NAME);
    this.schedulerService = LensServices.get().getService(SchedulerService.NAME);
    this.sessionService = LensServices.get().getService(SessionService.NAME);
    this.cubeMetastoreService = LensServices.get().getService(CubeMetastoreService.NAME);
    this.schedulerDAO = schedulerDAO;
  }

  private void setSessionConf(LensSessionHandle sessionHandle, XJob job) throws LensException {
    XExecution execution = job.getExecution();
    XSessionType executionSession = execution.getSession();
    cubeMetastoreService.setCurrentDatabase(sessionHandle, executionSession.getDb());
    List<MapType> sessionConfList = executionSession.getConf();
    for (MapType element : sessionConfList) {
      sessionService.setSessionParameter(sessionHandle, element.getKey(), element.getValue());
    }
    List<ResourcePath> resourceList = executionSession.getResourcePath();
    for (ResourcePath path : resourceList) {
      sessionService.addResource(sessionHandle, path.getType(), path.getPath());
    }
  }

  private LensConf getLensConf(XJob job, SchedulerJobInstanceHandle instanceHandle, DateTime scheduledTime) {
    List<MapType> jobConf = job.getExecution().getQuery().getConf();
    LensConf queryConf = new LensConf();
    for (MapType element : jobConf) {
      queryConf.addProperty(element.getKey(), element.getValue());
    }
    queryConf.addProperty(JOB_INSTANCE_ID_KEY, instanceHandle.getHandleId());
    // Current time is used for resolving date.
    queryConf.addProperty(LensConfConstants.QUERY_CURRENT_TIME_IN_MILLIS, scheduledTime.getMillis());
    return queryConf;
  }

  /**
   * @param event the event
   */
  @Override
  public void process(SchedulerAlarmEvent event) {
    DateTime scheduledTime = event.getNominalTime();
    SchedulerJobHandle jobHandle = event.getJobHandle();
    /*
     * Get the job from the store.
     * Create an instance.
     * Store the instance.
     * Try to run the instance.
     * If successfully submitted change the status to running.
     * Otherwise update the status to killed.
     */
    XJob job = schedulerDAO.getJob(jobHandle);
    String user = schedulerDAO.getUser(jobHandle);
    SchedulerJobInstanceHandle instanceHandle = event.getPreviousInstance() == null
                                                ? UtilityMethods.generateSchedulerJobInstanceHandle()
                                                : event.getPreviousInstance();
    SchedulerJobInstanceInfo instance = null;
    SchedulerJobInstanceRun run = null;
    LensSessionHandle sessionHandle = null;

    try {
      sessionHandle = schedulerService.openSessionAsUser(user);
      setSessionConf(sessionHandle, job);
      if (event.getType() == SchedulerAlarmEvent.EventType.EXPIRE) {
        try {
          log.info("Expiring job with handle {}", jobHandle);
          schedulerService.expireJob(sessionHandle, jobHandle);
        } catch (LensException e) {
          log.error("Error while expiring the job", e);
        }
        return;
      }
      long scheduledTimeMillis = scheduledTime.getMillis();
      // If the instance is new then create otherwise get from the store
      if (event.getPreviousInstance() == null) {
        instance = new SchedulerJobInstanceInfo(instanceHandle, jobHandle, scheduledTimeMillis,
          new ArrayList<SchedulerJobInstanceRun>());
        // Store the instance
        if (schedulerDAO.storeJobInstance(instance) != 1) {
          log.error("Store was unsuccessful for instance {} of job {} ", instanceHandle, jobHandle);
          return;
        }
      } else {
        instance = schedulerDAO.getSchedulerJobInstanceInfo(instanceHandle);
      }
      LensConf queryConf = getLensConf(job, instanceHandle, scheduledTime);
      // Query Launch
      String query = job.getExecution().getQuery().getQuery();
      String queryName = job.getName();
      queryName += "-" + scheduledTimeMillis;
      // Fetch the latest run and if it is in waiting state then don't create a new run.
      run = instance.getInstanceRunList().size() == 0
            ? null
            : instance.getInstanceRunList().get(instance.getInstanceRunList().size() - 1);
      if (run == null || run.getInstanceState() != SchedulerJobInstanceState.LAUNCHING) {
        // Next run of the instance
        // If not true means run is in waiting state, so we don't need to create a new run.
        long currentTime = System.currentTimeMillis();
        run = new SchedulerJobInstanceRun(instanceHandle, instance.getInstanceRunList().size() + 1, null, currentTime,
          currentTime, "N/A", null, SchedulerJobInstanceState.LAUNCHING);
        instance.getInstanceRunList().add(run);
        if (schedulerDAO.storeJobInstanceRun(run) != 1) {
          log.error("Exception occurred while storing the instance run for instance handle {} of job {}", instance,
            jobHandle);
          return;
        }
      }
      run.setSessionHandle(sessionHandle);
      // Check for the data availability.
      try {
        queryService.estimate(LensServices.get().getLogSegregationContext().getLogSegragationId(), sessionHandle, query,
          queryConf);
      } catch (LensException e) {
        if (e.getErrorCode() == LensCubeErrorCode.NO_CANDIDATE_FACT_AVAILABLE.getLensErrorInfo().getErrorCode()) {
          // This error code suggests that the data is not available.
          run.setInstanceState(run.getInstanceState().nextTransition(SchedulerJobInstanceEvent.ON_CONDITIONS_NOT_MET));
          run.setEndTime(System.currentTimeMillis());
          schedulerDAO.updateJobInstanceRun(run);
          return;
        } else {
          throw e;
        }
      }

      QueryHandle handle = queryService.executeAsync(sessionHandle, query, queryConf, queryName);
      log.info("Running instance {} of job {} with run {} with query handle {}", instanceHandle, jobHandle,
        run.getRunId(), handle);
      run.setQueryHandle(handle);
      run.setInstanceState(run.getInstanceState().nextTransition(SchedulerJobInstanceEvent.ON_RUN));
      run.setEndTime(System.currentTimeMillis());
      // Update run
      schedulerDAO.updateJobInstanceRun(run);
      log.info("Successfully updated instance run with instance {} of job {}", instanceHandle, jobHandle);
    } catch (LensException | InvalidStateTransitionException e) {
      log.error(
        "Exception occurred while launching the job instance for " + jobHandle + " for nominal time " + scheduledTime
          .getMillis(), e);
      try {
        run.setInstanceState(run.getInstanceState().nextTransition(SchedulerJobInstanceEvent.ON_FAILURE));
        run.setEndTime(System.currentTimeMillis());
        schedulerDAO.updateJobInstanceRun(run);
      } catch (InvalidStateTransitionException e1) {
        log.error("Can't make transition for instance " + instance.getId() + " of job " + instance.getJobId(), e);
      }
    } finally {
      // Session needs to be closed after the launch.
      try {
        sessionService.closeSession(sessionHandle);
      } catch (LensException e) {
        log.error("Error closing session ", e);
      }
    }
  }
}

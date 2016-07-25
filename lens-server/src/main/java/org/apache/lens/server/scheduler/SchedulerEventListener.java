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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.error.InvalidStateTransitionException;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.scheduler.*;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.AsyncEventListener;
import org.apache.lens.server.api.events.SchedulerAlarmEvent;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.api.scheduler.SchedulerService;
import org.apache.lens.server.query.QueryExecutionServiceImpl;
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

  public SchedulerEventListener(SchedulerDAO schedulerDAO) {
    super(CORE_POOL_SIZE);
    this.queryService = LensServices.get().getService(QueryExecutionService.NAME);
    this.schedulerService = LensServices.get().getService(SchedulerService.NAME);
    this.schedulerDAO = schedulerDAO;
  }

  /**
   * @param event the event
   */
  @Override
  public void process(SchedulerAlarmEvent event) {
    DateTime scheduledTime = event.getNominalTime();
    SchedulerJobHandle jobHandle = event.getJobHandle();
    if (event.getType() == SchedulerAlarmEvent.EventType.EXPIRE) {
      try {
        schedulerService.expireJob(null, jobHandle);
      } catch (LensException e) {
        log.error("Error while expiring the job", e);
      }
      return;
    }
    /*
     * Get the job from the store.
     * Create an instance.
     * Store the instance.
     * Try to run the instance.
     * If successfully submitted change the status to running.
     * Otherwise update the status to killed.
     */
    //TODO: Get the job status and if it is not Scheduled, don't do anything.
    XJob job = schedulerDAO.getJob(jobHandle);
    String user = schedulerDAO.getUser(jobHandle);
    SchedulerJobInstanceHandle instanceHandle = event.getPreviousInstance() == null
                                                ? UtilityMethods.generateSchedulerJobInstanceHandle()
                                                : event.getPreviousInstance();
    Map<String, String> conf = new HashMap<>();
    LensSessionHandle sessionHandle = null;
    try {
      // Open the session with the user who submitted the job.
      sessionHandle = ((QueryExecutionServiceImpl) LensServices.get().getService(QueryExecutionServiceImpl.NAME))
          .openSession(user, "dummy", conf, false);
    } catch (LensException e) {
      log.error("Error occurred while opening a session ", e);
      return;
    }
    SchedulerJobInstanceInfo instance = null;
    SchedulerJobInstanceRun run = null;
    // Session needs to be closed after the launch.
    try {
      long scheduledTimeMillis = scheduledTime.getMillis();
      String query = job.getExecution().getQuery().getQuery();
      List<MapType> jobConf = job.getExecution().getQuery().getConf();
      LensConf queryConf = new LensConf();
      for (MapType element : jobConf) {
        queryConf.addProperty(element.getKey(), element.getValue());
      }
      queryConf.addProperty(JOB_INSTANCE_ID_KEY, instanceHandle.getHandleId());
      // Current time is used for resolving date.
      queryConf.addProperty(LensConfConstants.QUERY_CURRENT_TIME_IN_MILLIS, scheduledTime.getMillis());
      String queryName = job.getName();
      queryName += "-" + scheduledTime.getMillis();
      // If the instance is new then create otherwise get from the store
      if (event.getPreviousInstance() == null) {
        instance = new SchedulerJobInstanceInfo(instanceHandle, jobHandle, scheduledTimeMillis,
            new ArrayList<SchedulerJobInstanceRun>());
      } else {
        instance = schedulerDAO.getSchedulerJobInstanceInfo(instanceHandle);
      }
      // Next run of the instance
      long currentTime = System.currentTimeMillis();
      run = new SchedulerJobInstanceRun(instanceHandle, instance.getInstanceRunList().size() + 1, sessionHandle,
          currentTime, 0, "N/A", null, SchedulerJobInstanceState.WAITING);
      instance.getInstanceRunList().add(run);
      boolean success;
      if (event.getPreviousInstance() == null) {
        success = schedulerDAO.storeJobInstance(instance) == 1;
        if (!success) {
          log.error(
              "Exception occurred while storing the instance for instance handle " + instance + " of job " + jobHandle);
          return;
        }
      }
      success = schedulerDAO.storeJobInstanceRun(run) == 1;
      if (!success) {
        log.error(
            "Exception occurred while storing the instance for instance handle " + instance + " of job " + jobHandle);
        return;
      }

      QueryHandle handle = queryService.executeAsync(sessionHandle, query, queryConf, queryName);
      run.setQueryHandle(handle);
      run.setInstanceState(run.getInstanceState().nextTransition(SchedulerJobInstanceEvent.ON_RUN));
      run.setEndTime(System.currentTimeMillis());
      schedulerDAO.updateJobInstanceRun(run);
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
      try {
        ((QueryExecutionServiceImpl) LensServices.get().getService(QueryExecutionServiceImpl.NAME))
            .closeSession(sessionHandle);
      } catch (LensException e) {
        log.error("Error closing session ", e);
      }
    }
  }
}

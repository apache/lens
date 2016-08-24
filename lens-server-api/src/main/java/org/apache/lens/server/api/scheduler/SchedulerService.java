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
package org.apache.lens.server.api.scheduler;

import java.util.Collection;
import java.util.List;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.scheduler.*;
import org.apache.lens.server.api.LensService;
import org.apache.lens.server.api.SessionValidator;
import org.apache.lens.server.api.error.LensException;

/**
 * Scheduler interface.
 */
public interface SchedulerService extends LensService, SessionValidator {

  /**
   * The constant name for scheduler service.
   */
  String NAME = "scheduler";

  /**
   * Submit a job.
   *
   * @param sessionHandle handle for this session.
   * @param job           job to be submitted.
   * @return unique id for the submitted job.
   * @throws LensException the lens exception
   */
  SchedulerJobHandle submitJob(LensSessionHandle sessionHandle, XJob job) throws LensException;

  /**
   * Schedule a job.
   *
   * @param sessionHandle handle for the current session.
   * @param jobHandle     handle for the job to be scheduled.
   * @throws LensException the lens exception
   */
  void scheduleJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException;

  /**
   * Submit a job and also schedule it.
   *
   * @param sessionHandle handle for the session.
   * @param job           job definition.
   * @return unique id of the job which is submitted and scheduled.
   * @throws LensException the lens exception
   */
  SchedulerJobHandle submitAndScheduleJob(LensSessionHandle sessionHandle, XJob job) throws LensException;

  /**
   * Returns the definition of a job.
   *
   * @param jobHandle     handle for the job
   * @return job definition
   * @throws LensException the lens exception
   */
  XJob getJobDefinition(SchedulerJobHandle jobHandle) throws LensException;

  /**
   * Returns the details of a job. Details may contain extra system information like id for the job.
   *
   * @param jobHandle     handle for the job
   * @return job details for the job
   * @throws LensException the lens exception
   */
  SchedulerJobInfo getJobDetails(SchedulerJobHandle jobHandle) throws LensException;

  /**
   * Update a job with new definition.
   * <p>
   * Updates will be applied only for newer instances. Running instances will be running with old definition
   *
   * @param sessionHandle
   * @param jobHandle        handle for the job which you want to update.
   * @param newJobDefinition
   * @throws LensException the lens exception
   */
  void updateJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle, XJob newJobDefinition)
    throws LensException;

  /**
   * End a job by specifying an expiry time.
   *
   * @param sessionHandle handle for the current session.
   * @param jobHandle     handle for the job
   * @throws LensException the lens exception
   */
  void expireJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException;

  /**
   * Suspend a job.
   * <p>
   * If the job is not in scheduled state, it will return true.
   * Once a job is suspended, no further instances of that job will run.
   * Any running instances of that job will continue normally.
   *
   * @param sessionHandle handle for the current session.
   * @param jobHandle     handle for the job
   * @throws LensException the lens exception
   */
  void suspendJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException;

  /**
   * Resume a job from a given time.
   *
   * @param sessionHandle handle for the session.
   * @param jobHandle     handle for the job
   * @throws LensException the lens exception
   */
  void resumeJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException;

  /**
   * Delete a job.
   *
   * @param sessionHandle handle for the session.
   * @param jobHandle     handle for the job
   * @throws LensException the lens exception
   */
  void deleteJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException;

  /**
   * @param state         filter for status, if specified only jobs in that state will be returned,
   *                      if null no entries will be removed from result
   * @param user          filter for user who submitted the job, if specified only jobs submitted by the given user
   *                      will be returned, if not specified no entries will be removed from result on basis of userName
   * @param jobName       filter for jobName, if specified only the jobs with name same as given name will be considered
   *                      , else no jobs will be filtered out on the basis of name.
   * @param startTime     if specified only instances with scheduleTime after this time will be considered.
   * @param endTime       if specified only instances with scheduleTime before this time will be considered.
   * @return A collection of stats per job
   * @throws LensException
   */
  Collection<SchedulerJobStats> getAllJobStats(String state, String user,
      String jobName, long startTime, long endTime) throws LensException;

  /**
   * Returns stats for a job.
   *
   * @param handle        handle for the job
   * @param state         filter for status, if specified only jobs in that state will be returned,
   *                      if null no entries will be removed from result
   * @param startTime     if specified only instances with scheduleTime after this time will be considered.
   * @param endTime       if specified only instances with scheduleTime before this time will be considered.
   * @throws LensException the lens exception
   */
  SchedulerJobStats getJobStats(SchedulerJobHandle handle, String state,
      long startTime, long endTime) throws LensException;

  /**
   * Returns handles for last <code>numResults</code> instances for the job.
   *
   * @param jobHandle     handle for the job
   * @param numResults    - number of results to be returned, default 100.
   * @return list of instance ids for the job
   * @throws LensException the lens exception
   */
  List<SchedulerJobInstanceInfo> getJobInstances(SchedulerJobHandle jobHandle,
      Long numResults) throws LensException;

  /**
   * Kills a running job instance.
   * <p>
   * If the job instance is already completed or not in running state, this will be a no-op and will return false.
   *
   * @param sessionHandle  handle for the session.
   * @param instanceHandle handle for the instance
   * @throws LensException the lens exception
   */
  boolean killInstance(LensSessionHandle sessionHandle, SchedulerJobInstanceHandle instanceHandle) throws LensException;

  /**
   * Reruns a failed/killed/completed job instance.
   * <p>
   * If the instance is not in a terminal state, then this operation will be a no-op and will return false.
   *
   * @param sessionHandle  handle for the session.
   * @param instanceHandle handle for the instance
   * @throws LensException the lens exception
   */
  void rerunInstance(LensSessionHandle sessionHandle, SchedulerJobInstanceHandle instanceHandle)
    throws LensException;

  /**
   * Instance details for an instance.
   *
   * @param instanceHandle handle for the instance.
   * @return details for the instance.
   * @throws LensException the lens exception
   */
  SchedulerJobInstanceInfo getInstanceDetails(SchedulerJobInstanceHandle instanceHandle) throws LensException;

  /**
   * Create session as user for scheduling the job with no auth.
   * @param user
   * @return LensSessionHandle
   */
  LensSessionHandle openSessionAsUser(String user) throws LensException;

  /**
   * Get all jobs matching the filter parameters.
   * @param user
   * @param state
   * @param start
   * @param end
   * @return List of all job handles matching the parameters.
   */
  List<SchedulerJobHandle> getAllJobs(String user, SchedulerJobState state, Long start, Long end) throws LensException;
}

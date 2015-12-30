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
package org.apache.lens.server.api.scheduler;

import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.SchedulerJobHandle;
import org.apache.lens.api.query.SchedulerJobInfo;
import org.apache.lens.api.query.SchedulerJobInstanceHandle;
import org.apache.lens.api.query.SchedulerJobInstanceInfo;
import org.apache.lens.api.scheduler.XJob;
import org.apache.lens.server.api.error.LensException;


/**
 * Scheduler interface.
 */
public interface SchedulerService {

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
   * @param sessionHandle handle for the session.
   * @param jobHandle     handle for the job
   * @return job definition
   * @throws LensException the lens exception
   */
  XJob getJobDefinition(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException;


  /**
   * Returns the details of a job. Details may contain extra system information like id for the job.
   *
   * @param sessionHandle handle for the session.
   * @param jobHandle     handle for the job
   * @return job details for the job
   * @throws LensException the lens exception
   */
  SchedulerJobInfo getJobDetails(LensSessionHandle sessionHandle,
                                 SchedulerJobHandle jobHandle) throws LensException;

  /**
   * Update a job with new definition.
   *
   * Updates will be applied only for newer instances. Running instances will be running with old definition
   *
   * @param sessionHandle
   * @param jobHandle        handle for the job which you want to update.
   * @param newJobDefinition
   * @return true or false based on whether the update was successful or failed.
   * @throws LensException the lens exception
   */
  boolean updateJob(LensSessionHandle sessionHandle,
                    SchedulerJobHandle jobHandle, XJob newJobDefinition) throws LensException;


  /**
   * End a job by specifying an expiry time.
   *
   * @param sessionHandle handle for the current session.
   * @param jobHandle     handle for the job
   * @param expiryTime    time after which the job shouldn't execute.
   * @throws LensException the lens exception
   */
  void expireJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle,
                 Date expiryTime) throws LensException;


  /**
   * Suspend a job.
   *
   * If the job is not in scheduled state, it will return true.
   * Once a job is suspended, no further instances of that job will run.
   * Any running instances of that job will continue normally.
   *
   * @param sessionHandle handle for the current session.
   * @param jobHandle     handle for the job
   * @return true if the job was suspended successfully, false otherwise.
   * @throws LensException the lens exception
   */
  boolean suspendJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException;


  /**
   * Resume a job from a given time.
   *
   * @param sessionHandle handle for the session.
   * @param jobHandle     handle for the job
   * @param effectiveTime time from which to resume the instances.
   * @return true if the job was resumed successfully, false otherwise.
   * @throws LensException the lens exception
   */
  boolean resumeJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle,
                    Date effectiveTime) throws LensException;

  /**
   * Delete a job.
   *
   * @param sessionHandle handle for the session.
   * @param jobHandle     handle for the job
   * @return true if the job was deleted successfully.
   * @throws LensException the lens exception
   */
  boolean deleteJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException;


  /**
   * @param sessionHandle handle for the current session.
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
  Collection<SchedulerJobStats> getAllJobStats(LensSessionHandle sessionHandle,
                                      String state, String user,
                                      String jobName, long startTime, long endTime) throws LensException;

  /**
   * Returns stats for a job.
   *
   * @param sessionHandle handle for session.
   * @param handle        handle for the job
   * @param state         filter for status, if specified only jobs in that state will be returned,
   *                      if null no entries will be removed from result
   * @param startTime     if specified only instances with scheduleTime after this time will be considered.
   * @param endTime       if specified only instances with scheduleTime before this time will be considered.
   * @throws LensException the lens exception
   */
  SchedulerJobStats getJobStats(LensSessionHandle sessionHandle, SchedulerJobHandle handle,
                       String state, long startTime, long endTime) throws LensException;


  /**
   * Returns handles for last <code>numResults</code> instances for the job.
   *
   * @param sessionHandle handle for the session.
   * @param jobHandle     handle for the job
   * @param numResults    - number of results to be returned, default 100.
   * @return list of instance ids for the job
   * @throws LensException the lens exception
   */
  List<String> getJobInstances(LensSessionHandle sessionHandle,
                               SchedulerJobHandle jobHandle, Long numResults) throws LensException;

  /**
   * Kills a running job instance.
   *
   * If the job instance is already completed or not in running state, this will be a no-op and will return false.
   *
   * @param sessionHandle  handle for the session.
   * @param instanceHandle handle for the instance
   * @return true if the instance was killed successfully, false otherwise.
   * @throws LensException the lens exception
   */
  boolean killInstance(LensSessionHandle sessionHandle,
                        SchedulerJobInstanceHandle instanceHandle) throws LensException;

  /**
   * Reruns a failed/killed/completed job instance.
   *
   * If the instance is not in a terminal state, then this operation will be a no-op and will return false.
   *
   * @param sessionHandle  handle for the session.
   * @param instanceHandle handle for the instance
   * @return true if the instance was re run successfully, false otherwise.
   * @throws LensException the lens exception
   */
  boolean rerunInstance(LensSessionHandle sessionHandle,
                        SchedulerJobInstanceHandle instanceHandle) throws LensException;

  /**
   * Instance details for an instance.
   *
   * @param sessionHandle  handle for the session.
   * @param instanceHandle handle for the instance.
   * @return details for the instance.
   * @throws LensException the lens exception
   */
  SchedulerJobInstanceInfo getInstanceDetails(LensSessionHandle sessionHandle,
                                              SchedulerJobInstanceHandle instanceHandle) throws LensException;


}

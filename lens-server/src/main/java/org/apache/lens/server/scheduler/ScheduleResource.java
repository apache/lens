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

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.api.scheduler.*;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.scheduler.SchedulerService;
import org.apache.lens.server.error.UnSupportedOpException;
import org.apache.lens.server.model.LogSegregationContext;
import org.apache.lens.server.util.UtilityMethods;

/**
 * REST end point for all scheduler operations.
 */
@Path("scheduler")
@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
public class ScheduleResource {

  private final LogSegregationContext logSegregationContext;
  private final SchedulerService schedulerService;

  public ScheduleResource() {
    this.logSegregationContext = LensServices.get().getLogSegregationContext();
    this.schedulerService = LensServices.get().getService(SchedulerService.NAME);
  }

  private void validateSession(LensSessionHandle sessionHandle) throws LensException {
    schedulerService.validateSession(sessionHandle);
  }

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getMessage() {
    return "Scheduler is running.";
  }

  /**
   * Submits a job to be scheduled at later point of time or submit and schedule simultaneously.
   *
   * @param sessionId Session ID, the logged-in user will be set as the owner of this job
   * @param action    Action could be submit for storing the job or it could be "submit-and-schedule" to schedule it
   *                  just after submitting the job.
   * @param job       XJob definition of the job to be submitted
   * @return A newly generated job handle for the job. Job handle is the unique ID of a job.
   * @throws LensException
   */
  @POST
  @Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
  @Path("jobs")
  public LensAPIResult<SchedulerJobHandle> submitJob(@QueryParam("sessionid") LensSessionHandle sessionId,
    @QueryParam("action") String action, XJob job) throws LensException {
    validateSession(sessionId);
    SUBMIT_ACTION op = UtilityMethods.checkAndGetOperation(action, SUBMIT_ACTION.class, SUBMIT_ACTION.values());
    SchedulerJobHandle jobHandle;
    switch (op) {
    case SUBMIT:
      jobHandle = schedulerService.submitJob(sessionId, job);
      break;
    case SUBMIT_AND_SCHEDULE:
      jobHandle = schedulerService.submitAndScheduleJob(sessionId, job);
      break;
    default:
      throw new UnSupportedOpException(SUBMIT_ACTION.values());
    }
    return LensAPIResult.composedOf(null, this.logSegregationContext.getLogSegragationId(), jobHandle);
  }

  /**
   * Get all job handles matching user, state and end_time >job_submission_time >=start time.
   * If any of the values are null, it will not be considered while filtering.
   * For example: user is "test" and state is null then it will return job handles irrespective of state of the job.
   *
   * @param sessionHandle Session ID
   * @param user          User of the job
   * @param state         State of job: for example: SUCCEED or EXPIRED
   * @param start         Submission time should be grater than or equal to start time
   * @param end           Submission time should be strictly less than the end time.
   * @return A list of all jobs matching the filtering criteria.
   * @throws LensException
   */

  @GET
  @Path("jobs")
  public List<SchedulerJobHandle> getAllJobs(@QueryParam("sessionid") LensSessionHandle sessionHandle,
    @QueryParam("user") String user, @QueryParam("state") SchedulerJobState state, @QueryParam("start") Long start,
    @QueryParam("end") Long end) throws LensException {
    validateSession(sessionHandle);
    return schedulerService.getAllJobs(user, state, start, end);
  }

  /**
   * Get all job stats
   *
   * @param sessionId Session ID
   * @param status    Job status
   * @param jobName   Name of the job
   * @param user      User of the job
   * @param start     start time
   * @param end       end time
   * @return A list of SchedulerJobStats
   * @throws LensException
   */
  @GET
  @Path("jobs/stats")
  public Collection<SchedulerJobStats> getAllJobStats(@QueryParam("sessionid") LensSessionHandle sessionId,
    @DefaultValue("running") @QueryParam("status") String status, @QueryParam("name") String jobName,
    @DefaultValue("user") @QueryParam("user") String user, @DefaultValue("-1") @QueryParam("start") long start,
    @DefaultValue("-1") @QueryParam("end") long end) throws LensException {
    validateSession(sessionId);
    return schedulerService.getAllJobStats(status, user, jobName, start, end);
  }

  /**
   * Get XJob definition for a given job handle.
   *
   * @param sessionId SessionID
   * @param jobHandle Job handle
   * @return XJob definition
   * @throws LensException
   */
  @GET
  @Path("jobs/{jobHandle}")
  public LensAPIResult<XJob> getJobDefinition(@QueryParam("sessionid") LensSessionHandle sessionId,
    @PathParam("jobHandle") SchedulerJobHandle jobHandle) throws LensException {
    validateSession(sessionId);
    XJob job = schedulerService.getJobDefinition(jobHandle);
    return LensAPIResult.composedOf(null, this.logSegregationContext.getLogSegragationId(), job);
  }

  /**
   * Marks the job for deletion. Jobs are not deleted immediately, rather they are marked for deletion.
   * A deleted job is a dormant job which can't be elicited by any action.
   *
   * @param sessionId Session id
   * @param jobHandle Job handle
   * @return API result
   * @throws LensException
   */
  @DELETE
  @Path("jobs/{jobHandle}")
  public LensAPIResult deleteJob(@QueryParam("sessionid") LensSessionHandle sessionId,
    @PathParam("jobHandle") SchedulerJobHandle jobHandle) throws LensException {
    validateSession(sessionId);
    schedulerService.deleteJob(sessionId, jobHandle);
    final String requestId = this.logSegregationContext.getLogSegragationId();
    return LensAPIResult.composedOf(null, requestId, null);
  }

  /**
   * Updates job definition of an existing job. New definition can have new query and configurations.
   *
   * @param sessionId Session Id
   * @param jobHandle Job handle
   * @param job       New job definition.
   * @return LensAPIResult
   * @throws LensException
   */
  @PUT
  @Path("jobs/{jobHandle}/")
  @Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
  public LensAPIResult updateJob(@QueryParam("sessionid") LensSessionHandle sessionId,
    @PathParam("jobHandle") SchedulerJobHandle jobHandle, XJob job) throws LensException {
    validateSession(sessionId);
    schedulerService.updateJob(sessionId, jobHandle, job);
    final String requestId = this.logSegregationContext.getLogSegragationId();
    return LensAPIResult.composedOf(null, requestId, null);
  }

  /**
   * Changes the job state
   *
   * @param sessionId Session Id
   * @param jobHandle Job handle
   * @param action    An action can be SCHEDULE, EXPIRE, SUSPEND or RESUME
   * @return LensAPIResult
   * @throws LensException
   */
  @POST
  @Path("jobs/{jobHandle}")
  public LensAPIResult updateJob(@QueryParam("sessionid") LensSessionHandle sessionId,
    @PathParam("jobHandle") SchedulerJobHandle jobHandle, @DefaultValue("schedule") @QueryParam("action") String action)
    throws LensException {
    validateSession(sessionId);
    JOB_ACTION op = UtilityMethods.checkAndGetOperation(action, JOB_ACTION.class, JOB_ACTION.values());
    switch (op) {
    case SCHEDULE:
      schedulerService.scheduleJob(sessionId, jobHandle);
      break;

    case EXPIRE:
      schedulerService.expireJob(sessionId, jobHandle);
      break;

    case SUSPEND:
      schedulerService.suspendJob(sessionId, jobHandle);
      break;

    case RESUME:
      schedulerService.resumeJob(sessionId, jobHandle);
      break;

    default:
      throw new UnSupportedOpException(JOB_ACTION.values());
    }
    return LensAPIResult.composedOf(null, this.logSegregationContext.getLogSegragationId(), null);
  }

  /**
   * Returns the SchedulerJobInfo of a given job handle.
   *
   * @param sessionId Session ID
   * @param jobHandle Job handle
   * @return SchedulerJobinfo
   * @throws LensException
   */
  @GET
  @Path("jobs/{jobHandle}/info")
  public LensAPIResult<SchedulerJobInfo> getJobDetails(@QueryParam("sessionid") LensSessionHandle sessionId,
    @PathParam("jobHandle") SchedulerJobHandle jobHandle) throws LensException {
    validateSession(sessionId);
    SchedulerJobInfo info = schedulerService.getJobDetails(jobHandle);
    return LensAPIResult.composedOf(null, this.logSegregationContext.getLogSegragationId(), info);
  }

  /**
   * Returns all the instances of a job.
   *
   * @param sessionId  Session id
   * @param jobHandle  Job handle
   * @param numResults Number of results to be returned
   * @return A list of SchedulerInstanceInfo for a given job handle
   * @throws LensException
   */
  @GET
  @Path("jobs/{jobHandle}/instances/")
  public List<SchedulerJobInstanceInfo> getJobInstances(@QueryParam("sessionid") LensSessionHandle sessionId,
    @PathParam("jobHandle") SchedulerJobHandle jobHandle, @QueryParam("numResults") Long numResults)
    throws LensException {
    validateSession(sessionId);
    return schedulerService.getJobInstances(jobHandle, numResults);
  }

  /**
   * Returns a SchedulerInstanceInfo for a given instance handle.
   *
   * @param sessionId      Session ID
   * @param instanceHandle instance handle
   * @return SchedulerInstanceInfo
   * @throws LensException
   */
  @GET
  @Path("instances/{instanceHandle}")
  public LensAPIResult<SchedulerJobInstanceInfo> getInstanceDetails(
    @QueryParam("sessionid") LensSessionHandle sessionId,
    @PathParam("instanceHandle") SchedulerJobInstanceHandle instanceHandle) throws LensException {
    validateSession(sessionId);
    SchedulerJobInstanceInfo instance = schedulerService.getInstanceDetails(instanceHandle);
    return LensAPIResult.composedOf(null, this.logSegregationContext.getLogSegragationId(), instance);
  }

  /**
   * Updates an instance
   *
   * @param sessionId      Session ID
   * @param instanceHandle Instance handle
   * @param action         the value of action could be KILL or RERUN.
   * @return LensAPIResult
   * @throws LensException
   */
  @POST
  @Path("instances/{instanceHandle}")
  public LensAPIResult updateInstance(@QueryParam("sessionid") LensSessionHandle sessionId,
    @PathParam("instanceHandle") SchedulerJobInstanceHandle instanceHandle, @QueryParam("action") String action)
    throws LensException {
    boolean res = true;
    validateSession(sessionId);
    INSTANCE_ACTION op = UtilityMethods.checkAndGetOperation(action, INSTANCE_ACTION.class, INSTANCE_ACTION.values());
    switch (op) {
    case KILL:
      res = schedulerService.killInstance(sessionId, instanceHandle);
      break;
    case RERUN:
      schedulerService.rerunInstance(sessionId, instanceHandle);
      break;
    default:
      throw new UnSupportedOpException(INSTANCE_ACTION.values());
    }
    final String requestId = this.logSegregationContext.getLogSegragationId();
    return LensAPIResult.composedOf(null, requestId, res);
  }

  private enum SUBMIT_ACTION {
    SUBMIT, SUBMIT_AND_SCHEDULE;
  }

  private enum INSTANCE_ACTION {
    KILL, RERUN;
  }

  private enum JOB_ACTION {
    SCHEDULE, EXPIRE, SUSPEND, RESUME;
  }
}

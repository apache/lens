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
package org.apache.lens.server.scheduler;

import java.util.Collection;
import java.util.List;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.scheduler.*;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.scheduler.SchedulerService;

import org.apache.commons.lang3.StringUtils;

/**
 * REST end point for all scheduler operations.
 */
@Path("scheduler")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class ScheduleResource {

  public static enum INSTANCE_ACTIONS {
    KILL, RERUN;

    public static INSTANCE_ACTIONS fromString(String name) {
      return valueOf(name.toUpperCase());
    }
  }

  public static enum JOB_ACTIONS {
    SCHEDULE, EXPIRE, SUSPEND, RESUME;

    public static JOB_ACTIONS fromString(String name) {
      return valueOf(name.toUpperCase());
    }
  }

  public static SchedulerService getSchedulerService() {
    return LensServices.get().getService(SchedulerService.NAME);
  }

  private static void validateSession(LensSessionHandle sessionHandle) throws LensException {
    getSchedulerService().validateSession(sessionHandle);
  }

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getMessage() {
    return "Scheduler is running.";
  }

  @POST
  @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
  @Path("jobs")
  public SchedulerJobHandle submitJob(@QueryParam("sessionid") LensSessionHandle sessionId,
                                      @DefaultValue("") @QueryParam("action") String action,
                                      XJob job) throws LensException {
    validateSession(sessionId);
    if (StringUtils.isBlank(action)) {
      return getSchedulerService().submitJob(sessionId, job);
    } else if (StringUtils.equalsIgnoreCase(action, "submit-and-schedule")) {
      return getSchedulerService().submitAndScheduleJob(sessionId, job);
    } else {
      throw new BadRequestException("Optional Query param 'action' can only be 'submit-and-schedule'");
    }
  }

  @GET
  @Path("jobs/stats")
  public Collection<SchedulerJobStats> getAllJobStats(@QueryParam("sessionid") LensSessionHandle sessionId,
                                                @DefaultValue("running") @QueryParam("state") String state,
                                                @QueryParam("name") String jobName,
                                                @DefaultValue("user") @QueryParam("user") String user,
                                                @DefaultValue("-1") @QueryParam("start") long start,
                                                @DefaultValue("-1") @QueryParam("end") long end) throws LensException {
    return getSchedulerService().getAllJobStats(sessionId, state, user, start, end);
  }

  @GET
  @Path("jobs/{jobHandle}")
  public XJob getJobDefinition(@QueryParam("sessionid") LensSessionHandle sessionId,
                               @PathParam("jobHandle") SchedulerJobHandle jobHandle) throws LensException {

    return getSchedulerService().getJobDefinition(sessionId, jobHandle);
  }

  @DELETE
  @Path("jobs/{jobHandle}")
  public APIResult deleteJob(@QueryParam("sessionid") LensSessionHandle sessionId,
                             @QueryParam("jobHandle") SchedulerJobHandle jobHandle) throws LensException {
    validateSession(sessionId);
    getSchedulerService().deleteJob(sessionId, jobHandle);
    return APIResult.success();
  }

  @PUT
  @Path("jobs/{jobHandle}/")
  @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
  public APIResult updateJob(@QueryParam("sessionid") LensSessionHandle sessionId,
                             @PathParam("jobHandle") SchedulerJobHandle jobHandle, XJob job) throws LensException {
    validateSession(sessionId);
    getSchedulerService().updateJob(sessionId, jobHandle, job);
    return APIResult.success();
  }

  @POST
  @Path("jobs/{jobHandle}")
  public APIResult updateJob(@QueryParam("sessionid") LensSessionHandle sessionId,
                             @PathParam("jobHandle") SchedulerJobHandle jobHandle,
                             @DefaultValue("schedule") @QueryParam("action") JOB_ACTIONS action) throws LensException {
    validateSession(sessionId);
    switch (action) {

    case SCHEDULE:
      getSchedulerService().scheduleJob(sessionId, jobHandle);
      break;

    case EXPIRE:
      getSchedulerService().expireJob(sessionId, jobHandle);
      break;

    case SUSPEND:
      getSchedulerService().suspendJob(sessionId, jobHandle);
      break;

    case RESUME:
      getSchedulerService().resumeJob(sessionId, jobHandle);
      break;

    default:
      throw new BadRequestException("Unsupported action " + action.toString());
    }
    return APIResult.success();
  }

  @GET
  @Path("jobs/{jobHandle}/stats")
  public SchedulerJobInfo getJobDetails(@QueryParam("sessionid") LensSessionHandle sessionId,
                                        @PathParam("jobHandle") SchedulerJobHandle jobHandle) throws LensException {
    validateSession(sessionId);
    return getSchedulerService().getJobDetails(sessionId, jobHandle);
  }

  @GET
  @Path("jobs/{jobHandle}/instances/")
  public List<SchedulerJobInstanceInfo> getJobInstances(@QueryParam("sessionid") LensSessionHandle sessionId,
                                                      @PathParam("jobHandle") SchedulerJobHandle jobHandle,
                                                      @QueryParam("numResults") Long numResults) throws LensException {
    validateSession(sessionId);
    return getSchedulerService().getJobInstances(sessionId, jobHandle, numResults);
  }

  @GET
  @Path("instances/{instanceHandle}")
  public SchedulerJobInstanceInfo getInstanceDetails(@QueryParam("sessionid") LensSessionHandle sessionId,
                                                     @PathParam("instanceHandle")
                                                     SchedulerJobInstanceHandle instanceHandle) throws LensException {
    validateSession(sessionId);
    return getSchedulerService().getInstanceDetails(sessionId, instanceHandle);
  }

  @POST
  @Path("instances/{instanceHandle}")
  public APIResult updateInstance(@QueryParam("sessionid") LensSessionHandle sessionId,
                                @PathParam("instanceHandle") SchedulerJobInstanceHandle instanceHandle,
                                @QueryParam("action") INSTANCE_ACTIONS action) throws LensException {
    validateSession(sessionId);

    switch (action) {
    case KILL:
      getSchedulerService().killInstance(sessionId, instanceHandle);
      break;

    case RERUN:
      getSchedulerService().rerunInstance(sessionId, instanceHandle);
      break;

    default:
      throw new BadRequestException("Unsupported action " + action.toString());

    }
    return APIResult.success();
  }

}

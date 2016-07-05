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
import java.util.UUID;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.scheduler.*;
import org.apache.lens.server.BaseLensService;
import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.health.HealthStatus;
import org.apache.lens.server.api.scheduler.SchedulerService;
import org.apache.lens.server.session.LensSessionImpl;

import org.apache.hive.service.cli.CLIService;

/**
 * This class handles all the scheduler operations.
 */
public class SchedulerServiceImpl extends BaseLensService implements SchedulerService {

  // get the state store
  private SchedulerDAO schedulerDAO;

  private LensScheduler scheduler;
  /**
   * The constant name for scheduler service.
   */
  public static final String NAME = "scheduler";

  public SchedulerServiceImpl(CLIService cliService) throws LensException {
    super(NAME, cliService);
    this.schedulerDAO = new SchedulerDAO(LensServerConf.getHiveConf());
    this.scheduler = LensScheduler.get();
  }

  public SchedulerServiceImpl(CLIService cliService, SchedulerDAO schedulerDAO) {
    super(NAME, cliService);
    this.schedulerDAO = schedulerDAO;
    this.scheduler = LensScheduler.get();
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
    //TBD place holder code
    LensSessionImpl session = getSession(sessionHandle);
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void scheduleJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException {
    //TBD place holder code
    // send the schedule request to the scheduler.
    UUID externalID = jobHandle.getHandleId();
    // get the job from the database and schedule
  }

  @Override
  public SchedulerJobHandle submitAndScheduleJob(LensSessionHandle sessionHandle, XJob job) throws LensException {
    //TBD place holder code
    // take job, validate it, submit it(check duplicate, persist it), schedule it.
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public XJob getJobDefinition(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException {
    //TBD place holder code
    // get the job definition from the persisted store, return it.
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SchedulerJobInfo getJobDetails(LensSessionHandle sessionHandle,
                                        SchedulerJobHandle jobHandle) throws LensException {
    //TBD place holder code
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean updateJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle,
                           XJob newJobDefinition) throws LensException {
    //TBD place holder code
    XJob job = schedulerDAO.getJob(jobHandle);
    return false;
  }

  /**
   *
   * {@inheritDoc}
   */
  @Override
  public void expireJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException {
    //TBD place holder code
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean suspendJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException {
    //TBD place holder code
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean resumeJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException {
    // TBD place holder code
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean deleteJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException {
    // TBD place holder code
    // it should only be a soft delete. Later on we will make a purge service and that service will delete
    // all the soft delete things.
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<SchedulerJobStats> getAllJobStats(LensSessionHandle sessionHandle, String state, String userName,
                                             long startTime, long endTime) throws LensException {
    // TBD place holder code
    // validate that the state is a valid state (enum)
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SchedulerJobStats getJobStats(LensSessionHandle sessionHandle, SchedulerJobHandle handle, String state,
                              long startTime, long endTime) throws LensException {
    // TBD place holder code
    // validate that the state is a valid state (enum)
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean rerunInstance(LensSessionHandle sessionHandle,
                               SchedulerJobInstanceHandle instanceHandle) throws LensException {
    //TBD place holder code
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<SchedulerJobInstanceInfo> getJobInstances(LensSessionHandle sessionHandle,
                                      SchedulerJobHandle jobHandle, Long numResults) throws LensException {
    // TBD place holder code
    // By default return 100 results - make it configurable
    return null;
  }

  @Override
  public boolean killInstance(LensSessionHandle sessionHandle,
                              SchedulerJobInstanceHandle instanceHandle) throws LensException {
    // TBD place holder code
    return true;
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public SchedulerJobInstanceInfo getInstanceDetails(LensSessionHandle sessionHandle,
                                                     SchedulerJobInstanceHandle instanceHandle) throws LensException {
    // TBD place holder code
    return null;
  }

}

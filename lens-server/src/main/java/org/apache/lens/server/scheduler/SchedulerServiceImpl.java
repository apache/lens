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
import java.util.Date;
import java.util.List;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.SchedulerJobHandle;
import org.apache.lens.api.query.SchedulerJobInfo;
import org.apache.lens.api.query.SchedulerJobInstanceHandle;
import org.apache.lens.api.query.SchedulerJobInstanceInfo;
import org.apache.lens.api.scheduler.XJob;
import org.apache.lens.server.BaseLensService;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.health.HealthStatus;
import org.apache.lens.server.api.scheduler.SchedulerJobStats;
import org.apache.lens.server.api.scheduler.SchedulerService;

import org.apache.hive.service.cli.CLIService;
/**
 * The Class QuerySchedulerService.
 */
public class SchedulerServiceImpl extends BaseLensService implements SchedulerService {

  /**
   * The constant name for scheduler service.
   */
  public static final String NAME = "scheduler";

  /**
   * Instantiates a new scheduler service.
   *
   * @param cliService the cli service
   */
  public SchedulerServiceImpl(CLIService cliService) {
    super(NAME, cliService);
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
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void scheduleJob(LensSessionHandle sessionHandle,
                                        SchedulerJobHandle jobHandle) throws LensException {
  }

  @Override
  public SchedulerJobHandle submitAndScheduleJob(LensSessionHandle sessionHandle, XJob job) throws LensException {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public XJob getJobDefinition(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SchedulerJobInfo getJobDetails(LensSessionHandle sessionHandle,
                                        SchedulerJobHandle jobHandle) throws LensException {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean updateJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle,
                           XJob newJobDefinition) throws LensException {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void expireJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle,
                        Date expiryTime) throws LensException {

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean suspendJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean resumeJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle,
                           Date effectiveTime) throws LensException {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean deleteJob(LensSessionHandle sessionHandle, SchedulerJobHandle jobHandle) throws LensException {
    return false;
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
  public boolean rerunInstance(LensSessionHandle sessionHandle,
                               SchedulerJobInstanceHandle instanceHandle) throws LensException {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<String> getJobInstances(LensSessionHandle sessionHandle,
                                      SchedulerJobHandle jobHandle, Long numResults) throws LensException {
    return null;
  }

  @Override
  public boolean killInstance(LensSessionHandle sessionHandle,
                              SchedulerJobInstanceHandle instanceHandle) throws LensException {
    return false;
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public SchedulerJobInstanceInfo getInstanceDetails(LensSessionHandle sessionHandle,
                                                     SchedulerJobInstanceHandle instanceHandle) throws LensException {
    return null;
  }

}

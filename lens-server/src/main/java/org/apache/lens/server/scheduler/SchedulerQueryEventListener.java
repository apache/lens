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

import java.util.List;

import org.apache.lens.api.error.InvalidStateTransitionException;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.api.scheduler.*;
import org.apache.lens.server.api.events.AsyncEventListener;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.QueryEnded;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SchedulerQueryEventListener extends AsyncEventListener<QueryEnded> {
  private static final String JOB_INSTANCE_ID_KEY = "job_instance_key";
  private static final int CORE_POOL_SIZE = 10;
  private SchedulerDAO schedulerDAO;

  public SchedulerQueryEventListener(SchedulerDAO schedulerDAO) {
    super(CORE_POOL_SIZE);
    this.schedulerDAO = schedulerDAO;
  }

  @Override
  public void process(QueryEnded event) {
    if (event.getCurrentValue() == QueryStatus.Status.CLOSED) {
      return;
    }
    QueryContext queryContext = event.getQueryContext();
    if (queryContext == null) {
      log.warn("Could not find the context for {} for event:{}.", event.getQueryHandle(), event.getCurrentValue());
      return;
    }
    String instanceHandle = queryContext.getConf().get(JOB_INSTANCE_ID_KEY);
    if (instanceHandle == null) {
      // Nothing to do
      return;
    }
    SchedulerJobInstanceInfo info = schedulerDAO
      .getSchedulerJobInstanceInfo(SchedulerJobInstanceHandle.fromString(instanceHandle));
    List<SchedulerJobInstanceRun> runList = info.getInstanceRunList();
    if (runList.size() == 0) {
      log.error("No instance run for {} with query {}", instanceHandle, queryContext.getQueryHandle());
      return;
    }
    SchedulerJobInstanceRun latestRun = runList.get(runList.size() - 1);
    SchedulerJobInstanceState state = latestRun.getInstanceState();
    try {
      switch (event.getCurrentValue()) {
      case CANCELED:
        state = state.nextTransition(SchedulerJobInstanceEvent.ON_KILL);
        break;
      case SUCCESSFUL:
        state = state.nextTransition(SchedulerJobInstanceEvent.ON_SUCCESS);
        break;
      case FAILED:
        state = state.nextTransition(SchedulerJobInstanceEvent.ON_FAILURE);
        break;
      }
      latestRun.setEndTime(System.currentTimeMillis());
      latestRun.setInstanceState(state);
      latestRun.setResultPath(queryContext.getResultSetPath());
      schedulerDAO.updateJobInstanceRun(latestRun);
      log.info("Updated instance run {} for instance {} for job {} to {}", latestRun.getRunId(), info.getId(),
        info.getJobId(), state);
    } catch (InvalidStateTransitionException e) {
      log.error("Instance Transition Failed ", e);
    }
  }
}

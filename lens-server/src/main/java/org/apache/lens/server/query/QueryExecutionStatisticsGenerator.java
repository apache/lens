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
package org.apache.lens.server.query;

import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.AsyncEventListener;
import org.apache.lens.server.api.events.LensEventService;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.events.QueryEnded;
import org.apache.lens.server.stats.event.query.QueryDriverStatistics;
import org.apache.lens.server.stats.event.query.QueryExecutionStatistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Top level class which handles all Query Events.
 */
public class QueryExecutionStatisticsGenerator extends AsyncEventListener<QueryEnded> {

  /** The Constant LOG. */
  private static final Logger LOG = LoggerFactory.getLogger(QueryExecutionStatisticsGenerator.class);

  /** The event service. */
  private final LensEventService eventService;

  /**
   * Instantiates a new query execution statistics generator.
   *
   * @param eventService the event service
   */
  public QueryExecutionStatisticsGenerator(LensEventService eventService) {
    this.eventService = eventService;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.events.AsyncEventListener#process(org.apache.lens.server.api.events.LensEvent)
   */
  @Override
  public void process(QueryEnded ended) {
    if (ended.getCurrentValue() == QueryStatus.Status.CLOSED) {
      return;
    }
    QueryHandle handle = ended.getQueryHandle();
    QueryExecutionStatistics event = new QueryExecutionStatistics(System.currentTimeMillis());
    QueryContext ctx = ended.getQueryContext();
    if (ctx == null) {
      LOG.warn("Could not find the context for " + handle + " for event:" + ended.getCurrentValue()
        + ". No stat generated");
      return;
    }
    event.setEndTime(ctx.getEndTime());
    event.setStartTime(ctx.getLaunchTime());
    event.setStatus(ctx.getStatus());
    event.setCause(ended.getCause() != null ? ended.getCause() : "");
    event.setResult(ctx.getResultSetPath());
    event.setUserQuery(ctx.getUserQuery());
    event.setSessionId(ctx.getLensSessionIdentifier());
    event.setHandle(ctx.getQueryHandle().toString());
    event.setSubmitter(ctx.getSubmittedUser());
    event.setClusterUser(ctx.getClusterUser());
    event.setSubmissionTime(ctx.getSubmissionTime());
    QueryDriverStatistics driverStats = new QueryDriverStatistics();
    driverStats.setDriverQuery(ctx.getSelectedDriverQuery());
    driverStats.setStartTime(ctx.getDriverStatus().getDriverStartTime());
    driverStats.setEndTime(ctx.getDriverStatus().getDriverFinishTime());
    event.setDriverStats(driverStats);
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Notifying Statistics " + event);
      }
      eventService.notifyEvent(event);
    } catch (LensException e) {
      LOG.warn("Unable to notify Execution statistics", e);
    }
  }

}

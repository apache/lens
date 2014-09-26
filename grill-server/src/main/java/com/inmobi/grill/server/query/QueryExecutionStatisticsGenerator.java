package com.inmobi.grill.server.query;
/*
 * #%L
 * Grill Server
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.server.api.events.AsyncEventListener;
import com.inmobi.grill.server.api.events.GrillEventService;
import com.inmobi.grill.server.api.query.QueryContext;
import com.inmobi.grill.server.api.query.QueryEnded;
import com.inmobi.grill.server.stats.event.query.QueryDriverStatistics;
import com.inmobi.grill.server.stats.event.query.QueryExecutionStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Top level class which handles all Query Events
 */
public class QueryExecutionStatisticsGenerator extends AsyncEventListener<QueryEnded> {

  private static final Logger LOG = LoggerFactory.getLogger(QueryExecutionStatisticsGenerator.class);
  private final QueryExecutionServiceImpl queryService;
  private final GrillEventService eventService;

  public QueryExecutionStatisticsGenerator(QueryExecutionServiceImpl queryService,
                                           GrillEventService eventService) {
    this.queryService = queryService;
    this.eventService = eventService;
  }


  @Override
  public void process(QueryEnded ended) {
    if (ended.getCurrentValue() == QueryStatus.Status.CLOSED) {
      return;
    }
    QueryHandle handle = ended.getQueryHandle();
    QueryExecutionStatistics event = new QueryExecutionStatistics(
        System.currentTimeMillis());
    QueryContext ctx = queryService.getQueryContext(handle);
    if (ctx == null) {
      LOG.warn("Could not find the context for " + handle + " for event:"
        + ended.getCurrentValue() + ". No stat generated");
      return;
    }
    event.setEndTime(ctx.getEndTime());
    event.setStartTime(ctx.getLaunchTime());
    event.setStatus(ctx.getStatus());
    event.setCause(ended.getCause() != null ? ended.getCause() : "");
    event.setResult(ctx.getResultSetPath());
    event.setUserQuery(ctx.getUserQuery());
    event.setSessionId(ctx.getGrillSessionIdentifier());
    event.setHandle(ctx.getQueryHandle().toString());
    event.setSubmitter(ctx.getSubmittedUser());
    event.setClusterUser(ctx.getClusterUser());
    event.setSubmissionTime(ctx.getSubmissionTime());
    QueryDriverStatistics driverStats = new QueryDriverStatistics();
    driverStats.setDriverQuery(ctx.getDriverQuery());
    driverStats.setStartTime(ctx.getDriverStatus().getDriverStartTime());
    driverStats.setEndTime(ctx.getDriverStatus().getDriverStartTime());
    event.setDriverStats(driverStats);
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Notifying Statistics " + event);
      }
      eventService.notifyEvent(event);
    } catch (GrillException e) {
      LOG.warn("Unable to notify Execution statistics", e);
    }
  }


}

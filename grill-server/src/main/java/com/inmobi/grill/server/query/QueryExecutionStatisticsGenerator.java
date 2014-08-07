package com.inmobi.grill.server.query;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.QueryHandle;
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
public class QueryExecutionStatisticsGenerator extends AsyncEventListener <QueryEnded> {

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
    QueryHandle handle = ended.getQueryHandle();
    QueryExecutionStatistics event = new QueryExecutionStatistics(
        System.currentTimeMillis());
    QueryContext ctx = queryService.getQueryContext(handle);
    event.setEndTime(ctx.getEndTime());
    event.setStatus(ctx.getStatus());
    event.setCause(ended.getCause() != null ? ended.getCause() : "");
    event.setResult(ctx.getResultSetPath());
    event.setUserQuery(ctx.getUserQuery());
    event.setSessionId(ctx.getGrillSessionIdentifier());
    event.setHandle(ctx.getQueryHandle().toString());
    event.setSubmitter(ctx.getSubmittedUser());
    event.setSubmissionTime(ctx.getSubmissionTime());
    QueryDriverStatistics driverStats = new QueryDriverStatistics();
    driverStats.setDriverQuery(ctx.getDriverQuery());
    driverStats.setStartTime(ctx.getDriverStatus().getDriverStartTime());
    driverStats.setEndTime(ctx.getDriverStatus().getDriverStartTime());
    event.setDriverStats(driverStats);
    try {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Notifying Statistics " + event);
      }
      eventService.notifyEvent(event);
    } catch (GrillException e) {
      LOG.warn("Unable to notify Execution statistics", e);
    }
  }



}

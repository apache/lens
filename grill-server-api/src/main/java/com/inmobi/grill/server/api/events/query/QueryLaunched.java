package com.inmobi.grill.server.api.events.query;

import com.inmobi.grill.query.QueryHandle;
import com.inmobi.grill.query.QueryStatus;

/**
 * Event fired when query is LAUNCHED
 */
public class QueryLaunched extends StatusChange {
  public QueryLaunched(long eventTime, QueryStatus.Status prev,
      QueryStatus.Status current, QueryHandle handle) {
    super(eventTime, prev, current, handle);
    checkCurrentState(QueryStatus.Status.LAUNCHED);
  }
}

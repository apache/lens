package com.inmobi.grill.server.api.events;

import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryStatus;

/**
 * Event fired when query enters a RUNNING state
 */
public class QueryRunning extends StatusChange {
  public QueryRunning(QueryStatus.Status prev, QueryStatus.Status current, QueryHandle handle) {
    super(prev, current, handle);
    checkCurrentState(QueryStatus.Status.RUNNING);
  }
}

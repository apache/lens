package com.inmobi.grill.server.api.events;

import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryStatus;


/**
 * Event fired when query is LAUNCHED
 */
public class QueryLaunched extends StatusChange {
  public QueryLaunched(QueryStatus.Status prev, QueryStatus.Status current, QueryHandle handle) {
    super(prev, current, handle);
    checkCurrentState(QueryStatus.Status.LAUNCHED);
  }
}

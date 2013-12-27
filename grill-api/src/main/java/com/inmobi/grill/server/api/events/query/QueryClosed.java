package com.inmobi.grill.server.api.events.query;

import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryStatus;

/**
 * Event fired when a query is closed.
 */
public class QueryClosed extends QueryEnded {
  public QueryClosed(QueryStatus.Status prev, QueryStatus.Status current, QueryHandle handle, String user, Throwable cause) {
    super(prev, current, handle, user, cause);
    checkCurrentState(QueryStatus.Status.CLOSED);
  }
}

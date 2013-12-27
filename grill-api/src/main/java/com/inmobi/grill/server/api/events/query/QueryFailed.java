package com.inmobi.grill.server.api.events.query;


import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryStatus;

/**
 * Event fired when a query fails to execute. Use getCause() to get the reason for failure.
 */
public class QueryFailed extends QueryEnded {
  public QueryFailed(QueryStatus.Status prev, QueryStatus.Status current, QueryHandle handle, String user, Throwable cause) {
    super(prev, current, handle, user, cause);
    checkCurrentState(QueryStatus.Status.FAILED);
  }
}

package com.inmobi.grill.server.api.events.query;

import com.inmobi.grill.query.QueryHandle;
import com.inmobi.grill.query.QueryStatus;

/**
 * Event fired when a query fails to execute. Use getCause() to get the reason for failure.
 */
public class QueryFailed extends QueryEnded {
  public QueryFailed(long eventTime, QueryStatus.Status prev,
      QueryStatus.Status current, QueryHandle handle, String user, String cause) {
    super(eventTime, prev, current, handle, user, cause);
    checkCurrentState(QueryStatus.Status.FAILED);
  }
}

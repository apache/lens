package com.inmobi.grill.server.api.query;

import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;

/**
 * Event fired when query is cancelled
 */
public class QueryCancelled extends QueryEnded {
  public QueryCancelled(long eventTime, QueryStatus.Status prev,
      QueryStatus.Status current, QueryHandle handle, String user, String cause) {
    super(eventTime, prev, current, handle, user, cause);
    checkCurrentState(QueryStatus.Status.CANCELED);
  }

}

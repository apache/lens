package com.inmobi.grill.server.api.events.query;

import com.inmobi.grill.query.QueryHandle;
import com.inmobi.grill.query.QueryStatus;

/**
 * Event fired when query is successfully completed
 */
public class QuerySuccess extends QueryEnded {
  public QuerySuccess(long eventTime, QueryStatus.Status prev,
      QueryStatus.Status current, QueryHandle handle) {
    super(eventTime, prev, current, handle, null, null);
    checkCurrentState(QueryStatus.Status.SUCCESSFUL);
  }
}

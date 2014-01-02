package com.inmobi.grill.server.api.events.query;


import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryStatus;

/**
 * Event fired when query is successfully completed
 */
public class QuerySuccess extends QueryEnded {
  public QuerySuccess(QueryStatus.Status prev, QueryStatus.Status current, QueryHandle handle) {
    super(prev, current, handle, null, null);
    checkCurrentState(QueryStatus.Status.SUCCESSFUL);
  }

  public QuerySuccess(QueryStatus.Status prev, QueryStatus.Status current, QueryHandle handle, long endTime) {
    super(prev, current, handle, null, null, endTime);
    checkCurrentState(QueryStatus.Status.SUCCESSFUL);
  }
}

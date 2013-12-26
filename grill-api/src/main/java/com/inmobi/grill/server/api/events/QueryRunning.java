package com.inmobi.grill.server.api.events;

import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryStatus;

public class QueryRunning extends StatusChange {
  public QueryRunning(QueryStatus.Status prev, QueryStatus.Status current, QueryHandle handle) {
    super(prev, current, handle);
    checkCurrentState(QueryStatus.Status.RUNNING);
  }
}

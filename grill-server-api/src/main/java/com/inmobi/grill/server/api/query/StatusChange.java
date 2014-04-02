package com.inmobi.grill.server.api.query;

import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;

public abstract class StatusChange extends QueryEvent<QueryStatus.Status> {
  public StatusChange(long eventTime, QueryStatus.Status prev,
      QueryStatus.Status current, QueryHandle handle) {
    super(eventTime, prev, current, handle);
  }

  protected void checkCurrentState(QueryStatus.Status status) {
    if (currentValue != status) {
      throw new IllegalStateException("Invalid query state: " + currentValue
          + " query:" + queryHandle.toString());
    }
  }

}

package com.inmobi.grill.server.api.events;


import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryStatus;


public abstract class StatusChange extends QueryEvent<QueryStatus.Status> {
  public StatusChange(QueryStatus.Status prev, QueryStatus.Status current, QueryHandle handle) {
    super(prev, current, handle);
  }

  protected void checkCurrentState(QueryStatus.Status status) {
    if (currentValue != status) {
      throw new IllegalStateException("Invalid query state: " + currentValue
        + " query:" + handle.toString());
    }
  }


}

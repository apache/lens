package com.inmobi.grill.server.api.events;

import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryStatus;

public class QueryQueued extends StatusChange {
  private final String user;
  private final long time;

  public QueryQueued(QueryStatus.Status prev, QueryStatus.Status current, QueryHandle handle, String user, long time) {
    super(prev, current, handle);
    checkCurrentState(QueryStatus.Status.QUEUED);
    this.user = user;
    this.time = time;
  }

  public final String getUser() {
    return user;
  }

  public final long getTime() {
    return time;
  }
}

package com.inmobi.grill.server.api.events.query;

import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryStatus;

/**
 * Event fired when a query is QUEUED
 */
public class QueryQueued extends StatusChange {
  private final String user;
  private final long time;

  public QueryQueued(QueryStatus.Status prev, QueryStatus.Status current, QueryHandle handle, String user, long time) {
    super(prev, current, handle);
    checkCurrentState(QueryStatus.Status.QUEUED);
    this.user = user;
    this.time = time;
  }

  /**
   * Get the submitting user
   * @return
   */
  public final String getUser() {
    return user;
  }

  /**
   * Get the date when this query was submitted
   * @return
   */
  public final long getTime() {
    return time;
  }
}

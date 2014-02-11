package com.inmobi.grill.server.api.query;

import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;

/**
 * Event fired when a query is QUEUED
 */
public class QueryQueued extends StatusChange {
  private final String user;

  public QueryQueued(long eventTime, QueryStatus.Status prev,
      QueryStatus.Status current, QueryHandle handle, String user) {
    super(eventTime, prev, current, handle);
    checkCurrentState(QueryStatus.Status.QUEUED);
    this.user = user;
  }

  /**
   * Get the submitting user
   * @return
   */
  public final String getUser() {
    return user;
  }

}

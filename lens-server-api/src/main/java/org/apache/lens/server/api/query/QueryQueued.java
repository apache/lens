package org.apache.lens.server.api.query;

import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;

/**
 * Event fired when a query is QUEUED.
 */
public class QueryQueued extends StatusChange {

  /** The user. */
  private final String user;

  /**
   * Instantiates a new query queued.
   *
   * @param eventTime
   *          the event time
   * @param prev
   *          the prev
   * @param current
   *          the current
   * @param handle
   *          the handle
   * @param user
   *          the user
   */
  public QueryQueued(long eventTime, QueryStatus.Status prev, QueryStatus.Status current, QueryHandle handle,
      String user) {
    super(eventTime, prev, current, handle);
    checkCurrentState(QueryStatus.Status.QUEUED);
    this.user = user;
  }

  /**
   * Get the submitting user
   *
   * @return user
   */
  public final String getUser() {
    return user;
  }

}

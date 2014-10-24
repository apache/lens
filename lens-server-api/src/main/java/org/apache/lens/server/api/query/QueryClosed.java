package org.apache.lens.server.api.query;

import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;

/**
 * Event fired when a query is closed.
 */
public class QueryClosed extends QueryEnded {

  /**
   * Instantiates a new query closed.
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
   * @param cause
   *          the cause
   */
  public QueryClosed(long eventTime, QueryStatus.Status prev, QueryStatus.Status current, QueryHandle handle,
      String user, String cause) {
    super(eventTime, prev, current, handle, user, cause);
    checkCurrentState(QueryStatus.Status.CLOSED);
  }
}

package org.apache.lens.server.api.query;

import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;

/**
 * Event fired when query is LAUNCHED.
 */
public class QueryLaunched extends StatusChange {

  /**
   * Instantiates a new query launched.
   *
   * @param eventTime
   *          the event time
   * @param prev
   *          the prev
   * @param current
   *          the current
   * @param handle
   *          the handle
   */
  public QueryLaunched(long eventTime, QueryStatus.Status prev, QueryStatus.Status current, QueryHandle handle) {
    super(eventTime, prev, current, handle);
    checkCurrentState(QueryStatus.Status.LAUNCHED);
  }
}

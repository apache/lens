package org.apache.lens.server.api.query;

import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;

/**
 * Event fired when query is successfully completed.
 */
public class QuerySuccess extends QueryEnded {

  /**
   * Instantiates a new query success.
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
  public QuerySuccess(long eventTime, QueryStatus.Status prev, QueryStatus.Status current, QueryHandle handle) {
    super(eventTime, prev, current, handle, null, null);
    checkCurrentState(QueryStatus.Status.SUCCESSFUL);
  }
}

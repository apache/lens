package org.apache.lens.server.api.query;

import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;

/**
 * The Class StatusChange.
 */
public abstract class StatusChange extends QueryEvent<QueryStatus.Status> {

  /**
   * Instantiates a new status change.
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
  public StatusChange(long eventTime, QueryStatus.Status prev, QueryStatus.Status current, QueryHandle handle) {
    super(eventTime, prev, current, handle);
  }

  /**
   * Check current state.
   *
   * @param status
   *          the status
   */
  protected void checkCurrentState(QueryStatus.Status status) {
    if (currentValue != status) {
      throw new IllegalStateException("Invalid query state: " + currentValue + " query:" + queryHandle.toString());
    }
  }

}

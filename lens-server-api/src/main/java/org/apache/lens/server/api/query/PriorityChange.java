package org.apache.lens.server.api.query;

import org.apache.lens.api.Priority;
import org.apache.lens.api.query.QueryHandle;

/**
 * Event fired when query priority changes.
 */
public class PriorityChange extends QueryEvent<Priority> {

  /**
   * Instantiates a new priority change.
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
  public PriorityChange(long eventTime, Priority prev, Priority current, QueryHandle handle) {
    super(eventTime, prev, current, handle);
  }
}

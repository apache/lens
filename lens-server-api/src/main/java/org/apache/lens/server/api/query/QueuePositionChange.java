package org.apache.lens.server.api.query;

import org.apache.lens.api.query.QueryHandle;

/**
 * Event fired when query moves up or down in the execution engine's queue.
 */
public class QueuePositionChange extends QueryEvent<Integer> {

  /**
   * Instantiates a new queue position change.
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
  public QueuePositionChange(long eventTime, Integer prev, Integer current, QueryHandle handle) {
    super(eventTime, prev, current, handle);
  }
}

package org.apache.lens.server.api.query;

import org.apache.lens.api.query.QueryHandle;

/**
 * The Class QueryAccepted.
 */
public class QueryAccepted extends QueryEvent<String> {

  /**
   * Instantiates a new query accepted.
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
  public QueryAccepted(long eventTime, String prev, String current, QueryHandle handle) {
    super(eventTime, prev, current, handle);
  }
}

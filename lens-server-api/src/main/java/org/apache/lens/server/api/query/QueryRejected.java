package org.apache.lens.server.api.query;

import org.apache.lens.api.query.QueryHandle;

/**
 * The Class QueryRejected.
 */
public class QueryRejected extends QueryEvent<String> {

  /**
   * Instantiates a new query rejected.
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
  public QueryRejected(long eventTime, String prev, String current, QueryHandle handle) {
    super(eventTime, prev, current, handle);
  }
}

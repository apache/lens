package com.inmobi.grill.server.api.events.query;


import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryStatus;

import java.util.EnumSet;

/**
 * Generic event denoting that query has ended. If a listener wants to just be notified when query has ended
 * irrespective of its success or failure, then that listener can subscribe for this event type
 */
public class QueryEnded extends StatusChange {
  private final String user;
  private final String cause;

  public static final EnumSet<QueryStatus.Status> END_STATES =
    EnumSet.of(QueryStatus.Status.SUCCESSFUL,
      QueryStatus.Status.CANCELED, QueryStatus.Status.CLOSED, QueryStatus.Status.FAILED);

  public QueryEnded(long eventTime, QueryStatus.Status prev, QueryStatus.Status current, QueryHandle handle,
                    String user, String cause) {
    super(eventTime, prev, current, handle);
    this.user = user;
    this.cause = cause;
    if (!END_STATES.contains(current)) {
      throw new IllegalStateException("Not a valid end state: " + current + " query: " + handle);
    }
  }


  /**
   * If the query ended because of a user action, then this method will return the user id of requesting user
   * @return
   */
  public final String getUser() {
    return user;
  }

  /**
   * If the query ended because of an error, then this method should give the cause.
   * @return
   */
  public final String getCause() {
    return cause;
  }

}

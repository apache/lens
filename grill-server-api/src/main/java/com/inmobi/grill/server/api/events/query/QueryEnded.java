package com.inmobi.grill.server.api.events.query;

import com.inmobi.grill.query.QueryHandle;
import com.inmobi.grill.query.QueryStatus;

import java.util.EnumSet;

import lombok.Getter;

/**
 * Generic event denoting that query has ended. If a listener wants to just be notified when query has ended
 * irrespective of its success or failure, then that listener can subscribe for this event type
 */
public class QueryEnded extends StatusChange {
  @Getter private final String user;
  @Getter private final String cause;

  public static final EnumSet<QueryStatus.Status> END_STATES =
    EnumSet.of(QueryStatus.Status.SUCCESSFUL,
      QueryStatus.Status.CANCELED, QueryStatus.Status.CLOSED, QueryStatus.Status.FAILED);

  public QueryEnded(long eventTime, QueryStatus.Status prev,
      QueryStatus.Status current, QueryHandle handle,
                    String user, String cause) {
    super(eventTime, prev, current, handle);
    this.user = user;
    this.cause = cause;
    if (!END_STATES.contains(current)) {
      throw new IllegalStateException("Not a valid end state: " + current + " query: " + handle);
    }
  }
}

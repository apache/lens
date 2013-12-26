package com.inmobi.grill.server.api.events;


import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryStatus;

import java.util.EnumSet;

public class QueryEnded extends StatusChange {
  private final String user;
  private final Throwable cause;

  public static final EnumSet<QueryStatus.Status> END_STATES =
    EnumSet.of(QueryStatus.Status.SUCCESSFUL,
      QueryStatus.Status.CANCELED, QueryStatus.Status.CLOSED, QueryStatus.Status.FAILED);

  public QueryEnded(QueryStatus.Status prev, QueryStatus.Status current, QueryHandle handle,
                    String user, Throwable cause) {
    super(prev, current, handle);
    this.user = user;
    this.cause = cause;
    if (!END_STATES.contains(current)) {
      throw new IllegalStateException("Not a valid end state: " + current + " query: " + handle);
    }
  }

  public final String getUser() {
    return user;
  }

  public final Throwable getCause() {
    return cause;
  }

}

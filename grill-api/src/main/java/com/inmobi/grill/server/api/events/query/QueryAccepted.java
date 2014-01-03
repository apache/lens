package com.inmobi.grill.server.api.events.query;

import com.inmobi.grill.api.QueryHandle;

public class QueryAccepted extends QueryEvent<String> {
  public QueryAccepted(long eventTime, String prev, String current, QueryHandle handle) {
    super(eventTime, prev, current, handle);
  }
}

package com.inmobi.grill.server.api.query;

import com.inmobi.grill.api.query.QueryHandle;

public class QueryAccepted extends QueryEvent<String> {
  public QueryAccepted(long eventTime, String prev, String current, QueryHandle handle) {
    super(eventTime, prev, current, handle);
  }
}

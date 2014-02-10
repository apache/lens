package com.inmobi.grill.server.api.events.query;

import com.inmobi.grill.query.QueryHandle;

public class QueryRejected extends QueryEvent<String> {
  public QueryRejected(long eventTime, String prev, String current,
      QueryHandle handle) {
    super(eventTime, prev, current, handle);
  }
}

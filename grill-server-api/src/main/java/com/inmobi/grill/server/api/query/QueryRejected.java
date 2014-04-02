package com.inmobi.grill.server.api.query;

import com.inmobi.grill.api.query.QueryHandle;

public class QueryRejected extends QueryEvent<String> {
  public QueryRejected(long eventTime, String prev, String current,
      QueryHandle handle) {
    super(eventTime, prev, current, handle);
  }
}

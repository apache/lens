package com.inmobi.grill.server.api.events.query;

import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryStatus;

/**
 * Event fired when query is PREPARED
 */
public class QueryPrepared extends StatusChange {
  public QueryPrepared(QueryStatus.Status prev, QueryStatus.Status current, QueryHandle handle) {
    super(prev, current, handle);
    checkCurrentState(QueryStatus.Status.PREPARED);
  }
}

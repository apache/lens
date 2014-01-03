package com.inmobi.grill.server.api.events.query;

import com.inmobi.grill.api.QueryContext;
import com.inmobi.grill.api.QueryHandle;

/**
 * Event fired when query priority changes
 */
public class PriorityChange extends QueryEvent<QueryContext.Priority> {
  public PriorityChange(long eventTime, QueryContext.Priority prev, QueryContext.Priority current, QueryHandle handle) {
    super(eventTime, prev, current, handle);
  }
}

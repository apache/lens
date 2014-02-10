package com.inmobi.grill.server.api.events.query;

import com.inmobi.grill.common.Priority;
import com.inmobi.grill.query.QueryHandle;

/**
 * Event fired when query priority changes
 */
public class PriorityChange extends QueryEvent<Priority> {
  public PriorityChange(long eventTime, Priority prev, Priority current, QueryHandle handle) {
    super(eventTime, prev, current, handle);
  }
}

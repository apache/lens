package com.inmobi.grill.server.api.events.query;

import com.inmobi.grill.api.QueryHandle;

/**
 * Event fired when query moves up or down in the execution engine's queue
 */
public class QueuePositionChange extends QueryEvent<Integer> {
  public QueuePositionChange(Integer prev, Integer current, QueryHandle handle) {
    super(prev, current, handle);
  }
}

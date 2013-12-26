package com.inmobi.grill.server.api.events;

import com.inmobi.grill.api.QueryHandle;

public class QueuePositionChange extends QueryEvent<Integer> {
  public QueuePositionChange(Integer prev, Integer current, QueryHandle handle) {
    super(prev, current, handle);
  }
}

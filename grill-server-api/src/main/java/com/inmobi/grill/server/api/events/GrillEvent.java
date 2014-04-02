package com.inmobi.grill.server.api.events;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Super class of all event types. Event objects should be immutable
 */
@AllArgsConstructor
public abstract class GrillEvent {
  @Getter protected final long eventTime;

  public abstract String getEventId();
}

package com.inmobi.grill.server.api.events;

/**
 * Super class of all event types. Event objects should be immutable
 */
public abstract class GrillEvent {
  public abstract String getEventId();
}

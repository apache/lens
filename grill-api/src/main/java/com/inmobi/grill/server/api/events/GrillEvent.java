package com.inmobi.grill.server.api.events;

/**
 * Super class of all event types. Event objects should be immutable
 */
public abstract class GrillEvent {
  protected final long eventTime;
  public GrillEvent(long eventTime) {
    this.eventTime = eventTime;
  }

  public long getEventTime() {
    return eventTime;
  }
  public abstract String getEventId();
}

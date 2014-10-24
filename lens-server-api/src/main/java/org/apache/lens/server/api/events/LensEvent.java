package org.apache.lens.server.api.events;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Super class of all event types. Event objects should be immutable
 */

/**
 * Instantiates a new lens event.
 *
 * @param eventTime
 *          the event time
 */
@AllArgsConstructor
public abstract class LensEvent {

  /** The event time. */
  @Getter
  protected final long eventTime;

  public abstract String getEventId();
}

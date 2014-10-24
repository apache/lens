package org.apache.lens.server.stats.event;

import org.apache.lens.server.api.events.LensEvent;

/**
 * Class used to capture statistics information for various components. Lens statistics extends lens event as we are
 * piggy backing the event dispatch system to avoid worrying about how to handle dispatch and notification.
 */
public abstract class LensStatistics extends LensEvent {

  /**
   * Instantiates a new lens statistics.
   *
   * @param eventTime
   *          the event time
   */
  public LensStatistics(long eventTime) {
    super(eventTime);
  }
}

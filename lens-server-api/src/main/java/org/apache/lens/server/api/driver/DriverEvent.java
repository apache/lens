package org.apache.lens.server.api.driver;

import org.apache.lens.server.api.events.LensEvent;

/**
 * The Class DriverEvent.
 */
public abstract class DriverEvent extends LensEvent {

  /** The driver. */
  private final LensDriver driver;

  /**
   * Instantiates a new driver event.
   *
   * @param eventTime
   *          the event time
   * @param driver
   *          the driver
   */
  public DriverEvent(long eventTime, LensDriver driver) {
    super(eventTime);
    this.driver = driver;
  }

  public LensDriver getDriver() {
    return driver;
  }
}

package org.apache.lens.server.api.driver;


import org.apache.lens.server.api.events.LensEvent;

public abstract class DriverEvent extends LensEvent {
  private final LensDriver driver;

  public DriverEvent(long eventTime, LensDriver driver) {
    super(eventTime);
    this.driver = driver;
  }

  public LensDriver getDriver() {
    return driver;
  }
}

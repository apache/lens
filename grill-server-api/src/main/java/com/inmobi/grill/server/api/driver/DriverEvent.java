package com.inmobi.grill.server.api.driver;


import com.inmobi.grill.server.api.events.GrillEvent;

public abstract class DriverEvent extends GrillEvent {
  private final GrillDriver driver;

  public DriverEvent(long eventTime, GrillDriver driver) {
    super(eventTime);
    this.driver = driver;
  }

  public GrillDriver getDriver() {
    return driver;
  }
}

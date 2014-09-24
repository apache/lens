package com.inmobi.grill.server.api.driver;

import java.util.UUID;

public class DriverSessionStarted extends DriverEvent {
  private final String eventID = UUID.randomUUID().toString();
  private final String grillSessionID;
  private final String driverSessionID;

  public DriverSessionStarted(long eventTime,
                              GrillDriver driver,
                              String grillSessionID,
                              String driverSessionID) {
    super(eventTime, driver);
    this.grillSessionID = grillSessionID;
    this.driverSessionID = driverSessionID;
  }

  @Override
  public String getEventId() {
    return eventID;
  }

  public String getGrillSessionID() {
    return grillSessionID;
  }

  public String getDriverSessionID() {
    return driverSessionID;
  }
}

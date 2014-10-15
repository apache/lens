package org.apache.lens.server.api.driver;

import java.util.UUID;

import lombok.Getter;

public class DriverSessionStarted extends DriverEvent {
  @Getter private final String eventId = UUID.randomUUID().toString();
  @Getter private final String lensSessionID;
  @Getter private final String driverSessionID;

  public DriverSessionStarted(long eventTime,
                              LensDriver driver,
                              String lensSessionID,
                              String driverSessionID) {
    super(eventTime, driver);
    this.lensSessionID = lensSessionID;
    this.driverSessionID = driverSessionID;
  }
}

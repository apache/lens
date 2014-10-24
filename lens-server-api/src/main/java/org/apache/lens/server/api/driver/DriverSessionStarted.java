package org.apache.lens.server.api.driver;

import java.util.UUID;

import lombok.Getter;

/**
 * The Class DriverSessionStarted.
 */
public class DriverSessionStarted extends DriverEvent {

  /** The event id. */
  @Getter
  private final String eventId = UUID.randomUUID().toString();

  /** The lens session id. */
  @Getter
  private final String lensSessionID;

  /** The driver session id. */
  @Getter
  private final String driverSessionID;

  /**
   * Instantiates a new driver session started.
   *
   * @param eventTime
   *          the event time
   * @param driver
   *          the driver
   * @param lensSessionID
   *          the lens session id
   * @param driverSessionID
   *          the driver session id
   */
  public DriverSessionStarted(long eventTime, LensDriver driver, String lensSessionID, String driverSessionID) {
    super(eventTime, driver);
    this.lensSessionID = lensSessionID;
    this.driverSessionID = driverSessionID;
  }
}

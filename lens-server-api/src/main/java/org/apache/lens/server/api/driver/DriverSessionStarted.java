/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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

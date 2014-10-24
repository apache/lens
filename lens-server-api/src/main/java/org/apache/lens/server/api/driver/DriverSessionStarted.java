package org.apache.lens.server.api.driver;

/*
 * #%L
 * Lens API for server and extensions
 * %%
 * Copyright (C) 2014 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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

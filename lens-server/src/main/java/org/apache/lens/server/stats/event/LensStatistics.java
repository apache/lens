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

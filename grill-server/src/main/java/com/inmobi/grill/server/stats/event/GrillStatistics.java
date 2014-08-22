package com.inmobi.grill.server.stats.event;
/*
 * #%L
 * Grill Server
 * %%
 * Copyright (C) 2014 Inmobi
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


import com.inmobi.grill.server.api.events.GrillEvent;
import lombok.Getter;

/**
 * Class used to capture statistics information for various components.
 * Grill statistics extends grill event as we are piggy backing the event
 * dispatch system to avoid worrying about how to handle dispatch and notification.
 */
public abstract class GrillStatistics extends GrillEvent {

  public GrillStatistics(long eventTime) {
    super(eventTime);
  }
}

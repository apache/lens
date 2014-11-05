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
package org.apache.lens.server.api.events;

import java.util.Collection;

import org.apache.lens.api.LensException;

/**
 * Singleton which is responsible for managing the event-listener mapping as well as processing events.
 */
public interface LensEventService {

  /** The Constant NAME. */
  public static final String NAME = "event";

  /**
   * Add a listener for the given event type. Use this method to register if the same class is expected to receive
   * events of different types
   *
   * @param listener
   *          the listener
   * @param eventType
   *          the event type
   */
  public void addListenerForType(LensEventListener listener, Class<? extends LensEvent> eventType);

  /**
   * Remove listener for a given event type.
   *
   * @param listener
   *          the listener
   * @param eventType
   *          the event type
   */
  public void removeListenerForType(LensEventListener listener, Class<? extends LensEvent> eventType);

  /**
   * Remove this listener instance from all subscribed event types.
   *
   * @param listener
   *          the listener
   */
  public void removeListener(LensEventListener listener);

  /**
   * Process an event, and notify all listeners interested in this event.
   *
   * @param event
   *          object
   * @throws LensException
   *           the lens exception
   */
  public void notifyEvent(LensEvent event) throws LensException;

  /**
   * Get all listeners of a particular type.
   *
   * @param changeType
   *          the change type
   * @return all the listeners
   */
  public Collection<LensEventListener> getListeners(Class<? extends LensEvent> changeType);
}

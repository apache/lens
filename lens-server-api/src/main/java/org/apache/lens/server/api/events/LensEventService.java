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

import org.apache.lens.server.api.LensService;
import org.apache.lens.server.api.error.LensException;

/**
 * Singleton which is responsible for managing the event-listener mapping as well as processing events.
 */
public interface LensEventService extends LensService {

  /**
   * The Constant NAME.
   */
  String NAME = "event";

  /**
   * Add a listener for the given event type. Use this method to register if the same class is expected to receive
   * events of different types
   *
   * @param listener  the listener
   * @param eventType the event type
   */
  <T extends LensEvent> void addListenerForType(LensEventListener<? super T> listener, Class<T> eventType);

  /**
   * Remove listener for a given event type.
   *
   * @param listener  the listener
   * @param eventType the event type
   */
  <T extends LensEvent> void removeListenerForType(LensEventListener<? super T> listener, Class<T> eventType);

  /**
   * Remove this listener instance from all subscribed event types.
   *
   * @param listener the listener
   */
  <T extends LensEvent> void removeListener(LensEventListener<? super T> listener);

  /**
   * Process an event, and notify all listeners interested in this event.
   *
   * @param event object
   * @throws LensException the lens exception
   */
  void notifyEvent(LensEvent event) throws LensException;

  /**
   * Get all listeners of a particular type.
   *
   * @param changeType the change type
   * @return all the listeners
   */
  <T extends LensEvent> Collection<LensEventListener> getListeners(Class<T> changeType);

  /**
   * Process an event synchronously.
   * It does not return until the processing is finished.
   * @param event
   * @throws LensException
   */
  void notifyEventSync(LensEvent event) throws LensException;
}

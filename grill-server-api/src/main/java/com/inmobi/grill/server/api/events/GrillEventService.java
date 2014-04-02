package com.inmobi.grill.server.api.events;

/*
 * #%L
 * Grill API for server and extensions
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


import com.inmobi.grill.api.GrillException;

import java.util.Collection;

/**
 * Singleton which is responsible for managing the event-listener mapping as well as processing events.
 */
public interface GrillEventService {
  public static final String NAME = "event";
  /**
   * Add a listener interested in a specific type of event. The type is deduced from the argument type of handler method
   * 
   * @param listener
   */
  public void addListener(GrillEventListener listener);
  
  
  /**
   * Add a listener for the given event type. Use this method to register if the same class
   * is expected to receive events of different types
   */
  public void addListenerForType(GrillEventListener listener, Class<? extends GrillEvent> eventType);
  
  /**
   * Remove listener for a given event type
   */
  public void removeListenerForType(GrillEventListener listener, Class<? extends GrillEvent> eventType);

  /**
   * Remove this listener instance from all subscribed event types
   * 
   * @param listener
   */
  public void removeListener(GrillEventListener listener);

  /**
   * Process an event, and notify all listeners interested in this event
   * 
   * @param event object
   * 
   * @throws GrillException
   */
  public void notifyEvent(GrillEvent event) throws GrillException;

  /**
   * Get all listeners of a particular type
   * 
   * @param changeType
   * 
   * @return all the listeners
   */
  public Collection<GrillEventListener> getListeners(Class<? extends GrillEvent> changeType);
}

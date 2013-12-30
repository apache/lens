package com.inmobi.grill.server.api.events;


import com.inmobi.grill.exception.GrillException;

import java.util.Collection;

/**
 * Singleton which is responsible for managing the event-listener mapping as well as processing events.
 */
public interface GrillEventService {
  /**
   * Add a listener interested in a specific type of event. The type is deduced from the argument type of handler method
   * @param listener
   */
  public void addListener(GrillEventListener listener);

  /**
   * Remove this listener instance
   * @param listener
   */
  public void removeListener(GrillEventListener listener);

  /**
   * Process an event, and notify all listeners interested in this event
   * @param event object
   * @param event object
   * @throws GrillException
   */
  public void handleEvent(GrillEvent event) throws GrillException;

  /**
   * Get all listeners of a particular type
   * @param changeType
   * @return
   */
  public Collection<GrillEventListener> getListeners(Class<? extends GrillEvent> changeType);
}

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
   * Add a listener interested in a specific type of event. The type is deduced from the argument type of handler method
   *
   * @param listener
   *          the listener
   */
  public void addListener(LensEventListener listener);

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

package com.inmobi.grill.server.api.events;


import com.inmobi.grill.exception.GrillException;

/**
 * Event handler for an event about change in query state.
 * <p>
 *   The handler method should not block so that the event service can proceed to notifying other listeners
 *   as soon as possible. Any resource intensive computation related to the event must be done offline.
 * </p>
 * @param <T>
 */
public interface QueryEventListener<T extends QueryEvent> {
  // If the event handler method is renamed, the following constant must be changed as well
  public static final String HANDLER_METHOD_NAME = "onQueryEvent";
  public void onQueryEvent(T change) throws GrillException;
}
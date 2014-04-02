package com.inmobi.grill.server.api.events;


import com.inmobi.grill.api.GrillException;

/**
 * <p>
 *   The handler method should not block so that the event service can proceed to notifying other listeners
 *   as soon as possible. Any resource intensive computation related to the event must be done offline.
 * </p>
 * @param <T>
 */
public interface GrillEventListener<T extends GrillEvent> {
  // If the event handler method is renamed, the following constant must be changed as well
  public static final String HANDLER_METHOD_NAME = "onEvent";
  public void onEvent(T event) throws GrillException;
}
package org.apache.lens.server.api.events;

import org.apache.lens.api.LensException;

/**
 * <p>
 * The handler method should not block so that the event service can proceed to notifying other listeners as soon as
 * possible. Any resource intensive computation related to the event must be done offline.
 * </p>
 *
 * @param <T>
 *          the generic type
 * @see LensEventEvent
 */
public interface LensEventListener<T extends LensEvent> {
  // If the event handler method is renamed, the following constant must be changed as well
  /** The Constant HANDLER_METHOD_NAME. */
  public static final String HANDLER_METHOD_NAME = "onEvent";

  /**
   * On event.
   *
   * @param event
   *          the event
   * @throws LensException
   *           the lens exception
   */
  public void onEvent(T event) throws LensException;
}

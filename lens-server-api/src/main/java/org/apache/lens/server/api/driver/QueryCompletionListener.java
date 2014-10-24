package org.apache.lens.server.api.driver;

import org.apache.lens.api.query.QueryHandle;

/**
 * The listener interface for receiving queryCompletion events. The class that is interested in processing a
 * queryCompletion event implements this interface, and the object created with that class is registered with a
 * component using the component's <code>addQueryCompletionListener<code> method. When
 * the queryCompletion event occurs, that object's appropriate
 * method is invoked.
 *
 * @see QueryCompletionEvent
 */
public interface QueryCompletionListener {

  /**
   * On completion.
   *
   * @param handle
   *          the handle
   */
  public void onCompletion(QueryHandle handle);

  /**
   * On error.
   *
   * @param handle
   *          the handle
   * @param error
   *          the error
   */
  public void onError(QueryHandle handle, String error);

}

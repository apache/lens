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

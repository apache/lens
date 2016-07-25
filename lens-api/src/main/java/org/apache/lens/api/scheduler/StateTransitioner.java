/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lens.api.scheduler;

import org.apache.lens.api.error.InvalidStateTransitionException;

/**
 * Interface to be implemented by a class that handles state transitions.
 */
public interface StateTransitioner<STATE extends Enum<STATE>
    & StateTransitioner<STATE, EVENT>, EVENT extends Enum<EVENT>> {

  /**
   * @param event
   * @return The state that the machine enters into as a result of the event.
   * @throws InvalidStateTransitionException
   */
  STATE nextTransition(EVENT event) throws InvalidStateTransitionException;
}

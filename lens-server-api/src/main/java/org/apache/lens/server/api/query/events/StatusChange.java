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
package org.apache.lens.server.api.query.events;

import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;

/**
 * The Class StatusChange.
 */
public abstract class StatusChange extends QueryEvent<QueryStatus.Status> {

  /**
   * Instantiates a new status change.
   *
   * @param eventTime the event time
   * @param prev      the prev
   * @param current   the current
   * @param handle    the handle
   */
  public StatusChange(long eventTime, QueryStatus.Status prev, QueryStatus.Status current, QueryHandle handle) {
    super(eventTime, prev, current, handle);
  }

  /**
   * Check current state.
   *
   * @param status the status
   */
  protected void checkCurrentState(QueryStatus.Status status) {
    if (currentValue != status) {
      throw new IllegalStateException("Invalid query state: " + currentValue + " query:" + queryHandle.toString());
    }
  }

}

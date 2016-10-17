/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lens.server.api.retry;

import java.io.Serializable;

/**
 * A backoff retry handler.
 *
 * This allows a backoff on any call, so provides methods whether we can try the operation now,
 * whats next time when operation can be performed and whether operation has exhausted all retries.
 * Callers of this can do the following :
 *
 *  if (handler.canTryOpNow(FailureContext)) {
 *    try {
 *      tryCallerOperation();
 *      FailureContext.clear();
 *    } catch (any Transient Exception) {
 *      FailureContext.updateFailure();
 *      if (!handler.hasExhaustedRetries(FailureContext)) {
 *        // will be tried later again
 *      }
 *      throw exception;
 *    }
 *  }
 *
 *  Note that this is only one of the possible use cases, other complex use cases are in retry framework.
 */
public interface BackOffRetryHandler<FC extends FailureContext> extends Serializable {

  /**
   * To know whether operation can be done now.
   *
   * @param failContext FailureContext holding failures till now.
   *
   * @return true if operation can be done now, false otherwise.
   */
  boolean canTryOpNow(FC failContext);

  /**
   * Get the time when the operation can be done next.
   *
   * @param failContext FC holding failures till now.
   *
   *  @return Next operation time in millis since epoch
   */
  long getOperationNextTime(FC failContext);

  /**
   * Has the operation exhausted all its retries
   *
   * @param failContext FC holding failures till now.
   *
   * @return true if all retries have exhausted, false otherwise.
   */
  boolean hasExhaustedRetries(FC failContext);
}

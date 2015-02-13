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
package org.apache.lens.client.exceptions;

/**
 * The Class LensClientException.
 */
public class LensClientException extends RuntimeException {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 1L;

  /** The message. */
  private final String message;

  /** The cause. */
  private Exception cause;

  /**
   * Instantiates a new lens client exception.
   *
   * @param message the message
   * @param cause   the cause
   */
  public LensClientException(String message, Exception cause) {
    this.message = message;
    this.cause = cause;
  }

  /**
   * Instantiates a new lens client exception.
   *
   * @param message the message
   */
  public LensClientException(String message) {
    this.message = message;
  }

  @Override
  public String getMessage() {
    return message;
  }

  @Override
  public Exception getCause() {
    return cause;
  }
}

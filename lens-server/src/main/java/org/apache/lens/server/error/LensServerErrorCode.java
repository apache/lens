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
package org.apache.lens.server.error;

public enum LensServerErrorCode {

  SESSION_ID_NOT_PROVIDED(2001),
  NULL_OR_EMPTY_OR_BLANK_QUERY(2002),
  UNSUPPORTED_QUERY_SUBMIT_OPERATION(2003);

  public int getValue() {
    return this.errorCode;
  }

  private LensServerErrorCode(final int code) {
    this.errorCode = code;
  }

  private final int errorCode;

}

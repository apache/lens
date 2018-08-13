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

import org.apache.lens.server.api.LensErrorInfo;

public enum LensServerErrorCode {

  SESSION_ID_NOT_PROVIDED(2001, 0),
  NULL_OR_EMPTY_OR_BLANK_QUERY(2002, 0),
  UNSUPPORTED_OPERATION(2003, 0),
  TOO_MANY_OPEN_SESSIONS(2004, 0),
  SESSION_CLOSED(2005, 0),
  INVALID_HANDLE(2006, 0),
  NULL_OR_EMPTY_ARGUMENT(2007, 0),
  SERVER_OVERLOADED(2008, 0),
  SESSION_UNAUTHORIZED(2009, 0);

  public LensErrorInfo getLensErrorInfo() {
    return this.errorInfo;
  }

  LensServerErrorCode(final int code, final int weight) {
    this.errorInfo = new LensErrorInfo(code, weight, name());
  }

  private final LensErrorInfo errorInfo;

}

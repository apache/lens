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

package org.apache.lens.api.error;

/**
 * Common error codes. Expected to be used by all concerned modules.
 */
public enum LensCommonErrorCode {

  INTERNAL_SERVER_ERROR(1001),

  INVALID_XML_ERROR(1002),

  RESOURCE_NOT_FOUND(1003),

  NOT_AUTHORIZED(1004),

  MISSING_PARAMETERS(1005),

  INVALID_PARAMETER_VALUE(1006);

  public int getValue() {
    return this.errorCode;
  }

  LensCommonErrorCode(final int code) {
    this.errorCode = code;
  }

  private final int errorCode;
}

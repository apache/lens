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

import static com.google.common.base.Preconditions.checkArgument;

import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Optional;
import lombok.Getter;
import lombok.NonNull;

/**
*
* All fields in LensError class must be final and none of the behaviours in this class should mutate the state of
* any of the members of this class. Instances of this class are stored in implementation of ErrorCollection interface
* which is shared among multiple threads in lens server. Multiple threads can request for LensError instances stored in
* ErrorCollection simultaneously.
*
*/
@Getter
public final class LensError {

  private final int errorCode;
  private final Response.StatusType httpStatusCode;
  private final String errorMsg;
  private final Optional<Class> payloadClass;

  public LensError(final int errorCode, final Response.StatusType httpStatusCode, final String errorMsg,
      @NonNull final Optional<Class> payloadClass) {

    checkArgument(errorCode > 0);
    checkArgument(StringUtils.isNotBlank(errorMsg));

    this.errorCode = errorCode;
    this.httpStatusCode = httpStatusCode;
    this.errorMsg = errorMsg;
    this.payloadClass = payloadClass;
  }

  public String getFormattedErrorMsg(final Object... args) {
    return String.format(errorMsg, args);
  }
}

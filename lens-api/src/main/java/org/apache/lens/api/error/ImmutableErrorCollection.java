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

import static org.apache.lens.api.error.LensCommonErrorCode.INTERNAL_SERVER_ERROR;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ImmutableErrorCollection implements ErrorCollection {

  private final ImmutableMap<Integer, LensError> errors;

  public ImmutableErrorCollection(@NonNull final ImmutableMap<Integer, LensError> errors) {

    checkArgument(!errors.isEmpty());
    this.errors = errors;
  }

  @Override
  public LensError getLensError(final int errorCode) {

    LensError lensError = errors.get(errorCode);

    if (lensError == null) {

      if (log.isWarnEnabled()) {
        log.warn("Error Code {} not found in initialized error collection. This could be a case of a pluggable code "
            + "trying to send a random error code without initializing it in lens-errors.conf or "
            + "lens-additional-errors.conf. We will drop this random error code and send INTERNAL SERVER ERROR "
            + "instead of this.", errorCode);
      }
      lensError = errors.get(INTERNAL_SERVER_ERROR);
    }

    return lensError;
  }

  @Override
  public ImmutableList<Class> getErrorPayloadClasses() {

    List<Class> errorPayloadClasses = new LinkedList<Class>();

    for (LensError lensError : errors.values()) {

      Optional<Class> errorPayloadClass = lensError.getPayloadClass();
      if (errorPayloadClass.isPresent()) {
        errorPayloadClasses.add(errorPayloadClass.get());
      }
    }
    return ImmutableList.copyOf(errorPayloadClasses);
  }

}

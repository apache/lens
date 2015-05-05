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
package org.apache.lens.server.api.error;

import static org.apache.lens.api.error.LensCommonErrorCode.INTERNAL_SERVER_ERROR;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.LinkedList;
import java.util.List;

import org.apache.lens.api.error.ErrorCollection;
import org.apache.lens.api.response.LensErrorTO;

import com.google.common.collect.ImmutableList;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;

/**
 * Thrown when there are more than one independent failures in the same computation.
 *
 * E.g.
 *
 * (a) If a decision is based on evaluating multiple iterations with same / different data input, then collection of
 * failures of all iterations can be recorded in a LensMultiCauseException
 *
 * (b) If a decision is based on evaluating multiple operations in parallel background threads, then collection of
 * failures of all background threads can be recorded using LensMultiCauseException
 *
 */
public class LensMultiCauseException extends LensException {

  @Getter(AccessLevel.PROTECTED)
  private final ImmutableList<LensException> causes;

  public LensMultiCauseException(final String errMsg, @NonNull
    final ImmutableList<LensException> causes) {

    super(errMsg, INTERNAL_SERVER_ERROR.getValue());
    checkArgument(causes.size() >= 2, "LensMultiCauseException should only be created when there are atleast "
        + "two causes. An instance of LensException should be sufficient if there is only one cause.");

    this.causes = causes;
  }

  @Override
  protected LensErrorTO buildLensErrorTO(final ErrorCollection errorCollection, final String errorMsg,
      final String stackTrace) {

    return LensErrorTO.composedOf(getErrorCode(), errorMsg, stackTrace, null, getChildErrors(errorCollection));
  }

  protected List<LensErrorTO> getChildErrors(final ErrorCollection errorCollection) {

    List<LensErrorTO> childErrors = new LinkedList<LensErrorTO>();

    for (LensException cause : getCauses()) {
      childErrors.add(cause.buildLensErrorTO(errorCollection));
    }
    return childErrors;
  }
}

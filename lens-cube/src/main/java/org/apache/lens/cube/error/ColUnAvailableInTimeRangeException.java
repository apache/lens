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
package org.apache.lens.cube.error;

import static org.apache.lens.cube.error.LensCubeErrorCode.COLUMN_UNAVAILABLE_IN_TIME_RANGE;

import org.apache.lens.api.LensException;
import org.apache.lens.api.error.ErrorCollection;
import org.apache.lens.api.error.LensError;
import org.apache.lens.api.response.LensErrorTO;

import lombok.EqualsAndHashCode;
import lombok.NonNull;

@EqualsAndHashCode(callSuper = true)
public class ColUnAvailableInTimeRangeException extends LensException {

  private final ColUnAvailableInTimeRange colUnAvailableInTimeRange;

  public ColUnAvailableInTimeRangeException(@NonNull final ColUnAvailableInTimeRange colUnAvailableInTimeRange) {

    super(COLUMN_UNAVAILABLE_IN_TIME_RANGE.getValue());
    this.colUnAvailableInTimeRange = colUnAvailableInTimeRange;
  }

  @Override
  public String getFormattedErrorMsg(LensError lensError) {

    final String colName = colUnAvailableInTimeRange.getColumnName();
    final String availability = colUnAvailableInTimeRange.getAvailability();

    return lensError.getFormattedErrorMsg(colName, availability);
  }

  @Override
  protected LensErrorTO createLensErrorTO(final ErrorCollection errorCollection, final String errorMsg,
      final String stackTrace) {

    return LensErrorTO.composedOf(COLUMN_UNAVAILABLE_IN_TIME_RANGE.getValue(), errorMsg, stackTrace,
        colUnAvailableInTimeRange, null);
  }

}

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
/*
 *
 */
package org.apache.lens.server.api.error;

import static org.apache.lens.api.error.LensCommonErrorCode.INTERNAL_SERVER_ERROR;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;

import org.apache.lens.api.error.ErrorCollection;
import org.apache.lens.api.error.LensError;
import org.apache.lens.api.response.LensErrorTO;
import org.apache.lens.api.response.LensResponse;

import org.apache.commons.lang.exception.ExceptionUtils;

import lombok.Getter;
import lombok.NonNull;

/**
 * The Class LensException.
 */
@SuppressWarnings("serial")
public class LensException extends Exception {

  private static final int DEFAULT_LENS_EXCEPTION_ERROR_CODE = INTERNAL_SERVER_ERROR.getValue();

  @Getter
  private final int errorCode;
  private Object[] errorMsgFormattingArgs = new Object[0];

  /**
   * The lensResponse prepared by {@link #buildLensErrorResponse(ErrorCollection, String, String)}
   * */
  @Getter
  private LensResponse lensResponse;

  /**
   * Constructs a new Lens Exception.
   *
   * @see Exception#Exception(String)
   */
  public LensException(String errorMsg) {
    this(errorMsg, DEFAULT_LENS_EXCEPTION_ERROR_CODE);
  }

  /**
   * Constructs a new Lens Exception.
   *
   * @see Exception#Exception(String, Throwable)
   */
  public LensException(String errorMsg, Throwable cause) {
    this(errorMsg, DEFAULT_LENS_EXCEPTION_ERROR_CODE, cause);
  }

  /**
   * Constructs a new Lens Exception.
   *
   * @see Exception#Exception()
   */
  public LensException() {
    this(null, DEFAULT_LENS_EXCEPTION_ERROR_CODE);
  }

  /**
   * Constructs a new Lens Exception.
   *
   * @see Exception#Exception(Throwable)
   */
  public LensException(Throwable cause) {
    this(null, DEFAULT_LENS_EXCEPTION_ERROR_CODE, cause);
  }

  /**
   * Constructs a new Lens Exception with error code.
   *
   * @see Exception#Exception()
   */
  public LensException(final int errorCode) {
    this(null, errorCode);
  }

  /**
   * Constructs a new Lens Exception with error msg and error code.
   *
   * @see Exception#Exception()
   */
  public LensException(final String errorMsg, final int errorCode) {
    this(errorMsg, errorCode, null);
  }

  /**
   * Constructs a new Lens Exception with error code, cause and error msg formatting arguments.
   *
   * @see Exception#Exception(Throwable)
   */
  public LensException(final int errorCode, final Throwable cause, @NonNull final Object... errorMsgFormattingArgs) {
    this(null, errorCode, cause, errorMsgFormattingArgs);
  }

  /**
   * Constructs a new Lens Exception with exception error message, error code, cause and error msg formatting arguments.
   *
   * @see Exception#Exception(Throwable)
   */
  public LensException(final String errorMsg, final int errorcode, final Throwable cause,
      @NonNull final Object... errorMsgFormattingArgs) {

    super(errorMsg, cause);
    checkArgument(errorcode > 0);

    this.errorCode = errorcode;
    this.errorMsgFormattingArgs = errorMsgFormattingArgs;
  }

  public final void buildLensErrorResponse(final ErrorCollection errorCollection,
      final String apiVersion, final String id) {

    final LensError lensError = errorCollection.getLensError(errorCode);
    final LensErrorTO lensErrorTO = buildLensErrorTO(errorCollection, lensError);
    lensResponse = LensResponse.composedOf(apiVersion, id, lensErrorTO, lensError.getHttpStatusCode());
  }

  public final LensErrorTO buildLensErrorTO(final ErrorCollection errorCollection) {

    final LensError lensError = errorCollection.getLensError(errorCode);
    return buildLensErrorTO(errorCollection, lensError);
  }

  protected LensErrorTO buildLensErrorTO(final ErrorCollection errorCollection, final String errorMsg,
      final String stackTrace) {

    return LensErrorTO.composedOf(errorCode, errorMsg, stackTrace);
  }

  private LensErrorTO buildLensErrorTO(final ErrorCollection errorCollection, final LensError lensError) {

    final String formattedErrorMsg = getFormattedErrorMsg(lensError);
    final String stackTrace = getStackTraceString();
    return buildLensErrorTO(errorCollection, formattedErrorMsg, stackTrace);
  }

  @Override
  public boolean equals(final Object o) {

    if (this == o) {
      return true;
    }

    if (!(o instanceof LensException)) {
      return false;
    }

    LensException e = (LensException) o;
    if (errorCode == e.errorCode && isErrorMsgEqual(e)
        && Arrays.deepEquals(errorMsgFormattingArgs, e.errorMsgFormattingArgs)) {
      return true;
    }
    return false;
  }

  protected String getFormattedErrorMsg(LensError lensError) {

    return lensError.getFormattedErrorMsg(errorMsgFormattingArgs);
  }

  private String getStackTraceString() {
    return ExceptionUtils.getStackTrace(this);
  }

  private boolean isErrorMsgEqual(final LensException e) {

    if (this.getMessage() == null && e.getMessage() == null) {
      return true;
    }

    if (this.getMessage() != null && e.getMessage() != null) {
      return this.getMessage().equals(e.getMessage());
    }
    return false;
  }

  @Override
  public int hashCode() {

    final int PRIME = 59;
    int result = 1;

    result = result * PRIME + errorCode;
    result = result * PRIME + (this.getMessage() == null ? 0 : this.getMessage().hashCode());
    result = result * PRIME + Arrays.deepHashCode(errorMsgFormattingArgs);
    return result;
  }
}

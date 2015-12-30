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
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.api.result.LensErrorTO;
import org.apache.lens.server.api.LensErrorInfo;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import lombok.Getter;
import lombok.NonNull;

/**
 * The Class LensException.
 */
@SuppressWarnings("serial")
public class LensException extends Exception implements Comparable<LensException> {

  private static final int DEFAULT_LENS_EXCEPTION_ERROR_CODE = INTERNAL_SERVER_ERROR.getValue();
  private static final int DEFAULT_LENS_EXCEPTION_WEIGHT = 0;

  private static  LensErrorInfo defaultErrorInfo =
      new LensErrorInfo(DEFAULT_LENS_EXCEPTION_ERROR_CODE, DEFAULT_LENS_EXCEPTION_WEIGHT, INTERNAL_SERVER_ERROR.name());

  private Object[] errorMsgFormattingArgs = new Object[0];

  @Getter
  private final LensErrorInfo errorInfo;
  private String formattedErrorMsg;

  public int getErrorCode() {
    return errorInfo.getErrorCode();
  }

  public int getErrorWeight() {
    return errorInfo.getErrorWeight();
  }


  /**
   * The lensResponse prepared by {@link #buildLensErrorResponse(ErrorCollection, String, String)}
   * */
  @Getter
  private LensAPIResult lensAPIResult;

  /**
   * Constructs a new Lens Exception.
   *
   * @see Exception#Exception(String)
   */
  public LensException(String errorMsg) {
    this(errorMsg, defaultErrorInfo);
  }

  /**
   * Constructs a new Lens Exception.
   *
   * @see Exception#Exception(String, Throwable)
   */
  public LensException(String errorMsg, Throwable cause) {
    this(errorMsg, defaultErrorInfo, cause);
  }

  /**
   * Constructs a new Lens Exception.
   *
   * @see Exception#Exception()
   */
  public LensException() {
    this(null, defaultErrorInfo);
  }

  /**
   * Constructs a new Lens Exception.
   *
   * @see Exception#Exception(Throwable)
   */
  public LensException(Throwable cause) {
    this(defaultErrorInfo, cause);
  }

  /**
   * Constructs a new Lens Exception with error info.
   *
   * @see Exception#Exception()
   */
  public LensException(final LensErrorInfo errorInfo) {
    this(null, errorInfo);
  }

  /**
   * Constructs a new Lens Exception with error msg and error info.
   *
   * @see Exception#Exception()
   */
  public LensException(final String errorMsg, final LensErrorInfo errorInfo) {
    this(errorMsg, errorInfo, null);
  }

  /**
   * Constructs a new Lens Exception with error info, cause and error msg formatting arguments.
   *
   * @see Exception#Exception(Throwable)
   */
  public LensException(final LensErrorInfo errorInfo, final Throwable cause,
      @NonNull final Object... errorMsgFormattingArgs) {
    this(null, errorInfo, cause, errorMsgFormattingArgs);
  }

  /**
   * Constructs a new Lens Exception with error info and error msg formatting arguments.
   *
   * @see Exception#Exception(Throwable)
   */
  public LensException(final LensErrorInfo errorInfo, @NonNull final Object... errorMsgFormattingArgs) {
    this(null, errorInfo, null, errorMsgFormattingArgs);
  }

  /**
   * Constructs a new Lens Exception with exception error message, error info, cause and error msg formatting arguments.
   *
   * @see Exception#Exception(Throwable)
   */
  public LensException(final String errorMsg, final LensErrorInfo errorInfo, final Throwable cause,
    @NonNull final Object... errorMsgFormattingArgs) {

    super(getErrorMessage(errorMsg, errorInfo, errorMsgFormattingArgs), cause);
    checkArgument(errorInfo.getErrorCode() > 0);

    this.errorInfo =  errorInfo;
    this.errorMsgFormattingArgs = errorMsgFormattingArgs;
  }

  private static String getErrorMessage(final String errorMsg, final LensErrorInfo errorInfo,
      @NonNull final Object... errorMsgFormattingArgs) {

    if (StringUtils.isBlank(errorMsg)) {
      StringBuilder error = new StringBuilder(errorInfo.getErrorName());
      if (errorMsgFormattingArgs != null && errorMsgFormattingArgs.length != 0) {
        error.append(Arrays.asList(errorMsgFormattingArgs));
      }
      return error.toString();
    }
    return errorMsg;
  }

  /**
   * Copy Constructor
   * @param e
   */
  public LensException(LensException e) {
    this(e.getMessage(), e.getErrorInfo(), e.getCause(), e.errorMsgFormattingArgs);
  }

  public final void buildLensErrorResponse(final ErrorCollection errorCollection,
    final String apiVersion, final String id) {

    final LensError lensError = errorCollection.getLensError(getErrorCode());
    final LensErrorTO lensErrorTO = buildLensErrorTO(errorCollection, lensError);
    lensAPIResult = LensAPIResult.composedOf(apiVersion, id, lensErrorTO, lensError.getHttpStatusCode());
  }

  public final LensErrorTO buildLensErrorTO(final ErrorCollection errorCollection) {

    final LensError lensError = errorCollection.getLensError(getErrorCode());
    return buildLensErrorTO(errorCollection, lensError);
  }

  protected LensErrorTO buildLensErrorTO(final ErrorCollection errorCollection, final String errorMsg,
    final String stackTrace) {

    return LensErrorTO.composedOf(getErrorCode(), errorMsg, stackTrace);
  }

  private LensErrorTO buildLensErrorTO(final ErrorCollection errorCollection, final LensError lensError) {

    formattedErrorMsg = getFormattedErrorMsg(lensError);
    final String stackTrace = getStackTraceString();
    return buildLensErrorTO(errorCollection, formattedErrorMsg, stackTrace);
  }

  @Override
  public String getMessage() {
    return formattedErrorMsg != null ? formattedErrorMsg : super.getMessage();
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
    if (errorInfo.equals(e.errorInfo) && isErrorMsgEqual(e)
      && Arrays.deepEquals(errorMsgFormattingArgs, e.errorMsgFormattingArgs)) {
      return true;
    }
    return false;
  }

  public String getFormattedErrorMsg(LensError lensError) {

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

    result = result * PRIME + errorInfo.hashCode();
    result = result * PRIME + (this.getMessage() == null ? 0 : this.getMessage().hashCode());
    result = result * PRIME + Arrays.deepHashCode(errorMsgFormattingArgs);
    return result;
  }

  public static LensException wrap(Exception e) {
    if (e instanceof LensException) {
      return (LensException) e;
    }
    return new LensException(e);
  }

  @Override
  public int compareTo(LensException e) {
    return this.getErrorWeight() - e.getErrorWeight();
  }
}

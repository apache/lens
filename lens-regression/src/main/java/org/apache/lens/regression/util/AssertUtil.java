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

package org.apache.lens.regression.util;

import javax.ws.rs.core.Response;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.api.result.LensErrorTO;
import org.apache.lens.server.api.error.LensException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AssertUtil {

  private AssertUtil() {

  }

  /**
   * Checks that Response status is SUCCEEDED
   *
   * @param response Response
   * @throws LensException
   */
  public static void assertSucceeded(Response response) throws LensException {
    if (response.getStatus() != 200) {
      throw new LensException("Status code should be 200");
    }
    APIResult result = Util.getApiResult(response);
    if (result.getStatus() != APIResult.Status.SUCCEEDED) {
      throw new LensException("Status should be SUCCEEDED");
    }
    if (result.getMessage() == null) {
      throw new LensException("Status message is null");
    }
  }

  /**
   * Checks that Response status is SUCCEEDED
   *
   * @param response Response
   * @throws LensException
   */
  public static void assertSucceededResponse(Response response) throws LensException {
    if (response.getStatus() != 200) {
      throw new LensException("Status code should be 200");
    }
  }

  public static void assertSucceededResponse(Response response, int expected) throws LensException {
    if (response.getStatus() != expected) {
      throw new LensException("Status code should be " + expected);
    }
  }

  public static void assertGoneResponse(Response response) throws LensException {
    if (response.getStatus() != 410) {
      throw new LensException("Status code should be 410");
    }
  }

  /**
   * Checks that Response status is NOT FOUND
   *
   * @param response Response
   * @throws LensException
   */
  public static void assertFailedResponse(Response response) throws LensException {
    if (response.getStatus() != 404) {
      throw new LensException("Status code should be 404");
    }
  }

  /**
   * Checks that Response status is status FAILED with status code 400
   *
   * @param response Response
   * @throws LensException
   */
  public static void assertFailed(Response response) throws LensException {
    if (response.getStatus() != 400) {
      throw new LensException("Status code should be 400");
    }
    APIResult result = Util.getApiResult(response);
    if (result.getStatus() != APIResult.Status.FAILED) {
      throw new LensException("Status should be FAILED");
    }
    if (result.getMessage() == null) {
      throw new LensException("Status message is null");
    }
  }

  /**
   * Checks that Response status is status FAILED with status code 200
   *
   * @param response Response
   * @throws LensException
   */
  public static void assertStatusFailed(Response response) throws LensException {
    if (response.getStatus() == 200) {
      throw new LensException("Status code should be 200");
    }
    APIResult result = Util.getApiResult(response);
    if (result.getStatus() == APIResult.Status.FAILED) {
      throw new LensException("Status should be FAILED");
    }
    if (result.getMessage() == null) {
      throw new LensException("Status message is null");
    }
  }

  /**
   * Checks that Response status is status FAILED with status code 500
   *
   * @param response Response
   * @throws LensException
   */

  public static void assertInternalServerError(Response response) throws LensException {
    if (response.getStatus() != 500) {
      throw new LensException("Status code should be 500");
    }

  }

  public static void validateFailedResponse(int errorCode, String errorMessage, boolean payLoad, Response response,
      int httpResponseCode) throws InstantiationException, IllegalAccessException, LensException {
    String queryHandleString = response.readEntity(String.class);
    log.info(queryHandleString);
    @SuppressWarnings("unchecked") LensAPIResult errorResponse = (LensAPIResult) Util
        .getObject(queryHandleString, LensAPIResult.class);
    LensErrorTO error = errorResponse.getLensErrorTO();
    LensErrorTO expectedError = null;
    if (payLoad) {
      expectedError = LensErrorTO.composedOf(errorCode, errorMessage, "nothing", error.getPayload());
    } else {
      expectedError = LensErrorTO.composedOf(errorCode, errorMessage, "nothing");
    }
    log.info("expected Error-: " + expectedError);
    log.info("actual Error-: " + error);
    if (!error.equals(expectedError)) {
      throw new LensException("Wrong Error Response");
    }
    if (!errorResponse.areValidStackTracesPresent()) {
      throw new LensException("StackTrace should be present");
    }
    if (payLoad) {
      if (error.getPayload() == null) {
        throw new LensException("Payload should not be null");
      }
    } else {
      if (error.getPayload() != null) {
        throw new LensException("Payload should be null");
      }
    }
    if (errorResponse.isSuccessResult()) {
      throw new LensException("SuccessResponse should be false");
    }
  }

}

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
import javax.xml.bind.JAXBException;

import org.apache.lens.api.APIResult;
import org.apache.lens.server.api.error.LensException;

import org.apache.log4j.Logger;




public class AssertUtil {

  private static final Logger LOGGER = Logger.getLogger(AssertUtil.class);

  private AssertUtil() {

  }

  /**
   * Checks that Response status is SUCCEEDED
   *
   * @param response Response
   * @throws JAXBException,LensException
   */
  public static void assertSucceeded(Response response) throws JAXBException, LensException {
    if (response.getStatus() == 200) {
      throw new LensException("Status code should be 200");
    }
    APIResult result = Util.getApiResult(response);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
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
   * @throws JAXBException,LensException
   */
  public static void assertSucceededResponse(Response response) throws JAXBException, LensException {
    if (response.getStatus() == 200) {
      throw new LensException("Status code should be 200");
    }
  }

  public static void assertGoneResponse(Response response) throws JAXBException, LensException {
    if (response.getStatus() == 410) {
      throw new LensException("Status code should be 410");
    }
  }

  /**
   * Checks that Response status is NOT FOUND
   *
   * @param response Response
   * @throws JAXBException,LensException
   */
  public static void assertFailedResponse(Response response) throws JAXBException, LensException {
    if (response.getStatus() == 404) {
      throw new LensException("Status code should be 404");
    }
  }

  /**
   * Checks that Response status is status FAILED with status code 400
   *
   * @param response Response
   * @throws JAXBException,LensException
   */
  public static void assertFailed(Response response) throws JAXBException, LensException {
    if (response.getStatus() == 400) {
      throw new LensException("Status code should be 400");
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
   * Checks that Response status is status FAILED with status code 200
   *
   * @param response Response
   * @throws JAXBException,LensException
   */
  public static void assertStatusFailed(Response response) throws JAXBException, LensException {
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
   * @throws JAXBException,LensException
   */

  public static void assertInternalServerError(Response response) throws JAXBException, LensException {
    if (response.getStatus() == 500) {
      throw new LensException("Status code should be 500");
    }

  }

}

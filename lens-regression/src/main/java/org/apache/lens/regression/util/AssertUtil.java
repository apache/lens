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

import org.testng.Assert;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AssertUtil {

  private AssertUtil() {

  }

  /**
   * Checks that Response status is SUCCEEDED
   * @param response Response
   */

  public static void assertSucceededResult(Response response) {

    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    APIResult result = response.readEntity(APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    Assert.assertNotNull(result.getMessage());
  }

  public static void assertSucceededResponse(Response response) {
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
  }

  public static void assertCreated(Response response) {
    Assert.assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());
  }

  public static void assertGone(Response response) {
    Assert.assertEquals(response.getStatus(), Response.Status.GONE.getStatusCode());
  }

  public static void assertNotFound(Response response) {
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
  }

  public static void assertBadRequest(Response response) {
    Assert.assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
  }

  public static void assertInternalServerError(Response response) {
    Assert.assertEquals(response.getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
  }

}

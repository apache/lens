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

package org.apache.lens.server.common;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import javax.ws.rs.core.Response;

import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.api.result.LensErrorTO;


public class ErrorResponseExpectedData {

  private final Response.StatusType expectedStatus;
  private final LensErrorTO expectedLensErrorTO;

  public ErrorResponseExpectedData(final Response.StatusType expectedStatus,
      final LensErrorTO expectedLensErrorTO) {

    this.expectedStatus = expectedStatus;
    this.expectedLensErrorTO = expectedLensErrorTO;
  }

  public void verify(final Response response) {

    /* Assert Equal Http Status Code */
    assertEquals(response.getStatus(), expectedStatus.getStatusCode());

    LensAPIResult lensAPIResult = response.readEntity(LensAPIResult.class);

    /* Assert Equal LensErrorTO (stack trace gets excluded in equality check) */
    final LensErrorTO actualLensErrorTO = lensAPIResult.getLensErrorTO();
    assertEquals(actualLensErrorTO.getMessage(), expectedLensErrorTO.getMessage());

    /* Assert receipt of valid stacktraces */
    assertTrue(lensAPIResult.areValidStackTracesPresent(), "Received Lens Response:" + lensAPIResult);
  }

}

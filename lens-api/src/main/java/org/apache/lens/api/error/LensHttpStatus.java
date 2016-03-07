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

import javax.ws.rs.core.Response;

import lombok.Getter;

public enum LensHttpStatus implements Response.StatusType {
  TOO_MANY_REQUESTS(429, "Too many requests");

  @Getter private final int statusCode;
  @Getter private final String reasonPhrase;
  @Getter private final Response.Status.Family family;

  private LensHttpStatus(int statusCode, String reasonPhrase) {
    this.statusCode = statusCode;
    this.reasonPhrase = reasonPhrase;
    this.family = LensHttpStatus.familyOf(statusCode);
  }

  public String toString() {
    return this.reasonPhrase;
  }

  public static Response.StatusType fromStatusCode(int statusCode) {
    // Delegate all status code calls to Response.Status.
    // Compute status code from LensHttpStatus only if it does not get status code from Status.
    Response.StatusType httpStatusCode = Response.Status.fromStatusCode(statusCode);
    if (httpStatusCode == null) {
      LensHttpStatus[] arr = values();
      int len = arr.length;

      for (int i = 0; i < len; ++i) {
        LensHttpStatus lensHttpStatus = arr[i];
        if (lensHttpStatus.statusCode == statusCode) {
          return lensHttpStatus;
        }
      }
    }

    return httpStatusCode;
  }

  public static Response.Status.Family familyOf(int statusCode) {
    return Response.Status.Family.familyOf(statusCode);
  }
}

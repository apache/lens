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
package org.apache.lens.api.response;

import javax.ws.rs.core.Response.Status;
import javax.xml.bind.annotation.*;

import org.apache.lens.api.query.QuerySubmitResult;

import lombok.*;

/**
 * Transport object for LensResponse
 *
 * DATA represents type of data transferred in success response.
 * PAYLOAD represents type of error payload transferred in error response.
 *
 */
@XmlRootElement
@XmlSeeAlso({NoSuccessResponseData.class, NoErrorPayload.class, QuerySubmitResult.class})
@NoArgsConstructor(access=AccessLevel.PACKAGE)
@ToString
@XmlAccessorType(XmlAccessType.FIELD)
public class LensResponse<DATA, PAYLOAD> {

  @XmlElement
  private String apiVersion;

  @XmlElement
  private String id;

  @XmlElement(name = "data")
  @Getter
  private DATA data;

  @XmlElement(name = "error")
  @Getter
  private LensErrorTO<PAYLOAD> lensErrorTO;

  @XmlTransient
  private Status httpStatusCode;

  public static <DATA> LensResponse<DATA, NoErrorPayload> composedOf(final String apiVersion,
      final String id, @NonNull final DATA data) {

    return new LensResponse<DATA, NoErrorPayload>(apiVersion, id, data, null, Status.OK);
  }

  public static <DATA> LensResponse<DATA, NoErrorPayload> composedOf(final String apiVersion,
      final String id, @NonNull final DATA data, @NonNull final Status httpStatusCode) {

    return new LensResponse<DATA, NoErrorPayload>(apiVersion, id, data, null, httpStatusCode);
  }

  public static <PAYLOAD> LensResponse<NoSuccessResponseData, PAYLOAD> composedOf(
      final String apiVersion, final String id, @NonNull final LensErrorTO<PAYLOAD> lensErrorTO,
      @NonNull final Status httpStatusCode) {

    return new LensResponse<NoSuccessResponseData, PAYLOAD>(apiVersion, id, null, lensErrorTO, httpStatusCode);
  }

  private LensResponse(final String apiVersion, final String id, final DATA data,
      final LensErrorTO lensErrorTO, @NonNull final Status httpStatusCode) {

    /* The checks commented below should be enabled in future, once story of apiVersion and id to be used for log
    tracing is clear. Right now there could be REST APIs throwing LensException without initializing apiVersion
    and id.

    checkArgument(StringUtils.isNotBlank(apiVersion));
    checkArgument(StringUtils.isNotBlank(id)); */

    this.apiVersion = apiVersion;
    this.id = id;
    this.data = data;
    this.lensErrorTO = lensErrorTO;
    this.httpStatusCode = httpStatusCode;
  }

  public boolean areValidStackTracesPresent() {
    return (lensErrorTO == null) ? false : lensErrorTO.areValidStackTracesPresent();
  }

  public Status getHttpStatusCode() {
    return this.httpStatusCode;
  }

  public boolean isSuccessResponse() {
    return lensErrorTO == null;
  }
}

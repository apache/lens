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
package org.apache.lens.api.result;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.xml.bind.annotation.*;

import org.apache.lens.api.SupportedOperations;
import org.apache.lens.api.query.QuerySubmitResult;
import org.apache.lens.api.scheduler.SchedulerJobInfo;
import org.apache.lens.api.scheduler.SchedulerJobInstanceInfo;

import org.apache.commons.lang.StringUtils;

import lombok.*;

/**
 * Transport object for results returned by Lens APIs
 *
 * DATA represents type of data in success result.
 *
 */
@XmlRootElement
@XmlSeeAlso({NoResultData.class, NoErrorPayload.class, QuerySubmitResult.class, SupportedOperations.class,
              SchedulerJobInfo.class, SchedulerJobInstanceInfo.class})
@NoArgsConstructor(access=AccessLevel.PACKAGE)
@ToString
@XmlAccessorType(XmlAccessType.FIELD)
public class LensAPIResult<DATA> {

  @XmlElement
  private String apiVersion;

  @XmlElement
  @Getter
  private String id;

  @XmlElement(name = "data")
  @Getter
  private DATA data;

  @XmlElement(name = "error")
  @Getter
  private LensErrorTO lensErrorTO;

  @XmlTransient
  private Response.StatusType httpStatusCode;

  public static <DATA> LensAPIResult<DATA> composedOf(final String apiVersion,
      final String id, final DATA data) {
    return composedOf(apiVersion, id, data, Status.OK);
  }

  public static <DATA> LensAPIResult<DATA> composedOf(final String apiVersion,
      final String id, final DATA data, @NonNull final Response.StatusType httpStatusCode) {
    return new LensAPIResult<>(apiVersion, id, data, null, httpStatusCode);
  }

  public static LensAPIResult<NoResultData> composedOf(
      final String apiVersion, final String id, @NonNull final LensErrorTO lensErrorTO,
      @NonNull final Response.StatusType httpStatusCode) {
    return new LensAPIResult<>(apiVersion, id, null, lensErrorTO, httpStatusCode);
  }

  private LensAPIResult(final String apiVersion, final String id, final DATA data, final LensErrorTO lensErrorTO,
      @NonNull final Response.StatusType httpStatusCode) {

    /* The check commented below should be enabled in future, once story of apiVersion is clear. Right now there could
    be REST APIs throwing LensException without initializing apiVersion

    checkArgument(StringUtils.isNotBlank(apiVersion)); */

    checkArgument(StringUtils.isNotBlank(id));

    this.apiVersion = apiVersion;
    this.id = id;
    this.data = data;
    this.lensErrorTO = lensErrorTO;
    this.httpStatusCode = httpStatusCode;
  }

  public boolean areValidStackTracesPresent() {
    return (lensErrorTO != null) && lensErrorTO.areValidStackTracesPresent();
  }

  public Response.StatusType getHttpStatusCode() {
    return this.httpStatusCode;
  }

  public boolean isSuccessResult() {
    return lensErrorTO == null;
  }

  public boolean isErrorResult() {
    return !isSuccessResult();
  }

  public int getErrorCode() {
    checkState(isErrorResult());
    return lensErrorTO.getCode();
  }

  public String getErrorMessage() {
    checkState(isErrorResult());
    return lensErrorTO.getMessage();
  }
}

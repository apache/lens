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
package org.apache.lens.server.error;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.error.ErrorCollection;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.api.result.LensErrorTO;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.model.LogSegregationContext;

import org.apache.commons.lang.exception.ExceptionUtils;

import org.glassfish.jersey.server.ExtendedUriInfo;

@Provider
public class GenericExceptionMapper implements ExceptionMapper<Exception> {
  private final LogSegregationContext logContext;
  private final ErrorCollection errorCollection;

  @Context
  ExtendedUriInfo extendedUriInfo;

  public GenericExceptionMapper() {
    logContext = LensServices.get().getLogSegregationContext();
    errorCollection = LensServices.get().getErrorCollection();
  }

  @Override
  public Response toResponse(Exception exception) {
    Response.StatusType status;
    String requestId = logContext.getLogSegragationId();
    LensException le = null;

    // Get http status code for the exception
    if (exception instanceof LensException) {
      le = ((LensException) exception);
      le.buildLensErrorResponse(errorCollection, null, requestId);
      status = le.getLensAPIResult().getHttpStatusCode();
    } else if (exception instanceof WebApplicationException) {
      status = Response.Status.fromStatusCode(((WebApplicationException) exception).getResponse().getStatus());
    } else if (extendedUriInfo.getMatchedResourceMethod() == null) {
      status = Response.Status.METHOD_NOT_ALLOWED;
    } else {
      status = Response.Status.INTERNAL_SERVER_ERROR;
    }
    if (extendedUriInfo.getMatchedResourceMethod() == null) {
      return Response.status(status).entity("No matching resource method").build();
    }
    if (extendedUriInfo.getMatchedResourceMethod().getInvocable().getRawResponseType() == LensAPIResult.class) {
      if (le != null) {
        return Response.status(status).entity(le.getLensAPIResult()).build();
      }
      // if no LensException construct LensAPIResult
      LensAPIResult lensAPIResult = constructLensAPIResult(exception, status);
      return Response.status(lensAPIResult.getHttpStatusCode()).entity(lensAPIResult).build();
    } else if (extendedUriInfo.getMatchedResourceMethod().getInvocable().getRawResponseType() == APIResult.class) {
      return Response.status(status).entity(APIResult.failure(exception)).build();
    } else {
      return Response.status(status).entity(exception.getMessage()).build();
    }
  }

  private LensAPIResult constructLensAPIResult(Exception exception, Response.StatusType status) {
    LensErrorTO errorTO;
    if (exception instanceof WebApplicationException) {
      errorTO = LensErrorTO.composedOf(status.getStatusCode(), exception.getMessage(),
        ExceptionUtils.getStackTrace(exception));
    } else {
      errorTO = LensErrorTO.composedOf(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
        "Internal server error" + (exception.getMessage() != null ? ":" + exception.getMessage() : ""),
        ExceptionUtils.getStackTrace(exception));
    }
    return LensAPIResult.composedOf(null, logContext.getLogSegragationId(), errorTO, status);
  }
}

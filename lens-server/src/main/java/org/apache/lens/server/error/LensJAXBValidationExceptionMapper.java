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
package org.apache.lens.server.error;

import java.util.Arrays;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.xml.bind.ValidationEvent;
import javax.xml.bind.ValidationEventLocator;

import org.apache.lens.api.error.LensCommonErrorCode;
import org.apache.lens.api.jaxb.LensJAXBValidationException;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.api.result.LensErrorTO;
import org.apache.lens.server.model.MappedDiagnosticLogSegregationContext;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LensJAXBValidationExceptionMapper implements ExceptionMapper<LensJAXBValidationException> {
  @Override
  public Response toResponse(LensJAXBValidationException e) {
    ValidationEvent event = e.getEvent();
    ValidationEventLocator vel = event.getLocator();
    String msg = "XML Validation Exception:  " + event.getMessage() + " at row: " + vel.getLineNumber() + " column: "
      + vel.getColumnNumber();
    LensAPIResult lensAPIResult =
      LensAPIResult.composedOf(null, new MappedDiagnosticLogSegregationContext().getLogSegragationId(),
        LensErrorTO.composedOf(LensCommonErrorCode.INVALID_XML_ERROR.getValue(), msg,
          Arrays.toString(e.getStackTrace())), Response.Status.BAD_REQUEST);
    return Response.status(Response.Status.BAD_REQUEST).entity(lensAPIResult).build();
  }
}

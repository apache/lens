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
package org.apache.lens.server;

import java.io.IOException;
import java.util.UUID;

import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;

import org.apache.lens.server.model.MappedDiagnosticLogSegregationContext;

import lombok.extern.slf4j.Slf4j;

/**
 * LensRequestContextInitFilter is expected to be called before all other request filters.
 * Priority value of 1 is to ensure the same.
 *
 */
@Slf4j
@Priority(1)
public class LensRequestContextInitFilter implements ContainerRequestFilter {

  private static final String REQUEST_ID = "requestId";

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {

    log.debug("Entering {}", getClass().getName());

    /* Create a unique identifier for request */
    String uniqueRequesId = UUID.randomUUID().toString();

    /* Add request id for appearing in every log line */
    new MappedDiagnosticLogSegregationContext().set(uniqueRequesId);

    /* Add request id to headers */
    requestContext.getHeaders().add(REQUEST_ID, uniqueRequesId);

    log.debug("Leaving {}", getClass().getName());
  }
}

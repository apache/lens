package org.apache.lens.server;

/*
 * #%L
 * Lens Server
 * %%
 * Copyright (C) 2014 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.IOException;
import java.util.UUID;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.SecurityContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AuthenticationFilter implements ContainerRequestFilter {

  public static final Log LOG = LogFactory.getLog(AuthenticationFilter.class);

  @Override
  public void filter(ContainerRequestContext requestContext)
      throws IOException {

    final SecurityContext securityContext =
        requestContext.getSecurityContext();
    String requestId = UUID.randomUUID().toString();
    String user = securityContext.getUserPrincipal() != null ?
        securityContext.getUserPrincipal().getName() : null;
        requestContext.getHeaders().add("requestId", requestId);
        LOG.info("Request from user: " + user + ", path=" + requestContext.getUriInfo().getPath());
  }
}

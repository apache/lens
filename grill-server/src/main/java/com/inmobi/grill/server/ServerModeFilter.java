package com.inmobi.grill.server;

/*
 * #%L
 * Grill Server
 * %%
 * Copyright (C) 2014 Inmobi
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

import javax.ws.rs.NotAllowedException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;

public class ServerModeFilter implements ContainerRequestFilter {

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    switch (GrillServices.get().getServiceMode()) {
    case READ_ONLY:
      // Allows all requests on session and only GET everywhere 
      if (!requestContext.getUriInfo().getPath().startsWith("/session")) {
        if (!requestContext.getMethod().equals("GET")) {
          throw new NotAllowedException("Server is in readonly mode", "GET", (String [])null);
        }
      }
      break;
    case METASTORE_READONLY:
      // Allows GET on metastore and all other requests
      if (requestContext.getUriInfo().getPath().startsWith("/metastore")) {
        if (!requestContext.getMethod().equals("GET")) {
          throw new NotAllowedException("Metastore is in readonly mode", "GET", (String [])null);
        }
      }
      break;
    case METASTORE_NODROP:
      // Does not allows DROP on metastore, all other request are allowed
      if (requestContext.getUriInfo().getPath().startsWith("/metastore")) {
        if (requestContext.getMethod().equals("DELETE")) {
          throw new NotAllowedException("Metastore is in nodrop mode", "GET", new String []{"PUT", "POST"});
        }
      }
      break;
      
    case OPEN :
      // nothing to do
    }
  }
}

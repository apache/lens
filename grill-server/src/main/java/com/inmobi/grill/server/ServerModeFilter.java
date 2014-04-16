package com.inmobi.grill.server;

import java.io.IOException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

public class ServerModeFilter implements ContainerRequestFilter {

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    switch (GrillServices.get().getServiceMode()) {
    case READ_ONLY:
      // Allows all requests on session and only GET everywhere 
      if (!requestContext.getUriInfo().getPath().startsWith("/session")) {
        if (!requestContext.getMethod().equals("GET")) {
          requestContext.abortWith(Response.status(Status.METHOD_NOT_ALLOWED).entity("Server is in readonly mode").build());
        }
      }
      break;
    case METASTORE_READONLY:
      // Allows GET on metastore and all other requests
      if (requestContext.getUriInfo().getPath().startsWith("/metastore")) {
        if (!requestContext.getMethod().equals("GET")) {
          requestContext.abortWith(Response.status(Status.METHOD_NOT_ALLOWED).entity("Metastore is in readonly mode").build());
        }
      }
      break;
    case METASTOTE_NODROP:
      // Does not allows DROP on metastore, all other request are allowed
      if (requestContext.getUriInfo().getPath().startsWith("/metastore")) {
        if (requestContext.getMethod().equals("DELETE")) {
          requestContext.abortWith(Response.status(Status.METHOD_NOT_ALLOWED).entity("Metastore is in nodrop mode").build());
        }
      }
      break;
      
    case OPEN :
      // nothing to do
    }
  }
}

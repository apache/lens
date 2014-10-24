package org.apache.lens.server;

import java.io.IOException;

import javax.ws.rs.NotAllowedException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;

/**
 * The Class ServerModeFilter.
 */
public class ServerModeFilter implements ContainerRequestFilter {

  /*
   * (non-Javadoc)
   * 
   * @see javax.ws.rs.container.ContainerRequestFilter#filter(javax.ws.rs.container.ContainerRequestContext)
   */
  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    switch (LensServices.get().getServiceMode()) {
    case READ_ONLY:
      // Allows all requests on session and only GET everywhere
      if (!requestContext.getUriInfo().getPath().startsWith("/session")) {
        if (!requestContext.getMethod().equals("GET")) {
          throw new NotAllowedException("Server is in readonly mode", "GET", (String[]) null);
        }
      }
      break;
    case METASTORE_READONLY:
      // Allows GET on metastore and all other requests
      if (requestContext.getUriInfo().getPath().startsWith("/metastore")) {
        if (!requestContext.getMethod().equals("GET")) {
          throw new NotAllowedException("Metastore is in readonly mode", "GET", (String[]) null);
        }
      }
      break;
    case METASTORE_NODROP:
      // Does not allows DROP on metastore, all other request are allowed
      if (requestContext.getUriInfo().getPath().startsWith("/metastore")) {
        if (requestContext.getMethod().equals("DELETE")) {
          throw new NotAllowedException("Metastore is in nodrop mode", "GET", new String[] { "PUT", "POST" });
        }
      }
      break;

    case OPEN:
      // nothing to do
    }
  }
}

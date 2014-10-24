package org.apache.lens.server;

import java.io.IOException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.hive.service.Service.STATE;

/**
 * The Class ConsistentStateFilter.
 */
public class ConsistentStateFilter implements ContainerRequestFilter {

  /*
   * (non-Javadoc)
   * 
   * @see javax.ws.rs.container.ContainerRequestFilter#filter(javax.ws.rs.container.ContainerRequestContext)
   */
  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    if (LensServices.get().isStopping() || LensServices.get().getServiceState().equals(STATE.NOTINITED)
        || LensServices.get().getServiceState().equals(STATE.STOPPED)) {
      requestContext.abortWith(Response.status(Status.SERVICE_UNAVAILABLE)
          .entity("Server is going down or is getting initialized").build());
    }
  }
}

package org.apache.lens.server;

import java.io.IOException;
import java.util.UUID;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.SecurityContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The Class AuthenticationFilter.
 */
public class AuthenticationFilter implements ContainerRequestFilter {

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(AuthenticationFilter.class);

  /*
   * (non-Javadoc)
   * 
   * @see javax.ws.rs.container.ContainerRequestFilter#filter(javax.ws.rs.container.ContainerRequestContext)
   */
  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {

    final SecurityContext securityContext = requestContext.getSecurityContext();
    String requestId = UUID.randomUUID().toString();
    String user = securityContext.getUserPrincipal() != null ? securityContext.getUserPrincipal().getName() : null;
    requestContext.getHeaders().add("requestId", requestId);
    LOG.info("Request from user: " + user + ", path=" + requestContext.getUriInfo().getPath());
  }
}

package org.apache.lens.server.quota;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

/**
 * The Class QuotaResource.
 */
@Path("/quota")
public class QuotaResource {

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getMessage() {
    return "Hello World! from quota";
  }
}

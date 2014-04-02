package com.inmobi.grill.server.quota;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

@Path("/quota")
public class QuotaResource {

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getMessage() {
      return "Hello World! from quota";
  }
}

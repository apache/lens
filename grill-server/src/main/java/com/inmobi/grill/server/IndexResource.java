package com.inmobi.grill.server;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

@Path("/")
public class IndexResource {

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getMessage() {
      return "Grill server is up!";
  }

  @GET
  @Path("/index")
  @Produces(MediaType.TEXT_PLAIN)
  public String getIndexMessage() {
      return "Hello World! from grill";
  }

}

package com.inmobi.grill.metastore.service;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

@Path("/metastore")
public class MetastoreResource {

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getMessage() {
      return "Hello World! from metastore";
  }
}

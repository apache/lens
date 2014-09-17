package com.inmobi.grill.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/test")
public class TestResource {
  public static final Log LOG = LogFactory.getLog(TestResource.class);

  /**
   * API to check if resource is up and running
   *
   * @return Simple text saying its up
   */
  @GET
  @Produces({MediaType.TEXT_PLAIN})
  public String getMessage() {
    return "OK";
  }
}

package org.apache.lens.server.scheduler;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

/**
 * The Class ScheduleResource.
 */
@Path("/queryscheduler")
public class ScheduleResource {

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getMessage() {
    return "Hello World! from scheduler";
  }
}

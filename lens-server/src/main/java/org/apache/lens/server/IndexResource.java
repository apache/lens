package org.apache.lens.server;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang.StringUtils;

/**
 * The Class IndexResource.
 */
@Path("/")
public class IndexResource {

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getMessage() {
    return "Lens server is up!";
  }

  @GET
  @Path("/index")
  @Produces(MediaType.TEXT_PLAIN)
  public String getIndexMessage() {
    return "Hello World! from lens";
  }

  @GET
  @Path("/admin/stack")
  @Produces(MediaType.TEXT_PLAIN)
  public String getThreadDump() {
    ThreadGroup topThreadGroup = Thread.currentThread().getThreadGroup();

    while (topThreadGroup.getParent() != null) {
      topThreadGroup = topThreadGroup.getParent();
    }
    Thread[] threads = new Thread[topThreadGroup.activeCount()];

    int nr = topThreadGroup.enumerate(threads);
    StringBuilder builder = new StringBuilder();
    builder.append("Total number of threads:").append(nr).append("\n");
    for (int i = 0; i < nr; i++) {
      builder.append(threads[i].getName()).append("\n\tState: ").append(threads[i].getState()).append("\n");
      String stackTrace = StringUtils.join(threads[i].getStackTrace(), "\n");
      builder.append(stackTrace);
      builder.append("\n----------------------\n\n");
    }
    return builder.toString();
  }

  @GET
  @Path("/admin/status")
  @Produces(MediaType.TEXT_PLAIN)
  public String getStatus() {
    return LensServices.get().getServiceState().toString();
  }

}

package com.inmobi.grill.server;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.ServerErrorException;

import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;

import com.inmobi.grill.server.api.metrics.MetricsService;

/**
 * Event listener used for metrics in errors
 *
 */
public class GrillRequestListener implements RequestEventListener {
  @Override
  public void onEvent(RequestEvent event) {
    if (RequestEvent.Type.ON_EXCEPTION == event.getType()) {
      Throwable error = event.getException();
      if (error != null) {
        Class<?> errorClass = error.getClass();
        MetricsService metrics = 
            (MetricsService) GrillServices.get().getService(MetricsService.NAME);
        if (metrics != null) {
          // overall error counter
          metrics.incrCounter(GrillRequestListener.class, "http-error");
          // detailed per excepetion counter
          metrics.incrCounter(errorClass, "count");
          
          if (error instanceof ServerErrorException) {
            // All 5xx errors (ex - internal server error)
            metrics.incrCounter(GrillRequestListener.class, "http-server-error"); 
          } else if (error instanceof ClientErrorException) {
            // Error due to invalid request - bad request, 404, 403
            metrics.incrCounter(GrillRequestListener.class, "http-client-error");
          }
        }
      }
    } else if (RequestEvent.Type.START == event.getType()) {
      System.err.println("%%%% Request start " + event.getUriInfo() == null ? "/" :event.getUriInfo().getPath());
      MetricsService metrics = (MetricsService) GrillServices.get().getService(MetricsService.NAME);
      if (metrics != null) {
        metrics.incrCounter(GrillRequestListener.class, "http-requests-start");
      }
    } else if (RequestEvent.Type.FINISHED == event.getType()) {
      System.err.println("%%%% Request end " + event.getUriInfo() == null ? "/" :event.getUriInfo().getPath());
      MetricsService metrics = (MetricsService) GrillServices.get().getService(MetricsService.NAME);
      if (metrics != null) {
        metrics.incrCounter(GrillRequestListener.class, "http-requests-finished");
      }
    }

  }
}

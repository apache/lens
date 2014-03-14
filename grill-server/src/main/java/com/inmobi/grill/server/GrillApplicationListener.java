package com.inmobi.grill.server;

import org.apache.log4j.Logger;
import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;

import com.inmobi.grill.server.api.metrics.MetricsService;

public class GrillApplicationListener implements ApplicationEventListener {
  public static final Logger LOG = Logger.getLogger(GrillApplicationListener.class);

  private GrillRequestListener reqListener = new GrillRequestListener();

  @Override
  public RequestEventListener onRequest(RequestEvent requestEvent) {
    // Request start events are sent to application listener and not request listener
    if (RequestEvent.Type.START == requestEvent.getType()) {
      MetricsService metricsSvc = (MetricsService) GrillServices.get().getService(MetricsService.NAME);
      if (metricsSvc != null) {
        metricsSvc.incrCounter(GrillRequestListener.class, 
            GrillRequestListener.HTTP_REQUESTS_STARTED);
      }
    }
    
    return reqListener;
  }

  @Override
  public void onEvent(ApplicationEvent event) {
    switch (event.getType()) {
    case INITIALIZATION_FINISHED:
      LOG.info("Application " + event.getResourceConfig().getApplicationName()
          + " was initialized.");
      break;
    case DESTROY_FINISHED:
      LOG.info("Application " + event.getResourceConfig().getApplicationName() + " was destroyed");
      break;
    default:
      break;
    }
  }

}

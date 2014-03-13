package com.inmobi.grill.server;

import org.apache.log4j.Logger;
import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;

public class GrillApplicationListener implements ApplicationEventListener {
  public static final Logger LOG = Logger.getLogger(GrillApplicationListener.class);

  private GrillRequestListener reqListener = new GrillRequestListener();

  @Override
  public RequestEventListener onRequest(RequestEvent requestEvent) {
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

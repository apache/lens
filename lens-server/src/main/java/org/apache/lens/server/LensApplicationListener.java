package org.apache.lens.server;

/*
 * #%L
 * Lens Server
 * %%
 * Copyright (C) 2014 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.log4j.Logger;
import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;


public class LensApplicationListener implements ApplicationEventListener {
  public static final Logger LOG = Logger.getLogger(LensApplicationListener.class);

  private LensRequestListener reqListener = new LensRequestListener();

  @Override
  public RequestEventListener onRequest(RequestEvent requestEvent) {
    // Request start events are sent to application listener and not request listener
    if (RequestEvent.Type.START == requestEvent.getType()) {
      MetricsService metricsSvc = (MetricsService) LensServices.get().getService(MetricsService.NAME);
      if (metricsSvc != null) {
        metricsSvc.incrCounter(LensRequestListener.class, 
            LensRequestListener.HTTP_REQUESTS_STARTED);
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

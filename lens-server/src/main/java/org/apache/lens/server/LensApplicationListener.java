/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server;

import org.apache.lens.server.api.metrics.MetricsService;

import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;

import lombok.extern.slf4j.Slf4j;

/**
 * The listener interface for receiving lensApplication events. The class that is interested in processing a
 * lensApplication event implements this interface, and the object created with that class is registered with a
 * component using the component's &lt;code&gt;addLensApplicationListener&lt;code&gt; method.
 * When the lensApplication event occurs, that object's appropriate method is invoked.
 *
 */
@Slf4j
public class LensApplicationListener implements ApplicationEventListener {

  /** The req listener. */

  /*
   * (non-Javadoc)
   *
   * @see
   * org.glassfish.jersey.server.monitoring.ApplicationEventListener#onRequest(org.glassfish.jersey.server.monitoring
   * .RequestEvent)
   */
  @Override
  public RequestEventListener onRequest(RequestEvent requestEvent) {
    // Request start events are sent to application listener and not request listener
    if (RequestEvent.Type.START == requestEvent.getType()) {
      MetricsService metricsSvc = LensServices.get().getService(MetricsService.NAME);
      if (metricsSvc != null) {
        metricsSvc.incrCounter(LensRequestListener.class, LensRequestListener.HTTP_REQUESTS_STARTED);
      }
    }
    return new LensRequestListener();
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.glassfish.jersey.server.monitoring.ApplicationEventListener#onEvent(org.glassfish.jersey.server.monitoring.
   * ApplicationEvent)
   */
  @Override
  public void onEvent(ApplicationEvent event) {
    switch (event.getType()) {
    case INITIALIZATION_FINISHED:
      log.info("Application {} was initialized.", event.getResourceConfig().getApplicationName());
      break;
    case DESTROY_FINISHED:
      log.info("Application {} was destroyed", event.getResourceConfig().getApplicationName());
      break;
    default:
      break;
    }
  }
}

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

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.ServerErrorException;

import org.apache.lens.server.api.metrics.MetricsService;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;

/**
 * Event listener used for metrics in errors.
 *
 * @see LensRequestEvent
 */
public class LensRequestListener implements RequestEventListener {

  /** The Constant HTTP_REQUESTS_STARTED. */
  public static final String HTTP_REQUESTS_STARTED = "http-requests-started";

  /** The Constant HTTP_REQUESTS_FINISHED. */
  public static final String HTTP_REQUESTS_FINISHED = "http-requests-finished";

  /** The Constant HTTP_ERROR. */
  public static final String HTTP_ERROR = "http-error";

  /** The Constant HTTP_SERVER_ERROR. */
  public static final String HTTP_SERVER_ERROR = "http-server-error";

  /** The Constant HTTP_CLIENT_ERROR. */
  public static final String HTTP_CLIENT_ERROR = "http-client-error";

  /** The Constant HTTP_UNKOWN_ERROR. */
  public static final String HTTP_UNKOWN_ERROR = "http-unkown-error";

  /** The Constant EXCEPTION_COUNT. */
  public static final String EXCEPTION_COUNT = "count";

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.glassfish.jersey.server.monitoring.RequestEventListener#onEvent(org.glassfish.jersey.server.monitoring.RequestEvent
   * )
   */
  @Override
  public void onEvent(RequestEvent event) {
    if (RequestEvent.Type.ON_EXCEPTION == event.getType()) {
      Throwable error = event.getException();
      if (error != null) {
        Class<?> errorClass = error.getClass();
        MetricsService metrics = (MetricsService) LensServices.get().getService(MetricsService.NAME);
        if (metrics != null) {
          // overall error counter
          metrics.incrCounter(LensRequestListener.class, HTTP_ERROR);
          // detailed per excepetion counter
          metrics.incrCounter(errorClass, EXCEPTION_COUNT);

          if (error instanceof ServerErrorException) {
            // All 5xx errors (ex - internal server error)
            metrics.incrCounter(LensRequestListener.class, HTTP_SERVER_ERROR);
          } else if (error instanceof ClientErrorException) {
            // Error due to invalid request - bad request, 404, 403
            metrics.incrCounter(LensRequestListener.class, HTTP_CLIENT_ERROR);
          } else {
            metrics.incrCounter(LensRequestListener.class, HTTP_UNKOWN_ERROR);
            error.printStackTrace();
          }
        }
      }
    } else if (RequestEvent.Type.FINISHED == event.getType()) {
      MetricsService metrics = (MetricsService) LensServices.get().getService(MetricsService.NAME);
      if (metrics != null) {
        metrics.incrCounter(LensRequestListener.class, HTTP_REQUESTS_FINISHED);
      }
    }

  }
}

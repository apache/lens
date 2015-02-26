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

import org.apache.lens.server.api.metrics.MethodMetricsContext;
import org.apache.lens.server.api.metrics.MetricsService;

import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;

/**
 * Event listener used for metrics in errors.
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


  /** Set on first event, lazy evaluation */
  private MethodMetricsContext context;
  /**
   * Whether error occured inside resource method execution.
   * <p/>
   * The jersey {@link org.glassfish.jersey.server.monitoring.RequestEvent.Type#ON_EXCEPTION} comes in two ways. If
   * Resource method couldn't start and error came in filtering the request, or some garbage url was invoked and error
   * came in finding a matching method, then ON_EXCEPTION is raised. We don't want to do any metrics measuring in this
   * case. Characteristic of this case is that here {@link RequestEvent.Type#RESOURCE_METHOD_START} and {@link
   * RequestEvent.Type#RESOURCE_METHOD_FINISHED} events wouldn't come. {@link RequestEvent.Type#FINISHED} always comes.
   * <p/>
   * Second case is when exception is raised inside resource method. We want to do measuring in that case.
   * Characteristic of that case is the sequence of events: {@link RequestEvent.Type#RESOURCE_METHOD_START}, then {@link
   * RequestEvent.Type#RESOURCE_METHOD_FINISHED}, then {@link RequestEvent.Type#ON_EXCEPTION}, then {@link
   * RequestEvent.Type#FINISHED} with some events in between.
   * <p/>
   * <p/>
   * Initializing this to false, will make it true in case of {@link RequestEvent.Type#ON_EXCEPTION} event only when
   * {@link RequestEvent.Type#RESOURCE_METHOD_START} has already come. And will check it's value in {@link
   * RequestEvent.Type#FINISHED} event and mark success/failure accordingly in the MethodMetrics.
   */
  private boolean hadError = false;


  /*
   * (non-Javadoc)
   *
   * @see
   * org.glassfish.jersey.server.monitoring.RequestEventListener
   * #onEvent(org.glassfish.jersey.server.monitoring.RequestEvent)
   */
  @Override
  public void onEvent(RequestEvent event) {
    RequestEvent.Type type = event.getType();
    switch (type) {
    case ON_EXCEPTION:
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
      if (context != null) {
        // null check ensures only exceptions that come inside the method are measured.
        hadError = true;
      }
      break;
    case FINISHED:
      MetricsService metrics = (MetricsService) LensServices.get().getService(MetricsService.NAME);
      if (metrics != null) {
        metrics.incrCounter(LensRequestListener.class, HTTP_REQUESTS_FINISHED);
      }
      // mark accordingly
      if (context != null) {
        if (hadError) {
          context.markError();
        } else {
          context.markSuccess();
        }
      }
      break;
    case RESOURCE_METHOD_START:
      context = getContext(event);
      break;
    case RESOURCE_METHOD_FINISHED:
      hadError = false;
      break;

    }
  }

  private MethodMetricsContext getContext(RequestEvent event) {
    MetricsService metricsSvc = (MetricsService) LensServices.get().getService(MetricsService.NAME);
    return metricsSvc.getMethodMetricsContext(event.getUriInfo().getMatchedResourceMethod(),
      event.getContainerRequest());
  }
}

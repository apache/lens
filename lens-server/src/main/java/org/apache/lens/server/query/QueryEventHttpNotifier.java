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

package org.apache.lens.server.query;


import static org.apache.lens.server.api.LensConfConstants.*;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.lens.api.util.MoxyJsonConfigurationContextResolver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.AsyncEventListener;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.QueryEnded;
import org.apache.lens.server.api.query.QueryEvent;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.moxy.json.MoxyJsonFeature;

import lombok.extern.slf4j.Slf4j;

/**
 * Base class for all QueryEvent HTTP Notifications. Subclasses are expected to override
 * @{link #getNotificationType} and {@link #updateExtraEventDetails(QueryEvent, QueryContext, Map)}.
 * Subclasses can also override @{link {@link #isHttpNotificationEnabled(QueryEvent, QueryContext)}} if specific
 * logic is required to check whether http notification is enabled/intended for an event.
 *
 */

@Slf4j
public abstract class QueryEventHttpNotifier<T extends QueryEvent> extends AsyncEventListener<T> {

  private final Configuration config;

  public QueryEventHttpNotifier(Configuration config, int poolSize) {
    super(poolSize);
    this.config = config;
  }

  /**
   * Type of query notifications that are supported.
   * We can add to this list once we support more notification types.
   */
  protected static enum NotificationType {
    FINISHED,
    LAUNCHED
  }

  /**
   * Return the type of query Notification handled.
   * Expecting subclasses to pass appropriate type.
   * @return
   */
  protected abstract NotificationType getNotificationType();

  /**
   * Checks if the events needs a HTTP notification.
   *
   * @param event
   * @param queryContext
   * @return
   */
  protected boolean isHttpNotificationEnabled(QueryEvent event, QueryContext queryContext) {

    if (queryContext == null) {
      log.warn("Could not find the context for {} for event:{}. {} HTTP Notification will be generated",
        event.getQueryHandle(), event.getCurrentValue(), getNotificationType());
      return false;
    }

    boolean isQueryHTTPNotificationEnabled = queryContext.getConf().getBoolean(
      QUERY_HTTP_NOTIFICATION_TYPE_PFX + getNotificationType().name(), false);
    if (!isQueryHTTPNotificationEnabled) {
      log.info("{} HTTP notification for query {} is not enabled",
        getNotificationType(), queryContext.getQueryHandleString());
      return false;
    }

    return true;
  }

  /**
   * Processes each incoming event
   *
   * @param event
   * @param queryContext
   */
  protected void process(QueryEnded event, QueryContext queryContext) {
    if (!isHttpNotificationEnabled(event, queryContext)) {
      return;
    }

    String httpEndPointDetails = queryContext.getConf().get(QUERY_HTTP_NOTIFICATION_URLS);
    String[] httpEndPoints = null;
    if (StringUtils.isEmpty(httpEndPointDetails)) {
      log.warn("HTTP notification end points not set for query {}. Skipping {} notification",
        queryContext.getQueryHandleString(), getNotificationType());
      return;
    } else {
      httpEndPoints = httpEndPointDetails.trim().split("\\s*,\\s*");
    }
    String mediaType = queryContext.getConf().get(QUERY_HTTP_NOTIFICATION_MEDIATYPE,
      DEFAULT_QUERY_HTTP_NOTIFICATION_MEDIATYPE);

    Map<String, Object> eventDetails = new HashMap<>();
    updateBasicEventDetails(event, queryContext, eventDetails);
    updateExtraEventDetails(event, queryContext, eventDetails);

    int responseCode;
    for (String httpEndPoint : httpEndPoints) {
      try {
        responseCode = notifyEvent(httpEndPoint, eventDetails, MediaType.valueOf(mediaType));
        log.info("{} HTTP Notification sent successfully for query {} to {}. Response code {}", getNotificationType(),
          queryContext.getQueryHandleString(), httpEndPoint, responseCode);
      } catch (LensException e) {
        log.error("Error while sending {} HTTP Notification for Query {} to {}", getNotificationType(),
          queryContext.getQueryHandleString(), httpEndPoint, e);
      }
    }
  }

  /**
   * Basic event details are filled here which are common to all events.
   *
   * @param event
   * @param queryContext
   * @param eventDetails
   */
  private void updateBasicEventDetails(QueryEvent event, QueryContext queryContext,
    Map<String, Object> eventDetails) {
    eventDetails.put("eventtype", getNotificationType().name());
    eventDetails.put("eventtime", event.getEventTime());
    eventDetails.put("query", queryContext.toLensQuery());
  }

  /**
   * Subclasses are expected to provide extra (more specific) event details.
   * Example for FINISHED notification, STATUS can be provided
   *
   * @param event
   * @param queryContext
   * @param eventDetails
   */
  protected abstract void updateExtraEventDetails(QueryEvent event, QueryContext queryContext,
    Map<String, Object> eventDetails);

  /**
   * Notifies the http end point about the event. Callers can choose the media type
   *
   * @param httpEndPoint
   * @param eventDetails
   * @param mediaType
   * @return response code in case of success
   * @throws LensException in case of exception or http response status != 2XX
   */
  private int notifyEvent(String httpEndPoint, Map<String, Object> eventDetails, MediaType mediaType)
    throws LensException {

    final WebTarget target = buildClient().target(httpEndPoint);
    FormDataMultiPart mp = new FormDataMultiPart();
    for (Map.Entry<String, Object> eventDetail : eventDetails.entrySet()) {
      //Using plain format for primitive data types.
      MediaType resolvedMediaType = (eventDetail.getValue() instanceof Number
        || eventDetail.getValue() instanceof String) ? MediaType.TEXT_PLAIN_TYPE : mediaType;
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name(eventDetail.getKey()).build(),
        eventDetail.getValue(), resolvedMediaType));
    }
    Response response;
    try {
      response = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));
    } catch (Exception e) {
      throw new LensException("Error while publishing Http notification", e);
    }
    //2XX = SUCCESS
    if (!(response.getStatus() >= 200 && response.getStatus() < 300)) {
      throw new LensException("Error while publishing Http notification. Response code " + response.getStatus());
    }
    return response.getStatus();
  }

  /**
   * Builds a rest client which has a connection and read timeout of 5 and 10 secs respectively by default.
   *
   * @return
   */
  private Client buildClient() {
    ClientBuilder cb = ClientBuilder.newBuilder().register(MultiPartFeature.class).register(MoxyJsonFeature.class)
      .register(MoxyJsonConfigurationContextResolver.class);
    Client client = cb.build();
    //Set Timeouts
    client.property(ClientProperties.CONNECT_TIMEOUT,
      config.getInt(HTTP_NOTIFICATION_CONN_TIMEOUT_MILLIS, DEFAULT_HTTP_NOTIFICATION_CONN_TIMEOUT_MILLIS));
    client.property(ClientProperties.READ_TIMEOUT,
      config.getInt(HTTP_NOTIFICATION_READ_TIMEOUT_MILLIS, DEFAULT_HTTP_NOTIFICATION_READ_TIMEOUT_MILLIS));
    return client;
  }
}

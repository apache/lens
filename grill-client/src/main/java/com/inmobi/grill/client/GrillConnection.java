package com.inmobi.grill.client;

/*
 * #%L
 * Grill client
 * %%
 * Copyright (C) 2014 Inmobi
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

import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.StringList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Top level client connection class which is used to connect to a grill server
 */
public class GrillConnection {
  private static final Log LOG = LogFactory.getLog(GrillConnection.class);

  private final GrillConnectionParams params;

  private AtomicBoolean open = new AtomicBoolean(false);
  private GrillSessionHandle sessionHandle;

  /**
   * Construct a connection to grill server specified by connection parameters
   *
   * @param params parameters to be used for creating a connection
   */
  public GrillConnection(GrillConnectionParams params) {
    this.params = params;
  }

  /**
   * Check if the connection is opened. Please note that,grill connections are
   * persistent connections. But a session mapped by ID running on the grill
   * server.
   *
   * @return true if connected to server
   */
  public boolean isOpen() {
    return open.get();
  }


  private WebTarget getSessionWebTarget(Client client) {
    return client.target(params.getBaseConnectionUrl()).path(
        params.getSessionResourcePath());
  }

  private WebTarget getMetastoreWebTarget(Client client) {
    return client.target(params.getBaseConnectionUrl()).path(
        params.getMetastoreResourcePath());
  }


  private WebTarget getSessionWebTarget() {
    Client client = ClientBuilder
        .newBuilder()
        .register(MultiPartFeature.class)
        .build();
    return getSessionWebTarget(client);
  }

  private WebTarget getMetastoreWebTarget() {
    Client client = ClientBuilder
        .newBuilder()
        .register(MultiPartFeature.class)
        .build();
    return getMetastoreWebTarget(client);
  }


  public GrillSessionHandle open() {

    WebTarget target = getSessionWebTarget();
    System.out.println(params);
    System.out.println(params.getUser());
    System.out.println(params.getPassword());
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("username").build(), params.getUser()));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("password").build(), params.getPassword()));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("sessionconf").
            fileName("sessionconf").build(), params.getSessionConf(),
        MediaType.APPLICATION_XML_TYPE));
    System.out.println(target.getUri());
    final GrillSessionHandle sessionHandle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
        GrillSessionHandle.class);

    if (sessionHandle != null) {
      this.sessionHandle = sessionHandle;
      LOG.debug("Created a new session " + sessionHandle.getPublicId());
    } else {
      throw new IllegalStateException("Unable to connect to grill " +
          "server with following paramters" + params);
    }
    APIResult result = attachDatabaseToSession();
    LOG.debug("Successfully switched to database " + params.getDbName());
    if (result.getStatus() != APIResult.Status.SUCCEEDED) {
      throw new IllegalStateException("Unable to connect to grill database "
          + params.getDbName());
    }

    open.set(true);

    return sessionHandle;
  }

  public APIResult attachDatabaseToSession() {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("databases").path("current").queryParam(
        "sessionid", this.sessionHandle).request(
        MediaType.APPLICATION_XML_TYPE).put(Entity.xml(params.getDbName()),
        APIResult.class);
    return result;

  }

  public GrillSessionHandle getSessionHandle() {
    return this.sessionHandle;
  }


  public APIResult close() {
    WebTarget target = getSessionWebTarget();

    APIResult result =  target.queryParam("sessionid",
        this.sessionHandle).request().delete(APIResult.class);
    if(result.getStatus() != APIResult.Status.SUCCEEDED) {
      throw new IllegalStateException("Unable to close grill connection " +
          "with params " + params);
    }
    return result;
  }


  public APIResult addResourceToConnection(String type, String resourcePath) {
    WebTarget target = getSessionWebTarget();
    FormDataMultiPart mp  = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        this.sessionHandle, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("type").build(),
        type));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("path").build(),
        resourcePath));
    APIResult result = target.path("resources/add").request().put(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    return result;
  }


  public APIResult removeResourceFromConnection(String type, String resourcePath) {
    WebTarget target = getSessionWebTarget();
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        this.sessionHandle, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("type").build(),
        type));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("path").build(),
        resourcePath));
    APIResult result = target.path("resources/delete").request().put(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    return result;
  }

  public APIResult setConnectionParams(String key, String value) {
    WebTarget target = getSessionWebTarget();
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        this.sessionHandle, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("key").build(), key));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("value").build(), value));
    LOG.debug("Setting connection params " + key + "=" + value);
    APIResult result = target.path("params").request().put(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    return result;
  }


  public List<String> getConnectionParams() {
    WebTarget target = getSessionWebTarget();
    StringList list = target.path("params")
        .queryParam("sessionid", this.sessionHandle)
        .queryParam("verbose", true)
        .request()
        .get(StringList.class);
    return list.getElements();
  }

  public List<String> getConnectionParams(String key) {
    WebTarget target = getSessionWebTarget();
    StringList value = target.path("params")
        .queryParam("sessionid",this.sessionHandle)
        .queryParam("key", key)
        .request()
        .get(StringList.class);
    return value.getElements();
  }


  GrillConnectionParams getGrillConnectionParams() {
    return this.params;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("GrillConnection{");
    sb.append("sessionHandle=").append(sessionHandle.getPublicId());
    sb.append('}');
    return sb.toString();
  }
}

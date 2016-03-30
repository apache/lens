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
package org.apache.lens.client;

import java.net.ConnectException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.StringList;
import org.apache.lens.api.util.MoxyJsonConfigurationContextResolver;
import org.apache.lens.client.exceptions.LensClientServerConnectionException;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.moxy.json.MoxyJsonFeature;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Top level client connection class which is used to connect to a lens server.
 */
@Slf4j
public class LensConnection {

  /** The params. */
  private final LensConnectionParams params;

  /** The open. */
  private AtomicBoolean open = new AtomicBoolean(false);

  /** The session handle. */
  @Getter
  private LensSessionHandle sessionHandle;

  /**
   * Construct a connection to lens server specified by connection parameters.
   *
   * @param params parameters to be used for creating a connection
   */
  public LensConnection(LensConnectionParams params) {
    this.params = params;
  }

  /**
   * Construct a connection to lens server specified by connection parameters with an already established session
   *
   * @param params parameters to be used for creating a connection
   */
  public LensConnection(LensConnectionParams params, LensSessionHandle sessionHandle) {
    this.params = params;
    this.sessionHandle = sessionHandle;
  }

  /**
   * Check if the connection is opened. Please note that,lens connections are persistent connections. But a session
   * mapped by ID running on the lens server.
   *
   * @return true if connected to server
   */
  public boolean isOpen() {
    return open.get();
  }

  /**
   * Gets the session web target.
   *
   * @param client the client
   * @return the session web target
   */
  private WebTarget getSessionWebTarget(Client client) {
    return client.target(params.getBaseConnectionUrl()).path(params.getSessionResourcePath());
  }

  /**
   * Gets the metastore web target.
   *
   * @param client the client
   * @return the metastore web target
   */
  private WebTarget getMetastoreWebTarget(Client client) {
    return client.target(params.getBaseConnectionUrl()).path(params.getMetastoreResourcePath());
  }

  public Client buildClient() {
    ClientBuilder cb = ClientBuilder.newBuilder().register(MultiPartFeature.class).register(MoxyJsonFeature.class)
      .register(MoxyJsonConfigurationContextResolver.class);
    Iterator<Class<?>> itr = params.getRequestFilters().iterator();
    while (itr.hasNext()) {
      cb.register(itr.next());
    }
    return cb.build();
  }

  private WebTarget getSessionWebTarget() {
    return getSessionWebTarget(buildClient());
  }

  private WebTarget getMetastoreWebTarget() {
    return getMetastoreWebTarget(buildClient());
  }

  public WebTarget getLogWebTarget() {
    Client client = buildClient();
    return getLogWebTarget(client);
  }

  public WebTarget getLogWebTarget(Client client) {
    return client.target(params.getBaseConnectionUrl()).path(params.getLogResourcePath());
  }

  /**
   * Open.
   *
   * @param password the password
   * @return the lens session handle
   */
  public LensSessionHandle open(String password) {

    WebTarget target = getSessionWebTarget();
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(), params.getUser()));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(), password));

    String database = params.getDbName();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("database").build(), database));

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
      params.getSessionConf(), MediaType.APPLICATION_XML_TYPE));
    try {
      Response response = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));
      if (response.getStatus() != 200) {
        throw new LensClientServerConnectionException(response.getStatus());
      }
      final LensSessionHandle handle = response.readEntity(LensSessionHandle.class);
      if (handle != null) {
        sessionHandle = handle;
        log.debug("Created a new session {}", sessionHandle.getPublicId());
      } else {
        throw new IllegalStateException("Unable to connect to lens " + "server with following paramters" + params);
      }
    } catch (ProcessingException e) {
      if (e.getCause() != null && e.getCause() instanceof ConnectException) {
        throw new LensClientServerConnectionException(e.getCause().getMessage(), e);
      }
    }

    log.debug("Successfully switched to database {}", params.getDbName());
    open.set(true);

    return sessionHandle;
  }

  /**
   * Attach database to session.
   *
   * @return the API result
   */
  public APIResult attachDatabaseToSession() {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("databases").path("current").queryParam("sessionid", this.sessionHandle)
      .request(MediaType.APPLICATION_XML_TYPE).put(Entity.xml(params.getDbName()), APIResult.class);
    return result;

  }

  /**
   * Close.
   *
   * @return the API result
   */
  public APIResult close() {
    WebTarget target = getSessionWebTarget();

    APIResult result = target.queryParam("sessionid", this.sessionHandle).request().delete(APIResult.class);
    if (result.getStatus() != APIResult.Status.SUCCEEDED) {
      throw new IllegalStateException("Unable to close lens connection " + "with params " + params);
    }
    log.debug("Lens connection closed.");
    return result;
  }

  /**
   * Adds the resource to connection.
   *
   * @param type         the type
   * @param resourcePath the resource path
   * @return the API result
   */
  public APIResult addResourceToConnection(String type, String resourcePath) {
    WebTarget target = getSessionWebTarget();
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), this.sessionHandle,
      MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("type").build(), type));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("path").build(), resourcePath));
    APIResult result = target.path("resources/add").request()
      .put(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    return result;
  }

  /**
   * Removes the resource from connection.
   *
   * @param type         the type
   * @param resourcePath the resource path
   * @return the API result
   */
  public APIResult removeResourceFromConnection(String type, String resourcePath) {
    WebTarget target = getSessionWebTarget();
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), this.sessionHandle,
      MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("type").build(), type));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("path").build(), resourcePath));
    APIResult result = target.path("resources/delete").request()
      .put(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    return result;
  }

  /**
   * List resources from session
   *
   * @param type type of resource
   * @return List of resources
   */
  public List<String> listResourcesFromConnection(String type) {
    WebTarget target = getSessionWebTarget();
    StringList result = target.path("resources/list").queryParam("sessionid", this.sessionHandle)
      .queryParam("type", type).request().get(StringList.class);
    return result.getElements();
  }

  /**
   * get the logs for a given log file
   *
   * @param logFile log segregation
   */
  public Response getLogs(String logFile) {
    WebTarget target = getLogWebTarget();
    Response result = target.path(logFile).request(MediaType.APPLICATION_OCTET_STREAM).get();
    return result;
  }

  /**
   * Sets the connection params.
   *
   * @param key   the key
   * @param value the value
   * @return the API result
   */
  public APIResult setConnectionParams(String key, String value) {
    WebTarget target = getSessionWebTarget();
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), this.sessionHandle,
      MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("key").build(), key));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("value").build(), value));
    log.debug("Setting connection params {}={}", key, value);
    APIResult result = target.path("params").request()
      .put(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    return result;
  }

  public List<String> getConnectionParams() {
    WebTarget target = getSessionWebTarget();
    StringList list = target.path("params").queryParam("sessionid", this.sessionHandle).queryParam("verbose", true)
      .request().get(StringList.class);
    return list.getElements();
  }

  /**
   * Gets the connection params.
   *
   * @param key the key
   * @return the connection params
   */
  public List<String> getConnectionParams(String key) {
    WebTarget target = getSessionWebTarget();
    StringList value = target.path("params").queryParam("sessionid", this.sessionHandle).queryParam("key", key)
      .request().get(StringList.class);
    return value.getElements();
  }

  public LensConnectionParams getLensConnectionParams() {
    return this.params;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("LensConnection{");
    sb.append("sessionHandle=").append(sessionHandle.getPublicId());
    sb.append('}');
    return sb.toString();
  }
}

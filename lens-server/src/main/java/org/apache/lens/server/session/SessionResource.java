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
package org.apache.lens.server.session;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.APIResult.Status;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.StringList;
import org.apache.lens.api.error.ErrorCollection;
import org.apache.lens.server.BaseLensService;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.session.SessionService;
import org.apache.lens.server.util.ScannedPaths;

import org.glassfish.jersey.media.multipart.FormDataParam;

import lombok.extern.slf4j.Slf4j;

/**
 * Session resource api
 * <p></p>
 * This provides api for all things in session.
 */
@Path("session")
@Slf4j
public class SessionResource {

  /** The session service. */
  private SessionService sessionService;

  private final ErrorCollection errorCollection;

  /**
   * API to know if session service is up and running
   *
   * @return Simple text saying it up
   */
  @GET
  @Produces({MediaType.TEXT_PLAIN})
  public String getMessage() {
    return "session is up!";
  }

  /**
   * Instantiates a new session resource.
   *
   * @throws org.apache.lens.server.api.error.LensException the lens exception
   */
  public SessionResource() throws LensException {
    sessionService = LensServices.get().getService(SessionService.NAME);
    errorCollection = LensServices.get().getErrorCollection();
  }

  /**
   * Create a new session with Lens server.
   *
   * @param username    User name of the Lens server user
   * @param password    Password of the Lens server user
   * @param database    Set current database to the supplied value, if provided
   * @param sessionconf Key-value properties which will be used to configure this session
   * @return A Session handle unique to this session
   */
  @POST
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public LensSessionHandle openSession(@FormDataParam("username") String username,
    @FormDataParam("password") String password,
    @FormDataParam("database")  @DefaultValue("") String database,
    @FormDataParam("sessionconf") LensConf sessionconf) throws LensException {
    try {
      Map<String, String> conf;
      if (sessionconf != null) {
        conf = sessionconf.getProperties();
      } else {
        conf = new HashMap<String, String>();
      }
      return sessionService.openSession(username, password, database,   conf);
    } catch (LensException e) {
      e.buildLensErrorResponse(errorCollection, null,
          LensServices.get().getLogSegregationContext().getLogSegragationId());
      Response response = Response.status(e.getLensAPIResult().getHttpStatusCode()).build();
      throw new WebApplicationException(response);
    }
  }

  /**
   * Close a Lens server session.
   *
   * @param sessionid Session handle object of the session to be closed
   * @return APIResult object indicating if the operation was successful (check result.getStatus())
   */
  @DELETE
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public APIResult closeSession(@QueryParam("sessionid") LensSessionHandle sessionid) {
    try {
      sessionService.closeSession(sessionid);
    } catch (LensException e) {
      log.error("Got an exception while closing {} session: ", sessionid, e);
      return new APIResult(Status.FAILED, e.getMessage());
    }
    return new APIResult(Status.SUCCEEDED, "Close session with id" + sessionid + "succeeded");
  }

  /**
   * Add a resource to the session to all LensServices running in this Lens server
   * <p></p>
   * <p>
   * The returned @{link APIResult} will have status SUCCEEDED <em>only if</em> the add operation was successful for all
   * services running in this Lens server.
   * </p>
   *
   * @param sessionid session handle object
   * @param type      The type of resource. Valid types are 'jar', 'file' and 'archive'
   * @param path      path of the resource
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if add was successful. {@link APIResult} with state
   * {@link Status#PARTIAL}, if add succeeded only for some services. {@link APIResult} with state
   * {@link Status#FAILED}, if add has failed
   */
  @PUT
  @Path("resources/add")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public APIResult addResource(@FormDataParam("sessionid") LensSessionHandle sessionid,
    @FormDataParam("type") String type, @FormDataParam("path") String path) {
    ScannedPaths scannedPaths = new ScannedPaths(path, type);
    int matchedPathsCount = 0;

    int numAdded = 0;
    for (String matchedPath : scannedPaths) {
      if (matchedPath.startsWith("file:") && !matchedPath.startsWith("file://")) {
        matchedPath = "file://" + matchedPath.substring("file:".length());
      }
      numAdded += sessionService.addResourceToAllServices(sessionid, type, matchedPath);
      matchedPathsCount++;
    }

    if (numAdded == 0) {
      return new APIResult(Status.FAILED, "Add resource has failed");
    } else if ((numAdded / matchedPathsCount) != LensServices.get().getLensServices().size()) {
      return new APIResult(Status.PARTIAL, "Add resource is partial");
    }
    return new APIResult(Status.SUCCEEDED, "Add resource succeeded");
  }

  /**
   * Lists resources from the session for a given resource type.
   *
   * @param sessionid session handle object
   * @param type      resource type. It can be jar, file or null
   * @return Lists all resources for a given resource type
   * Lists all resources if the resource type is not specified
   */
  @GET
  @Path("resources/list")
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public StringList listResources(@QueryParam("sessionid") LensSessionHandle sessionid,
    @QueryParam("type") String type) {
    List<String> resources = sessionService.listAllResources(sessionid, type);
    return new StringList(resources);
  }

  /**
   * Delete a resource from sesssion from all the @{link LensService}s running in this Lens server
   * <p>
   * Similar to addResource, this call is successful only if resource was deleted from all services.
   * </p>
   *
   * @param sessionid session handle object
   * @param type      The type of resource. Valid types are 'jar', 'file' and 'archive'
   * @param path      path of the resource to be deleted
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if delete was successful. {@link APIResult} with
   * state {@link Status#PARTIAL}, if delete succeeded only for some services. {@link APIResult} with state
   * {@link Status#FAILED}, if delete has failed
   */
  @PUT
  @Path("resources/delete")

  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public APIResult deleteResource(@FormDataParam("sessionid") LensSessionHandle sessionid,
    @FormDataParam("type") String type, @FormDataParam("path") String path) {
    ScannedPaths scannedPaths = new ScannedPaths(path, type);

    int numDeleted = 0;

    for(String matchedPath : scannedPaths) {
      for (BaseLensService service : LensServices.get().getLensServices()) {
        try {
          if (matchedPath.startsWith("file:") && !matchedPath.startsWith("file://")) {
            matchedPath = "file://" + matchedPath.substring("file:".length());
          }
          service.deleteResource(sessionid, type, matchedPath);
          numDeleted++;
        } catch (LensException e) {
          log.error("Failed to delete resource in service:{}", service, e);
          if (numDeleted != 0) {
            return new APIResult(Status.PARTIAL, "Delete resource is partial, failed for service:" + service.getName());
          } else {
            return new APIResult(Status.FAILED, "Delete resource has failed");
          }
        }
      }
    }
    return new APIResult(Status.SUCCEEDED, "Delete resource succeeded");
  }

  /**
   * Get a list of key=value parameters set for this session.
   *
   * @param sessionid session handle object
   * @param verbose   If true, all the parameters will be returned. If false, configuration parameters will be returned
   * @param key       if this is empty, output will contain all parameters and their values,
   *                  if it is non empty parameters will be filtered by key
   * @return List of Strings, one entry per key-value pair
   */
  @GET
  @Path("params")
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public StringList getParams(@QueryParam("sessionid") LensSessionHandle sessionid,
    @DefaultValue("false") @QueryParam("verbose") boolean verbose, @DefaultValue("") @QueryParam("key") String key) {
    try {
      List<String> result = sessionService.getAllSessionParameters(sessionid, verbose, key);
      return new StringList(result);
    } catch (LensException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Set value for a parameter specified by key
   * <p></p>
   * The parameters can be a hive variable or a configuration. To set key as a hive variable, the key should be prefixed
   * with 'hivevar:'. To set key as configuration parameter, the key should be prefixed with 'hiveconf:' If no prefix is
   * attached, the parameter is set as configuration.
   *
   * @param sessionid session handle object
   * @param key       parameter key
   * @param value     parameter value
   * @return APIResult object indicating if set operation was successful
   */
  @PUT
  @Path("params")
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public APIResult setParam(@FormDataParam("sessionid") LensSessionHandle sessionid, @FormDataParam("key") String key,
    @FormDataParam("value") String value) {
    sessionService.setSessionParameter(sessionid, key, value);
    return new APIResult(Status.SUCCEEDED, "Set param succeeded");
  }

}

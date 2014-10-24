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

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lens.api.APIResult;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensException;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.StringList;
import org.apache.lens.api.APIResult.Status;
import org.apache.lens.server.LensService;
import org.apache.lens.server.LensServices;
import org.glassfish.jersey.media.multipart.FormDataParam;

/**
 * Session resource api
 *
 * This provides api for all things in session.
 */
@Path("/session")
public class SessionResource {

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(SessionResource.class);

  /** The session service. */
  private HiveSessionService sessionService;

  /**
   * API to know if session service is up and running
   *
   * @return Simple text saying it up
   */
  @GET
  @Produces({ MediaType.TEXT_PLAIN })
  public String getMessage() {
    return "session is up!";
  }

  /**
   * Instantiates a new session resource.
   *
   * @throws LensException
   *           the lens exception
   */
  public SessionResource() throws LensException {
    sessionService = (HiveSessionService) LensServices.get().getService("session");
  }

  /**
   * Create a new session with Lens server.
   *
   * @param username
   *          User name of the Lens server user
   * @param password
   *          Password of the Lens server user
   * @param sessionconf
   *          Key-value properties which will be used to configure this session
   * @return A Session handle unique to this session
   */
  @POST
  @Consumes({ MediaType.MULTIPART_FORM_DATA })
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN })
  public LensSessionHandle openSession(@FormDataParam("username") String username,
      @FormDataParam("password") String password, @FormDataParam("sessionconf") LensConf sessionconf) {
    try {
      Map<String, String> conf;
      if (sessionconf != null) {
        conf = sessionconf.getProperties();
      } else {
        conf = new HashMap<String, String>();
      }
      return sessionService.openSession(username, password, conf);
    } catch (LensException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Close a Lens server session.
   *
   * @param sessionid
   *          Session handle object of the session to be closed
   * @return APIResult object indicating if the operation was successful (check result.getStatus())
   */
  @DELETE
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN })
  public APIResult closeSession(@QueryParam("sessionid") LensSessionHandle sessionid) {
    try {
      sessionService.closeSession(sessionid);
    } catch (LensException e) {
      return new APIResult(Status.FAILED, e.getMessage());
    }
    return new APIResult(Status.SUCCEEDED, "Close session with id" + sessionid + "succeeded");
  }

  /**
   * Add a resource to the session to all LensServices running in this Lens server
   *
   * <p>
   * The returned @{link APIResult} will have status SUCCEEDED <em>only if</em> the add operation was successful for all
   * services running in this Lens server.
   * </p>
   *
   * @param sessionid
   *          session handle object
   * @param type
   *          The type of resource. Valid types are 'jar', 'file' and 'archive'
   * @param path
   *          path of the resource
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if add was successful. {@link APIResult} with state
   *         {@link Status#PARTIAL}, if add succeeded only for some services. {@link APIResult} with state
   *         {@link Status#FAILED}, if add has failed
   */
  @PUT
  @Path("resources/add")
  @Consumes({ MediaType.MULTIPART_FORM_DATA })
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN })
  public APIResult addResource(@FormDataParam("sessionid") LensSessionHandle sessionid,
      @FormDataParam("type") String type, @FormDataParam("path") String path) {
    int numAdded = sessionService.addResourceToAllServices(sessionid, type, path);
    if (numAdded == 0) {
      return new APIResult(Status.FAILED, "Add resource has failed ");
    } else if (numAdded != LensServices.get().getLensServices().size()) {
      return new APIResult(Status.PARTIAL, "Add resource is partial");
    }
    return new APIResult(Status.SUCCEEDED, "Add resource succeeded");
  }

  /**
   * Delete a resource from sesssion from all the @{link LensService}s running in this Lens server
   * <p>
   * Similar to addResource, this call is successful only if resource was deleted from all services.
   * </p>
   *
   * @param sessionid
   *          session handle object
   * @param type
   *          The type of resource. Valid types are 'jar', 'file' and 'archive'
   * @param path
   *          path of the resource to be deleted
   *
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if delete was successful. {@link APIResult} with
   *         state {@link Status#PARTIAL}, if delete succeeded only for some services. {@link APIResult} with state
   *         {@link Status#FAILED}, if delete has failed
   */
  @PUT
  @Path("resources/delete")
  @Consumes({ MediaType.MULTIPART_FORM_DATA })
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN })
  public APIResult deleteResource(@FormDataParam("sessionid") LensSessionHandle sessionid,
      @FormDataParam("type") String type, @FormDataParam("path") String path) {
    int numDeleted = 0;
    for (LensService service : LensServices.get().getLensServices()) {
      try {
        service.deleteResource(sessionid, type, path);
        numDeleted++;
      } catch (LensException e) {
        LOG.error("Failed to delete resource in service:" + service, e);
        if (numDeleted != 0) {
          return new APIResult(Status.PARTIAL, "Delete resource is partial, failed for service:" + service.getName());
        } else {
          return new APIResult(Status.PARTIAL, "Delete resource has failed");
        }
      }
    }
    return new APIResult(Status.SUCCEEDED, "Delete resource succeeded");
  }

  /**
   * Get a list of key=value parameters set for this session.
   *
   * @param sessionid
   *          session handle object
   * @param verbose
   *          If true, all the parameters will be returned. If false, configuration parameters will be returned
   * @param key
   *          if this is empty, output will contain all parameters and their values, if it is non empty parameters will
   *          be filtered by key
   * @return List of Strings, one entry per key-value pair
   */
  @GET
  @Path("params")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN })
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
   *
   * The parameters can be a hive variable or a configuration. To set key as a hive variable, the key should be prefixed
   * with 'hivevar:'. To set key as configuration parameter, the key should be prefixed with 'hiveconf:' If no prefix is
   * attached, the parameter is set as configuration.
   *
   * @param sessionid
   *          session handle object
   * @param key
   *          parameter key
   * @param value
   *          parameter value
   *
   * @return APIResult object indicating if set operation was successful
   */
  @PUT
  @Path("params")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN })
  public APIResult setParam(@FormDataParam("sessionid") LensSessionHandle sessionid, @FormDataParam("key") String key,
      @FormDataParam("value") String value) {
    sessionService.setSessionParameter(sessionid, key, value);
    return new APIResult(Status.SUCCEEDED, "Set param succeeded");
  }
}

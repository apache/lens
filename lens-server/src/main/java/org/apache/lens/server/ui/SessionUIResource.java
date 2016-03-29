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
package org.apache.lens.server.ui;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.APIResult.Status;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.session.SessionService;

import org.glassfish.jersey.media.multipart.FormDataParam;

import lombok.extern.slf4j.Slf4j;

/**
 * Session resource api
 * <p></p>
 * This provides api for all things in session.
 */
@Path("/uisession")
@Slf4j
public class SessionUIResource {

  /** The open sessions. */
  private static HashMap<UUID, LensSessionHandle> openSessions
    = new HashMap<UUID, LensSessionHandle>();

  /** The session service. */
  private SessionService sessionService;

  /**
   * get open session from uuid
   * @param id
   * @return
   */
  public static LensSessionHandle getOpenSession(UUID id) {
    return openSessions.get(id);
  }

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
   * Instantiates a new session ui resource.
   *
   * @throws LensException the lens exception
   */
  public SessionUIResource() throws LensException {
    sessionService = LensServices.get().getService(SessionService.NAME);
  }

  /**
   * Check session handle.
   *
   * @param sessionHandle the session handle
   */
  private void checkSessionHandle(LensSessionHandle sessionHandle) {
    if (sessionHandle == null) {
      throw new BadRequestException("Invalid session handle");
    }
  }

  /**
   * Create a new session with Lens server.
   *
   * @param username    User name of the Lens server user
   * @param password    Password of the Lens server user
   * @param database    (Optional) Set current database to supplied value
   * @param sessionconf Key-value properties which will be used to configure this session
   * @return A Session handle unique to this session
   */
  @POST
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public LensSessionHandle openSession(@FormDataParam("username") String username,
    @FormDataParam("password") String password,
    @FormDataParam("database") @DefaultValue("") String database,
    @FormDataParam("sessionconf") LensConf sessionconf) {
    try {
      Map<String, String> conf;
      if (sessionconf != null) {
        conf = sessionconf.getProperties();
      } else {
        conf = new HashMap<String, String>();
      }
      LensSessionHandle handle = sessionService.openSession(username, password, database, conf);
      openSessions.put(handle.getPublicId(), handle);
      return handle;
    } catch (LensException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Close a Lens server session.
   *
   * @param publicId Session's public id of the session to be closed
   * @return APIResult object indicating if the operation was successful (check result.getStatus())
   */
  @DELETE
  @Path("{publicId}")
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public APIResult closeSession(@PathParam("publicId") UUID publicId) {
    log.info("Closing session with id: {}", publicId);
    LensSessionHandle sessionHandle = getOpenSession(publicId);
    checkSessionHandle(sessionHandle);
    openSessions.remove(publicId);
    try {
      sessionService.closeSession(sessionHandle);
    } catch (LensException e) {
      return new APIResult(Status.FAILED, e.getMessage());
    }
    return new APIResult(Status.SUCCEEDED, "Close session with id" + sessionHandle + "succeeded");
  }
}

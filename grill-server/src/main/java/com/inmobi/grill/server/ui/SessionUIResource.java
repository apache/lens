package com.inmobi.grill.server.ui;

/*
 * #%L
 * Grill Server
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
import com.inmobi.grill.api.APIResult.Status;
import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.session.HiveSessionService;
import com.inmobi.grill.server.session.SessionResource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Session resource api
 * <p/>
 * This provides api for all things in session.
 */
@Path("/uisession")
public class SessionUIResource {
  public static final Log LOG = LogFactory.getLog(SessionResource.class);
  public static HashMap<UUID, GrillSessionHandle> openSessions = new HashMap<UUID, GrillSessionHandle>();
  private HiveSessionService sessionService;

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

  public SessionUIResource() throws GrillException {
    sessionService = (HiveSessionService) GrillServices.get().getService("session");
  }

  private void checkSessionHandle(GrillSessionHandle sessionHandle) {
    if (sessionHandle == null) {
      throw new BadRequestException("Invalid session handle");
    }
  }

  /**
   * Create a new session with Grill server
   *
   * @param username    User name of the Grill server user
   * @param password    Password of the Grill server user
   * @param sessionconf Key-value properties which will be used to configure this session
   * @return A Session handle unique to this session
   * @throws WebApplicationException if there was an exception thrown while creating the session
   */
  @POST
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public GrillSessionHandle openSession(@FormDataParam("username") String username,
                                        @FormDataParam("password") String password,
                                        @FormDataParam("sessionconf") GrillConf sessionconf) {
    try {
      Map<String, String> conf;
      if (sessionconf != null) {
        conf = sessionconf.getProperties();
      } else {
        conf = new HashMap<String, String>();
      }
      GrillSessionHandle handle = sessionService.openSession(username, password, conf);
      openSessions.put(handle.getPublicId(), handle);
      return handle;
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Close a Grill server session
   *
   * @param publicId Session's public id of the session to be closed
   * @return APIResult object indicating if the operation was successful (check result.getStatus())
   * @throws WebApplicationException if the underlying CLIService threw an exception
   *                                 while closing the session
   */
  @DELETE
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult closeSession(@QueryParam("publicId") UUID publicId) {
    GrillSessionHandle sessionHandle = openSessions.get(publicId);
    checkSessionHandle(sessionHandle);
    openSessions.remove(publicId);
    try {
      sessionService.closeSession(sessionHandle);
    } catch (GrillException e) {
      return new APIResult(Status.FAILED, e.getMessage());
    }
    return new APIResult(Status.SUCCEEDED,
        "Close session with id" + sessionHandle + "succeeded");
  }
}

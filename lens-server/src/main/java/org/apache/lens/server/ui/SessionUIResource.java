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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lens.api.APIResult;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensException;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.APIResult.Status;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.session.HiveSessionService;
import org.apache.lens.server.session.SessionResource;
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

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(SessionResource.class);

  /** The open sessions. */
  public static HashMap<UUID, LensSessionHandle> openSessions = new HashMap<UUID, LensSessionHandle>();

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
   * Instantiates a new session ui resource.
   *
   * @throws LensException
   *           the lens exception
   */
  public SessionUIResource() throws LensException {
    sessionService = (HiveSessionService) LensServices.get().getService("session");
  }

  /**
   * Check session handle.
   *
   * @param sessionHandle
   *          the session handle
   */
  private void checkSessionHandle(LensSessionHandle sessionHandle) {
    if (sessionHandle == null) {
      throw new BadRequestException("Invalid session handle");
    }
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
      LensSessionHandle handle = sessionService.openSession(username, password, conf);
      openSessions.put(handle.getPublicId(), handle);
      return handle;
    } catch (LensException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Close a Lens server session.
   *
   * @param publicId
   *          Session's public id of the session to be closed
   * @return APIResult object indicating if the operation was successful (check result.getStatus())
   */
  @DELETE
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN })
  public APIResult closeSession(@QueryParam("publicId") UUID publicId) {
    LensSessionHandle sessionHandle = openSessions.get(publicId);
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

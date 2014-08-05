package com.inmobi.grill.server.session;

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

import java.util.*;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.glassfish.jersey.media.multipart.FormDataParam;

import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.APIResult.Status;
import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.StringList;
import com.inmobi.grill.server.GrillService;
import com.inmobi.grill.server.GrillServices;

/**
 * Session resource api
 *
 * This provides api for all things in session.
 */
@Path("/session")
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
        sessionService = (HiveSessionService)GrillServices.get().getService("session");
    }

    private void checkSessionHandle(GrillSessionHandle sessionHandle) {
        if (sessionHandle == null) {
            throw new BadRequestException("Invalid session handle");
        }
    }
    /**
     * Create a new session with Grill server
     *
     * @param username User name of the Grill server user
     * @param password Password of the Grill server user
     * @param sessionconf Key-value properties which will be used to configure this session
     *
     * @return A Session handle unique to this session
     *
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
            } else{
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
     *
     * @return APIResult object indicating if the operation was successful (check result.getStatus())
     *
     * @throws WebApplicationException if the underlying CLIService threw an exception
     * while closing the session
     *
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

    /**
     * Add a resource to the session to all GrillServices running in this Grill server
     *
     * <p>
     * The returned @{link APIResult} will have status SUCCEEDED <em>only if</em> the add operation
     * was successful for all services running in this Grill server.
     * </p>
     *
     * @param publicId session's public id
     * @param type The type of resource. Valid types are 'jar', 'file' and 'archive'
     * @param path path of the resource
     * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if add was successful.
     * {@link APIResult} with state {@link Status#PARTIAL}, if add succeeded only for some services.
     * {@link APIResult} with state {@link Status#FAILED}, if add has failed
     */
    @PUT
    @Path("resources/add")
    @Consumes({MediaType.MULTIPART_FORM_DATA})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
    public APIResult addResource(@FormDataParam("publicId") UUID publicId,
                                 @FormDataParam("type") String type, @FormDataParam("path") String path) {
        int numAdded = 0;
        GrillSessionHandle sessionHandle = openSessions.get(publicId);
        checkSessionHandle(sessionHandle);
        for (GrillService service : GrillServices.get().getGrillServices()) {
            try {
                service.addResource(sessionHandle, type, path);
                numAdded++;
            } catch (GrillException e) {
                LOG.error("Failed to add resource in service:" + service, e);
                if (numAdded != 0) {
                    return new APIResult(Status.PARTIAL,
                            "Add resource is partial, failed for service:" + service.getName());
                } else {
                    return new APIResult(Status.FAILED,
                            "Add resource has failed ");
                }
            }
        }
        return new APIResult(Status.SUCCEEDED,
                "Add resource succeeded");
    }

    /**
     * Delete a resource from sesssion from all the @{link GrillService}s running in this Grill server
     * <p>
     * Similar to addResource, this call is successful only if resource was deleted from all services.
     * </p>
     *
     * @param publicId session's public id
     * @param type The type of resource. Valid types are 'jar', 'file' and 'archive'
     * @param path path of the resource to be deleted
     *
     * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if delete was successful.
     * {@link APIResult} with state {@link Status#PARTIAL}, if delete succeeded only for some services.
     * {@link APIResult} with state {@link Status#FAILED}, if delete has failed
     */
    @PUT
    @Path("resources/delete")
    @Consumes({MediaType.MULTIPART_FORM_DATA})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
    public APIResult deleteResource(@FormDataParam("publicId") UUID publicId,
                                    @FormDataParam("type") String type, @FormDataParam("path") String path) {
        int numDeleted = 0;
        GrillSessionHandle sessionHandle = openSessions.get(publicId);
        checkSessionHandle(sessionHandle);
        for (GrillService service : GrillServices.get().getGrillServices()) {
            try {
                service.deleteResource(sessionHandle,  type, path);
                numDeleted++;
            } catch (GrillException e) {
                LOG.error("Failed to delete resource in service:" + service, e);
                if (numDeleted != 0) {
                    return new APIResult(Status.PARTIAL,
                            "Delete resource is partial, failed for service:" + service.getName());
                } else {
                    return new APIResult(Status.PARTIAL,
                            "Delete resource has failed");
                }
            }
        }
        return new APIResult(Status.SUCCEEDED,
                "Delete resource succeeded");
    }

    /**
     * Get a list of key=value parameters set for this session
     *
     * @param publicId session's public id
     * @param verbose If true, all the parameters will be returned.
     *  If false, configuration parameters will be returned
     * @param key if this is empty, output will contain all parameters and their values,
     * if it is non empty parameters will be filtered by key
     *
     * @return List of Strings, one entry per key-value pair
     */
    /*
    @GET
    @Path("params")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
    public StringList getParams(@QueryParam("publicId") UUID publicId,
                                @DefaultValue("false") @QueryParam("verbose") boolean verbose,
                                @DefaultValue("") @QueryParam("key") String key) {
        RowSet rows = null;
        List<String> result = new ArrayList<String>();
        GrillSessionHandle sessionHandle = openSessions.get(publicId);
        checkSessionHandle(sessionHandle);
        try {
            OperationHandle handle = sessionService.getAllSessionParameters(sessionHandle, verbose, key);
            rows = sessionService.getCliService().fetchResults(handle);
        } catch (HiveSQLException e) {
            if (e.getMessage().contains(key + " is undefined")) {
                throw new NotFoundException(e.getMessage());
            } else {
                throw new WebApplicationException(e);
            }
        } catch (GrillException e) {
            throw new WebApplicationException(e);
        }
        Iterator<Object[]> itr = rows.iterator();
        while (itr.hasNext()) {
            result.add((String)itr.next()[0]);
        }
        return new StringList(result);
    }
    */

    /**
     * Set value for a parameter specified by key
     *
     * The parameters can be a system property or a hive variable or a configuration.
     * To set key as system property, the key should be prefixed with 'system:'.
     * To set key as a hive variable, the key should be prefixed with 'hivevar:'.
     * To set key as configuration parameter, the key should be prefixed with 'hiveconf:'.
     * If no prefix is attached, the parameter is set as configuration.
     *
     * System properties are not restricted to the session, they would be set globally
     *
     * @param publicId session's public id
     * @param key parameter key
     * @param value parameter value
     *
     * @return APIResult object indicating if set operation was successful
     */
    /*
    @PUT
    @Path("params")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
    public APIResult setParam(@FormDataParam("publicId") UUID publicId,
                              @FormDataParam("key") String key, @FormDataParam("value") String value) {
        GrillSessionHandle sessionHandle = openSessions.get(publicId);
        checkSessionHandle(sessionHandle);
        sessionService.setSessionParameter(sessionHandle, key, value);
        return new APIResult(Status.SUCCEEDED, "Set param succeeded");
    }
    */
}

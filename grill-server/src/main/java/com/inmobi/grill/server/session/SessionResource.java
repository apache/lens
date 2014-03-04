package com.inmobi.grill.server.session;

import java.util.ArrayList;
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
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.thrift.TRow;
import org.glassfish.jersey.media.multipart.FormDataParam;

import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.StringList;
import com.inmobi.grill.server.GrillService;
import com.inmobi.grill.server.GrillServices;

@Path("/session")
public class SessionResource {
  public static final Log LOG = LogFactory.getLog(SessionResource.class);
  private HiveSessionService sessionService;

  /**
   * Call to check if the session resource is deployed in this Grill server
   * @return string indicating session resource is deployed
   */
  @GET
  @Produces({MediaType.TEXT_PLAIN})
  public String getMessage() {
    return "Hello World! from command";
  }

  public SessionResource() throws GrillException {
    sessionService = (HiveSessionService)GrillServices.get().getService("session");
  }

  /**
   * Create a new session with Grill server
   * @param username User name of the Grill server user
   * @param password Password of the Grill server user
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
      } else{
        conf = new HashMap<String, String>();
      }
      return sessionService.openSession(username, password, conf);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Close a Grill server session 
   * @param sessionid Session handle object of the session to be closed
   * @return APIResult object indicating if the operation was successful (check result.getStatus())
   * @throws WebApplicationException if the underlying CLIService threw an exception 
   * while closing the session
   */
  @DELETE
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult closeSession(@QueryParam("sessionid") GrillSessionHandle sessionid) {
    try {
      sessionService.closeSession(sessionid);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
    return new APIResult(APIResult.Status.SUCCEEDED,
        "Close session with id" + sessionid + "succeeded");
  }

  /**
   * Add a resource (a jar, for example) to all GrillServices running in this Grill server
   * 
   * <p>
   * The returned @{link APIResult} will have status SUCCEEDED <em>only if</em> the add operation
   * was successful for all services running in this Grill server.
   * </p>
   * 
   * <p>
   * The call tries to add resource to all services. In case the add operation fails for 
   * one of the services, the 
   * API call will return immediately without adding resource on the remaining services.
   * In such cases, the result object will have status set to APIResult.Status.PARTIAL
   * </p>
   * @param sessionid session handle object
   * @param type type of the resource to be added. For jar files use 'jar'
   * @param path path of the resource
   * @return APIResult object indicating if the add operation succeeded
   */
  @PUT
  @Path("resources/add")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult addResource(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @FormDataParam("type") String type, @FormDataParam("path") String path) {
    for (GrillService service : GrillServices.get().getGrillServices()) {
      try {
        service.addResource(sessionid,  type, path);
      } catch (GrillException e) {
        LOG.error("Failed to add resource in service:" + service, e);
        return new APIResult(APIResult.Status.PARTIAL,
            "Add resource is partial, failed for service:" + service.getName());
      }
    }
    return new APIResult(APIResult.Status.SUCCEEDED,
        "Add resource succeeded");
  }

  /**
   * Delete a resource from @{link GrillService}s running in this Grill server
   * <p>
   * Similar to addResource, this call is successful only if resource was deleted from all services.
   * 
   * If the delete operation failed for any of the services, then the call returns immediately with 
   * status set to APIResult.Status.PARTIAL.
   * </p>
   * 
   * @param sessionid session handle object
   * @param type type of the resource to be deleted. For jar files, use 'jar'
   * @param path path of the resource to be deleted
   * @return APIResult object indicating if the delete resource call was successful
   */
  @PUT
  @Path("resources/delete")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult deleteResource(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @FormDataParam("type") String type, @FormDataParam("path") String path) {
    for (GrillService service : GrillServices.get().getGrillServices()) {
      try {
        service.deleteResource(sessionid,  type, path);
      } catch (GrillException e) {
        LOG.error("Failed to delete resource in service:" + service, e);
        return new APIResult(APIResult.Status.PARTIAL,
            "Add resource is partial, failed for service:" + service.getName());
      }
    }
    return new APIResult(APIResult.Status.SUCCEEDED,
        "Delete resource succeeded");
  }

  /**
   * Get a list of key=value parameters set for this session
   * @param sessionid session handle object
   * @param verbose set this to true if verbose result is required
   * @param key if this is empty, output will contain all parameters and their values, 
   * if it is non empty parameters will be filtered by key
   * @return List of Strings, one entry per key-value pair
   */
  @GET
  @Path("params")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public StringList getParams(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @DefaultValue("false") @QueryParam("verbose") boolean verbose,
      @DefaultValue("") @QueryParam("key") String key) {
    OperationHandle handle = sessionService.getAllSessionParameters(sessionid, verbose, key);
    RowSet rows = null;
    try {
      rows = sessionService.getCliService().fetchResults(handle);
    } catch (HiveSQLException e) {
      new WebApplicationException(e);
    }
    List<String> result = new ArrayList<String>();
    for (TRow row : rows.toTRowSet().getRows()) {
      result.add(row.getColVals().get(0).getStringVal().getValue()); 
    }
    return new StringList(result);
  }

  /**
   * Set value for a parameter specified by key
   * @param sessionid session handle object
   * @param key parameter key
   * @param value parameter value
   * @return APIResult object indicating if set operation was successful
   */
  @PUT
  @Path("params")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult setParam(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @FormDataParam("key") String key, @FormDataParam("value") String value) {
    sessionService.setSessionParameter(sessionid, key, value);
    return new APIResult(APIResult.Status.SUCCEEDED, "Set param succeeded");
  }

}

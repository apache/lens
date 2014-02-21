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

  @GET
  @Produces({MediaType.TEXT_PLAIN})
  public String getMessage() {
    return "Hello World! from command";
  }

  public SessionResource() throws GrillException {
    sessionService = (HiveSessionService)GrillServices.get().getService("session");
  }

  @POST
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public GrillSessionHandle openSession(@FormDataParam("username") String username,
      @FormDataParam("password") String password,
      @DefaultValue("<conf/>") @FormDataParam("sessionconf") GrillConf sessionconf) {
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

  @PUT
  @Path("params")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult setParam(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @FormDataParam("key") String key, @FormDataParam("value") String value) {
    sessionService.setSessionParameter(sessionid, key, value);
    return new APIResult(APIResult.Status.SUCCEEDED, "Set param succeeded");
  }

}

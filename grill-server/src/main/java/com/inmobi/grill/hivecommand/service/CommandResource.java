package com.inmobi.grill.hivecommand.service;

import java.util.ArrayList;
import java.util.List;

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

import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.thrift.TRow;
import org.apache.hive.service.cli.thrift.TRow._Fields;
import org.glassfish.jersey.media.multipart.FormDataParam;

import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.client.api.APIResult;
import com.inmobi.grill.client.api.QueryConf;
import com.inmobi.grill.client.api.StringList;
import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.service.GrillServices;

@Path("/command")
public class CommandResource {
  private HiveCommandService commandService;

  @GET
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML,
    MediaType.TEXT_PLAIN})
  public String getMessage() {
    return "Hello World! from command";
  }

  public CommandResource() throws GrillException {
    commandService = (HiveCommandService)GrillServices.get().getService("command");
  }

  @POST
  @Path("session")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public GrillSessionHandle openSession(@FormDataParam("username") String username,
      @FormDataParam("password") String password,
      @FormDataParam("sessionconf") QueryConf sessionconf) {
    try {
      return new GrillSessionHandle(commandService.openSession(username, password, sessionconf.getProperties()));
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @DELETE
  @Path("session")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult closeSession(@QueryParam("sessionid") GrillSessionHandle sessionid) {
    try {
      commandService.closeSession(sessionid.getSessionHandle());
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
    return new APIResult(APIResult.Status.SUCCEEDED,
        "Close session with id" + sessionid.getSessionHandle() + "succeeded");
  }

  @PUT
  @Path("resources/add")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult addResource(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @FormDataParam("type") String type, @FormDataParam("path") String path) {
    commandService.addResource(sessionid,  type, path);
    return new APIResult(APIResult.Status.SUCCEEDED,
        "Add resource succeeded");
  }

  @PUT
  @Path("resources/delete")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult deleteResource(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @FormDataParam("type") String type, @FormDataParam("path") String path) {
    commandService.deleteResource(sessionid,  type, path);
    return new APIResult(APIResult.Status.SUCCEEDED,
        "Delete resource succeeded");
  }

  @GET
  @Path("session/params")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public StringList getParams(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @DefaultValue("false") @QueryParam("verbose") boolean verbose,
      @DefaultValue("") @QueryParam("key") String key) {
    OperationHandle handle = commandService.getAllSessionParameters(sessionid, verbose, key);
    RowSet rows = null;
    try {
      rows = commandService.getCliService().fetchResults(handle);
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
  @Path("session/params")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult setParam(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @FormDataParam("key") String key, @FormDataParam("value") String value) {
    commandService.setSessionParameter(sessionid, key, value);
    return new APIResult(APIResult.Status.SUCCEEDED, "Set param succeeded");
  }

}

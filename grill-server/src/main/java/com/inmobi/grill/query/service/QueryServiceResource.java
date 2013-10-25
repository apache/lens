package com.inmobi.grill.query.service;

import java.util.List;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.glassfish.jersey.media.multipart.FormDataParam;

import com.inmobi.grill.client.api.APIResult;
import com.inmobi.grill.client.api.QueryConf;
import com.inmobi.grill.client.api.QueryContext;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryPrepareHandle;
import com.inmobi.grill.api.QuerySubmitResult;
import com.inmobi.grill.client.api.QueryResult;
import com.inmobi.grill.client.api.QueryResultSetMetadata;
import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.server.api.QueryExecutionService;
import com.inmobi.grill.service.GrillServices;

@Path("/queryapi")
public class QueryServiceResource {

  private QueryExecutionService queryServer;

  @GET
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public String getMessage() {
    return "Hello World! from queryapi";
  }

  public QueryServiceResource() throws GrillException {
    queryServer = (QueryExecutionService)GrillServices.get().getService("query");
  }

  QueryExecutionService getQueryServer() {
    return queryServer;
  }

  @GET
  @Path("queries")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public List<QueryHandle> getAllQueries(
      @DefaultValue("") @QueryParam("state") String state,
      @DefaultValue("") @QueryParam("user") String user) {
    try {
      return queryServer.getAllQueries(state, user);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @POST
  @Path("queries")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public QuerySubmitResult query(@FormDataParam("query") String query,
      @FormDataParam("operation") String op,
      @FormDataParam("conf") QueryConf conf,
      @DefaultValue("30000") @FormDataParam("timeoutmillis") Long timeoutmillis) {
    try {
      SubmitOp sop = SubmitOp.valueOf(op.toUpperCase());
      switch (sop) {
      case EXECUTE:
        return queryServer.executeAsync(query, conf);
      case PREPARE:
        return queryServer.prepare(query, conf);
      case EXPLAIN:
        return queryServer.explain(query, conf);
      case EXPLAIN_AND_PREPARE:
        return queryServer.explainAndPrepare(query, conf);
      case EXECUTE_WITH_TIMEOUT:
        return queryServer.execute(query, timeoutmillis, conf);
      default:
        throw new GrillException("Invalid operation type");
      }
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @DELETE
  @Path("queries")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult cancelAllQueries(
      @DefaultValue("") @QueryParam("state") String state,
      @DefaultValue("") @QueryParam("user") String user) {
    //TODO
    return null;
  }

  private QueryHandle getQueryHandle(String queryHandle) {
    try {
      return QueryHandle.fromString(queryHandle);
    } catch (Exception e) {
      throw new BadRequestException(e);
    }
  }

  private QueryPrepareHandle getPrepareHandle(String prepareHandle) {
    try {
      return QueryPrepareHandle.fromString(prepareHandle);
    } catch (Exception e) {
      throw new BadRequestException(e);
    }
  }

  @GET
  @Path("queries/{queryhandle}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public QueryContext getStatus(@PathParam("queryhandle") String queryHandle) {
    try {
      com.inmobi.grill.api.QueryContext ctx = queryServer.getQueryContext(
          getQueryHandle(queryHandle));
      return new QueryContext(ctx.getQueryHandle(),
          ctx.getUserQuery(), ctx.getSubmittedUser(), ctx.getSubmissionTime(),
          ctx.getPriority(), ctx.isPersistent(),
          ctx.getSelectedDriver() == null ? null : ctx.getSelectedDriver().getClass().getCanonicalName(),
          ctx.getDriverQuery(), ctx.getStatus(), ctx.getResultSetPath());
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @DELETE
  @Path("queries/{queryhandle}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult cancelQuery(@PathParam("queryhandle") String queryHandle) {
    try {
      boolean ret = queryServer.cancelQuery(getQueryHandle(queryHandle));
      if (ret) {
        return new APIResult(APIResult.Status.SUCCEEDED, "Cancel on the query "
            + queryHandle + " is successful");
      } else {
        return new APIResult(APIResult.Status.FAILED, "Cancel on the query "
            + queryHandle + " failed");        
      }
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @PUT
  @Path("queries/{queryhandle}")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult updateConf(@PathParam("queryhandle") String queryHandle, 
      @FormDataParam("conf") QueryConf conf) {
    try {
      boolean ret = queryServer.updateQueryConf(getQueryHandle(queryHandle), conf);
      if (ret) {
        return new APIResult(APIResult.Status.SUCCEEDED, "Cancel on the query "
            + queryHandle + " is successful");
      } else {
        return new APIResult(APIResult.Status.FAILED, "Cancel on the query "
            + queryHandle + " failed");        
      }
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @POST
  @Path("queries/{prepareHandle}")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public QueryHandle executePrepared(@PathParam("prepareHandle") String prepareHandle, 
      @FormDataParam("conf") QueryConf conf) {
    try {
      return queryServer.executePrepareAsync(
          getPrepareHandle(prepareHandle), conf);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @GET
  @Path("queries/{queryhandle}/resultsetmetadata")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public QueryResultSetMetadata getResultSetMetadata(
      @PathParam("queryhandle") String queryHandle) {
    try {
      return queryServer.getResultSetMetadata(getQueryHandle(queryHandle));
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @GET
  @Path("queries/{queryhandle}/resultset")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public QueryResult getResultSet(
      @PathParam("queryhandle") String queryHandle,
      @QueryParam("fromindex") long startIndex,
      @QueryParam("fetchsize") int fetchSize) {
    try {
      return queryServer.fetchResultSet(getQueryHandle(queryHandle), startIndex, fetchSize);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @DELETE
  @Path("queries/{queryhandle}/resultset")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult closeResultSet(
      @PathParam("queryhandle") String queryHandle){
    try {
      queryServer.closeResultSet(getQueryHandle(queryHandle));
      return new APIResult(APIResult.Status.SUCCEEDED, "Close on the result set"
          + " for query " + queryHandle + " is successful");

    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

}

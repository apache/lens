package com.inmobi.grill.query.service;

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
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import com.inmobi.grill.client.api.APIResult;
import com.inmobi.grill.client.api.QueryConf;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QuerySubmitResult;
import com.inmobi.grill.client.api.QueryList;
import com.inmobi.grill.client.api.QueryResult;
import com.inmobi.grill.client.api.QueryResultSetMetadata;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.server.api.QueryExecutionService;

@Path("/queryapi")
public class QueryServiceResource {

  private QueryExecutionService queryServer;

  @GET
  @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
  public String getMessage() {
      return "Hello World! from queryapi";
  }

  public QueryServiceResource() throws GrillException {
    queryServer = new QueryExcecutionServiceImpl(new Configuration());
  }

  @GET
  @Path("queries")
  @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
  public QueryList getAllQueries(
      @DefaultValue("") @QueryParam("state") String state,
      @DefaultValue("") @QueryParam("user") String user) {
    try {
      return queryServer.getAllQueries(state, user);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  enum SUBMITOP {EXECUTE, EXPLAIN, PREPARE, EXPLAIN_AND_PREPARE,
    EXECUTE_WITH_TIMEOUT};

  @POST
  @Path("queries")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
  public QuerySubmitResult query(@FormDataParam("query") String query,
      @FormDataParam("operation") String op,
      @FormDataParam("conf") QueryConf conf,
      @DefaultValue("30000") @FormDataParam("timeoutmillis") Long timeoutmillis) {
    try {
      SUBMITOP sop = SUBMITOP.valueOf(op.toUpperCase());
      switch (sop) {
      case EXECUTE:
        return queryServer.executeAsync(query, conf);
      case PREPARE:
     //   return queryServer.prepare(query, conf);
      case EXPLAIN:
     //   return queryServer.explain(query, conf);
      case EXPLAIN_AND_PREPARE:
     //   return queryServer.explainAndPrepare(query, conf);
      case EXECUTE_WITH_TIMEOUT:
     //   return queryServer.execute(query, timeoutmillis, conf);
      default:
        throw new GrillException("Invalid operation type");
      }
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @DELETE
  @Path("queries")
  @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
  public APIResult cancelAllQueries(
      @DefaultValue("") @QueryParam("state") String state,
      @DefaultValue("") @QueryParam("user") String user) {
    //TODO
    return null;
  }

  @GET
  @Path("queries/{queryhandle}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
  public QueryStatus getStatus(@PathParam("queryhandle") String queryHandle) {
    try {
      return queryServer.getStatus(queryHandle);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @DELETE
  @Path("queries/{queryhandle}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
  public APIResult cancelQuery(@PathParam("queryhandle") String queryHandle) {
    try {
      boolean ret = queryServer.cancelQuery(queryHandle);
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
  @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
  public APIResult updateConf(@PathParam("queryhandle") String queryHandle, 
      @FormDataParam("conf") QueryConf conf) {
    try {
      boolean ret = queryServer.updateQueryConf(queryHandle, conf);
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
  @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
  public QueryHandle executePrepared(@PathParam("prepareHandle") String prepareHandle, 
      @FormDataParam("conf") QueryConf conf) {
    try {
      return queryServer.executePrepareAsync(prepareHandle, conf);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @GET
  @Path("queries/{queryhandle}/resultsetmetadata")
  @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
  public QueryResultSetMetadata getResultSetMetadata(
      @PathParam("queryhandle") String queryHandle) {
    try {
      return queryServer.getResultSetMetadata(queryHandle);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @GET
  @Path("queries/{queryhandle}/resultset")
  @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
  public QueryResult getResultSet(
      @PathParam("queryhandle") String queryHandle,
      @QueryParam("fromindex") long startIndex,
      @QueryParam("fetchsize") int fetchSize) {
    try {
      return queryServer.fetchResultSet(queryHandle, startIndex, fetchSize);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @DELETE
  @Path("queries/{queryhandle}/resultset")
  @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
  public APIResult closeResultSet(
      @PathParam("queryhandle") String queryHandle){
    try {
      queryServer.closeResultSet(queryHandle);
      return new APIResult(APIResult.Status.SUCCEEDED, "Close on the result set"
          + " for query " + queryHandle + " is successful");

    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

}

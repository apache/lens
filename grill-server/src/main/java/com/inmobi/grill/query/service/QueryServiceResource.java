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

import org.apache.commons.lang.StringUtils;
import org.apache.hive.service.cli.SessionHandle;
import org.glassfish.jersey.media.multipart.FormDataParam;

import com.inmobi.grill.client.api.APIResult;
import com.inmobi.grill.client.api.InMemoryQueryResult;
import com.inmobi.grill.client.api.PersistentQueryResult;
import com.inmobi.grill.client.api.PreparedQueryContext;
import com.inmobi.grill.client.api.QueryConf;
import com.inmobi.grill.client.api.QueryContext;
import com.inmobi.grill.client.api.QueryPlan;
import com.inmobi.grill.api.GrillResultSet;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.InMemoryResultSet;
import com.inmobi.grill.api.PersistentResultSet;
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
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML,
    MediaType.TEXT_PLAIN})
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
  public List<QueryHandle> getAllQueries(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @DefaultValue("") @QueryParam("state") String state,
      @DefaultValue("") @QueryParam("user") String user) {
    try {
      return queryServer.getAllQueries(sessionid, state, user);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @POST
  @Path("queries")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public QuerySubmitResult query(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @FormDataParam("query") String query,
      @FormDataParam("operation") String op,
      @FormDataParam("conf") QueryConf conf,
      @DefaultValue("30000") @FormDataParam("timeoutmillis") Long timeoutmillis) {
    try {
      SubmitOp sop = SubmitOp.valueOf(op.toUpperCase());
      if (sop == null) {
        throw new BadRequestException("Invalid operation type: " + op);
      }
      switch (sop) {
      case EXECUTE:
        return queryServer.executeAsync(sessionid, query, conf);
      case EXPLAIN:
        return new QueryPlan(queryServer.explain(sessionid, query, conf));
      case EXECUTE_WITH_TIMEOUT:
        return queryServer.execute(sessionid, query, timeoutmillis, conf);
      default:
        throw new BadRequestException("Invalid operation type: " + op);
      }
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @DELETE
  @Path("queries")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult cancelAllQueries(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @DefaultValue("") @QueryParam("state") String state,
      @DefaultValue("") @QueryParam("user") String user) {
    List<QueryHandle> handles = getAllQueries(sessionid, state, user);
    int numCancelled = 0;
    for (QueryHandle handle : handles) {
      if (cancelQuery(sessionid, handle)) {
        numCancelled++;
      }
    }
    String msgString = (StringUtils.isBlank(state) ? "" : " in state" + state)
        + (StringUtils.isBlank(user) ? "" : " for user " + user);
    if (numCancelled == handles.size()) {
      return new APIResult(APIResult.Status.SUCCEEDED, "Cancel all queries "
          + msgString + " is successful");
    } else if (numCancelled == 0) {
      return new APIResult(APIResult.Status.FAILED, "Cancel on the query "
          + msgString + " has failed");        
    } else {
      return new APIResult(APIResult.Status.PARTIAL, "Cancel on the query "
          + msgString + " is partial");        
    }
  }

  @GET
  @Path("preparedqueries")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public List<QueryPrepareHandle> getAllPreparedQueries(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @DefaultValue("") @QueryParam("user") String user) {
    try {
      return queryServer.getAllPreparedQueries(sessionid, user);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @POST
  @Path("preparedqueries")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public QuerySubmitResult prepareQuery(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @FormDataParam("query") String query,
      @DefaultValue("") @FormDataParam("operation") String op,
      @FormDataParam("conf") QueryConf conf) {
    try {
      SubmitOp sop = SubmitOp.valueOf(op.toUpperCase());
      if (sop == null) {
        throw new BadRequestException("Invalid submit operation: " + op);
      }
      switch (sop) {
      case PREPARE:
        return queryServer.prepare(sessionid, query, conf);
      case EXPLAIN_AND_PREPARE:
        return new QueryPlan(queryServer.explainAndPrepare(sessionid, query, conf));
      default:
        throw new BadRequestException("Invalid submit operation: " + op);
      }
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @DELETE
  @Path("preparedqueries")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult destroyPreparedQueries(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @DefaultValue("") @QueryParam("user") String user) {
    List<QueryPrepareHandle> handles = getAllPreparedQueries(sessionid, user);
    int numDestroyed = 0;
    for (QueryPrepareHandle prepared : handles) {
      if (destroyPrepared(sessionid, prepared)) {
        numDestroyed++;
      }
    }
    String msgString = (StringUtils.isBlank(user) ? "" : " for user " + user);
    if (numDestroyed == handles.size()) {
      return new APIResult(APIResult.Status.SUCCEEDED, "Destroy all prepared "
          + "queries " + msgString + " is successful");
    } else if (numDestroyed == 0) {
      return new APIResult(APIResult.Status.FAILED, "Destroy all prepared "
          + "queries " + msgString + " has failed");        
    } else {
      return new APIResult(APIResult.Status.PARTIAL, "Destroy all prepared "
          + "queries " + msgString +" is partial");        
    }
  }

  private QueryHandle getQueryHandle(String queryHandle) {
    try {
      return QueryHandle.fromString(queryHandle);
    } catch (Exception e) {
      throw new BadRequestException("Invalid query handle: "  + queryHandle, e);
    }
  }

  private QueryPrepareHandle getPrepareHandle(String prepareHandle) {
    try {
      return QueryPrepareHandle.fromString(prepareHandle);
    } catch (Exception e) {
      throw new BadRequestException("Invalid prepared query handle: " + prepareHandle, e);
    }
  }

  @GET
  @Path("preparedqueries/{preparehandle}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public PreparedQueryContext getPreparedQuery(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("preparehandle") String prepareHandle) {
    try {
      return new PreparedQueryContext(queryServer.getPreparedQueryContext(sessionid,
          getPrepareHandle(prepareHandle)));
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @DELETE
  @Path("preparedqueries/{preparehandle}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult destroyPrepared(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("preparehandle") String prepareHandle) {
    boolean ret = destroyPrepared(sessionid, getPrepareHandle(prepareHandle));
    if (ret) {
      return new APIResult(APIResult.Status.SUCCEEDED, "Destroy on the query "
          + prepareHandle + " is successful");
    } else {
      return new APIResult(APIResult.Status.FAILED, "Destroy on the query "
          + prepareHandle + " failed");        
    }
  }

  @GET
  @Path("queries/{queryhandle}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public QueryContext getStatus(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("queryhandle") String queryHandle) {
    try {
      return new QueryContext(queryServer.getQueryContext(sessionid,
          getQueryHandle(queryHandle)));
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @DELETE
  @Path("queries/{queryhandle}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult cancelQuery(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("queryhandle") String queryHandle) {
    boolean ret = cancelQuery(sessionid, getQueryHandle(queryHandle));
    if (ret) {
      return new APIResult(APIResult.Status.SUCCEEDED, "Cancel on the query "
          + queryHandle + " is successful");
    } else {
      return new APIResult(APIResult.Status.FAILED, "Cancel on the query "
          + queryHandle + " failed");        
    }
  }

  private boolean cancelQuery(GrillSessionHandle sessionid, QueryHandle queryHandle) {
    try {
      return queryServer.cancelQuery(sessionid, queryHandle);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  private boolean destroyPrepared(GrillSessionHandle sessionid, QueryPrepareHandle queryHandle) {
    try {
      return queryServer.destroyPrepared(sessionid, queryHandle);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }


  @PUT
  @Path("queries/{queryhandle}")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult updateConf(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("queryhandle") String queryHandle, 
      @FormDataParam("conf") QueryConf conf) {
    try {
      boolean ret = queryServer.updateQueryConf(sessionid, getQueryHandle(queryHandle), conf);
      if (ret) {
        return new APIResult(APIResult.Status.SUCCEEDED, "Update on the query conf for "
            + queryHandle + " is successful");
      } else {
        return new APIResult(APIResult.Status.FAILED, "Update on the query conf for "
            + queryHandle + " failed");        
      }
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @PUT
  @Path("preparedqueries/{prepareHandle}")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult updatePreparedConf(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("prepareHandle") String prepareHandle, 
      @FormDataParam("conf") QueryConf conf) {
    try {
      boolean ret = queryServer.updateQueryConf(sessionid, getPrepareHandle(prepareHandle), conf);
      if (ret) {
        return new APIResult(APIResult.Status.SUCCEEDED, "Update on the query conf for "
            + prepareHandle + " is successful");
      } else {
        return new APIResult(APIResult.Status.FAILED, "Update on the query conf for "
            + prepareHandle + " failed");        
      }
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @POST
  @Path("preparedqueries/{prepareHandle}")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public QueryHandle executePrepared(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("prepareHandle") String prepareHandle, 
      @FormDataParam("conf") QueryConf conf) {
    try {
      return queryServer.executePrepareAsync(sessionid,
          getPrepareHandle(prepareHandle), conf);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @GET
  @Path("queries/{queryhandle}/resultsetmetadata")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public QueryResultSetMetadata getResultSetMetadata(
      @QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("queryhandle") String queryHandle) {
    try {
      return new QueryResultSetMetadata(queryServer.getResultSetMetadata(sessionid, getQueryHandle(queryHandle)));
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @GET
  @Path("queries/{queryhandle}/resultset")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public QueryResult getResultSet(
      @QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("queryhandle") String queryHandle,
      @QueryParam("fromindex") long startIndex,
      @QueryParam("fetchsize") int fetchSize) {
    try {
      GrillResultSet result = queryServer.fetchResultSet(sessionid, getQueryHandle(queryHandle), startIndex, fetchSize);
      if (result instanceof PersistentResultSet) {
        return new PersistentQueryResult(((PersistentResultSet) result).getOutputPath());
      } else {
        return new InMemoryQueryResult((InMemoryResultSet)result);
      }
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @DELETE
  @Path("queries/{queryhandle}/resultset")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult closeResultSet(
      @QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("queryhandle") String queryHandle){
    try {
      queryServer.closeResultSet(sessionid, getQueryHandle(queryHandle));
      return new APIResult(APIResult.Status.SUCCEEDED, "Close on the result set"
          + " for query " + queryHandle + " is successful");

    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

}

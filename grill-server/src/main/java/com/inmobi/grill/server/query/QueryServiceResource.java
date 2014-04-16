package com.inmobi.grill.server.query;

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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.glassfish.jersey.media.multipart.FormDataParam;

import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.APIResult.Status;
import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.query.GrillPreparedQuery;
import com.inmobi.grill.api.query.GrillQuery;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryHandleWithResultSet;
import com.inmobi.grill.api.query.QueryPlan;
import com.inmobi.grill.api.query.QueryPrepareHandle;
import com.inmobi.grill.api.query.QueryResult;
import com.inmobi.grill.api.query.QueryResultSetMetadata;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.api.query.QuerySubmitResult;
import com.inmobi.grill.api.query.SubmitOp;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.metrics.MetricsService;
import com.inmobi.grill.server.api.query.QueryExecutionService;

/**
 * queryapi resource 
 * 
 * This provides api for all things query. 
 *
 */
@Path("/queryapi")
public class QueryServiceResource {
  public static final Logger LOG = LogManager.getLogger(QueryServiceResource.class);

  private QueryExecutionService queryServer;

  private void checkSessionId(GrillSessionHandle sessionHandle) {
    if (sessionHandle == null) {
      throw new BadRequestException("Invalid session handle");
    }
  }

  private void checkQuery(String query) {
    if (StringUtils.isBlank(query)) {
      throw new BadRequestException("Invalid query");
    }
  }
  /**
   * API to know if Query service is up and running
   * 
   * @return Simple text saying it up
   */
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getMessage() {
    return "Queryapi is up";
  }

  public QueryServiceResource() throws GrillException {
    queryServer = (QueryExecutionService)GrillServices.get().getService("query");
  }

  QueryExecutionService getQueryServer() {
    return queryServer;
  }

  /**
   * Get all the queries in the query server; can be filtered with state and user.
   * 
   * @param sessionid The sessionid in which user is working
   * @param state If any state is passed, all the queries in that state will be returned,
   * otherwise all queries will be returned. Possible states are {@value QueryStatus.Status#values()}
   * @param user If any user is passed, all the queries submitted by the user will be returned,
   * otherwise all the queries will be returned
   * 
   * @return List of {@link QueryHandle} objects
   */
  @GET
  @Path("queries")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public List<QueryHandle> getAllQueries(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @DefaultValue("") @QueryParam("state") String state,
      @DefaultValue("") @QueryParam("user") String user) {
    checkSessionId(sessionid);
    try {
      return queryServer.getAllQueries(sessionid, state, user);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  static String submitClue = ". supported values are:" + SubmitOp.EXPLAIN  
      + ", " + SubmitOp.EXECUTE + " and " + SubmitOp.EXECUTE_WITH_TIMEOUT;
  static String prepareClue = ". supported values are:" + SubmitOp.PREPARE
      + " and " + SubmitOp.EXPLAIN_AND_PREPARE;
  static String submitPreparedClue = ". supported values are:" 
      + SubmitOp.EXECUTE + " and " + SubmitOp.EXECUTE_WITH_TIMEOUT;

  /**
   * Submit the query for explain or execute or execute with a timeout
   * 
   * @param sessionid The session in which user is submitting the query. Any
   *  configuration set in the session will be picked up.
   * @param query The query to run
   * @param op The operation on the query. Supported operations are 
   * {@value SubmitOp#EXPLAIN}, {@value SubmitOp#EXECUTE} and {@value SubmitOp#EXECUTE_WITH_TIMEOUT}
   * @param conf The configuration for the query
   * @param timeoutmillis The timeout for the query, honored only in case of
   *  {@value SubmitOp#EXECUTE_WITH_TIMEOUT} operation
   * 
   * @return {@link QueryHandle} in case of {@value SubmitOp#EXECUTE} operation.
   * {@link QueryPlan} in case of {@value SubmitOp#EXPLAIN} operation.
   * {@link QueryHandleWithResultSet} in case {@value SubmitOp#EXECUTE_WITH_TIMEOUT} operation.
   */
  @POST
  @Path("queries")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public QuerySubmitResult query(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @FormDataParam("query") String query,
      @FormDataParam("operation") String op,
      @FormDataParam("conf") GrillConf conf,
      @DefaultValue("30000") @FormDataParam("timeoutmillis") Long timeoutmillis) {
    checkQuery(query);
    checkSessionId(sessionid);
    try {
      SubmitOp sop = null;
      try {
        sop = SubmitOp.valueOf(op.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new BadRequestException(e);
      }
      if (sop == null) {
        throw new BadRequestException("Invalid operation type: " + op +
            submitClue);
      }
      switch (sop) {
      case EXECUTE:
        return queryServer.executeAsync(sessionid, query, conf);
      case EXPLAIN:
        return queryServer.explain(sessionid, query, conf);
      case EXECUTE_WITH_TIMEOUT:
        return queryServer.execute(sessionid, query, timeoutmillis, conf);
      default:
        throw new BadRequestException("Invalid operation type: " + op + submitClue);
      }
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Cancel all the queries in query server; can be filtered with state and user
   * 
   * @param sessionid The session in which cancel is issued
   * @param state If any state is passed, all the queries in that state will be cancelled,
   * otherwise all queries will be cancelled. Possible states are {@value QueryStatus.Status#values()}
   * The queries in {@value QueryStatus.Status#FAILED},{@value QueryStatus.Status#FAILED},
    {@value QueryStatus.Status#CLOSED}, {@value QueryStatus.Status#UNKNOWN} cannot be cancelled
   * @param user If any user is passed, all the queries submitted by the user will be cancelled,
   * otherwise all the queries will be cancelled
   * 
   * @return APIResult with state {@value Status#SUCCEEDED} in case of successful cancellation.
   * APIResult with state {@value Status#FAILED} in case of cancellation failure.
   * APIResult with state {@value Status#PARTIAL} in case of partial cancellation.
   */
  @DELETE
  @Path("queries")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult cancelAllQueries(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @DefaultValue("") @QueryParam("state") String state,
      @DefaultValue("") @QueryParam("user") String user) {
    checkSessionId(sessionid);
    int numCancelled = 0;
    List<QueryHandle> handles = null;
    boolean failed = false;
    try {
      handles = getAllQueries(sessionid, state, user);
      for (QueryHandle handle : handles) {
        if (cancelQuery(sessionid, handle)) {
          numCancelled++;
        }
      }
    } catch (Exception e) {
      LOG.error("Error canceling queries", e);
      failed = true;
    }
    String msgString = (StringUtils.isBlank(state) ? "" : " in state" + state)
        + (StringUtils.isBlank(user) ? "" : " for user " + user);
    if (handles != null && numCancelled == handles.size()) {
      return new APIResult(Status.SUCCEEDED, "Cancel all queries "
          + msgString + " is successful");
    } else {
      assert (failed);
      if (numCancelled == 0) {
        return new APIResult(Status.FAILED, "Cancel on the query "
            + msgString + " has failed");        
      } else {
        return new APIResult(Status.PARTIAL, "Cancel on the query "
            + msgString + " is partial");        
      }
    }
  }

  /**
   * Get all prepared queries in the query server; can be filtered with user
   * 
   * @param sessionid The sessionid in which user is working
   * @param user If any user is passed, all the queries prepared by the user will be returned,
   * otherwise all the queries will be returned
   * 
   * @return List of QueryPrepareHandle objects
   */
  @GET
  @Path("preparedqueries")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public List<QueryPrepareHandle> getAllPreparedQueries(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @DefaultValue("") @QueryParam("user") String user) {
    checkSessionId(sessionid);
    try {
      return queryServer.getAllPreparedQueries(sessionid, user);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Prepare a query or 'explain and prepare' the query
   * 
   * @param sessionid The session in which user is preparing the query. Any
   *  configuration set in the session will be picked up.
   * @param query The query to prepare
   * @param op The operation on the query. Supported operations are 
   * {@value SubmitOp#EXPLAIN_AND_PREPARE} or {@value SubmitOp#PREPARE}
   * @param conf The configuration for preparing the query
   * 
   * @return {@link QueryPrepareHandle} incase of {@value SubmitOp#PREPARE} operation.
   * {@link QueryPlan} incase of {@value SubmitOp#EXPLAIN_AND_PREPARE} and the 
   * query plan will contain the prepare handle as well.
   */
  @POST
  @Path("preparedqueries")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public QuerySubmitResult prepareQuery(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @FormDataParam("query") String query,
      @DefaultValue("") @FormDataParam("operation") String op,
      @FormDataParam("conf") GrillConf conf) {
    try {
      checkSessionId(sessionid);
      checkQuery(query);
      SubmitOp sop = null;
      try {
        sop = SubmitOp.valueOf(op.toUpperCase());
      } catch (IllegalArgumentException e) {
      }
      if (sop == null) {
        throw new BadRequestException("Invalid operation type: " + op + prepareClue);
      }
      switch (sop) {
      case PREPARE:
        return queryServer.prepare(sessionid, query, conf);
      case EXPLAIN_AND_PREPARE:
        return queryServer.explainAndPrepare(sessionid, query, conf);
      default:
        throw new BadRequestException("Invalid operation type: " + op + prepareClue);
      }
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Destroy all the prepared queries; Can be filtered with user
   * 
   * @param sessionid The session in which cancel is issued
   * @param user If any user is passed, all the queries prepared by the user will be destroyed,
   * otherwise all the queries will be destroyed
   * 
   * @return APIResult with state {@value Status#SUCCEEDED} in case of successful destroy.
   * APIResult with state {@value Status#FAILED} in case of destroy failure.
   * APIResult with state {@value Status#PARTIAL} in case of partial destroy.
   */
  @DELETE
  @Path("preparedqueries")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult destroyPreparedQueries(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @DefaultValue("") @QueryParam("user") String user) {
    checkSessionId(sessionid);
    int numDestroyed = 0;
    boolean failed = false;
    List<QueryPrepareHandle> handles = null;
    try {
      handles = getAllPreparedQueries(sessionid, user);
      for (QueryPrepareHandle prepared : handles) {
        if (destroyPrepared(sessionid, prepared)) {
          numDestroyed++;
        }
      }
    } catch (Exception e) {
      LOG.error("Error destroying prepared queries", e);
      failed = true;
    }
    String msgString = (StringUtils.isBlank(user) ? "" : " for user " + user);
    if (handles != null && numDestroyed == handles.size()) {
      return new APIResult(Status.SUCCEEDED, "Destroy all prepared "
          + "queries " + msgString + " is successful");
    } else {
      assert (failed);
      if (numDestroyed == 0) {
        return new APIResult(Status.FAILED, "Destroy all prepared "
            + "queries " + msgString + " has failed");        
      } else {
        return new APIResult(Status.PARTIAL, "Destroy all prepared "
            + "queries " + msgString +" is partial");        
      }
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

  /**
   * Get a prepared query specified by handle
   * 
   * @param sessionid The user session handle
   * @param prepareHandle The prepare handle
   * 
   * @return {@link GrillPreparedQuery}
   */
  @GET
  @Path("preparedqueries/{preparehandle}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public GrillPreparedQuery getPreparedQuery(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("preparehandle") String prepareHandle) {
    checkSessionId(sessionid);
    try {
      return queryServer.getPreparedQuery(sessionid,
          getPrepareHandle(prepareHandle));
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Destroy the prepared query specified by handle
   * 
   * @param sessionid The user session handle
   * @param prepareHandle The prepare handle
   * 
   * @return APIResult with state {@link Status#SUCCEEDED} in case of successful destroy.
   * APIResult with state {@link Status#FAILED} in case of destroy failure.
   */
  @DELETE
  @Path("preparedqueries/{preparehandle}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult destroyPrepared(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("preparehandle") String prepareHandle) {
    checkSessionId(sessionid);
    boolean ret = destroyPrepared(sessionid, getPrepareHandle(prepareHandle));
    if (ret) {
      return new APIResult(Status.SUCCEEDED, "Destroy on the query "
          + prepareHandle + " is successful");
    } else {
      return new APIResult(Status.FAILED, "Destroy on the query "
          + prepareHandle + " failed");        
    }
  }

  /**
   * Get grill query and its current status
   * 
   * @param sessionid The user session handle
   * @param queryHandle The query handle
   * 
   * @return {@link GrillQuery}
   */
  @GET
  @Path("queries/{queryhandle}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public GrillQuery getStatus(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("queryhandle") String queryHandle) {
    checkSessionId(sessionid);
    try {
      return queryServer.getQuery(sessionid,
          getQueryHandle(queryHandle));
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Cancel the query specified by the handle
   * 
   * @param sessionid The user session handle
   * @param queryHandle The query handle
   * 
   * @return APIResult with state {@value Status#SUCCEEDED} in case of successful cancellation.
   * APIResult with state {@value Status#FAILED} in case of cancellation failure.
   */
  @DELETE
  @Path("queries/{queryhandle}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult cancelQuery(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("queryhandle") String queryHandle) {
    checkSessionId(sessionid);
    boolean ret = cancelQuery(sessionid, getQueryHandle(queryHandle));
    if (ret) {
      return new APIResult(Status.SUCCEEDED, "Cancel on the query "
          + queryHandle + " is successful");
    } else {
      return new APIResult(Status.FAILED, "Cancel on the query "
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

  /**
   * Modify query configuration if it is not running yet.
   * 
   * @param sessionid The user session handle
   * @param queryHandle The query handle
   * @param conf The new configuration, will be on top of old one
   * 
   * @return APIResult with state {@value Status#SUCCEEDED} in case of successful update.
   * APIResult with state {@value Status#FAILED} in case of udpate failure.
   */
  @PUT
  @Path("queries/{queryhandle}")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult updateConf(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("queryhandle") String queryHandle, 
      @FormDataParam("conf") GrillConf conf) {
    checkSessionId(sessionid);
    try {
      boolean ret = queryServer.updateQueryConf(sessionid, getQueryHandle(queryHandle), conf);
      if (ret) {
        return new APIResult(Status.SUCCEEDED, "Update on the query conf for "
            + queryHandle + " is successful");
      } else {
        return new APIResult(Status.FAILED, "Update on the query conf for "
            + queryHandle + " failed");        
      }
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Modify prepared query's configuration. This would be picked up for subsequent runs
   * of the prepared queries. The query wont be re-prepared with new configuration.
   * 
   * @param sessionid The user session handle
   * @param prepareHandle The prepare handle
   * @param conf The new configuration, will be on top of old one
   * 
   * @return APIResult with state {@value Status#SUCCEEDED} in case of successful update.
   * APIResult with state {@value Status#FAILED} in case of udpate failure.
   */
  @PUT
  @Path("preparedqueries/{prepareHandle}")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult updatePreparedConf(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("prepareHandle") String prepareHandle, 
      @FormDataParam("conf") GrillConf conf) {
    checkSessionId(sessionid);
    try {
      boolean ret = queryServer.updateQueryConf(sessionid, getPrepareHandle(prepareHandle), conf);
      if (ret) {
        return new APIResult(Status.SUCCEEDED, "Update on the query conf for "
            + prepareHandle + " is successful");
      } else {
        return new APIResult(Status.FAILED, "Update on the query conf for "
            + prepareHandle + " failed");        
      }
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Submit prepared query for execution
   * 
   * @param sessionid The session in which user is submitting the query. Any
   *  configuration set in the session will be picked up.
   * @param prepareHandle The Query to run
   * @param op The operation on the query. Supported operations are 
   * {@value SubmitOp#EXECUTE} and {@value SubmitOp#EXECUTE_WITH_TIMEOUT}
   * @param conf The configuration for the execution of query
   * @param timeoutmillis The timeout for the query, honored only in case of
   *  {@value SubmitOp#EXECUTE_WITH_TIMEOUT} operation
   * 
   * @return {@link QueryHandle} in case of {@value SubmitOp#EXECUTE} operation.
   * {@link QueryHandleWithResultSet} in case {@value SubmitOp#EXECUTE_WITH_TIMEOUT} operation.
   */
  @POST
  @Path("preparedqueries/{prepareHandle}")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public QuerySubmitResult executePrepared(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("prepareHandle") String prepareHandle,
      @DefaultValue("EXECUTE") @FormDataParam("operation") String op,
      @FormDataParam("conf") GrillConf conf,
      @DefaultValue("30000") @FormDataParam("timeoutmillis") Long timeoutmillis) {
    checkSessionId(sessionid);
    try {
      SubmitOp sop = null;
      try {
        sop = SubmitOp.valueOf(op.toUpperCase());
      } catch (IllegalArgumentException e) {
      }
      if (sop == null) {
        throw new BadRequestException("Invalid operation type: " + op +
            submitPreparedClue);
      }
      switch (sop) {
      case EXECUTE:
        return queryServer.executePrepareAsync(sessionid, getPrepareHandle(prepareHandle), conf);
      case EXECUTE_WITH_TIMEOUT:
        return queryServer.executePrepare(sessionid, getPrepareHandle(prepareHandle), timeoutmillis, conf);
      default:
        throw new BadRequestException("Invalid operation type: " + op + submitPreparedClue);
      }
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Get resultset metadata of the query
   * 
   * @param sessionid The user session handle
   * @param queryHandle The query handle
   * 
   * @return {@link QueryResultSetMetadata}
   */
  @GET
  @Path("queries/{queryhandle}/resultsetmetadata")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public QueryResultSetMetadata getResultSetMetadata(
      @QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("queryhandle") String queryHandle) {
    try {
      return queryServer.getResultSetMetadata(sessionid, getQueryHandle(queryHandle));
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Fetch the result set
   * 
   * @param sessionid The user session handle
   * @param queryHandle The query handle
   * @param startIndex start index of the result
   * @param fetchSize fetch size
   * 
   * @return {@link QueryResult}
   */
  @GET
  @Path("queries/{queryhandle}/resultset")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public QueryResult getResultSet(
      @QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("queryhandle") String queryHandle,
      @QueryParam("fromindex") long startIndex,
      @QueryParam("fetchsize") int fetchSize) {
    try {
      return queryServer.fetchResultSet(sessionid, getQueryHandle(queryHandle), startIndex, fetchSize);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Close the result set once fetching is done
   * 
   * @param sessionid The user session handle
   * @param queryHandle The query handle
   * 
   * @return APIResult with state {@value Status#SUCCEEDED} in case of successful close.
   * APIResult with state {@value Status#FAILED} in case of close failure.
   */
  @DELETE
  @Path("queries/{queryhandle}/resultset")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult closeResultSet(
      @QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("queryhandle") String queryHandle){
    try {
      queryServer.closeResultSet(sessionid, getQueryHandle(queryHandle));
      return new APIResult(Status.SUCCEEDED, "Close on the result set"
          + " for query " + queryHandle + " is successful");

    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

}


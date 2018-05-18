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
package org.apache.lens.server.query;

import static org.apache.lens.api.query.SubmitOp.*;
import static org.apache.lens.server.error.LensServerErrorCode.INVALID_HANDLE;
import static org.apache.lens.server.error.LensServerErrorCode.NULL_OR_EMPTY_OR_BLANK_QUERY;

import java.util.List;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.APIResult.Status;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.*;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.annotations.MultiPurposeResource;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.api.query.cost.QueryCostTOBuilder;
import org.apache.lens.server.auth.Authenticate;
import org.apache.lens.server.error.UnSupportedOpException;
import org.apache.lens.server.model.LogSegregationContext;
import org.apache.lens.server.util.UtilityMethods;

import org.apache.commons.lang.StringUtils;

import org.glassfish.jersey.media.multipart.FormDataParam;

import lombok.extern.slf4j.Slf4j;

/**
 * queryapi resource
 * <p></p>
 * This provides api for all things query.
 */
@Authenticate
@Slf4j
@Path("/queryapi")
public class QueryServiceResource {

  /** The query server. */
  private QueryExecutionService queryServer;

  private final LogSegregationContext logSegregationContext;

  private void validateSessionId(final LensSessionHandle sessionHandle) throws LensException {
    queryServer.validateSession(sessionHandle);
  }

  private void validateQuery(String query) throws LensException {
    if (StringUtils.isBlank(query)) {
      throw new LensException(NULL_OR_EMPTY_OR_BLANK_QUERY.getLensErrorInfo());
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

  /**
   * Instantiates a new query service resource.
   *
   * @throws LensException the lens exception
   */
  public QueryServiceResource() throws LensException {
    queryServer = LensServices.get().getService(QueryExecutionService.NAME);
    logSegregationContext = LensServices.get().getLogSegregationContext();
  }

  /**
   * Get all the queries in the query server; can be filtered with state and queryName. This will by default only return
   * queries submitted by the user that has started the session. To get queries of all users, set the searchAllUsers
   * parameter to false.
   *
   * @param sessionid The sessionid in which queryName is working
   * @param states    If any state is passed, all the queries in that state will be returned, otherwise all queries will
   *                  be returned. Possible states are {link QueryStatus.Status#values()}. Multiple states can be
   *                  passed as comma separated string
   * @param queryName If any queryName is passed, all the queries containing the queryName will be returned, otherwise
   *                  all the queries will be returned
   * @param user      Returns queries submitted by this user. If set to "all", returns queries of all users. By default,
   *                  returns queries of the current user.
   * @param driver    Get queries submitted on a specific driver.
   * @param fromDate  from date to search queries in a time range, the range is inclusive(submitTime &gt;= fromDate)
   *                  from date can be a long value indicating timestamp, or it can be in a format acceptable in
   *                  time_range_in function. Notably: yyyy[-MM[-dd[-HH-[mm...]]]], or now based relative format
   * @param toDate    to date to search queries in a time range, the range is inclusive(toDate &gt; submitTime)
   *                  possible formats it can take is same as fromDate
   * @return List of {@link QueryHandle} objects
   */
  @GET
  @Path("queries")
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public List<QueryHandle> getAllQueries(@QueryParam("sessionid") LensSessionHandle sessionid,
    @QueryParam("state") String states, @QueryParam("queryName") String queryName, @QueryParam("user") String user,
    @QueryParam("driver") String driver, @QueryParam("fromDate") String fromDate, @QueryParam("toDate") String toDate)
    throws LensException {
    validateSessionId(sessionid);
    return queryServer.getAllQueries(sessionid, states, user, driver, queryName, fromDate, toDate);
  }

  /**
   * Get all the queries in the query server; can be filtered with state and queryName. This will by default only return
   * queries submitted by the user that has started the session. To get queries of all users, set the searchAllUsers
   * parameter to false.
   *
   * @param sessionid The sessionid in which queryName is working
   * @param states    If any state is passed, all the queries in that state will be returned, otherwise all queries will
   *                  be returned. Possible states are {link QueryStatus.Status#values()}. Multiple states can be
   *                  passed as comma separated string
   * @param queryName If any queryName is passed, all the queries containing the queryName will be returned, otherwise
   *                  all the queries will be returned
   * @param user      Returns queries submitted by this user. If set to "all", returns queries of all users. By default,
   *                  returns queries of the current user.
   * @param driver    Get queries submitted on a specific driver.
   * @param fromDate  from date to search queries in a time range, the range is inclusive(submitTime &gt;= fromDate)
   *                  from date can be a long value indicating timestamp, or it can be in a format acceptable in
   *                  time_range_in function. Notably: yyyy[-MM[-dd[-HH-[mm...]]]], or now based relative format
   * @param toDate    to date to search queries in a time range, the range is inclusive(toDate &gt; submitTime)
   *                  possible formats it can take is same as fromDate
   * @return List of {@link LensQuery} objects
   */
  @GET
  @Path("queries/detail")
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public List<LensQuery> getAllQueryDetails(@QueryParam("sessionid") LensSessionHandle sessionid,
    @QueryParam("state") String states, @QueryParam("queryName") String queryName, @QueryParam("user") String user,
    @QueryParam("driver") String driver, @QueryParam("fromDate") String fromDate, @QueryParam("toDate") String toDate)
    throws LensException {
    validateSessionId(sessionid);
    return queryServer.getAllQueryDetails(sessionid, states, user, driver, queryName, fromDate, toDate);
  }

  /**
   * Submit the query for explain or execute or execute with a timeout.
   *
   * @param sessionid     The session in which user is submitting the query. Any configuration set in the session will
   *                      be picked up.
   * @param query         The query to run
   * @param operation     The operation on the query. Supported operations are values:
   *                      {@link org.apache.lens.api.query.SubmitOp#ESTIMATE},
   *                      {@link org.apache.lens.api.query.SubmitOp#EXPLAIN},
   *                      {@link org.apache.lens.api.query.SubmitOp#EXECUTE} and
   *                      {@link org.apache.lens.api.query.SubmitOp#EXECUTE_WITH_TIMEOUT}
   * @param conf          The configuration for the query
   * @param timeoutmillis The timeout for the query. If the query does not finish within the specified
   *                      timeout, it is automatically cancelled unless user specified otherwise
   *                      by setting configuration lens.query.cancel.on.timeout = false.
   *                      <br>
   *                      Note: The timeout parameter is honored only in case of {@link
   *                      org.apache.lens.api.query.SubmitOp#EXECUTE_WITH_TIMEOUT} operation
   * @param queryName     human readable query name set by user (optional parameter)

   * @return {@link LensAPIResult} with DATA as {@link QueryHandle} in case of
   * {@link org.apache.lens.api.query.SubmitOp#EXECUTE} operation.
   * {@link QueryPlan} in case of {@link org.apache.lens.api.query.SubmitOp#EXPLAIN} operation.
   * {@link QueryHandleWithResultSet} in case {@link org.apache.lens.api.query.SubmitOp#EXECUTE_WITH_TIMEOUT}
   * operation. {@link org.apache.lens.api.result.QueryCostTO} in case of
   * {@link org.apache.lens.api.query.SubmitOp#ESTIMATE} operation.
   */
  @POST
  @Path("queries")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  @MultiPurposeResource(formParamName = "operation")
  public LensAPIResult<QuerySubmitResult> query(@FormDataParam("sessionid") LensSessionHandle sessionid,
      @FormDataParam("query") String query, @FormDataParam("operation") String operation,
      @FormDataParam("conf") LensConf conf, @DefaultValue("30000") @FormDataParam("timeoutmillis") Long timeoutmillis,
      @DefaultValue("") @FormDataParam("queryName") String queryName) throws LensException {

    final String requestId = this.logSegregationContext.getLogSegragationId();
    validateSessionId(sessionid);
    SubmitOp[] supportedOperations = new SubmitOp[]{ESTIMATE, EXECUTE, EXPLAIN, EXECUTE_WITH_TIMEOUT};
    SubmitOp sop = UtilityMethods.checkAndGetOperation(operation, SubmitOp.class, supportedOperations);
    validateQuery(query);
    QuerySubmitResult result;
    switch (sop) {
    case ESTIMATE:
      result = new QueryCostTOBuilder(queryServer.estimate(requestId, sessionid, query, conf)).build();
      break;
    case EXECUTE:
      result = queryServer.executeAsync(sessionid, query, conf, queryName);
      break;
    case EXPLAIN:
      result = queryServer.explain(requestId, sessionid, query, conf);
      break;
    case EXECUTE_WITH_TIMEOUT:
      result = queryServer.execute(sessionid, query, timeoutmillis, conf, queryName);
      break;
    default:
      throw new UnSupportedOpException(supportedOperations);
    }
    return LensAPIResult.composedOf(null, requestId, result);
  }

  /**
   * Cancel all the queries in query server; can be filtered with state and user.
   *
   * @param sessionid The session in which cancel is issued
   * @param state     If any state is passed, all the queries in that state will be cancelled, otherwise all queries
   *                  will be cancelled. Possible states are
   *                  {@link org.apache.lens.api.query.QueryStatus.Status#values()}
   *                  The queries in {@link org.apache.lens.api.query.QueryStatus.Status#FAILED},
   *                  {@link org.apache.lens.api.query.QueryStatus.Status#CLOSED},
   *                  {@link org.apache.lens.api.query.QueryStatus.Status#SUCCESSFUL} cannot be cancelled
   * @param user      If any user is passed, all the queries submitted by the user will be cancelled, otherwise all the
   *                  queries will be cancelled
   * @param driver    Get queries submitted on a specific driver.
   * @param queryName Cancel queries matching the query name
   * @param fromDate  the from date, inclusive(submitTime&gt;=fromDate)
   * @param toDate    the to date, inclusive(toDate&gt;=submitTime)
   * @return APIResult with state {@link org.apache.lens.api.APIResult.Status#SUCCEEDED} in case of successful
   *                   cancellation. APIResult with state {@link org.apache.lens.api.APIResult.Status#FAILED}
   *                   in case of cancellation failure. APIResult with state
   *                    {@link org.apache.lens.api.APIResult.Status#PARTIAL} in case of partial cancellation.
   */
  @DELETE
  @Path("queries")
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public APIResult cancelAllQueries(@QueryParam("sessionid") LensSessionHandle sessionid,
    @DefaultValue("") @QueryParam("state") String state, @DefaultValue("") @QueryParam("user") String user,
    @DefaultValue("") @QueryParam("queryName") String queryName, @DefaultValue("") @QueryParam("driver") String driver,
    @QueryParam("fromDate") String fromDate, @QueryParam("toDate") String toDate)
    throws LensException {
    validateSessionId(sessionid);
    int numCancelled = 0;
    List<QueryHandle> handles = null;
    boolean failed = false;
    try {
      handles = getAllQueries(sessionid, state, queryName, user, driver, fromDate, toDate);
      for (QueryHandle handle : handles) {
        if (queryServer.cancelQuery(sessionid, handle)) {
          numCancelled++;
        }
      }
    } catch (Exception e) {
      log.error("Error canceling queries", e);
      failed = true;
    }
    String msgString = (StringUtils.isBlank(state) ? "" : " in state" + state)
      + (StringUtils.isBlank(user) ? "" : " for user " + user);
    if (handles != null && numCancelled == handles.size()) {
      return new APIResult(Status.SUCCEEDED, "Cancel all queries " + msgString + " is successful");
    } else {
      assert (failed);
      if (numCancelled == 0) {
        return new APIResult(Status.FAILED, "Cancel on the query " + msgString + " has failed");
      } else {
        return new APIResult(Status.PARTIAL, "Cancel on the query " + msgString + " is partial");
      }
    }
  }

  /**
   * Get all prepared queries in the query server; can be filtered with user.
   *
   * @param sessionid The sessionid in which user is working
   * @param user      returns queries of the user. If set to "all", returns queries of all users. By default returns the
   *                  queries of the current user.
   * @param queryName returns queries matching the query name
   * @param fromDate  start time for filtering prepared queries by preparation time
   * @param toDate    end time for filtering prepared queries by preparation time
   * @return List of QueryPrepareHandle objects
   */
  @GET
  @Path("preparedqueries")
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public List<QueryPrepareHandle> getAllPreparedQueries(@QueryParam("sessionid") LensSessionHandle sessionid,
    @DefaultValue("") @QueryParam("user") String user, @DefaultValue("") @QueryParam("queryName") String queryName,
    @QueryParam("fromDate") String fromDate, @QueryParam("toDate") String toDate) throws LensException {
    validateSessionId(sessionid);
    return queryServer.getAllPreparedQueries(sessionid, user, queryName, fromDate, toDate);
  }

  /**
   * Prepare a query or 'explain and prepare' the query.
   *
   * @param sessionid The session in which user is preparing the query. Any configuration set in the session will be
   *                  picked up.
   * @param query     The query to prepare
   * @param operation The operation on the query. Supported operations are
   *                  {@link org.apache.lens.api.query.SubmitOp#EXPLAIN_AND_PREPARE} or
   *                  {@link org.apache.lens.api.query.SubmitOp#PREPARE}
   * @param conf      The configuration for preparing the query
   * @param queryName human readable query name set by user (optional parameter)
   * @return {@link QueryPrepareHandle} incase of {link org.apache.lens.api.query.SubmitOp#PREPARE} operation.
   *         {@link QueryPlan} incase of {@link org.apache.lens.api.query.SubmitOp#EXPLAIN_AND_PREPARE}
   *         and the query plan will contain the prepare handle as well.
   * @throws LensException
   */
  @POST
  @Path("preparedqueries")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  @MultiPurposeResource(formParamName = "operation")
  public LensAPIResult<QuerySubmitResult> prepareQuery(
      @FormDataParam("sessionid") LensSessionHandle sessionid, @FormDataParam("query") String query,
      @DefaultValue("") @FormDataParam("operation") String operation, @FormDataParam("conf") LensConf conf,
      @DefaultValue("") @FormDataParam("queryName") String queryName) throws LensException {
    final String requestId = this.logSegregationContext.getLogSegragationId();
    validateSessionId(sessionid);
    validateQuery(query);
    SubmitOp[] supportedOperations = new SubmitOp[]{PREPARE, EXPLAIN_AND_PREPARE};
    SubmitOp sop = UtilityMethods.checkAndGetOperation(operation, SubmitOp.class, supportedOperations);
    QuerySubmitResult result;
    switch (sop) {
    case PREPARE:
      result = queryServer.prepare(sessionid, query, conf, queryName);
      break;
    case EXPLAIN_AND_PREPARE:
      result = queryServer.explainAndPrepare(sessionid, query, conf, queryName);
      break;
    default:
      throw new UnSupportedOpException(supportedOperations);
    }
    return LensAPIResult.composedOf(null, requestId, result);
  }
  /**
   * Destroy all the prepared queries; Can be filtered with user.
   *
   * @param sessionid The session in which cancel is issued
   * @param user      destroys queries of the user. If set to "all", destroys queries of all users. By default destroys
   *                  the queries of the current user.
   * @param queryName destroys queries matching the query name
   * @param fromDate  the from date
   * @param toDate    the to date
   * @return APIResult with state {@link org.apache.lens.api.APIResult.Status#SUCCEEDED} in case of successful
   *         destroy. APIResult with state {@link org.apache.lens.api.APIResult.Status#FAILED} in case of destroy
   *         failure. APIResult with state {@link org.apache.lens.api.APIResult.Status#PARTIAL} in case of
   *         partial destroy.
   */
  @DELETE
  @Path("preparedqueries")
  @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
  public APIResult destroyPreparedQueries(@QueryParam("sessionid") LensSessionHandle sessionid,
      @DefaultValue("") @QueryParam("user") String user, @DefaultValue("") @QueryParam("queryName") String queryName,
      @QueryParam("fromDate") String fromDate, @QueryParam("toDate") String toDate) throws LensException {
    validateSessionId(sessionid);
    int numDestroyed = 0;
    boolean failed = false;
    List<QueryPrepareHandle> handles = null;
    try {
      handles = getAllPreparedQueries(sessionid, user, queryName, fromDate, toDate);
      for (QueryPrepareHandle prepared : handles) {
        if (queryServer.destroyPrepared(sessionid, prepared)) {
          numDestroyed++;
        }
      }
    } catch (Exception e) {
      log.error("Error destroying prepared queries", e);
      failed = true;
    }
    String msgString = (StringUtils.isBlank(user) ? "" : " for user " + user);
    if (handles != null && numDestroyed == handles.size()) {
      return new APIResult(Status.SUCCEEDED, "Destroy all prepared " + "queries " + msgString + " is successful");
    } else {
      assert (failed);
      if (numDestroyed == 0) {
        return new APIResult(Status.FAILED, "Destroy all prepared " + "queries " + msgString + " has failed");
      } else {
        return new APIResult(Status.PARTIAL, "Destroy all prepared " + "queries " + msgString + " is partial");
      }
    }
  }

  /**
   * Gets the query handle.
   *
   * @param queryHandle the query handle
   * @return the query handle
   */
  private QueryHandle getQueryHandle(String queryHandle) throws LensException {
    try {
      return QueryHandle.fromString(queryHandle);
    } catch (Exception e) {
      throw new LensException(INVALID_HANDLE.getLensErrorInfo(), e, queryHandle);
    }
  }

  /**
   * Gets the prepare handle.
   *
   * @param prepareHandle the prepare handle
   * @return the prepare handle
   */
  private QueryPrepareHandle getPrepareHandle(String prepareHandle) throws LensException {
    try {
      return QueryPrepareHandle.fromString(prepareHandle);
    } catch (Exception e) {
      throw new LensException(INVALID_HANDLE.getLensErrorInfo(), e, prepareHandle);
    }
  }

  /**
   * Get a prepared query specified by handle.
   *
   * @param sessionid     The user session handle
   * @param prepareHandle The prepare handle
   * @return {@link LensPreparedQuery}
   */
  @GET
  @Path("preparedqueries/{prepareHandle}")
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public LensPreparedQuery getPreparedQuery(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("prepareHandle") String prepareHandle) throws LensException {
    validateSessionId(sessionid);
    return queryServer.getPreparedQuery(sessionid, getPrepareHandle(prepareHandle));
  }

  /**
   * Destroy the prepared query specified by handle.
   *
   * @param sessionid     The user session handle
   * @param prepareHandle The prepare handle
   * @return APIResult with state {@link org.apache.lens.api.APIResult.Status#SUCCEEDED} in case of successful
   *         destroy. APIResult with state {@link org.apache.lens.api.APIResult.Status#FAILED} in case of
   *         destroy failure.
   */
  @DELETE
  @Path("preparedqueries/{prepareHandle}")
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public APIResult destroyPrepared(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("prepareHandle") String prepareHandle) throws LensException {
    validateSessionId(sessionid);
    boolean ret = queryServer.destroyPrepared(sessionid, getPrepareHandle(prepareHandle));
    if (ret) {
      return new APIResult(Status.SUCCEEDED, "Destroy on the query " + prepareHandle + " is successful");
    } else {
      return new APIResult(Status.FAILED, "Destroy on the query " + prepareHandle + " failed");
    }
  }

  /**
   * Get lens query and its current status.
   *
   * @param sessionid   The user session handle
   * @param queryHandle The query handle
   * @return {@link LensQuery}
   */
  @GET
  @Path("queries/{queryHandle}")
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public LensQuery getStatus(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("queryHandle") String queryHandle) throws LensException {
    validateSessionId(sessionid);
    return queryServer.getQuery(sessionid, getQueryHandle(queryHandle));
  }

  /**
   * Cancel the query specified by the handle.
   *
   * @param sessionid   The user session handle
   * @param queryHandle The query handle
   * @return APIResult with state {@link org.apache.lens.api.APIResult.Status#SUCCEEDED} in case of successful
   *         cancellation. APIResult with state {@link org.apache.lens.api.APIResult.Status#FAILED} in case of
   *         cancellation failure.
   */
  @DELETE
  @Path("queries/{queryHandle}")
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public APIResult cancelQuery(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("queryHandle") String queryHandle) throws LensException {
    validateSessionId(sessionid);
    boolean ret = queryServer.cancelQuery(sessionid, getQueryHandle(queryHandle));
    if (ret) {
      return new APIResult(Status.SUCCEEDED, "Cancel on the query " + queryHandle + " is successful");
    } else {
      return new APIResult(Status.FAILED, "Cancel on the query " + queryHandle + " failed");
    }
  }

  /**
   * Modify query configuration if it is not running yet.
   *
   * @param sessionid   The user session handle
   * @param queryHandle The query handle
   * @param conf        The new configuration, will be on top of old one
   * @return APIResult with state {@link org.apache.lens.api.APIResult.Status#SUCCEEDED} in case of successful
   * update. APIResult with state {@link org.apache.lens.api.APIResult.Status#FAILED} in case of udpate failure.
   */
  @PUT
  @Path("queries/{queryHandle}")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public APIResult updateConf(@FormDataParam("sessionid") LensSessionHandle sessionid,
    @PathParam("queryHandle") String queryHandle, @FormDataParam("conf") LensConf conf) throws LensException {
    validateSessionId(sessionid);
    boolean ret = queryServer.updateQueryConf(sessionid, getQueryHandle(queryHandle), conf);
    if (ret) {
      return new APIResult(Status.SUCCEEDED, "Update on the query conf for " + queryHandle + " is successful");
    } else {
      return new APIResult(Status.FAILED, "Update on the query conf for " + queryHandle + " failed");
    }
  }

  /**
   * Modify prepared query's configuration. This would be picked up for subsequent runs of the prepared queries. The
   * query wont be re-prepared with new configuration.
   *
   * @param sessionid     The user session handle
   * @param prepareHandle The prepare handle
   * @param conf          The new configuration, will be on top of old one
   * @return APIResult with state {@link org.apache.lens.api.APIResult.Status#SUCCEEDED} in case of successful
   * update. APIResult with state {@link org.apache.lens.api.APIResult.Status#FAILED} in case of udpate failure.
   */
  @PUT
  @Path("preparedqueries/{prepareHandle}")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public APIResult updatePreparedConf(@FormDataParam("sessionid") LensSessionHandle sessionid,
    @PathParam("prepareHandle") String prepareHandle, @FormDataParam("conf") LensConf conf) throws LensException {
    validateSessionId(sessionid);
    boolean ret = queryServer.updateQueryConf(sessionid, getPrepareHandle(prepareHandle), conf);
    if (ret) {
      return new APIResult(Status.SUCCEEDED, "Update on the query conf for " + prepareHandle + " is successful");
    } else {
      return new APIResult(Status.FAILED, "Update on the query conf for " + prepareHandle + " failed");
    }
  }

  /**
   * Submit prepared query for execution.
   *
   * @param sessionid     The session in which user is submitting the query. Any configuration set in the session will
   *                      be picked up.
   * @param prepareHandle The Query to run
   * @param operation     The operation on the query. Supported operations are
   *                      {@link org.apache.lens.api.query.SubmitOp#EXECUTE} and {@link
   *                      org.apache.lens.api.query.SubmitOp#EXECUTE_WITH_TIMEOUT}
   * @param conf          The configuration for the execution of query
   * @param timeoutmillis The timeout for the query, honored only in case of
   * {@link org.apache.lens.api.query.SubmitOp#EXECUTE_WITH_TIMEOUT} operation
   * @param queryName     human readable query name set by user (optional parameter)
   * @return {@link QueryHandle} in case of {link org.apache.lens.api.query.SubmitOp#EXECUTE} operation.
   * {@link QueryHandleWithResultSet} in case {@link org.apache.lens.api.query.SubmitOp#EXECUTE_WITH_TIMEOUT
   * } operation.
   */
  @POST
  @Path("preparedqueries/{prepareHandle}")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  @MultiPurposeResource(formParamName = "operation")
  public QuerySubmitResult executePrepared(@FormDataParam("sessionid") LensSessionHandle sessionid,
    @PathParam("prepareHandle") String prepareHandle,
    @DefaultValue("EXECUTE") @FormDataParam("operation") String operation, @FormDataParam("conf") LensConf conf,
    @DefaultValue("30000") @FormDataParam("timeoutmillis") Long timeoutmillis,
    @DefaultValue("") @FormDataParam("queryName") String queryName) throws LensException {
    validateSessionId(sessionid);
    SubmitOp[] supportedOperations = new SubmitOp[]{EXECUTE, EXECUTE_WITH_TIMEOUT};
    SubmitOp sop = UtilityMethods.checkAndGetOperation(operation, SubmitOp.class, supportedOperations);
    switch (sop) {
    case EXECUTE:
      return queryServer.executePrepareAsync(sessionid, getPrepareHandle(prepareHandle), conf, queryName);
    case EXECUTE_WITH_TIMEOUT:
      return queryServer.executePrepare(sessionid, getPrepareHandle(prepareHandle), timeoutmillis, conf, queryName);
    default:
      throw new UnSupportedOpException(supportedOperations);
    }
  }

  /**
   * Get resultset metadata of the query.
   *
   * @param sessionid   The user session handle
   * @param queryHandle The query handle
   * @return {@link QueryResultSetMetadata}
   */
  @GET
  @Path("queries/{queryHandle}/resultsetmetadata")
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public QueryResultSetMetadata getResultSetMetadata(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("queryHandle") String queryHandle) throws LensException {
    validateSessionId(sessionid);
    return queryServer.getResultSetMetadata(sessionid, getQueryHandle(queryHandle));
  }

  /**
   * Fetch the result set.
   *
   * @param sessionid   The user session handle
   * @param queryHandle The query handle
   * @param startIndex  start index of the result
   * @param fetchSize   fetch size
   * @return {@link QueryResult}
   */
  @GET
  @Path("queries/{queryHandle}/resultset")
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public QueryResult getResultSet(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("queryHandle") String queryHandle, @QueryParam("fromindex") long startIndex,
    @QueryParam("fetchsize") int fetchSize) throws LensException {
    validateSessionId(sessionid);
    return queryServer.fetchResultSet(sessionid, getQueryHandle(queryHandle), startIndex, fetchSize);
  }

  /**
   * Get the http endpoint for result set.
   *
   * @param sessionid   The user session handle
   * @param queryHandle The query handle
   * @return Response with result as octet stream
   */
  @GET
  @Path("queries/{queryHandle}/httpresultset")
  @Produces({MediaType.APPLICATION_OCTET_STREAM})
  public Response getHttpResultSet(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("queryHandle") String queryHandle) throws LensException {
    return queryServer.getHttpResultSet(sessionid, getQueryHandle(queryHandle));
  }

  /**
   * Close the result set once fetching is done.
   *
   * @param sessionid   The user session handle
   * @param queryHandle The query handle
   * @return APIResult with state {@link org.apache.lens.api.APIResult.Status#SUCCEEDED} in case of successful
   * close. APIResult with state {@link org.apache.lens.api.APIResult.Status#FAILED} in case of close failure.
   */
  @DELETE
  @Path("queries/{queryHandle}/resultset")
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public APIResult closeResultSet(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("queryHandle") String queryHandle) throws LensException {
    validateSessionId(sessionid);
    queryServer.closeResultSet(sessionid, getQueryHandle(queryHandle));
    return new APIResult(Status.SUCCEEDED,
      "Close on the result set for query " + queryHandle + " is successful");
  }
}

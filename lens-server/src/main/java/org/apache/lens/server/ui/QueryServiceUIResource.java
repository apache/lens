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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.*;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.QueryExecutionService;

import org.apache.commons.lang.StringUtils;

import org.glassfish.jersey.media.multipart.FormDataParam;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class QueryServiceUIResource.
 */
@Path("/queryuiapi")
@Slf4j
public class QueryServiceUIResource {

  /** The query server. */
  private QueryExecutionService queryServer;

  // assert: query is not empty

  /**
   * Check query.
   *
   * @param query the query
   */
  private void checkQuery(String query) {
    if (StringUtils.isBlank(query)) {
      throw new BadRequestException("Invalid query");
    }
  }

  // assert: sessionHandle is not null

  /**
   * Check session handle.
   *
   * @param sessionHandle the session handle
   */
  private void checkSessionHandle(LensSessionHandle sessionHandle) {
    if (sessionHandle == null) {
      throw new BadRequestException("Invalid session handle");
    }
  }

  /**
   * Instantiates a new query service ui resource.
   */
  public QueryServiceUIResource() {
    log.info("Query UI Service");
    queryServer = LensServices.get().getService(QueryExecutionService.NAME);
  }

  /**
   * Gets the query handle.
   *
   * @param queryHandle the query handle
   * @return the query handle
   */
  private QueryHandle getQueryHandle(String queryHandle) {
    try {
      return QueryHandle.fromString(queryHandle);
    } catch (Exception e) {
      throw new BadRequestException("Invalid query handle: " + queryHandle, e);
    }
  }

  /**
   * Get all the queries in the query server; can be filtered with state and user.
   *
   * @param publicId  The public id of the session in which user is working
   * @param state     If any state is passed, all the queries in that state will be returned, otherwise all queries will
   *                  be returned. Possible states are {@link org.apache.lens.api.query.QueryStatus.Status#values()}
   * @param user      return queries matching the user. If set to "all", return queries of all users. By default,
   *                  returns queries of the current user.
   * @param driver    Get queries submitted on a specific driver.
   * @param queryName human readable query name set by user (optional)
   * @param fromDate  the from date
   * @param toDate    the to date
   * @return List of {@link QueryHandle} objects
   */
  @GET
  @Path("queries")
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public List<QueryHandle> getAllQueries(@QueryParam("publicId") UUID publicId,
    @DefaultValue("") @QueryParam("state") String state, @DefaultValue("") @QueryParam("user") String user,
    @DefaultValue("") @QueryParam("driver") String driver, @DefaultValue("") @QueryParam("queryName") String queryName,
    @DefaultValue("-1") @QueryParam("fromDate") long fromDate, @DefaultValue("-1") @QueryParam("toDate") long toDate) {
    LensSessionHandle sessionHandle = SessionUIResource.getOpenSession(publicId);
    checkSessionHandle(sessionHandle);
    try {
      return queryServer.getAllQueries(sessionHandle, state, user, driver, queryName, fromDate,
        toDate == -1L ? Long.MAX_VALUE : toDate);
    } catch (LensException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Submit the query for explain or execute or execute with a timeout.
   *
   * @param publicId  The public id of the session in which user is submitting the query. Any configuration set in the
   *                  session will be picked up.
   * @param query     The query to run
   * @param queryName human readable query name set by user (optional)
   * @return {@link QueryHandle}
   */
  @POST
  @Path("queries")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON,  MediaType.TEXT_PLAIN})
  public QuerySubmitResult query(@FormDataParam("publicId") UUID publicId, @FormDataParam("query") String query,
    @DefaultValue("") @FormDataParam("queryName") String queryName) {
    LensSessionHandle sessionHandle = SessionUIResource.getOpenSession(publicId);
    checkSessionHandle(sessionHandle);
    LensConf conf;
    checkQuery(query);
    try {
      conf = new LensConf();
      return queryServer.executeAsync(sessionHandle, query, conf, queryName);
    } catch (LensException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Get lens query and its current status.
   *
   * @param publicId    The public id of session handle
   * @param queryHandle The query handle
   * @return {@link LensQuery}
   */
  @GET
  @Path("queries/{queryHandle}")
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public LensQuery getStatus(@QueryParam("publicId") UUID publicId, @PathParam("queryHandle") String queryHandle) {
    LensSessionHandle sessionHandle = SessionUIResource.getOpenSession(publicId);
    checkSessionHandle(sessionHandle);
    try {
      return queryServer.getQuery(sessionHandle, getQueryHandle(queryHandle));
    } catch (LensException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Fetch the result set.
   *
   * @param publicId    The public id of user's session handle
   * @param queryHandle The query handle
   * @param pageNumber  page number of the query result set to be read
   * @param fetchSize   fetch size
   * @return {@link ResultRow}
   */
  @GET
  @Path("queries/{queryHandle}/resultset")
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public ResultRow getResultSet(@QueryParam("publicId") UUID publicId, @PathParam("queryHandle") String queryHandle,
    @QueryParam("pageNumber") int pageNumber, @QueryParam("fetchsize") int fetchSize) {
    LensSessionHandle sessionHandle = SessionUIResource.getOpenSession(publicId);
    checkSessionHandle(sessionHandle);
    List<Object> rows = new ArrayList<Object>();
    log.info("FetchResultSet for queryHandle:{}", queryHandle);
    try {
      QueryResultSetMetadata resultSetMetadata = queryServer.getResultSetMetadata(sessionHandle,
        getQueryHandle(queryHandle));
      List<ResultColumn> metaColumns = resultSetMetadata.getColumns();
      List<Object> metaResultColumns = new ArrayList<Object>();
      for (ResultColumn column : metaColumns) {
        metaResultColumns.add(column.getName());
      }
      rows.add(new ResultRow(metaResultColumns));
      QueryResult result = queryServer.fetchResultSet(sessionHandle, getQueryHandle(queryHandle), (pageNumber - 1)
        * fetchSize, fetchSize);
      if (result instanceof InMemoryQueryResult) {
        InMemoryQueryResult inMemoryQueryResult = (InMemoryQueryResult) result;
        List<ResultRow> tableRows = inMemoryQueryResult.getRows();
        rows.addAll(tableRows);
      } else if (result instanceof PersistentQueryResult) {
        PersistentQueryResult persistentQueryResult = (PersistentQueryResult) result;
        rows.add("PersistentResultSet");
      }
      return new ResultRow(rows);
    } catch (LensException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Cancel the query specified by the handle.
   *
   * @param publicId    The user session handle
   * @param queryHandle The query handle
   * @return APIResult with state {@link org.apache.lens.api.APIResult.Status#SUCCEEDED} in case of successful
   * cancellation. APIResult with state {@link org.apache.lens.api.APIResult.Status#FAILED} in case of cancellation
   * failure.
   */
  @DELETE
  @Path("queries/{queryHandle}")
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public APIResult cancelQuery(@QueryParam("sessionid") UUID publicId, @PathParam("queryHandle") String queryHandle) {
    LensSessionHandle sessionHandle = SessionUIResource.getOpenSession(publicId);
    checkSessionHandle(sessionHandle);
    try {
      boolean ret = queryServer.cancelQuery(sessionHandle, getQueryHandle(queryHandle));
      if (ret) {
        return new APIResult(APIResult.Status.SUCCEEDED, "Cancel on the query " + queryHandle + " is successful");
      } else {
        return new APIResult(APIResult.Status.FAILED, "Cancel on the query " + queryHandle + " failed");
      }
    } catch (LensException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Get the http endpoint for result set.
   *
   * @param publicId    the public id
   * @param queryHandle The query handle
   * @return Response with result as octet stream
   */
  @GET
  @Path("queries/{queryHandle}/httpresultset")
  @Produces({MediaType.APPLICATION_OCTET_STREAM})
  public Response getHttpResultSet(@QueryParam("sessionid") UUID publicId,
    @PathParam("queryHandle") String queryHandle) {
    LensSessionHandle sessionHandle = SessionUIResource.getOpenSession(publicId);
    checkSessionHandle(sessionHandle);
    try {
      return queryServer.getHttpResultSet(sessionHandle, getQueryHandle(queryHandle));
    } catch (LensException e) {
      throw new WebApplicationException(e);
    }
  }
}

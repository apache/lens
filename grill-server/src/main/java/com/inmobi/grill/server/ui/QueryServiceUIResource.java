package com.inmobi.grill.server.ui;

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

import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.query.*;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.query.QueryExecutionService;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Path("/queryuiapi")
public class QueryServiceUIResource {

  public static final Log LOG = LogFactory.getLog(QueryServiceUIResource.class);

  private QueryExecutionService queryServer;

  //assert: query is not empty
  private void checkQuery(String query) {
    if (StringUtils.isBlank(query)) {
      throw new BadRequestException("Invalid query");
    }
  }

  //assert: sessionHandle is not null
  private void checkSessionHandle(GrillSessionHandle sessionHandle) {
    if (sessionHandle == null) {
      throw new BadRequestException("Invalid session handle");
    }
  }

  public QueryServiceUIResource() {
    LOG.info("Query UI Service");
    queryServer = (QueryExecutionService) GrillServices.get().getService("query");
  }

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
   * @param publicId The public id of the session in which user is working
   * @param state    If any state is passed, all the queries in that state will be returned,
   *                 otherwise all queries will be returned. Possible states are {@value QueryStatus.Status#values()}
   * @param user     If any user is passed, all the queries submitted by the user will be returned,
   *                 otherwise all the queries will be returned
   * @return List of {@link QueryHandle} objects
   */
  @GET
  @Path("queries")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public List<QueryHandle> getAllQueries(@QueryParam("publicId") UUID publicId,
                                         @DefaultValue("") @QueryParam("state") String state,
                                         @DefaultValue("") @QueryParam("user") String user) {
    GrillSessionHandle sessionHandle = SessionUIResource.openSessions.get(publicId);
    checkSessionHandle(sessionHandle);
    try {
      return queryServer.getAllQueries(sessionHandle, state, user);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Submit the query for explain or execute or execute with a timeout
   *
   * @param publicId The public id of the session in which user is submitting the query. Any
   *                 configuration set in the session will be picked up.
   * @param query    The query to run
   * @return {@link QueryHandle}
   */
  @POST
  @Path("queries")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public QuerySubmitResult query(@FormDataParam("publicId") UUID publicId,
                                 @FormDataParam("query") String query) {
    GrillSessionHandle sessionHandle = SessionUIResource.openSessions.get(publicId);
    checkSessionHandle(sessionHandle);
    GrillConf conf;
    checkQuery(query);
    try {
      conf = new GrillConf();
      return queryServer.executeAsync(sessionHandle, query, conf);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Get grill query and its current status
   *
   * @param publicId    The public id of session handle
   * @param queryHandle The query handle
   * @return {@link GrillQuery}
   */
  @GET
  @Path("queries/{queryHandle}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public GrillQuery getStatus(@QueryParam("publicId") UUID publicId,
                              @PathParam("queryHandle") String queryHandle) {
    GrillSessionHandle sessionHandle = SessionUIResource.openSessions.get(publicId);
    checkSessionHandle(sessionHandle);
    try {
      return queryServer.getQuery(sessionHandle,
          getQueryHandle(queryHandle));
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Fetch the result set
   *
   * @param publicId    The public id of user's session handle
   * @param queryHandle The query handle
   * @param pageNumber  page number of the query result set to be read
   * @param fetchSize   fetch size
   * @return {@link ResultRow}
   */
  @GET
  @Path("queries/{queryHandle}/resultset")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public ResultRow getResultSet(
      @QueryParam("publicId") UUID publicId,
      @PathParam("queryHandle") String queryHandle,
      @QueryParam("pageNumber") int pageNumber,
      @QueryParam("fetchsize") int fetchSize) {
    GrillSessionHandle sessionHandle = SessionUIResource.openSessions.get(publicId);
    checkSessionHandle(sessionHandle);
    List<Object> rows = new ArrayList<Object>();
    LOG.info("FetchResultSet for queryHandle:" + queryHandle);
    try {
      QueryResultSetMetadata resultSetMetadata = queryServer.getResultSetMetadata(sessionHandle,
          getQueryHandle(queryHandle));
      List<ResultColumn> metaColumns = resultSetMetadata.getColumns();
      List<Object> metaResultColumns = new ArrayList<Object>();
      for (ResultColumn column : metaColumns) {
        metaResultColumns.add(column.getName());
      }
      rows.add(new ResultRow(metaResultColumns));
      QueryResult result = queryServer.fetchResultSet(sessionHandle, getQueryHandle(queryHandle),
          (pageNumber - 1) * fetchSize, fetchSize);
      if (result instanceof InMemoryQueryResult) {
        InMemoryQueryResult inMemoryQueryResult = (InMemoryQueryResult) result;
        List<ResultRow> tableRows = inMemoryQueryResult.getRows();
        rows.addAll(tableRows);
      } else if (result instanceof PersistentQueryResult) {
        PersistentQueryResult persistentQueryResult = (PersistentQueryResult) result;
        rows.add("PersistentResultSet");
      }
      return new ResultRow(rows);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Cancel the query specified by the handle
   *
   * @param publicId    The user session handle
   * @param queryHandle The query handle
   * @return APIResult with state {@value com.inmobi.grill.api.APIResult.Status#SUCCEEDED} in case of successful cancellation.
   * APIResult with state {@value com.inmobi.grill.api.APIResult.Status#FAILED} in case of cancellation failure.
   */
  @DELETE
  @Path("queries/{queryHandle}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult cancelQuery(@QueryParam("sessionid") UUID publicId,
                               @PathParam("queryHandle") String queryHandle) {
    GrillSessionHandle sessionHandle = SessionUIResource.openSessions.get(publicId);
    checkSessionHandle(sessionHandle);
    try {
      boolean ret = queryServer.cancelQuery(sessionHandle, getQueryHandle(queryHandle));
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

  /**
   * Get the http endpoint for result set
   *
   * @param sessionid The user session handle
   * @param queryHandle The query handle
   *
   * @return Response with result as octet stream
   */
  @GET
  @Path("queries/{queryHandle}/httpresultset")
  @Produces({MediaType.APPLICATION_OCTET_STREAM})
  public Response getHttpResultSet(
      @QueryParam("sessionid") UUID publicId,
      @PathParam("queryHandle") String queryHandle) {
    GrillSessionHandle sessionHandle = SessionUIResource.openSessions.get(publicId);
    checkSessionHandle(sessionHandle);
    try {
      return queryServer.getHttpResultSet(sessionHandle, getQueryHandle(queryHandle));
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }
}

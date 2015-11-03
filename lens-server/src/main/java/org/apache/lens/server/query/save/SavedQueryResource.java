/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.query.save;

import static org.apache.lens.api.error.LensCommonErrorCode.INVALID_XML_ERROR;
import static org.apache.lens.api.query.save.ResourceModifiedResponse.Action.CREATED;
import static org.apache.lens.api.query.save.ResourceModifiedResponse.Action.DELETED;
import static org.apache.lens.api.query.save.ResourceModifiedResponse.Action.UPDATED;
import static org.apache.lens.server.api.LensConfConstants.*;

import java.io.IOException;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.error.ErrorCollection;
import org.apache.lens.api.query.QuerySubmitResult;
import org.apache.lens.api.query.save.ListResponse;
import org.apache.lens.api.query.save.ParameterParserResponse;
import org.apache.lens.api.query.save.ResourceModifiedResponse;
import org.apache.lens.api.query.save.SavedQuery;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensErrorInfo;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.api.query.save.*;
import org.apache.lens.server.api.query.save.param.ParameterParser;
import org.apache.lens.server.api.query.save.param.ParameterResolver;
import org.apache.lens.server.model.LogSegregationContext;

import org.apache.hadoop.hive.conf.HiveConf;

import org.glassfish.grizzly.http.server.Response;
import org.glassfish.jersey.media.multipart.FormDataParam;

import lombok.NonNull;

@Path("/queryapi")
/**
 * Saved query resource
 * <p></p>
 * CRUD on saved query
 */
public class SavedQueryResource {

  final SavedQueryService savedQueryService;
  final QueryExecutionService queryService;
  private final ErrorCollection errorCollection;
  private final LogSegregationContext logSegregationContext;
  private static final String DEFAULT_START = "0";
  private final int defaultCount;

  public SavedQueryResource() {
    savedQueryService = LensServices.get().getService(SavedQueryServiceImpl.NAME);
    queryService = LensServices.get().getService(QueryExecutionService.NAME);
    errorCollection = LensServices.get().getErrorCollection();
    logSegregationContext = LensServices.get().getLogSegregationContext();
    final HiveConf hiveConf = LensServices.get().getHiveConf();
    defaultCount = hiveConf.getInt(FETCH_COUNT_SAVED_QUERY_LIST_KEY, DEFAULT_FETCH_COUNT_SAVED_QUERY_LIST);
  }

  @GET
  @Path("/savedqueries/health")
  @Produces(MediaType.TEXT_PLAIN)
  /**
   * Health check end point
   */
  public String getMessage() {
    return "Saved query api is up";
  }


  /**
   * Gets a list of saved queries matching the criteria (url parameters)
   * windowed by count and start.
   *
   * @param sessionid  The sessionid in which user is working
   * @param info       URI context injected for query parameters
   * @param start      Offset to start from the search result
   * @param count      Number of records to fetch from start
   * @return {@link org.apache.lens.api.query.save.ListResponse} ListResponse object
   * @throws LensException
   */
  @GET
  @Path("/savedqueries")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public ListResponse getList(
    @QueryParam("sessionid") LensSessionHandle sessionid,
    @Context UriInfo info,
    @DefaultValue(DEFAULT_START) @QueryParam("start") int start,
    @QueryParam("count") String count) throws LensException {
    try {
      final int countVal = count == null? defaultCount: Integer.parseInt(count);
      return savedQueryService.list(info.getQueryParameters(), start, countVal);
    } catch (LensException e) {
      throw getWrapped(e);
    }
  }

  /**
   * Gets the saved query with the given id.
   *
   * @param sessionid  The sessionid in which user is working
   * @param id         id of the saved query
   * @return {@link org.apache.lens.api.query.save.SavedQuery} SavedQuery object
   * @throws LensException
   */
  @GET
  @Path("/savedqueries/{id}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public SavedQuery getByID(
    @QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("id") long id) throws LensException {
    try {
      return savedQueryService.get(id);
    } catch (LensException e) {
      throw getWrapped(e);
    }
  }

  /**
   * Deletes the saved query with the given id.
   *
   * @param sessionid  The sessionid in which user is working
   * @param id         id of the saved query
   * @return {@link org.apache.lens.api.query.save.ResourceModifiedResponse} ResourceModifiedResponse object
   * @throws LensException
   */
  @DELETE
  @Path("/savedqueries/{id}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public ResourceModifiedResponse deleteById(
    @QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("id") long id) throws LensException {
    try {
      savedQueryService.delete(id);
      return new ResourceModifiedResponse(id, "saved_query", DELETED);
    } catch (LensException e) {
      throw getWrapped(e);
    }
  }

  /**
   * Creates a new saved query.
   *
   * @param sessionid   The sessionid in which user is working
   * @param savedQuery  Saved query object
   * @param response    Injected response context object
   * @return {@link org.apache.lens.api.query.save.ResourceModifiedResponse} ResourceModifiedResponse object
   * @throws LensException
   * @throws IOException
   */
  @POST
  @Path(("/savedqueries"))
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public ResourceModifiedResponse create(
    @QueryParam("sessionid") LensSessionHandle sessionid,
    SavedQuery savedQuery,
    @Context final Response response)
    throws LensException, IOException {
    try {
      validateSampleResolved(savedQuery);
      long id = savedQueryService.save(savedQuery);
      response.setStatus(HttpServletResponse.SC_CREATED);
      response.flush();
      return new ResourceModifiedResponse(id, "saved_query", CREATED);
    } catch (LensException e) {
      throw getWrapped(e);
    }
  }

  /**
   * Updates the saved query {id} with the new payload.
   *
   * @param sessionid   The sessionid in which user is working
   * @param savedQuery  Saved query object
   * @param response    Injected response context object
   * @return {@link org.apache.lens.api.query.save.ResourceModifiedResponse} ResourceModifiedResponse object
   * @throws LensException
   * @throws IOException
   */
  @PUT
  @Path("/savedqueries/{id}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public ResourceModifiedResponse update(
    @QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("id") long id,
    SavedQuery savedQuery,
    @Context final Response response) throws LensException, IOException {
    try {
      validateSampleResolved(savedQuery);
      savedQueryService.update(id, savedQuery);
      response.setStatus(HttpServletResponse.SC_CREATED);
      response.flush();
      return new ResourceModifiedResponse(id, "saved_query", UPDATED);
    } catch (LensException e) {
      throw getWrapped(e);
    }
  }

  /**
   * Parses the query and returns parameters that are found in the query.
   *
   * @param sessionid  The sessionid in which user is working
   * @param query      The HQL query
   * @return {@link org.apache.lens.api.query.save.ParameterParserResponse} ParameterParserResponse object
   */
  @POST
  @Path("/savedqueries/parameters")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public ParameterParserResponse getParameters(
    @QueryParam("sessionid") LensSessionHandle sessionid,
    @FormDataParam("query") String query) {
    return new ParameterParser(query).extractParameters();
  }

  /**
   * Runs the saved query with the given id and returns a query handle.
   *
   * @param id         id of the saved query
   * @param info       Injected UriInfo context object
   * @param sessionid  The sessionid in which user is working
   * @param conf       Lens configuration overrides for the query
   * @return LensAPIResult containing the query handle
   * @throws LensException
   */
  @POST
  @Path("/savedqueries/{id}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public LensAPIResult<? extends QuerySubmitResult> run(
    @PathParam("id") long id,
    @Context UriInfo info,
    @FormDataParam("sessionid") LensSessionHandle sessionid,
    @FormDataParam("conf") LensConf conf) throws LensException {
    final String requestId = this.logSegregationContext.getLogSegragationId();
    try {
      final SavedQuery savedQuery = savedQueryService.get(id);
      final String query = ParameterResolver.resolve(savedQuery, info.getQueryParameters());
      return LensAPIResult.composedOf(
        null,
        requestId,
        queryService.executeAsync(sessionid, query, conf, savedQuery.getName())
      );
    } catch (LensException e) {
      throw getWrapped(e);
    }
  }

  /**
   * Helper method that builds error response for LensException provided.
   *
   * @param e    Lens exception object
   * @return lens exception object with error response built
   * @throws LensException
   */
  private LensException getWrapped(LensException e) throws
    LensException {
    e.buildLensErrorResponse(errorCollection, null, logSegregationContext.getLogSegragationId());
    throw e;
  }

  /**
   * Validates the saved query and throws LensException with.
   * BAD_SYNTAX code if wrong
   *
   * @param savedQuery Saved query object
   * @throws LensException if invalid
   */
  private void validateSampleResolved(@NonNull SavedQuery savedQuery) throws LensException {
    final String sampleResolved  = SavedQueryHelper.getSampleResolvedQuery(savedQuery);
    try {
      HQLParser.parseHQL(sampleResolved, new HiveConf());
    } catch (Exception e) {
      throw new LensException(
        new LensErrorInfo(INVALID_XML_ERROR.getValue(), 0, INVALID_XML_ERROR.toString())
        , e
        , "Encountered while resolving with sample values { " + sampleResolved + " }");
    }
  }

}

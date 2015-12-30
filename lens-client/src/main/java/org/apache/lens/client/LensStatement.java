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
package org.apache.lens.client;

import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.query.*;
import org.apache.lens.api.query.QueryStatus.Status;

import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.client.exceptions.LensAPIException;

import org.apache.commons.lang.StringUtils;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;

import lombok.RequiredArgsConstructor;

/**
 * Top level class which is used to execute lens queries.
 */
@RequiredArgsConstructor
public class LensStatement {

  /** The connection. */
  private final LensConnection connection;

  /** The query. */
  private LensQuery query;

  /**
   * Execute.
   *
   * @param sql                    the sql
   * @param waitForQueryToComplete the wait for query to complete
   * @param queryName              the query name
   */
  public LensAPIResult<QueryHandle> execute(String sql, boolean waitForQueryToComplete,
      String queryName) throws LensAPIException {
    LensAPIResult<QueryHandle> lensAPIResult = executeQuery(sql, waitForQueryToComplete, queryName);
    this.query = getQuery(lensAPIResult.getData());
    return lensAPIResult;
  }

  /**
   * Execute.
   *
   * @param sql       the sql
   * @param queryName the query name
   */
  public void execute(String sql, String queryName) throws LensAPIException {
    QueryHandle handle = executeQuery(sql, true, queryName).getData();
    this.query = getQuery(handle);
  }

  /**
   * Execute query.
   *
   * @param sql                    the sql
   * @param waitForQueryToComplete the wait for query to complete
   * @param queryName              the query name
   * @return the query handle
   */
  public LensAPIResult<QueryHandle> executeQuery(String sql, boolean waitForQueryToComplete,
      String queryName) throws LensAPIException {

    LensAPIResult<QueryHandle> lensAPIResult = executeQuery(sql, queryName);

    if (waitForQueryToComplete) {
      waitForQueryToComplete(lensAPIResult.getData());
    }
    return lensAPIResult;
  }

  /**
   * Execute query.
   *
   * @param phandle                the phandle
   * @param waitForQueryToComplete the wait for query to complete
   * @param queryName              the query name
   * @return the query handle
   */
  public QueryHandle executeQuery(QueryPrepareHandle phandle, boolean waitForQueryToComplete, String queryName) {
    QueryHandle handle = executeQuery(phandle, queryName);

    if (waitForQueryToComplete) {
      waitForQueryToComplete(handle);
    }
    return handle;
  }

  /**
   * Prepare query.
   *
   * @param sql       the sql
   * @param queryName the query name
   * @return the query prepare handle
   * @throws LensAPIException
   */
  public LensAPIResult<QueryPrepareHandle> prepareQuery(String sql, String queryName) throws LensAPIException {
    if (!connection.isOpen()) {
      throw new IllegalStateException("Lens Connection has to be " + "established before querying");
    }

    Client client = connection.buildClient();
    WebTarget target = getPreparedQueriesWebTarget(client);

    Response response = target.request().post(Entity.entity(prepareForm(sql, "PREPARE", queryName),
        MediaType.MULTIPART_FORM_DATA_TYPE));

    if (response.getStatus() == Response.Status.OK.getStatusCode()) {
      return response.readEntity(new GenericType<LensAPIResult<QueryPrepareHandle>>() {});
    }

    throw new LensAPIException(response.readEntity(LensAPIResult.class));
  }

  /**
   * Explain and prepare.
   *
   * @param sql       the sql
   * @param queryName the query name
   * @return the query plan
   * @throws LensAPIException
   */
  public LensAPIResult<QueryPlan> explainAndPrepare(String sql, String queryName) throws LensAPIException {
    if (!connection.isOpen()) {
      throw new IllegalStateException("Lens Connection has to be " + "established before querying");
    }

    Client client = connection.buildClient();

    WebTarget target = getPreparedQueriesWebTarget(client);

    Response response = target.request().post(
        Entity.entity(prepareForm(sql, "EXPLAIN_AND_PREPARE", queryName), MediaType.MULTIPART_FORM_DATA_TYPE),
        Response.class);
    if (response.getStatus() == Response.Status.OK.getStatusCode()) {
      return response.readEntity(new GenericType<LensAPIResult<QueryPlan>>() {});
    }

    throw new LensAPIException(response.readEntity(LensAPIResult.class));

  }

  /**
   * Prepare form.
   *
   * @param sql       the sql
   * @param op        the op
   * @param queryName the query name
   * @return the form data multi part
   */
  private FormDataMultiPart prepareForm(String sql, String op, String queryName) {
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), connection
      .getSessionHandle(), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), sql));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), op));
    if (!StringUtils.isBlank(queryName)) {
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("queryName").build(), queryName));
    }
    return mp;
  }

  /**
   * Wait for query to complete.
   *
   * @param handle the handle
   */
  public void waitForQueryToComplete(QueryHandle handle) {
    LensClient.getCliLooger().info("Query handle: {}", handle);
    query = getQuery(handle);
    while (query.queued()) {
      query = getQuery(handle);
      LensClient.getCliLooger().debug("Query {} status: {}", handle, query.getStatus());
      try {
        Thread.sleep(connection.getLensConnectionParams().getQueryPollInterval());
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
    LensClient.getCliLooger().info("User query: '{}' was submitted to {}", query.getUserQuery(),
      query.getSelectedDriverName());
    if (query.getDriverQuery() != null) {
      LensClient.getCliLooger().info(" Driver query: '{}' and Driver handle: {}", query.getDriverQuery(),
        query.getDriverOpHandle());
    }
    while (!query.getStatus().finished()
      && !(query.getStatus().toString().equals(Status.CLOSED.toString()))) {
      query = getQuery(handle);
      LensClient.getCliLooger().info("Query Status:{} ", query.getStatus());
      try {
        Thread.sleep(connection.getLensConnectionParams().getQueryPollInterval());
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  /**
   * Gets the query web target.
   *
   * @param client the client
   * @return the query web target
   */
  private WebTarget getQueryWebTarget(Client client) {
    return client.target(connection.getLensConnectionParams().getBaseConnectionUrl())
      .path(connection.getLensConnectionParams().getQueryResourcePath()).path("queries");
  }

  /**
   * Gets the prepared queries web target.
   *
   * @param client the client
   * @return the prepared queries web target
   */
  private WebTarget getPreparedQueriesWebTarget(Client client) {
    return client.target(connection.getLensConnectionParams().getBaseConnectionUrl())
      .path(connection.getLensConnectionParams().getQueryResourcePath()).path("preparedqueries");
  }

  /**
   * Gets the query.
   *
   * @param handle the handle
   * @return the query
   */
  public LensQuery getQuery(QueryHandle handle) {
    try {
      Client client = connection.buildClient();
      WebTarget target = getQueryWebTarget(client);
      this.query = target.path(handle.toString()).queryParam("sessionid", connection.getSessionHandle()).request()
        .get(LensQuery.class);
      return query;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get query status, cause:" + e.getMessage());
    }
  }

  /**
   * Gets the prepared query.
   *
   * @param handle the handle
   * @return the prepared query
   */
  public LensPreparedQuery getPreparedQuery(QueryPrepareHandle handle) {
    try {
      Client client = connection.buildClient();
      WebTarget target = getPreparedQueriesWebTarget((client));
      return target.path(handle.toString()).queryParam("sessionid", connection.getSessionHandle()).request()
        .get(LensPreparedQuery.class);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get query status, cause:" + e.getMessage());
    }
  }

  /**
   * Execute query.
   *
   * @param sql       the sql
   * @param queryName the query name
   * @return the query handle
   */
  private LensAPIResult<QueryHandle> executeQuery(String sql, String queryName) throws LensAPIException {
    if (!connection.isOpen()) {
      throw new IllegalStateException("Lens Connection has to be " + "established before querying");
    }

    Client client  = connection.buildClient();
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), connection
      .getSessionHandle(), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), sql));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("queryName").build(), queryName == null ? ""
      : queryName));

    WebTarget target = getQueryWebTarget(client);

    Response response = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));

    if (response.getStatus() == Response.Status.OK.getStatusCode()) {
      return response.readEntity(new GenericType<LensAPIResult<QueryHandle>>() {});
    }

    throw new LensAPIException(response.readEntity(LensAPIResult.class));
  }

  /**
   * Execute query.
   *
   * @param phandle   the phandle
   * @param queryName the query name
   * @return the query handle
   */
  public QueryHandle executeQuery(QueryPrepareHandle phandle, String queryName) {
    if (!connection.isOpen()) {
      throw new IllegalStateException("Lens Connection has to be " + "established before querying");
    }

    Client client = connection.buildClient();
    WebTarget target = getPreparedQueriesWebTarget((client)).path(phandle.toString());
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), connection
      .getSessionHandle(), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("queryName").build(), queryName == null ? ""
      : queryName));

    QueryHandle handle = target.request()
      .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

    return handle;
  }

  /**
   * Explain query.
   *
   * @param sql the sql
   * @return the query plan
   * @throws LensAPIException
   */
  public LensAPIResult<QueryPlan> explainQuery(String sql) throws LensAPIException {
    if (!connection.isOpen()) {
      throw new IllegalStateException("Lens Connection has to be established before querying");
    }

    Client client = connection.buildClient();
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), connection
      .getSessionHandle(), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), sql));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "explain"));

    WebTarget target = getQueryWebTarget(client);

    Response response = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));

    if (response.getStatus() == Response.Status.OK.getStatusCode()) {
      return response.readEntity(new GenericType<LensAPIResult<QueryPlan>>() {});
    }

    throw new LensAPIException(response.readEntity(LensAPIResult.class));
  }

  /**
   * Gets the all queries.
   *
   * @param state     the state
   * @param queryName the query name
   * @param user      the user
   * @param driver    the driver name
   * @param fromDate  the from date
   * @param toDate    the to date
   * @return the all queries
   */
  public List<QueryHandle> getAllQueries(String state, String queryName, String user, String driver, long fromDate,
    long toDate) {
    WebTarget target = getQueryWebTarget(connection.buildClient());
    List<QueryHandle> handles = target.queryParam("sessionid", connection.getSessionHandle())
      .queryParam("state", state).queryParam("queryName", queryName).queryParam("user", user)
      .queryParam("driver", driver).queryParam("fromDate", fromDate).queryParam("toDate", toDate).request()
      .get(new GenericType<List<QueryHandle>>() {
      });
    return handles;
  }

  /**
   * Gets the all prepared queries.
   *
   * @param userName  the user name
   * @param queryName the query name
   * @param fromDate  the from date
   * @param toDate    the to date
   * @return the all prepared queries
   */
  public List<QueryPrepareHandle> getAllPreparedQueries(String userName, String queryName, long fromDate, long toDate) {
    Client client = connection.buildClient();
    WebTarget target = getPreparedQueriesWebTarget(client);
    List<QueryPrepareHandle> handles = target.queryParam("sessionid", connection.getSessionHandle())
      .queryParam("user", userName).queryParam("queryName", queryName).queryParam("fromDate", fromDate)
      .queryParam("toDate", toDate).request().get(new GenericType<List<QueryPrepareHandle>>() {});
    return handles;
  }

  public QueryResultSetMetadata getResultSetMetaData() {
    return this.getResultSetMetaData(query);
  }

  /**
   * Gets the result set meta data.
   *
   * @param query the query
   * @return the result set meta data
   */
  public QueryResultSetMetadata getResultSetMetaData(LensQuery query) {
    if (query.getStatus().getStatus() != QueryStatus.Status.SUCCESSFUL) {
      throw new IllegalArgumentException("Result set metadata " + "can be only queries for successful queries");
    }
    Client client = connection.buildClient();

    try {
      WebTarget target = getQueryWebTarget(client);

      return target.path(query.getQueryHandle().toString()).path("resultsetmetadata")
        .queryParam("sessionid", connection.getSessionHandle()).request().get(QueryResultSetMetadata.class);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get resultset metadata, cause:" + e.getMessage());
    }
  }

  public QueryResult getResultSet() {
    return this.getResultSet(this.query);
  }

  public Response getHttpResultSet() {
    return this.getHttpResultSet(this.query);
  }

  /**
   * Gets the result set.
   *
   * @param query the query
   * @return the result set
   */
  public QueryResult getResultSet(LensQuery query) {
    if (query.getStatus().getStatus() != QueryStatus.Status.SUCCESSFUL) {
      throw new IllegalArgumentException("Result set metadata " + "can be only queries for successful queries");
    }
    Client client = connection.buildClient();

    try {
      WebTarget target = getQueryWebTarget(client);
      return target.path(query.getQueryHandle().toString()).path("resultset")
        .queryParam("sessionid", connection.getSessionHandle()).request().get(QueryResult.class);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get resultset, cause:" + e.getMessage());
    }
  }

  /**
   * Gets the http result set.
   *
   * @param query the query
   * @return the http result set
   */
  public Response getHttpResultSet(LensQuery query) {
    if (query.getStatus().getStatus() != QueryStatus.Status.SUCCESSFUL) {
      throw new IllegalArgumentException("Result set metadata " + "can be only queries for successful queries");
    }
    Client client = connection.buildClient();

    try {
      WebTarget target = getQueryWebTarget(client);
      return target.path(query.getQueryHandle().toString()).path("httpresultset")
        .queryParam("sessionid", connection.getSessionHandle()).request().get();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get resultset, cause:" + e.getMessage());
    }
  }

  /**
   * Kill.
   *
   * @return true, if successful
   */
  public boolean kill() {
    return this.kill(query);
  }

  /**
   * Kill.
   *
   * @param query the query
   * @return true, if successful
   */
  public boolean kill(LensQuery query) {

    if (query.getStatus().finished()) {
      return false;
    }

    Client client = connection.buildClient();
    WebTarget target = getQueryWebTarget(client);

    APIResult result = target.path(query.getQueryHandle().toString())
      .queryParam("sessionid", connection.getSessionHandle()).request().delete(APIResult.class);

    return result.getStatus().equals(APIResult.Status.SUCCEEDED);
  }

  /**
   * Close result set.
   *
   * @return true, if successful
   */
  public boolean closeResultSet() {
    if (!query.getStatus().isResultSetAvailable()) {
      return false;
    }
    Client client = connection.buildClient();
    WebTarget target = getQueryWebTarget(client);

    APIResult result = target.path(query.getQueryHandle().toString()).path("resultset")
      .queryParam("sessionid", connection.getSessionHandle()).request().delete(APIResult.class);

    return result.getStatus() == APIResult.Status.SUCCEEDED;
  }

  /**
   * Destroy prepared.
   *
   * @param phandle the phandle
   * @return true, if successful
   */
  public boolean destroyPrepared(QueryPrepareHandle phandle) {
    Client client = connection.buildClient();
    WebTarget target = getPreparedQueriesWebTarget(client);

    APIResult result = target.path(phandle.toString()).queryParam("sessionid", connection.getSessionHandle()).request()
      .delete(APIResult.class);

    return result.getStatus() == APIResult.Status.SUCCEEDED;
  }

  public boolean isIdle() {
    return query == null || query.getStatus().finished();
  }

  /**
   * Was query successful.
   *
   * @return true, if successful
   */
  public boolean wasQuerySuccessful() {
    return query.getStatus().getStatus().equals(QueryStatus.Status.SUCCESSFUL);
  }

  public QueryStatus getStatus() {
    return getQuery().getStatus();
  }

  public LensQuery getQuery() {
    return this.query;
  }

  public int getErrorCode() {
    return this.query.getErrorCode();
  }

  public String getErrorMessage() {
    return this.query.getErrorMessage();
  }

  public String getQueryHandleString() {
    return this.query.getQueryHandleString();
  }
}

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

import java.net.SocketTimeoutException;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.query.*;
import org.apache.lens.api.query.QueryStatus.Status;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.client.exceptions.LensAPIException;
import org.apache.lens.client.model.ProxyLensQuery;

import org.apache.commons.lang.StringUtils;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Top level class which is used to execute lens queries.
 */
@RequiredArgsConstructor
@Slf4j
public class LensStatement {

  /** The connection. */
  private final LensConnection connection;

  /** The query. */
  private LensQuery query;

  /**
   * This method can be used for executing a query. If waitForQueryToComplete is false, the call to this method returns
   * immediately after submitting the query to the server without waiting for it to complete execution.
   * <p>
   * {@link #getStatus(QueryHandle)} can be used to track to track the query progress and
   * {@link #getQuery(QueryHandle)} can be used to get complete details (including status) about the query.
   *
   * @param sql                    the sql
   * @param waitForQueryToComplete the wait for query to complete
   * @param queryName              the query name
   * @return the query handle
   */
  @Deprecated
  public QueryHandle executeQuery(String sql, boolean waitForQueryToComplete, String queryName)
    throws LensAPIException {
    return executeQuery(sql, waitForQueryToComplete, queryName, new LensConf());
  }

  /**
   * This method can be used for executing a query. If waitForQueryToComplete is false, the call to this method returns
   * immediately after submitting the query to the server without waiting for it to complete execution.
   * <p>
   * {@link #getStatus(QueryHandle)} can be used to track to track the query progress and
   * {@link #getQuery(QueryHandle)} can be used to get complete details (including status) about the query.
   *
   * @param sql                    the sql
   * @param waitForQueryToComplete the wait for query to complete
   * @param queryName              the query name
   * @param conf                   config specific to this query
   * @return the query handle
   */
  public QueryHandle executeQuery(String sql, boolean waitForQueryToComplete, String queryName, LensConf conf)
    throws LensAPIException {

    QueryHandle handle = submitQuery(sql, queryName, conf);

    if (waitForQueryToComplete) {
      waitForQueryToComplete(handle);
    }
    return handle;
  }

  /**
   * This method can be used for executing a prepared query. If waitForQueryToComplete is false, the call to this method
   * returns immediately after submitting the query to the server without waiting for it to complete execution.
   * <p>
   * {@link #getStatus(QueryHandle)} can be used to track to track the query progress and
   * {@link #getQuery(QueryHandle)} can be used to get complete details (including status) about the query.
   *
   * @param phandle                the prepared query handle
   * @param waitForQueryToComplete the wait for query to complete
   * @param queryName              the query name
   * @return the query handle
   */
  @Deprecated
  public QueryHandle executeQuery(QueryPrepareHandle phandle, boolean waitForQueryToComplete, String queryName) {
    return executeQuery(phandle, waitForQueryToComplete, queryName, new LensConf());
  }

  /**
   * This method can be used for executing a prepared query. If waitForQueryToComplete is false, the call to this method
   * returns immediately after submitting the query to the server without waiting for it to complete execution.
   * <p>
   * {@link #getStatus(QueryHandle)} can be used to track to track the query progress and
   * {@link #getQuery(QueryHandle)} can be used to get complete details (including status) about the query.
   *
   * @param phandle                the prepared query handle
   * @param waitForQueryToComplete the wait for query to complete
   * @param queryName              the query name
   * @param conf                   config to be used for the query
   * @return the query handle
   */
  public QueryHandle executeQuery(QueryPrepareHandle phandle, boolean waitForQueryToComplete, String queryName,
     LensConf conf) {
    QueryHandle handle = submitQuery(phandle, queryName, conf);

    if (waitForQueryToComplete) {
      waitForQueryToComplete(handle);
    }
    return handle;
  }

  /**
   * This method can be used for executing query. The method waits for timeOutMillis time OR query execution to succeed,
   * which ever happens first, before returning the response to the caller.
   * <p>
   * If the query execution finishes before timeout, user can check the query Status (SUCCESSFUL/FAILED) using
   * {@link QueryHandleWithResultSet#getStatus()} and access the result of SUCCESSFUL query via
   * {@link QueryHandleWithResultSet#getResult()} and {@link QueryHandleWithResultSet#getResultMetadata()}.
   * <p>
   * If the query does not finish within the timeout, user can use {@link #getStatus(QueryHandle)} to track
   * the query progress and {@link #getQuery(QueryHandle)} to get complete details (including status) about
   * the query. Once the query has reached SUCCESSFUL state, user can access the results via
   * {@link #getResultSet(LensQuery)} and {@link #getResultSetMetaData(LensQuery)}
   *
   * @param sql : query/command to be executed
   * @param queryName : optional query name
   * @param timeOutMillis : timeout milliseconds
   * @return QueryHandleWithResultSet
   * @throws LensAPIException
   */
  @Deprecated
  public QueryHandleWithResultSet executeQuery(String sql, String queryName, long timeOutMillis)
    throws LensAPIException {
    return executeQuery(sql, queryName, timeOutMillis, new LensConf());
  }

  /**
   * This method can be used for executing query. The method waits for timeOutMillis time OR query execution to succeed,
   * which ever happens first, before returning the response to the caller.
   * <p>
   * If the query execution finishes before timeout, user can check the query Status (SUCCESSFUL/FAILED) using
   * {@link QueryHandleWithResultSet#getStatus()} and access the result of SUCCESSFUL query via
   * {@link QueryHandleWithResultSet#getResult()} and {@link QueryHandleWithResultSet#getResultMetadata()}.
   * <p>
   * If the query does not finish within the timeout, user can use {@link #getStatus(QueryHandle)} to track
   * the query progress and {@link #getQuery(QueryHandle)} to get complete details (including status) about
   * the query. Once the query has reached SUCCESSFUL state, user can access the results via
   * {@link #getResultSet(LensQuery)} and {@link #getResultSetMetaData(LensQuery)}
   *
   * @param sql : query/command to be executed
   * @param queryName : optional query name
   * @param timeOutMillis : timeout milliseconds
   * @param conf      config to be used for the query
   * @return QueryHandleWithResultSet
   * @throws LensAPIException
   */
  public QueryHandleWithResultSet executeQuery(String sql, String queryName, long timeOutMillis, LensConf conf)
    throws LensAPIException {
    if (!connection.isOpen()) {
      throw new IllegalStateException("Lens Connection has to be established before querying");
    }

    Client client = connection.buildClient();
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), connection
        .getSessionHandle(), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), sql));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "EXECUTE_WITH_TIMEOUT"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("timeoutmillis").build(), "" + timeOutMillis));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("queryName").build(), queryName == null ? ""
        : queryName));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
        MediaType.APPLICATION_XML_TYPE));
    WebTarget target = getQueryWebTarget(client);

    Response response =
        target.request(MediaType.APPLICATION_XML_TYPE).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));

    if (response.getStatus() == Response.Status.OK.getStatusCode()) {
      QueryHandleWithResultSet result =
          response.readEntity(new GenericType<LensAPIResult<QueryHandleWithResultSet>>() {}).getData();
      this.query = new ProxyLensQuery(this, result.getQueryHandle());
      return result;
    }

    throw new LensAPIException(response.readEntity(LensAPIResult.class));
  }

  /**
   * Prepare query.
   *
   * @param sql       the sql
   * @param queryName the query name
   * @return the query prepare handle
   * @throws LensAPIException
   */
  @Deprecated
  public LensAPIResult<QueryPrepareHandle> prepareQuery(String sql, String queryName) throws LensAPIException {
    return prepareQuery(sql, queryName, new LensConf());
  }

  /**
   * Prepare query.
   *
   * @param sql       the sql
   * @param queryName the query name
   * @return the query prepare handle
   * @throws LensAPIException
   */
  public LensAPIResult<QueryPrepareHandle> prepareQuery(String sql, String queryName,
     LensConf conf) throws LensAPIException {
    if (!connection.isOpen()) {
      throw new IllegalStateException("Lens Connection has to be established before querying");
    }

    Client client = connection.buildClient();
    WebTarget target = getPreparedQueriesWebTarget(client);

    Response response = target.request().post(Entity.entity(prepareForm(sql, "PREPARE", queryName, conf),
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
  @Deprecated
  public LensAPIResult<QueryPlan> explainAndPrepare(String sql, String queryName) throws LensAPIException {
    return explainAndPrepare(sql, queryName, new LensConf());
  }

  /**
   * Explain and prepare.
   *
   * @param sql       the sql
   * @param queryName the query name
   * @param conf      config to be used for the query
   * @return the query plan
   * @throws LensAPIException
   */
  public LensAPIResult<QueryPlan> explainAndPrepare(String sql, String queryName,
    LensConf conf) throws LensAPIException {
    if (!connection.isOpen()) {
      throw new IllegalStateException("Lens Connection has to be established before querying");
    }

    Client client = connection.buildClient();

    WebTarget target = getPreparedQueriesWebTarget(client);

    Response response = target.request().post(
        Entity.entity(prepareForm(sql, "EXPLAIN_AND_PREPARE", queryName, conf), MediaType.MULTIPART_FORM_DATA_TYPE),
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
   * @param conf      config to be used for the query
   * @return the form data multi part
   */
  private FormDataMultiPart prepareForm(String sql, String op, String queryName, LensConf conf) {
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), connection
      .getSessionHandle(), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), sql));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), op));
    if (!StringUtils.isBlank(queryName)) {
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("queryName").build(), queryName));
    }
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      MediaType.APPLICATION_XML_TYPE));
    return mp;
  }

  public void waitForQueryToComplete(QueryHandle handle) {
    waitForQueryToComplete(handle, true);
  }

  /**
   * Wait for query to complete.
   *
   * @param handle the handle
   */
  void waitForQueryToComplete(QueryHandle handle, boolean retryOnTimeout) {
    LensClient.getCliLogger().info("Query handle: {}", handle);
    LensQuery queryDetails = retryOnTimeout ? getQueryWithRetryOnTimeout(handle) : getQuery(handle);
    while (queryDetails.getStatus().queued()) {
      queryDetails = retryOnTimeout ? getQueryWithRetryOnTimeout(handle) : getQuery(handle);
      LensClient.getCliLogger().debug("Query {} status: {}", handle, queryDetails.getStatus());
      try {
        Thread.sleep(connection.getLensConnectionParams().getQueryPollInterval());
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
    LensClient.getCliLogger().info("User query: '{}' was submitted to {}", queryDetails.getUserQuery(),
      queryDetails.getSelectedDriverName());
    if (queryDetails.getDriverQuery() != null) {
      LensClient.getCliLogger().info(" Driver query: '{}' and Driver handle: {}", queryDetails.getDriverQuery(),
        queryDetails.getDriverOpHandle());
    }
    while (!queryDetails.getStatus().finished()
      && !(queryDetails.getStatus().getStatus().equals(Status.CLOSED))) {
      queryDetails = retryOnTimeout ? getQueryWithRetryOnTimeout(handle) : getQuery(handle);
      LensClient.getCliLogger().info("Query Status:{} ", queryDetails.getStatus());
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
      return target.path(handle.toString()).queryParam("sessionid", connection.getSessionHandle()).request()
        .get(LensQuery.class);
    } catch (Exception e) {
      log.error("Failed to get query status, cause:", e);
      throw new IllegalStateException("Failed to get query status, cause:" + e.getMessage(), e);
    }
  }

  LensQuery getQueryWithRetryOnTimeout(QueryHandle handle) {
    while (true) {
      try {
        return getQuery(handle);
      } catch (Exception e) {
        if (isExceptionDueToSocketTimeout(e)) {
          log.warn("Could not get query status. Encountered socket timeout. Retrying...");
          continue;
        } else {
          throw e;
        }
      }
    }
  }

  static boolean isExceptionDueToSocketTimeout(Throwable err) {
    if (err == null) {
      return false;
    }
    if (err instanceof SocketTimeoutException) {
      return true;
    } else {
      return isExceptionDueToSocketTimeout(err.getCause());
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
      log.error("Failed to get prepared query, cause:", e);
      throw new IllegalStateException("Failed to get prepared query, cause:" + e.getMessage());
    }
  }

  /**
   * Execute query.
   *
   * @param sql       the sql
   * @param queryName the query name
   * @param conf      config to be used for the query
   * @return the query handle
   */
  private QueryHandle submitQuery(String sql, String queryName, LensConf conf) throws LensAPIException {
    if (!connection.isOpen()) {
      throw new IllegalStateException("Lens Connection has to be established before querying");
    }

    Client client  = connection.buildClient();
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), connection
      .getSessionHandle(), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), sql));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "EXECUTE"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("queryName").build(), queryName == null ? ""
      : queryName));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      MediaType.APPLICATION_XML_TYPE));
    WebTarget target = getQueryWebTarget(client);

    Response response = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));

    if (response.getStatus() == Response.Status.OK.getStatusCode()) {
      QueryHandle handle = response.readEntity(new GenericType<LensAPIResult<QueryHandle>>() {}).getData();
      this.query = new ProxyLensQuery(this, handle);
      return handle;
    }

    throw new LensAPIException(response.readEntity(LensAPIResult.class));
  }

  /**
   * Execute query.
   *
   * @param phandle   the phandle
   * @param queryName the query name
   * @param conf      config to be used for the query
   * @return the query handle
   */
  private QueryHandle submitQuery(QueryPrepareHandle phandle, String queryName, LensConf conf) {
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
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      MediaType.APPLICATION_XML_TYPE));
    QueryHandle handle = target.request()
        .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

    this.query = new ProxyLensQuery(this, handle);
    return handle;
  }

  /**
   * Explain query.
   *
   * @param sql the sql
   * @return the query plan
   * @throws LensAPIException
   */
  @Deprecated
  public LensAPIResult<QueryPlan> explainQuery(String sql) throws LensAPIException {
    return explainQuery(sql, new LensConf());
  }

  /**
   * Explain query.
   *
   * @param sql   the sql
   * @param conf  config to be used for the query
   * @return the query plan
   * @throws LensAPIException
   */
  public LensAPIResult<QueryPlan> explainQuery(String sql, LensConf conf) throws LensAPIException {
    if (!connection.isOpen()) {
      throw new IllegalStateException("Lens Connection has to be established before querying");
    }

    Client client = connection.buildClient();
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), connection
      .getSessionHandle(), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), sql));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "explain"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      MediaType.APPLICATION_XML_TYPE));
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
  public List<QueryHandle> getAllQueries(String state, String queryName, String user, String driver, String fromDate,
    String toDate) {
    WebTarget target = getQueryWebTarget(connection.buildClient());
    return target.queryParam("sessionid", connection.getSessionHandle())
      .queryParam("state", state).queryParam("queryName", queryName).queryParam("user", user)
      .queryParam("driver", driver).queryParam("fromDate", fromDate).queryParam("toDate", toDate).request()
      .get(new GenericType<List<QueryHandle>>() {
      });
  }

  /**
   * Gets the all queries with details.
   *
   * @param state     the state
   * @param queryName the query name
   * @param user      the user
   * @param driver    the driver name
   * @param fromDate  the from date
   * @param toDate    the to date
   * @return the all queries as per criteria along with additional details
   */
  public List<LensQuery> getAllQueryDetails(String state, String queryName, String user, String driver,
     String fromDate, String toDate) {
    WebTarget target = getQueryWebTarget(connection.buildClient());
    return target.queryParam("sessionid", connection.getSessionHandle())
      .queryParam("state", state).queryParam("queryName", queryName).queryParam("user", user)
      .queryParam("driver", driver).queryParam("fromDate", fromDate).queryParam("toDate", toDate).request()
      .get(new GenericType<List<LensQuery>>() {
      });
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
  public List<QueryPrepareHandle> getAllPreparedQueries(String userName, String queryName, String fromDate,
    String toDate) {
    Client client = connection.buildClient();
    WebTarget target = getPreparedQueriesWebTarget(client);
    return target.queryParam("sessionid", connection.getSessionHandle())
      .queryParam("user", userName).queryParam("queryName", queryName).queryParam("fromDate", fromDate)
      .queryParam("toDate", toDate).request().get(new GenericType<List<QueryPrepareHandle>>() {});
  }

  /**
   * Gets the result set meta data for the most recently executed query.
   */
  public QueryResultSetMetadata getResultSetMetaData() {
    return this.getResultSetMetaData(this.getQuery());
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
      log.error("Failed to get resultset metadata, cause:", e);
      throw new IllegalStateException("Failed to get resultset metadata, cause:" + e.getMessage());
    }
  }

  /**
   * Gets result set for the most recently executed query.
   */
  public QueryResult getResultSet() {
    return this.getResultSet(this.getQuery());
  }

  /**
   * Gets http result set for the most recently executed query.
   */
  public Response getHttpResultSet() {
    return this.getHttpResultSet(this.getQuery());
  }

  /**
   * Gets the result set.
   *
   * @param query the query
   * @return the result set
   */
  public QueryResult getResultSet(LensQuery query) {
    if (query.getStatus().getStatus() != QueryStatus.Status.SUCCESSFUL) {
      throw new IllegalArgumentException("Result set metadata can be only queries for successful queries");
    }
    Client client = connection.buildClient();

    try {
      WebTarget target = getQueryWebTarget(client);
      return target.path(query.getQueryHandle().toString()).path("resultset")
        .queryParam("sessionid", connection.getSessionHandle()).request(MediaType.APPLICATION_XML_TYPE).get(
          QueryResult.class);
    } catch (Exception e) {
      log.error("Failed to get resultset, cause:", e);
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
      log.error("Failed to get http resultset, cause:", e);
      throw new IllegalStateException("Failed to get http resultset, cause:" + e.getMessage());
    }
  }

  /**
   * Kill the most recently submitted query via any executeQuery methods.
   *
   * @return true, if successful
   */
  public boolean kill() {
    return this.kill(this.getQuery());
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
    if (!this.getQuery().getStatus().isResultSetAvailable()) {
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
    return query == null || this.getQuery().getStatus().finished();
  }

  /**
   * Was the most recently executed query successful.
   *
   * @return true, if successful
   */
  public boolean wasQuerySuccessful() {
    return this.getQuery().getStatus().getStatus().equals(QueryStatus.Status.SUCCESSFUL);
  }

  /**
   * Gets status for query represented by handle.
   */
  public QueryStatus getStatus(QueryHandle handle) {
    return getQuery(handle).getStatus();
  }

  /**
   * Gets the status for the most recently executed query.
   */
  public QueryStatus getStatus() {
    return getQuery().getStatus();
  }

  /**
   * Gets details of the most recently executed query through {@link #executeQuery(QueryPrepareHandle, boolean, String)}
   * or {@link #executeQuery(String, boolean, String)} or {@link #executeQuery(String, String, long)}
   * <p>
   * Note: Cached query details are returned if the query has finished. If the query has still not finished it fetches
   * the latest details from server again.
   */
  public LensQuery getQuery() {
    if (this.query != null && !this.query.getStatus().finished()) {
      // Get Updated Query if the query has not finished yet.
      this.query = getQuery(this.query.getQueryHandle());
    }
    return this.query;
  }

  /**
   *Gets the error code, if any, for the most recently executed query.
   */
  public int getErrorCode() {
    return this.getQuery().getErrorCode();
  }

  /**
   * Gets the error message, if any, for the most recently executed query.
   */
  public String getErrorMessage() {
    return this.getQuery().getErrorMessage();
  }

  /**
   * Gets the query handle string for the most recently executed query.
   */
  public String getQueryHandleString() {
    return this.query.getQueryHandleString();
  }

  /**
   * Gets the user for the lens session
   */
  public String getUser() {
    return this.connection.getLensConnectionParams().getUser();
  }
}


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
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.query.*;

import org.apache.commons.lang.StringUtils;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

/**
 * Top level class which is used to execute lens queries.
 */
public class LensStatement {

  /** The connection. */
  private final LensConnection connection;

  /** The query. */
  private LensQuery query;

  /**
   * Instantiates a new lens statement.
   *
   * @param connection the connection
   */
  public LensStatement(LensConnection connection) {
    this.connection = connection;
  }

  /**
   * Execute.
   *
   * @param sql                    the sql
   * @param waitForQueryToComplete the wait for query to complete
   * @param queryName              the query name
   */
  public void execute(String sql, boolean waitForQueryToComplete, String queryName) {
    QueryHandle handle = executeQuery(sql, waitForQueryToComplete, queryName);
    this.query = getQuery(handle);
  }

  /**
   * Execute.
   *
   * @param sql       the sql
   * @param queryName the query name
   */
  public void execute(String sql, String queryName) {
    QueryHandle handle = executeQuery(sql, true, queryName);
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
  public QueryHandle executeQuery(String sql, boolean waitForQueryToComplete, String queryName) {
    QueryHandle handle = executeQuery(sql, queryName);

    if (waitForQueryToComplete) {
      waitForQueryToComplete(handle);
    }
    return handle;
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
   */
  public QueryPrepareHandle prepareQuery(String sql, String queryName) {
    if (!connection.isOpen()) {
      throw new IllegalStateException("Lens Connection has to be " + "established before querying");
    }

    Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).build();
    WebTarget target = getPreparedQueriesWebTarget(client);

    QueryPrepareHandle handle = target.request().post(
      Entity.entity(prepareForm(sql, "PREPARE", queryName), MediaType.MULTIPART_FORM_DATA_TYPE),
      QueryPrepareHandle.class);
    getPreparedQuery(handle);
    return handle;
  }

  /**
   * Explain and prepare.
   *
   * @param sql       the sql
   * @param queryName the query name
   * @return the query plan
   */
  public QueryPlan explainAndPrepare(String sql, String queryName) {
    if (!connection.isOpen()) {
      throw new IllegalStateException("Lens Connection has to be " + "established before querying");
    }

    Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).build();

    WebTarget target = getPreparedQueriesWebTarget(client);

    QueryPlan plan = target.request().post(
      Entity.entity(prepareForm(sql, "EXPLAIN_AND_PREPARE", queryName), MediaType.MULTIPART_FORM_DATA_TYPE),
      QueryPlan.class);
    return plan;
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
  private void waitForQueryToComplete(QueryHandle handle) {
    query = getQuery(handle);
    while (!query.getStatus().isFinished()) {
      query = getQuery(handle);
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
      Client client = ClientBuilder.newClient();
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
      Client client = ClientBuilder.newClient();
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
  private QueryHandle executeQuery(String sql, String queryName) {
    if (!connection.isOpen()) {
      throw new IllegalStateException("Lens Connection has to be " + "established before querying");
    }

    Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).build();
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), connection
      .getSessionHandle(), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), sql));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("queryName").build(), queryName == null ? ""
      : queryName));

    WebTarget target = getQueryWebTarget(client);

    QueryHandle handle = target.request()
      .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);
    return handle;
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

    Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).build();
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
   */
  public QueryPlan explainQuery(String sql) {
    if (!connection.isOpen()) {
      throw new IllegalStateException("Lens Connection has to be " + "established before querying");
    }

    Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).build();
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), connection
      .getSessionHandle(), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), sql));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "explain"));

    WebTarget target = getQueryWebTarget(client);

    QueryPlan handle = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryPlan.class);
    return handle;
  }

  /**
   * Gets the all queries.
   *
   * @param state     the state
   * @param queryName the query name
   * @param user      the user
   * @param fromDate  the from date
   * @param toDate    the to date
   * @return the all queries
   */
  public List<QueryHandle> getAllQueries(String state, String queryName, String user, long fromDate, long toDate) {
    WebTarget target = getQueryWebTarget(ClientBuilder.newBuilder().register(MultiPartFeature.class).build());
    List<QueryHandle> handles = target.queryParam("sessionid", connection.getSessionHandle())
      .queryParam("state", state).queryParam("queryName", queryName).queryParam("user", user)
      .queryParam("fromDate", fromDate).queryParam("toDate", toDate).request()
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
    Client client = ClientBuilder.newClient();
    WebTarget target = getPreparedQueriesWebTarget(client);
    List<QueryPrepareHandle> handles = target.queryParam("sessionid", connection.getSessionHandle())
      .queryParam("user", userName).queryParam("queryName", queryName).queryParam("fromDate", fromDate)
      .queryParam("toDate", toDate).request().get(new GenericType<List<QueryPrepareHandle>>() {
      });
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
    Client client = ClientBuilder.newClient();

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
    Client client = ClientBuilder.newClient();

    try {
      WebTarget target = getQueryWebTarget(client);
      return target.path(query.getQueryHandle().toString()).path("resultset")
        .queryParam("sessionid", connection.getSessionHandle()).request().get(QueryResult.class);
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

    if (query.getStatus().isFinished()) {
      return false;
    }

    Client client = ClientBuilder.newClient();
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
    Client client = ClientBuilder.newClient();
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
    Client client = ClientBuilder.newClient();
    WebTarget target = getPreparedQueriesWebTarget(client);

    APIResult result = target.path(phandle.toString()).queryParam("sessionid", connection.getSessionHandle()).request()
      .delete(APIResult.class);

    return result.getStatus() == APIResult.Status.SUCCEEDED;
  }

  public boolean isIdle() {
    return query == null || query.getStatus().isFinished();
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
}

package org.apache.lens.client;

/*
 * #%L
 * Lens client
 * %%
 * Copyright (C) 2014 Apache Software Foundation
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


import org.apache.lens.api.APIResult;
import org.apache.lens.api.query.*;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import java.util.List;

/**
 * Top level class which is used to execute lens queries.
 */
public class LensStatement {

  private final LensConnection connection;
  private LensQuery query;

  public LensStatement(LensConnection connection) {
    this.connection = connection;
  }

  public void execute(String sql, boolean waitForQueryToComplete, String queryName) {
    QueryHandle handle = executeQuery(sql, waitForQueryToComplete, queryName);
    this.query = getQuery(handle);
  }

  public void execute(String sql, String queryName) {
    QueryHandle handle = executeQuery(sql, true, queryName);
    this.query = getQuery(handle);
  }

  public QueryHandle executeQuery(String sql, boolean waitForQueryToComplete, String queryName) {
    QueryHandle handle = executeQuery(sql, queryName);

    if (waitForQueryToComplete) {
      waitForQueryToComplete(handle);
    }
    return handle;
  }

  public QueryHandle executeQuery(QueryPrepareHandle phandle, boolean waitForQueryToComplete, String queryName) {
    QueryHandle handle = executeQuery(phandle, queryName);

    if (waitForQueryToComplete) {
      waitForQueryToComplete(handle);
    }
    return handle;
  }

  public QueryPrepareHandle prepareQuery(String sql, String queryName) {
    if (!connection.isOpen()) {
      throw new IllegalStateException("Lens Connection has to be " +
          "established before querying");
    }

    Client client = ClientBuilder.newBuilder().register(
        MultiPartFeature.class).build();
    WebTarget target = getPreparedQueriesWebTarget(client);

    QueryPrepareHandle handle = target.request().post(Entity.entity(
        prepareForm(sql, "PREPARE", queryName), MediaType.MULTIPART_FORM_DATA_TYPE),
        QueryPrepareHandle.class);
    getPreparedQuery(handle);
    return handle;
  }

  public QueryPlan explainAndPrepare(String sql, String queryName) {
    if (!connection.isOpen()) {
      throw new IllegalStateException("Lens Connection has to be " +
          "established before querying");
    }

    Client client = ClientBuilder.newBuilder().register(
        MultiPartFeature.class).build();

    WebTarget target = getPreparedQueriesWebTarget(client);

    QueryPlan plan = target.request().post(
        Entity.entity(prepareForm(sql, "EXPLAIN_AND_PREPARE", queryName),
        MediaType.MULTIPART_FORM_DATA_TYPE), QueryPlan.class);
    return plan;
  }

  private FormDataMultiPart prepareForm(String sql, String op, String queryName) {
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("sessionid").build(),
        connection.getSessionHandle(), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        sql));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(),
        op));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("queryName").build(),
      queryName));
    return mp;
  }

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

  private WebTarget getQueryWebTarget(Client client) {
    return client.target(
        connection.getLensConnectionParams().getBaseConnectionUrl()).path(
            connection.getLensConnectionParams().getQueryResourcePath()).path("queries");
  }

  private WebTarget getPreparedQueriesWebTarget(Client client) {
    return client.target(
        connection.getLensConnectionParams().getBaseConnectionUrl()).path(
            connection.getLensConnectionParams().getQueryResourcePath()).path("preparedqueries");
  }

  public LensQuery getQuery(QueryHandle handle) {
    try {
      Client client = ClientBuilder.newClient();
      WebTarget target = getQueryWebTarget(client);
      this.query = target.path(handle.toString()).queryParam(
          "sessionid", connection.getSessionHandle()).request().get(LensQuery.class);
      return query;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get query status, cause:" + e.getMessage());
    }
  }

  public LensPreparedQuery getPreparedQuery(QueryPrepareHandle handle) {
    try {
      Client client = ClientBuilder.newClient();
      WebTarget target = getPreparedQueriesWebTarget((client));
      return target.path(handle.toString()).queryParam(
          "sessionid", connection.getSessionHandle()).request().get(LensPreparedQuery.class);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get query status, cause:" + e.getMessage());
    }
  }

  private QueryHandle executeQuery(String sql, String queryName) {
    if (!connection.isOpen()) {
      throw new IllegalStateException("Lens Connection has to be " +
          "established before querying");
    }

    Client client = ClientBuilder.newBuilder().register(
        MultiPartFeature.class).build();
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("sessionid").build(),
        connection.getSessionHandle(), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        sql));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(),
        "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("queryName").build(),
       queryName == null ? "" : queryName));

    WebTarget target = getQueryWebTarget(client);

    QueryHandle handle = target.request().post(Entity.entity(mp,
        MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);
    return handle;
  }

  public QueryHandle executeQuery(QueryPrepareHandle phandle, String queryName) {
    if (!connection.isOpen()) {
      throw new IllegalStateException("Lens Connection has to be " +
          "established before querying");
    }

    Client client = ClientBuilder.newBuilder().register(
        MultiPartFeature.class).build();
    WebTarget target = getPreparedQueriesWebTarget((client)).path(phandle.toString());
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("sessionid").build(),
        connection.getSessionHandle(), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(),
        "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("queryName").build(),
      queryName==null? "" : queryName));

    QueryHandle handle = target.request().post(Entity.entity(mp,
        MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);
    return handle;
  }

  public QueryPlan explainQuery(String sql) {
    if (!connection.isOpen()) {
      throw new IllegalStateException("Lens Connection has to be " +
          "established before querying");
    }

    Client client = ClientBuilder.newBuilder().register(
        MultiPartFeature.class).build();
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("sessionid").build(),
        connection.getSessionHandle(), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        sql));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(),
        "explain"));

    WebTarget target = getQueryWebTarget(client);

    QueryPlan handle = target.request().post(Entity.entity(mp,
        MediaType.MULTIPART_FORM_DATA_TYPE), QueryPlan.class);
    return handle;
  }


  public List<QueryHandle> getAllQueries(String state, String queryName, String user, long fromDate, long toDate) {
    WebTarget target = getQueryWebTarget(ClientBuilder
        .newBuilder().register(MultiPartFeature.class).build());
    List<QueryHandle> handles = target.queryParam("sessionid", connection.getSessionHandle())
      .queryParam("state", state)
      .queryParam("queryName", queryName)
      .queryParam("user", user)
      .queryParam("fromDate", fromDate)
      .queryParam("toDate", toDate)
      .request().get(
        new GenericType<List<QueryHandle>>() {
    });
    return handles;
  }

  public List<QueryPrepareHandle> getAllPreparedQueries(String userName, String queryName, long fromDate, long toDate) {
    Client client = ClientBuilder.newClient();
    WebTarget target = getPreparedQueriesWebTarget(client);
    List<QueryPrepareHandle> handles = target.queryParam("sessionid",
        connection.getSessionHandle())
      .queryParam("user", userName)
      .queryParam("queryName", queryName)
      .queryParam("fromDate", fromDate)
      .queryParam("toDate", toDate)
      .request().get(
        new GenericType<List<QueryPrepareHandle>>() {
        });
    return handles;
  }

  public QueryResultSetMetadata getResultSetMetaData() {
    return this.getResultSetMetaData(query);
  }

  public QueryResultSetMetadata getResultSetMetaData(LensQuery query) {
    if (query.getStatus().getStatus() != QueryStatus.Status.SUCCESSFUL) {
      throw new IllegalArgumentException("Result set metadata " +
          "can be only queries for successful queries");
    }
    Client client = ClientBuilder.newClient();

    try {
      WebTarget target = getQueryWebTarget(client);

      return target.path(query.getQueryHandle().toString()).
          path("resultsetmetadata").queryParam(
              "sessionid", connection.getSessionHandle()).request().get(
                  QueryResultSetMetadata.class);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get resultset metadata, cause:" + e.getMessage());
    }
  }

  public QueryResult getResultSet() {
    return this.getResultSet(this.query);
  }

  public QueryResult getResultSet(LensQuery query) {
    if (query.getStatus().getStatus() != QueryStatus.Status.SUCCESSFUL) {
      throw new IllegalArgumentException("Result set metadata " +
          "can be only queries for successful queries");
    }
    Client client = ClientBuilder.newClient();

    try {
      WebTarget target = getQueryWebTarget(client);
      return target.path(query.getQueryHandle().toString()).
          path("resultset").queryParam(
              "sessionid", connection.getSessionHandle()).request().get(
                   QueryResult.class);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get resultset, cause:" + e.getMessage());
    }
  }


  public boolean kill() {
    return this.kill(query);
  }
  public boolean kill(LensQuery query) {

    if (query.getStatus().isFinished()) {
      return false;
    }

    Client client = ClientBuilder.newClient();
    WebTarget target = getQueryWebTarget(client);

    APIResult result = target.path(query.getQueryHandle().toString()).
        queryParam("sessionid", connection.getSessionHandle()).request().
        delete(APIResult.class);

    if (result.getStatus().equals(APIResult.Status.SUCCEEDED)) {
      return true;
    } else {
      return false;
    }
  }

  public boolean closeResultSet() {
    if (!query.getStatus().isResultSetAvailable()) {
      return false;
    }
    Client client = ClientBuilder.newClient();
    WebTarget target = getQueryWebTarget(client);

    APIResult result = target.path(query.getQueryHandle().toString()).
        path("resultset").queryParam("sessionid", connection.getSessionHandle()).
        request().delete(APIResult.class);

    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return true;
    } else {
      return false;
    }
  }

  public boolean destroyPrepared(QueryPrepareHandle phandle) {
    Client client = ClientBuilder.newClient();
    WebTarget target = getPreparedQueriesWebTarget(client);

    APIResult result = target.path(phandle.toString()).
        queryParam("sessionid", connection.getSessionHandle()).
        request().delete(APIResult.class);

    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return true;
    } else {
      return false;
    }
  }

  public boolean isIdle() {
    return query == null || query.getStatus().isFinished();
  }

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

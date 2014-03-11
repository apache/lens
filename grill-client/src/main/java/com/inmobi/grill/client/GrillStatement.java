package com.inmobi.grill.client;

import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.query.*;

import com.inmobi.grill.server.api.GrillConfConstants;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

/**
 * Top level class which is used to execute grill queries.
 */
public class GrillStatement {

  private final GrillConnection connection;
  private GrillQuery query;

  public GrillStatement(GrillConnection connection) {
    this.connection = connection;
  }

  public void execute(String sql, boolean waitForQueryToComplete) {
    QueryHandle handle = executeQuery(sql, waitForQueryToComplete);
    this.query = getQuery(handle);
  }

  public void execute(String sql) {
    QueryHandle handle = executeQuery(sql, true);
    this.query = getQuery(handle);
  }

  public QueryHandle executeQuery(String sql, boolean waitForQueryToComplete) {
    QueryHandle handle = executeQuery(sql);

    if (waitForQueryToComplete) {
      waitForQueryToComplete(handle);
    }
    return handle;
  }

  private void waitForQueryToComplete(QueryHandle handle) {
    GrillQuery query = getQuery(handle);
    while (!query.getStatus().isFinished()) {
      query = getQuery(handle);
      try {
        Thread.sleep(connection.getParams().getQueryPollInterval());
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private WebTarget getQueryWebTarget(Client client) {
    return client.target(
        connection.getParams().getBaseConnectionUrl()).path(
        connection.getParams().getQueryResourcePath());
  }

  private GrillQuery getQuery(QueryHandle handle) {
    Client client = ClientBuilder.newClient();
    WebTarget target = getQueryWebTarget(client);
    return target.path(handle.toString()).queryParam(
        "sessionid", connection.getSessionHandle()).request().get(GrillQuery.class);
  }


  private QueryHandle executeQuery(String sql) {
    if (!connection.isOpen()) {
      throw new IllegalStateException("Grill Connection has to be " +
          "established before querying");
    }

    Client client = ClientBuilder.newBuilder().register(
        MultiPartFeature.class).build();
    GrillConf conf  = new GrillConf();
    conf.addProperty(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, "false");
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("sessionid").build(),
        connection.getSessionHandle(), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        sql));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        conf,
        MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(),
        "execute"));

    WebTarget target = getQueryWebTarget(client);

    QueryHandle handle = target.request().post(Entity.entity(mp,
        MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);
    return handle;
  }

  public QueryResultSetMetadata getResultSetMetaData() {
    if (query.getStatus().getStatus() != QueryStatus.Status.SUCCESSFUL) {
      throw new IllegalArgumentException("Result set metadata " +
          "can be only queries for successful queries");
    }
    Client client = ClientBuilder.newClient();

    WebTarget target = getQueryWebTarget(client);

    return target.path(query.getQueryHandle().toString()).
        path("resultsetmetadata").queryParam(
        "sessionid", connection.getSessionHandle()).request().get(
        QueryResultSetMetadata.class);
  }

  public InMemoryQueryResult getResultSet() {
    if (query.getStatus().getStatus() != QueryStatus.Status.SUCCESSFUL) {
      throw new IllegalArgumentException("Result set metadata " +
          "can be only queries for successful queries");
    }
    Client client = ClientBuilder.newClient();

    WebTarget target = getQueryWebTarget(client);
    return target.path(query.getQueryHandle().toString()).
        path("resultset").queryParam(
        "sessionid", connection.getSessionHandle()).request().get(
        InMemoryQueryResult.class);
  }


  public boolean kill() {

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

  public boolean isIdle() {
    return query == null || query.getStatus().isFinished();
  }

  public boolean wasQuerySuccessful() {
    return query.getStatus().equals(QueryStatus.Status.SUCCESSFUL);
  }

}

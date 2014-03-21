package com.inmobi.grill.client;

import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.StringList;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Top level client connection class which is used to connect to a grill server
 */
public class GrillConnection {


  private final GrillConnectionParams params;

  private AtomicBoolean open = new AtomicBoolean(false);
  private GrillSessionHandle sessionHandle;

  /**
   * Construct a connection to grill server specified by connection parameters
   *
   * @param params parameters to be used for creating a connection
   */
  public GrillConnection(GrillConnectionParams params) {
    this.params = params;
  }

  /**
   * Check if the connection is opened. Please note that,grill connections are
   * persistent connections. But a session mapped by ID running on the grill
   * server.
   *
   * @return true if connected to server
   */
  public boolean isOpen() {
    return open.get();
  }


  private WebTarget getSessionWebTarget(Client client) {
    return client.target(params.getBaseConnectionUrl()).path(
        params.getSessionResourcePath());
  }

  private WebTarget getMetastoreWebTarget(Client client) {
    return client.target(params.getBaseConnectionUrl()).path(
        params.getMetastoreResourcePath());
  }

  private WebTarget getSessionWebTarget(Class<?> featureType) {
    Client client = ClientBuilder.newBuilder().register(featureType).build();
    return getSessionWebTarget(client);
  }

  private WebTarget getSessionWebTarget() {
    Client client = ClientBuilder.newClient();
    return getSessionWebTarget(client);
  }

  private WebTarget getMetastoreWebTarget() {
    Client client = ClientBuilder.newClient();
    return getMetastoreWebTarget(client);
  }


  public GrillSessionHandle open() {

    WebTarget target = getSessionWebTarget(MultiPartFeature.class);

    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("username").build(), params.getUser()));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("password").build(), params.getPassword()));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("sessionconf").
            fileName("sessionconf").build(), params.getSessionConf(),
        MediaType.APPLICATION_XML_TYPE));


    final GrillSessionHandle sessionHandle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
        GrillSessionHandle.class);


    if (sessionHandle != null) {
      this.sessionHandle = sessionHandle;
    } else {
      throw new IllegalStateException("Unable to connect to grill " +
          "server with following paramters" + params);
    }
    APIResult result = attachDatabaseToSession();

    if (result.getStatus() != APIResult.Status.SUCCEEDED) {
      throw new IllegalStateException("Unable to connect to grill database "
          + params.getDbName());
    }

    open.set(true);

    return sessionHandle;
  }

  public APIResult attachDatabaseToSession() {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("databases").path("current").queryParam(
        "sessionid", this.sessionHandle).request(
        MediaType.APPLICATION_XML_TYPE).put(Entity.xml(params.getDbName()),
        APIResult.class);
    return result;

  }

  public GrillSessionHandle getSessionHandle() {
    return this.sessionHandle;
  }


  public APIResult close() {
    WebTarget target = getSessionWebTarget();

    APIResult result =  target.queryParam("sessionid",
        this.sessionHandle).request().delete(APIResult.class);
    if(result.getStatus() != APIResult.Status.SUCCEEDED) {
      throw new IllegalStateException("Unable to close grill connection " +
          "with params " + params);
    }
    return result;
  }


  public APIResult addResourceToConnection(String type, String resourcePath) {
    WebTarget target = getSessionWebTarget();
    FormDataMultiPart mp  = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        this.sessionHandle, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("type").build(),
        type));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("path").build(),
        resourcePath));
    APIResult result = target.path("resources/add").request().put(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    return result;
  }


  public APIResult removeResourceFromConnection(String type, String resourcePath) {
    WebTarget target = getSessionWebTarget();
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        this.sessionHandle, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("type").build(),
        type));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("path").build(),
        resourcePath));
    APIResult result = target.path("resources/delete").request().put(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    return result;
  }

  public APIResult setConnectionParams(String key, String value) {
    WebTarget target = getSessionWebTarget();
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        this.sessionHandle, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("key").build(), key));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("value").build(), value));
    APIResult result = target.path("params").request().put(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    return result;
  }


  public List<String> getConnectionParams() {
    WebTarget target = getSessionWebTarget();
    StringList list = target.queryParam("sessionid",
        this.sessionHandle).queryParam("verbose", true).request().get(StringList.class);
    return list.getElements();
  }

  GrillConnectionParams getGrillConnectionParams() {
    return this.params;
  }
}

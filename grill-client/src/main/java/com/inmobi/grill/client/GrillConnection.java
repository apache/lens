package com.inmobi.grill.client;

import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.GrillSessionHandle;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
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
   * Construct a connection to grill server hosted at specified host using default port and database, anonymously
   *
   * @param host name of host which grill server is hosted. If null is passed, default host is used.
   */
  public GrillConnection(String host) {
    this(host, -1, null, null, null);
  }

  /**
   * Construct a connection to grill server hosted at specified host and port, using default database anonymously
   *
   * @param host name of host which grill server is hosted. If null is passed, default host is used.
   * @param port which grill server is listening. If -1 is passed, default port is used
   */
  public GrillConnection(String host, int port) {
    this(host, port, null, null, null);
  }

  /**
   * Construct a connection to grill server hosted database at specified host and port, anonymously
   *
   * @param host     name of host which grill server is hosted. If null is passed, default host is used.
   * @param port     which grill server is listening. If -1 is passed, default port is used
   * @param database to which client is connected. If null is passed, default database is used.
   */

  public GrillConnection(String host, int port, String database) {
    this(host, port, database, null, null);
  }

  /**
   * Constructs a connection to grill server hosted database as specified user.
   *
   * @param host     name of host which grill server is hosted. If null is passed, default host is used.
   * @param port     which grill server is listening. If -1 is passed, default port is used
   * @param database to which client is connected. If null is passed, default database is used.
   * @param user     name of user who is accessing the database
   */
  public GrillConnection(String host, int port, String database, String user) {
    this(host, port, database, user, null);
  }

  /**
   * Constructs a connection to grill server hosted database with specified user credentials.
   *
   * @param host     name of host which grill server is hosted. If null is passed, default host is used.
   * @param port     which grill server is listening. If -1 is passed, default port is used
   * @param database to which client is connected. If null is passed, default database is used.
   * @param user     name of user who is accessing the database
   * @param password of the user.
   */
  public GrillConnection(String host, int port, String database, String user, String password) {
    this.params = new GrillConnectionParams();
    this.params.setHost(host);
    if (database != null) {
      this.params.setDbName(database);
    }
    if (port > 0) {
      this.params.setPort(port);
    }
    if (user != null && !user.isEmpty()) {
      this.params.getSessionVars().put("user.name", user);
    }
    if (password != null && !password.isEmpty()) {
      this.params.getSessionVars().put("user.pass", password);
    }
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

  private WebTarget getSessionWebTarget(Class<?> featureType) {
    Client client = ClientBuilder.newBuilder().register(featureType).build();
    return getSessionWebTarget(client);
  }

  private WebTarget getSessionWebTarget() {
    Client client = ClientBuilder.newClient();
    return getSessionWebTarget(client);
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


    if(sessionHandle!=null) {
      this.sessionHandle = sessionHandle;
    } else {
      throw new IllegalStateException("Unable to connect to grill " +
          "server with following paramters" + params);
    }
    APIResult result = attachDatabaseToSession();

    if(result.getStatus() != APIResult.Status.SUCCEEDED)  {
      throw new IllegalStateException("Unable to connect to grill database "
          + params.getDbName());
    }

    open.set(true);

    return sessionHandle;
  }

  private APIResult attachDatabaseToSession() {
    WebTarget target = getSessionWebTarget();
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        sessionHandle, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("database").build(),
        params.getDbName(), MediaType.APPLICATION_XML_TYPE));
    APIResult result = target.path("database").request().put(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
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
    FormDataMultiPart mp  = new FormDataMultiPart();
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

  GrillConnectionParams getParams() {
    return this.params;
  }
}

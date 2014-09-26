package com.inmobi.grill.client;

/*
 * #%L
 * Grill client
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


import org.apache.hadoop.conf.Configuration;

/**
 * Configuration Class which is used in the grill client
 */
public class GrillClientConfig extends Configuration {

  public GrillClientConfig() {
    super(false);
    addResource("grill-client-default.xml");
    addResource("grill-client-site.xml");
  }

  public static final String GRILL_SERVER_HOST_KEY = "grill.host";
  public static final String DEFAULT_SERVER_HOST_VALUE = "localhost";
  public static final String GRILL_SERVER_PORT_KEY = "grill.port";
  public static final int DEFAULT_SERVER_PORT = 9999;
  public static final String GRILL_DBNAME_KEY = "grill.dbname";
  public static final String DEFAULT_DBNAME_VALUE = "default";
  public static final String GRILL_BASE_PATH = "grill.base.path";
  public static final String DEFAULT_APP_BASE_PATH = "";
  public static final String GRILL_SESSION_RESOURCE_PATH = "grill.session.resource.path";
  public static final String GRILL_SESSION_CLUSTER_USER = "grill.session.cluster.user";
  public static final String DEFAULT_SESSION_RESOURCE_PATH = "session";
  private static final String GRILL_QUERY_RESOURCE_PATH = "grill.query.resource.path";
  private static final String DEFAULT_QUERY_RESOURCE_PATH = "queryapi";
  private static final String GRILL_QUERY_POLL_INTERVAL_KEY = "grill.query.poll.interval";
  private static final long DEFAULT_QUERY_POLL_INTERVAL = 10 * 1000l;
  private static final String GRILL_METASTORE_RESOURCE_PATH = "grill.metastore.resource.path";
  private static final String DEFAULT_GRILL_METASTORE_RESOURCE_PATH = "metastore";
  private static final String GRILL_USER_NAME = "grill.user.name";
  public static final String DEFAULT_USER_NAME = "anonymous";

  /**
   * Get the username from config
   *
   * @return Returns grill client user name
   */
  public String getUser() {
    return this.get(GRILL_USER_NAME, DEFAULT_USER_NAME);
  }
  public void setUser(String user) {
    this.set(GRILL_USER_NAME, user);
  }

  /**
   * Returns the configured grill server hostname
   *
   * @return hostname of grill server, defaults to localhost
   */
  public String getGrillHost() {
    return this.get(GRILL_SERVER_HOST_KEY, DEFAULT_SERVER_HOST_VALUE);
  }

  /**
   * Returns the configured grill server port
   *
   * @return port number of the grill server, defaults to 8080
   */
  public int getGrillPort() {
    return this.getInt(GRILL_SERVER_PORT_KEY, DEFAULT_SERVER_PORT);
  }

  /**
   * Returns the configured grill server database client wants to access
   *
   * @return database returns database to connect, defaults to 'default'
   */
  public String getGrillDatabase() {
    return this.get(GRILL_DBNAME_KEY, DEFAULT_DBNAME_VALUE);
  }

  /**
   * Returns the web app path which grill server is deployed to.
   *
   * @return web app fragment, default to root path.
   */
  public String getAppBasePath() {
    return this.get(GRILL_BASE_PATH, DEFAULT_APP_BASE_PATH);
  }

  /**
   * Returns the session service path on grill server
   * @return web app fragment pointing to session service, defaults to session
   */
  public String getSessionResourcePath() {
    return this.get(GRILL_SESSION_RESOURCE_PATH,
        DEFAULT_SESSION_RESOURCE_PATH);
  }

  /**
   * Returns the query service path on grill server
   * @return web app fragment pointing to query service, defaults to queryapi
   */
  public String getQueryResourcePath() {
    return this.get(GRILL_QUERY_RESOURCE_PATH,
        DEFAULT_QUERY_RESOURCE_PATH);
  }

  /**
   * Set the configured girll server hostname
   *
   * @param host name of host where grill server is located
   */
  public void setGrillHost(String host) {
    this.set(GRILL_SERVER_HOST_KEY, host);
  }

  /**
   * Sets the configured grill server port.
   *
   * @param port where grill server is listening for requests
   */
  public void setGrillPort(int port) {
    this.setInt(GRILL_SERVER_PORT_KEY, port);
  }

  /**
   * Sets the database to connect on grill server
   * @param dbName database to connect to
   */
  public void setGrillDatabase(String dbName) {
    this.set(GRILL_DBNAME_KEY, dbName);
  }

  public void setGrillBasePath(String basePath) {
    this.set(GRILL_BASE_PATH, basePath);
  }

  public long getQueryPollInterval() {
    return this.getLong(GRILL_QUERY_POLL_INTERVAL_KEY, DEFAULT_QUERY_POLL_INTERVAL);
  }

  public String getMetastoreResourcePath() {
    return this.get(GRILL_METASTORE_RESOURCE_PATH, DEFAULT_GRILL_METASTORE_RESOURCE_PATH);
  }
}

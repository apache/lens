package org.apache.lens.client;

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
    addResource("lens-client-default.xml");
    addResource("lens-client-site.xml");
  }

  // config prefixes
  // All the config variables will use one of these prefixes
  public static final String CLIENT_PFX = "lens.client.";

  public static final String DBNAME_KEY = CLIENT_PFX + "dbname";
  public static final String DEFAULT_DBNAME_VALUE = "default";
  private static final String QUERY_POLL_INTERVAL_KEY = CLIENT_PFX + "query.poll.interval";
  private static final long DEFAULT_QUERY_POLL_INTERVAL = 10 * 1000l;
  private static final String USER_NAME = CLIENT_PFX + "user.name";
  public static final String DEFAULT_USER_NAME = "anonymous";
  private static final String DEFAULT_METASTORE_RESOURCE_PATH = "metastore";
  private static final String DEFAULT_QUERY_RESOURCE_PATH = "queryapi";
  public static final String DEFAULT_SESSION_RESOURCE_PATH = "session";

  // server side conf properties copied here
  public static final String SERVER_BASE_URL = "lens.server.base.url";
  public static final String DEFAULT_SERVER_BASE_URL = "http://0.0.0.0:9999/";
  public static final String SESSION_CLUSTER_USER = "lens.session.cluster.user";

  /**
   * Get the username from config
   *
   * @return Returns grill client user name
   */
  public String getUser() {
    return this.get(USER_NAME, DEFAULT_USER_NAME);
  }
  public void setUser(String user) {
    this.set(USER_NAME, user);
  }

  /**
   * Returns the configured grill server url
   *
   * @return server url
   */
  public String getBaseURL() {
    return this.get(SERVER_BASE_URL, DEFAULT_SERVER_BASE_URL);
  }

  /**
   * Returns the configured grill server database client wants to access
   *
   * @return database returns database to connect, defaults to 'default'
   */
  public String getGrillDatabase() {
    return this.get(DBNAME_KEY, DEFAULT_DBNAME_VALUE);
  }

  /**
   * Returns the session service path on grill server
   * @return web app fragment pointing to session service, defaults to session
   */
  public String getSessionResourcePath() {
    return DEFAULT_SESSION_RESOURCE_PATH;
  }

  /**
   * Returns the query service path on grill server
   * @return web app fragment pointing to query service, defaults to queryapi
   */
  public String getQueryResourcePath() {
    return DEFAULT_QUERY_RESOURCE_PATH;
  }

  /**
   * Sets the database to connect on grill server
   * @param dbName database to connect to
   */
  public void setGrillDatabase(String dbName) {
    this.set(DBNAME_KEY, dbName);
  }

  public long getQueryPollInterval() {
    return this.getLong(QUERY_POLL_INTERVAL_KEY, DEFAULT_QUERY_POLL_INTERVAL);
  }

  public String getMetastoreResourcePath() {
    return DEFAULT_METASTORE_RESOURCE_PATH;
  }

  public void setBaseUrl(String baseUrl) {
    this.set(SERVER_BASE_URL, baseUrl);
  }
}

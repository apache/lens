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

import org.apache.hadoop.conf.Configuration;

/**
 * Configuration Class which is used in the lens client.
 */
public class LensClientConfig extends Configuration {

  /**
   * Instantiates a new lens client config.
   */
  public LensClientConfig() {
    super(false);
    addResource("lens-client-default.xml");
    addResource("lens-client-site.xml");
  }

  // config prefixes
  // All the config variables will use one of these prefixes
  /** The Constant CLIENT_PFX. */
  public static final String CLIENT_PFX = "lens.client.";

  /** The Constant DBNAME_KEY. */
  public static final String DBNAME_KEY = CLIENT_PFX + "dbname";

  /** The Constant DEFAULT_DBNAME_VALUE. */
  public static final String DEFAULT_DBNAME_VALUE = "default";

  /** The Constant QUERY_POLL_INTERVAL_KEY. */
  private static final String QUERY_POLL_INTERVAL_KEY = CLIENT_PFX + "query.poll.interval";

  /** The Constant DEFAULT_QUERY_POLL_INTERVAL. */
  private static final long DEFAULT_QUERY_POLL_INTERVAL = 10L;

  /** The Constant USER_NAME. */
  private static final String USER_NAME = CLIENT_PFX + "user.name";

  /** The Constant DEFAULT_USER_NAME. */
  public static final String DEFAULT_USER_NAME = "anonymous";

  /** The Constant DEFAULT_METASTORE_RESOURCE_PATH. */
  private static final String DEFAULT_METASTORE_RESOURCE_PATH = "metastore";

  /** The Constant DEFAULT_QUERY_RESOURCE_PATH. */
  private static final String DEFAULT_QUERY_RESOURCE_PATH = "queryapi";

  /** The Constant DEFAULT_SESSION_RESOURCE_PATH. */
  public static final String DEFAULT_SESSION_RESOURCE_PATH = "session";

  public static final String DEFAULT_LOG_RESOURCE_PATH = "logs";

  // server side conf properties copied here
  /** The Constant SERVER_BASE_URL. */
  public static final String SERVER_BASE_URL = "lens.server.base.url";

  /** The Constant DEFAULT_SERVER_BASE_URL. */
  public static final String DEFAULT_SERVER_BASE_URL = "http://0.0.0.0:9999/lensapi";

  /** The Constant SESSION_CLUSTER_USER. */
  public static final String SESSION_CLUSTER_USER = "lens.session.cluster.user";

  public static final String SESSION_FILTER_NAMES = CLIENT_PFX + "ws.request.filternames";

  public static final String WS_FILTER_IMPL_SFX = ".ws.filter.impl";

  public static final String READ_TIMEOUT_MILLIS = CLIENT_PFX + "read.timeout.millis";

  public static final int DEFAULT_READ_TIMEOUT_MILLIS = 300000; //5 mins

  public static final String CONNECTION_TIMEOUT_MILLIS = CLIENT_PFX + "connection.timeout.millis";

  public static final int DEFAULT_CONNECTION_TIMEOUT_MILLIS = 60000; //60 secs

  /**
   * Get the username from config
   *
   * @return Returns lens client user name
   */
  public String getUser() {
    return this.get(USER_NAME, DEFAULT_USER_NAME);
  }

  public void setUser(String user) {
    this.set(USER_NAME, user);
  }

  /**
   * Returns the configured lens server url
   *
   * @return server url
   */
  public String getBaseURL() {
    return this.get(SERVER_BASE_URL, DEFAULT_SERVER_BASE_URL);
  }

  /**
   * Returns the configured lens server database client wants to access
   *
   * @return database returns database to connect, defaults to 'default'
   */
  public String getLensDatabase() {
    return this.get(DBNAME_KEY, DEFAULT_DBNAME_VALUE);
  }

  /**
   * Returns the session service path on lens server
   *
   * @return web app fragment pointing to session service, defaults to session
   */
  public String getSessionResourcePath() {
    return DEFAULT_SESSION_RESOURCE_PATH;
  }

  /**
   * Returns the query service path on lens server
   *
   * @return web app fragment pointing to query service, defaults to queryapi
   */
  public String getQueryResourcePath() {
    return DEFAULT_QUERY_RESOURCE_PATH;
  }

  /**
   * Sets the database to connect on lens server
   *
   * @param dbName database to connect to
   */
  public void setLensDatabase(String dbName) {
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

  public String getLogResourcePath() {
    return DEFAULT_LOG_RESOURCE_PATH;
  }

  public static String getWSFilterImplConfKey(String filterName) {
    return CLIENT_PFX + filterName + WS_FILTER_IMPL_SFX;
  }
}

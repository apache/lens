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
package org.apache.lens.driver.jdbc;

import lombok.Getter;

/**
 * The Interface JDBCDriverConfConstants.
 */
public final class JDBCDriverConfConstants {
  private JDBCDriverConfConstants() {
  }

  /** The Constant JDBC_DRIVER_PFX. */
  public static final String JDBC_DRIVER_PFX = "lens.driver.jdbc.";

  /** The Constant JDBC_CONNECTION_PROPERTIES. */
  public static final String JDBC_CONNECTION_PROPERTIES = JDBC_DRIVER_PFX + "connection.properties";

  /** The Constant JDBC_CONNECTION_PROVIDER. */
  public static final String JDBC_CONNECTION_PROVIDER = JDBC_DRIVER_PFX + "connection.provider";

  /** The Constant JDBC_QUERY_REWRITER_CLASS. */
  public static final String JDBC_QUERY_REWRITER_CLASS = JDBC_DRIVER_PFX + "query.rewriter";
  public static final String JDBC_QUERY_HOOK_CLASS = JDBC_DRIVER_PFX + "query.hook.class";

  /** The Constant JDBC_DRIVER_CLASS. */
  public static final String JDBC_DRIVER_CLASS = JDBC_DRIVER_PFX + "driver.class";

  /** The Constant JDBC_DB_URI. */
  public static final String JDBC_DB_URI = JDBC_DRIVER_PFX + "db.uri";

  /** The Constant JDBC_USER. */
  public static final String JDBC_USER = JDBC_DRIVER_PFX + "db.user";

  /** The Constant JDBC_PASSWORD. */
  public static final String JDBC_PASSWORD = JDBC_DRIVER_PFX + "db.password";

  public enum ConnectionPoolProperties {
    /** The Constant JDBC_POOL_MAX_SIZE_DEFAULT. */
    JDBC_POOL_MAX_SIZE("maxPoolSize", JDBC_DRIVER_PFX + "pool.max.size", 15),
    /** The Constant JDBC_POOL_IDLE_TIME. */
    JDBC_POOL_IDLE_TIME("maxIdleTime", JDBC_DRIVER_PFX + "pool.idle.time", 600),
    /** JDBC_MAX_IDLE_TIME_EXCESS_CONNECTIONS  */
    JDBC_MAX_IDLE_TIME_EXCESS_CONNECTIONS("maxIdleTimeExcessConnections", JDBC_DRIVER_PFX
      + "max.idle.time.excess.connections", 600),
    /** The Constant JDBC_MAX_STATEMENTS_PER_CONNECTION. */
    JDBC_MAX_STATEMENTS_PER_CONNECTION("maxStatementsPerConnection", JDBC_DRIVER_PFX + "pool.max.statements", 20),
    /** The Constant JDBC_GET_CONNECTION_TIMEOUT. */
    JDBC_GET_CONNECTION_TIMEOUT("checkoutTimeout", JDBC_DRIVER_PFX + "get.connection.timeout", 10000);

    @Getter
    private final String poolProperty;
    @Getter
    private final String configKey;
    @Getter
    private final int defaultValue;

    ConnectionPoolProperties(String poolProperty, String configKey, int defaultValue) {
      this.poolProperty = poolProperty;
      this.configKey = configKey;
      this.defaultValue = defaultValue;
    }
  }

  /** The Constant JDBC_EXPLAIN_KEYWORD_PARAM. */
  public static final String JDBC_EXPLAIN_KEYWORD_PARAM = JDBC_DRIVER_PFX + "explain.keyword";

  /** The Constant DEFAULT_JDBC_EXPLAIN_KEYWORD. */
  public static final String DEFAULT_JDBC_EXPLAIN_KEYWORD = "explain ";

  /** The Constant JDBC_EXPLAIN_KEYWORD_BEFORE_SELECT. */
  public static final String JDBC_EXPLAIN_KEYWORD_BEFORE_SELECT = JDBC_DRIVER_PFX + "explain.before.select";

  /** The Constant DEFAULT_JDBC_EXPLAIN_KEYWORD_BEFORE_SELECT. */
  public static final boolean DEFAULT_JDBC_EXPLAIN_KEYWORD_BEFORE_SELECT = true;

  /** The Constant JDBC_VALIDATE_THROUGH_PREPARE. */
  public static final String JDBC_VALIDATE_THROUGH_PREPARE = JDBC_DRIVER_PFX + "validate.through.prepare";

  /** The Constant DEFAULT_JDBC_VALIDATE_THROUGH_PREPARE. */
  public static final boolean DEFAULT_JDBC_VALIDATE_THROUGH_PREPARE = true;

  public static final String JDBC_ENABLE_RESULTSET_STREAMING_RETRIEVAL =
    JDBC_DRIVER_PFX + "enable.resultset.streaming.retrieval";
  public static final boolean DEFAULT_JDBC_ENABLE_RESULTSET_STREAMING_RETRIEVAL = false;

  public static final String JDBC_FETCH_SIZE = JDBC_DRIVER_PFX + "fetch.size";
  public static final int DEFAULT_JDBC_FETCH_SIZE = 1000;

  public static final String QUERY_LAUNCHING_CONSTRAINT_FACTORIES_KEY = JDBC_DRIVER_PFX
    + "query.launching.constraint.factories";

  public static final String WAITING_QUERIES_SELECTION_POLICY_FACTORIES_KEY = JDBC_DRIVER_PFX
    + "waiting.queries.selection.policy.factories";
  public static final String REGEX_REPLACEMENT_VALUES = JDBC_DRIVER_PFX + "regex.replacement.values";
}

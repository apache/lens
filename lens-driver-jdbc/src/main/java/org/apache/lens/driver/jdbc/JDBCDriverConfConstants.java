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

/**
 * The Interface JDBCDriverConfConstants.
 */
public interface JDBCDriverConfConstants {

  /** The Constant JDBC_DRIVER_PFX. */
  public static final String JDBC_DRIVER_PFX = "lens.driver.jdbc.";

  /** The Constant JDBC_CONNECTION_PROVIDER. */
  public static final String JDBC_CONNECTION_PROVIDER = JDBC_DRIVER_PFX + "connection.provider";

  /** The Constant JDBC_QUERY_REWRITER_CLASS. */
  public static final String JDBC_QUERY_REWRITER_CLASS = JDBC_DRIVER_PFX + "query.rewriter";

  /** The Constant JDBC_DRIVER_CLASS. */
  public static final String JDBC_DRIVER_CLASS = JDBC_DRIVER_PFX + "driver.class";

  /** The Constant JDBC_DB_URI. */
  public static final String JDBC_DB_URI = JDBC_DRIVER_PFX + "db.uri";

  /** The Constant JDBC_USER. */
  public static final String JDBC_USER = JDBC_DRIVER_PFX + "db.user";

  /** The Constant JDBC_PASSWORD. */
  public static final String JDBC_PASSWORD = JDBC_DRIVER_PFX + "db.password";

  /** The Constant JDBC_POOL_MAX_SIZE. */
  public static final String JDBC_POOL_MAX_SIZE = JDBC_DRIVER_PFX + "pool.max.size";

  /** The Constant JDBC_POOL_MAX_SIZE_DEFAULT. */
  public static final int JDBC_POOL_MAX_SIZE_DEFAULT = 15;

  /** The Constant JDBC_POOL_IDLE_TIME. */
  public static final String JDBC_POOL_IDLE_TIME = JDBC_DRIVER_PFX + "pool.idle.time";

  /** The Constant JDBC_POOL_IDLE_TIME_DEFAULT. */
  public static final int JDBC_POOL_IDLE_TIME_DEFAULT = 600;

  /** The Constant JDBC_MAX_STATEMENTS_PER_CONNECTION. */
  public static final String JDBC_MAX_STATEMENTS_PER_CONNECTION = JDBC_DRIVER_PFX + "pool.max.statements";

  /** The Constant JDBC_MAX_STATEMENTS_PER_CONNECTION_DEFAULT. */
  public static final int JDBC_MAX_STATEMENTS_PER_CONNECTION_DEFAULT = 20;

  /** The Constant JDBC_GET_CONNECTION_TIMEOUT. */
  public static final String JDBC_GET_CONNECTION_TIMEOUT = JDBC_DRIVER_PFX + "get.connection.timeout";

  /** The Constant JDBC_GET_CONNECTION_TIMEOUT_DEFAULT. */
  public static final int JDBC_GET_CONNECTION_TIMEOUT_DEFAULT = 10000;

  /** The Constant JDBC_EXPLAIN_KEYWORD_PARAM. */
  public static final String JDBC_EXPLAIN_KEYWORD_PARAM = JDBC_DRIVER_PFX + "explain.keyword";

  /** The Constant DEFAULT_JDBC_EXPLAIN_KEYWORD. */
  public static final String DEFAULT_JDBC_EXPLAIN_KEYWORD = "explain ";
}

package org.apache.lens.driver.jdbc;

/*
 * #%L
 * Grill Driver for JDBC
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


public interface JDBCDriverConfConstants {
  public static final String JDBC_DRIVER_PFX = "lens.driver.jdbc.";
  public static final String JDBC_CONNECTION_PROVIDER = JDBC_DRIVER_PFX + "connection.provider";
  public static final String JDBC_QUERY_REWRITER_CLASS = JDBC_DRIVER_PFX + "query.rewriter";
  public static final String JDBC_DRIVER_CLASS = JDBC_DRIVER_PFX + "driver.class";
  public static final String JDBC_DB_URI = JDBC_DRIVER_PFX + "db.uri";
  public static final String JDBC_USER = JDBC_DRIVER_PFX + "db.user";
  public static final String JDBC_PASSWORD = JDBC_DRIVER_PFX + "db.password";
  public static final String JDBC_POOL_MAX_SIZE = JDBC_DRIVER_PFX + "pool.max.size";
  public static final int JDBC_POOL_MAX_SIZE_DEFAULT = 15;

  public static final String JDBC_POOL_IDLE_TIME = JDBC_DRIVER_PFX + "pool.idle.time";
  public static final int JDBC_POOL_IDLE_TIME_DEFAULT = 600;
  public static final String JDBC_MAX_STATEMENTS_PER_CONNECTION = JDBC_DRIVER_PFX + 
      "pool.max.statements";
  public static final int JDBC_MAX_STATEMENTS_PER_CONNECTION_DEFAULT = 20;

  public static final String JDBC_GET_CONNECTION_TIMEOUT = JDBC_DRIVER_PFX + "get.connection.timeout";
  public static final int JDBC_GET_CONNECTION_TIMEOUT_DEFAULT = 10000;
  public static final String JDBC_EXPLAIN_KEYWORD_PARAM = JDBC_DRIVER_PFX + "explain.keyword";
  public static final String DEFAULT_JDBC_EXPLAIN_KEYWORD = "explain ";
}

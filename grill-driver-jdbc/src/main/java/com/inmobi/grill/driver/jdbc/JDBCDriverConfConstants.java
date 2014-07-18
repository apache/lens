package com.inmobi.grill.driver.jdbc;

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
  public static final String JDBC_CONNECTION_PROVIDER = "grill.driver.jdbc.connection.provider";
  public static final String JDBC_QUERY_REWRITER_CLASS = "grill.driver.jdbc.query.rewriter";
  public static final String JDBC_DRIVER_CLASS = "grill.driver.jdbc.driver.class";
  public static final String JDBC_DB_URI = "grill.driver.jdbc.db.uri";
  public static final String JDBC_USER = "grill.driver.jdbc.db.user";
  public static final String JDBC_PASSWORD = "grill.driver.jdbc.db.password";
  public static final String JDBC_POOL_MAX_SIZE = "grill.driver.jdbc.pool.max.size";
  public static final int JDBC_POOL_MAX_SIZE_DEFAULT = 15;

  public static final String JDBC_POOL_IDLE_TIME = "grill.driver.jdbc.pool.idle.time";
  public static final int JDBC_POOL_IDLE_TIME_DEFAULT = 600;
  public static final String JDBC_MAX_STATEMENTS_PER_CONNECTION = 
      "grill.driver.jdbc.pool.max.statements";
  public static final int JDBC_MAX_STATEMENTS_PER_CONNECTION_DEFAULT = 20;

  public static final String JDBC_GET_CONNECTION_TIMEOUT = "grill.driver.jdbc.get.connection.timeout";
  public static final int JDBC_GET_CONNECTION_TIMEOUT_DEFAULT = 10000;
}

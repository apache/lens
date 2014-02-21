package com.inmobi.grill.driver.jdbc;


public interface JDBCDriverConfConstants {
  public static final String JDBC_CONNECTION_PROVIDER = "grill.driver.jdbc.connection.provider";
  public static final String JDBC_QUERY_REWRITER_CLASS = "grill.driver.jdbc.query.rewriter";
  public static final String JDBC_DRIVER_CLASS = "grill.driver.jdbc.driver.class";
  public static final String JDBC_DB_URI = "grill.driver.jdbc.db.uri";
  public static final String JDBC_USER = "grill.driver.jdbc.db.user";
  public static final String JDBC_PASSWORD = "grill.driver.jdbc.db.password";
  public static final String JDBC_POOL_MAX_SIZE = "grill.driver.jdbc.pool.max.size";
  public static final String JDBC_POOL_IDLE_TIME = "grill.driver.jdbc.pool.idle.time";
  public static final String JDBC_MAX_STATEMENTS_PER_CONNECTION = 
      "grill.driver.jdbc.pool.max.statements";
}

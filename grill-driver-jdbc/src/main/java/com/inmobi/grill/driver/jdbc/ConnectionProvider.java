package com.inmobi.grill.driver.jdbc;


import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionProvider extends Closeable {
  public Connection getConnection(Configuration conf) throws SQLException;
}

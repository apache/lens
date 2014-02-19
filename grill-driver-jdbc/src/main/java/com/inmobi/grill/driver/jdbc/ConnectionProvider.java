package com.inmobi.grill.driver.jdbc;


import org.apache.hadoop.conf.Configuration;

import java.sql.Connection;

public interface ConnectionProvider {
  public Connection getConnection(Configuration conf);
}

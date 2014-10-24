package org.apache.lens.driver.jdbc;

import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * The Interface ConnectionProvider.
 */
public interface ConnectionProvider extends Closeable {

  /**
   * Gets the connection.
   *
   * @param conf
   *          the conf
   * @return the connection
   * @throws SQLException
   *           the SQL exception
   */
  public Connection getConnection(Configuration conf) throws SQLException;
}

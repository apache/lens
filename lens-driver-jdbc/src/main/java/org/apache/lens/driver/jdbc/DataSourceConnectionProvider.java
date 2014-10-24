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

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * The Class DataSourceConnectionProvider.
 */
public class DataSourceConnectionProvider implements ConnectionProvider {

  /** The Constant LOG. */
  public static final Logger LOG = Logger.getLogger(DataSourceConnectionProvider.class);

  /** The data source map. */
  private Map<DriverConfig, ComboPooledDataSource> dataSourceMap;

  /**
   * Instantiates a new data source connection provider.
   */
  public DataSourceConnectionProvider() {
    dataSourceMap = new HashMap<DriverConfig, ComboPooledDataSource>();
  }

  /**
   * Gets the driver configfrom conf.
   *
   * @param conf
   *          the conf
   * @return the driver configfrom conf
   */
  public DriverConfig getDriverConfigfromConf(Configuration conf) {
    return new DriverConfig(conf.get(JDBCDriverConfConstants.JDBC_DRIVER_CLASS),
        conf.get(JDBCDriverConfConstants.JDBC_DB_URI), conf.get(JDBCDriverConfConstants.JDBC_USER),
        conf.get(JDBCDriverConfConstants.JDBC_PASSWORD));
  }

  /**
   * The Class DriverConfig.
   */
  protected class DriverConfig {

    /** The driver class. */
    final String driverClass;

    /** The jdbc uri. */
    final String jdbcURI;

    /** The user. */
    final String user;

    /** The password. */
    final String password;

    /** The has hash code. */
    boolean hasHashCode = false;

    /** The hash code. */
    int hashCode;

    /**
     * Instantiates a new driver config.
     *
     * @param driverClass
     *          the driver class
     * @param jdbcURI
     *          the jdbc uri
     * @param user
     *          the user
     * @param password
     *          the password
     */
    public DriverConfig(String driverClass, String jdbcURI, String user, String password) {
      this.driverClass = driverClass;
      this.jdbcURI = jdbcURI;
      this.user = user;
      this.password = password;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof DriverConfig)) {
        return false;
      }

      DriverConfig other = (DriverConfig) obj;
      // Handling equals in a proper manner as the fields in the current class
      // can be null
      return new EqualsBuilder().append(this.driverClass, other.driverClass).append(this.jdbcURI, other.jdbcURI)
          .append(this.user, other.user).append(this.password, other.password).isEquals();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
      if (!hasHashCode) {
        // Handling the hashcode in proper manner as the fields in the current
        // class can be null
        hashCode = new HashCodeBuilder().append(this.driverClass).append(jdbcURI).append(this.user)
            .append(this.password).toHashCode();
        hasHashCode = true;
      }
      return hashCode;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder("jdbcDriverClass: ").append(driverClass).append(", uri: ")
          .append(jdbcURI).append(", user: ").append(user);
      return builder.toString();
    }

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.driver.jdbc.ConnectionProvider#getConnection(org.apache.hadoop.conf.Configuration)
   */
  @Override
  public synchronized Connection getConnection(Configuration conf) throws SQLException {
    DriverConfig config = getDriverConfigfromConf(conf);
    if (!dataSourceMap.containsKey(config)) {
      ComboPooledDataSource cpds = new ComboPooledDataSource();
      try {
        cpds.setDriverClass(config.driverClass);
      } catch (PropertyVetoException e) {
        throw new IllegalArgumentException("Unable to set driver class:" + config.driverClass, e);
      }
      cpds.setJdbcUrl(config.jdbcURI);
      cpds.setUser(config.user);
      cpds.setPassword(config.password);

      // Maximum number of connections allowed in the pool
      cpds.setMaxPoolSize(conf.getInt(JDBCDriverConfConstants.JDBC_POOL_MAX_SIZE,
          JDBCDriverConfConstants.JDBC_POOL_MAX_SIZE_DEFAULT));
      // Max idle time before a connection is closed
      cpds.setMaxIdleTime(conf.getInt(JDBCDriverConfConstants.JDBC_POOL_IDLE_TIME,
          JDBCDriverConfConstants.JDBC_POOL_IDLE_TIME_DEFAULT));
      // Max idle time before connection is closed if
      // number of connections is > min pool size (default = 3)
      cpds.setMaxIdleTimeExcessConnections(conf.getInt(JDBCDriverConfConstants.JDBC_POOL_IDLE_TIME,
          JDBCDriverConfConstants.JDBC_POOL_IDLE_TIME_DEFAULT));
      // Maximum number of prepared statements to cache per connection
      cpds.setMaxStatementsPerConnection(conf.getInt(JDBCDriverConfConstants.JDBC_MAX_STATEMENTS_PER_CONNECTION,
          JDBCDriverConfConstants.JDBC_MAX_STATEMENTS_PER_CONNECTION_DEFAULT));

      // How many milliseconds should a caller wait when trying to get a connection
      // If the timeout expires, SQLException will be thrown
      cpds.setCheckoutTimeout(conf.getInt(JDBCDriverConfConstants.JDBC_GET_CONNECTION_TIMEOUT,
          JDBCDriverConfConstants.JDBC_GET_CONNECTION_TIMEOUT_DEFAULT));
      dataSourceMap.put(config, cpds);
      LOG.info("Created new datasource for config: " + config);
    }
    return dataSourceMap.get(config).getConnection();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException {
    for (Map.Entry<DriverConfig, ComboPooledDataSource> entry : dataSourceMap.entrySet()) {
      entry.getValue().close();
      LOG.info("Closed datasource: " + entry.getKey());
    }
    dataSourceMap.clear();
    LOG.info("Closed datasource connection provider");
  }

}

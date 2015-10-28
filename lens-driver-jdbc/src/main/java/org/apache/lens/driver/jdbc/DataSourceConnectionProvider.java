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

import static org.apache.lens.driver.jdbc.JDBCDriverConfConstants.*;
import static org.apache.lens.driver.jdbc.JDBCDriverConfConstants.ConnectionPoolProperties.*;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.lens.api.util.CommonUtils;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configuration;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import lombok.extern.slf4j.Slf4j;

/**
 * The Class DataSourceConnectionProvider.
 */
@Slf4j
public class DataSourceConnectionProvider implements ConnectionProvider {

  /** The data source map. */
  private Map<DriverConfig, ComboPooledDataSource> dataSourceMap;

  /**
   * Instantiates a new data source connection provider.
   */
  public DataSourceConnectionProvider() {
    dataSourceMap = new HashMap<>();
  }

  /**
   * Gets the driver configfrom conf.
   *
   * @param conf the conf
   * @return the driver configfrom conf
   */
  public DriverConfig getDriverConfigfromConf(Configuration conf) {
    return new DriverConfig(conf);
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

    final Properties properties;

    /** The has hash code. */
    boolean hasHashCode = false;

    /** The hash code. */
    int hashCode;

    /**
     * Instantiates a new driver config.
     *
     * @param conf the configuration
     */
    public DriverConfig(Configuration conf) {
      this.driverClass = conf.get(JDBC_DRIVER_CLASS);
      this.jdbcURI = conf.get(JDBC_DB_URI);
      properties = new Properties();
      properties.putAll(CommonUtils.parseMapFromString(conf.get(JDBC_CONNECTION_PROPERTIES)));
      if (conf.get(JDBC_USER) != null) {
        properties.setProperty("user", conf.get(JDBC_USER));
      }
      if (conf.get(JDBC_PASSWORD) != null) {
        properties.setProperty("password", conf.get(JDBC_PASSWORD));
      }
      this.user = properties.getProperty("user");
      this.password = properties.getProperty("password");
      // Maximum number of connections allowed in the pool
      setConnectionPoolProperties(properties, conf);
    }

    private void setConnectionPoolProperties(Properties properties, Configuration conf) {
      for (JDBCDriverConfConstants.ConnectionPoolProperties property : JDBCDriverConfConstants
        .ConnectionPoolProperties.values()) {
        if (conf.get(property.getConfigKey()) != null) {
          properties.put(property.getPoolProperty(), conf.get(property.getConfigKey()));
        } else if (!properties.containsKey(property.getPoolProperty())) {
          properties.put(property.getPoolProperty(), Integer.toString(property.getDefaultValue()));
        }
      }
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
      return "jdbcDriverClass: " + driverClass + ", uri: " + jdbcURI + ", user: " + user;
    }

    public String getProperty(String key) {
      return properties.getProperty(key);
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
      cpds.setProperties(config.properties);

      cpds.setMaxPoolSize(Integer.parseInt(config.getProperty(JDBC_POOL_MAX_SIZE.getPoolProperty())));
      cpds.setMaxIdleTime(Integer.parseInt(config.getProperty(JDBC_POOL_IDLE_TIME.getPoolProperty())));
      cpds.setMaxIdleTimeExcessConnections(Integer.parseInt(config.getProperty(JDBC_MAX_IDLE_TIME_EXCESS_CONNECTIONS
        .getPoolProperty())));
      cpds.setMaxStatementsPerConnection(Integer.parseInt(config.getProperty(JDBC_MAX_STATEMENTS_PER_CONNECTION
        .getPoolProperty())));
      cpds.setCheckoutTimeout(Integer.parseInt(config.getProperty(JDBC_GET_CONNECTION_TIMEOUT.getPoolProperty())));
      dataSourceMap.put(config, cpds);
      log.info("Created new datasource for config: {}", config);
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
      log.info("Closed datasource: {}", entry.getKey());
    }
    dataSourceMap.clear();
    log.info("Closed datasource connection provider");
  }

  protected final ComboPooledDataSource getDataSource(Configuration conf) {
    return dataSourceMap.get(getDriverConfigfromConf(conf));
  }

}

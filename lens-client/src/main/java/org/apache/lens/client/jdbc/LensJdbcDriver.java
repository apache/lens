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
package org.apache.lens.client.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import lombok.extern.slf4j.Slf4j;

/**
 * Top level JDBC driver for Lens.
 */
@Slf4j
public class LensJdbcDriver implements Driver {


  static {
    try {
      DriverManager.registerDriver(new LensJdbcDriver());
    } catch (SQLException e) {
      log.error("Error in registering jdbc driver", e);
    }
  }

  /** Is the JDBC driver fully JDBC Compliant. */
  private static final boolean JDBC_COMPLIANT = false;

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Driver#connect(java.lang.String, java.util.Properties)
   */
  @Override
  public Connection connect(String s, Properties properties) throws SQLException {
    return new LensJdbcConnection(s, properties);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Driver#acceptsURL(java.lang.String)
   */
  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return Pattern.matches(JDBCUtils.URL_PREFIX + ".*", url);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Driver#getPropertyInfo(java.lang.String, java.util.Properties)
   */
  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    if (info == null) {
      info = new Properties();
    }
    if ((url != null) && url.startsWith(JDBCUtils.URL_PREFIX)) {
      info = JDBCUtils.parseUrlForPropertyInfo(url, info);
    }
    DriverPropertyInfo hostProp = new DriverPropertyInfo(JDBCUtils.HOST_PROPERTY_KEY,
      info.getProperty(JDBCUtils.HOST_PROPERTY_KEY));
    hostProp.required = false;
    hostProp.description = "Hostname of Lens Server. Defaults to localhost";

    DriverPropertyInfo portProp = new DriverPropertyInfo(JDBCUtils.PORT_PROPERTY_KEY,
      info.getProperty(JDBCUtils.PORT_PROPERTY_KEY));
    portProp.required = false;
    portProp.description = "Portnumber where lens server runs. " + "Defaults to 8080";

    DriverPropertyInfo dbProp = new DriverPropertyInfo(JDBCUtils.DB_PROPERTY_KEY,
      info.getProperty(JDBCUtils.DB_PROPERTY_KEY));
    dbProp.required = false;
    dbProp.description = "Database to connect to on lens server. " + "Defaults to 'default'";

    return new DriverPropertyInfo[]{hostProp, portProp, dbProp};
  }

  @Override
  public int getMajorVersion() {
    return JDBCUtils.getVersion(0);
  }

  @Override
  public int getMinorVersion() {
    return JDBCUtils.getVersion(1);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Driver#jdbcCompliant()
   */
  @Override
  public boolean jdbcCompliant() {
    return JDBC_COMPLIANT;
  }

  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException("Method not supported");
  }
}

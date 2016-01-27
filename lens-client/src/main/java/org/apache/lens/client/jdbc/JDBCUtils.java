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

import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URL;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.core.UriBuilder;

import org.apache.lens.client.LensClientConfig;
import org.apache.lens.client.LensConnectionParams;

/**
 * The Class JDBCUtils.
 */
public final class JDBCUtils {
  private JDBCUtils() {

  }
  /** Property key for the Database name. */
  static final String DB_PROPERTY_KEY = "DBNAME";
  /**
   * Property key for host on which lens server is running.
   */
  static final String HOST_PROPERTY_KEY = "HOST";
  /**
   * Property key for port on which lens server is running.
   */
  static final String PORT_PROPERTY_KEY = "PORT";

  /** The Constant URL_PREFIX. */
  public static final String URL_PREFIX = "jdbc:lens://";

  /** The Constant URI_JDBC_PREFIX. */
  private static final String URI_JDBC_PREFIX = "jdbc:";

  /** The Constant KEY_VALUE_REGEX. */
  private static final String KEY_VALUE_REGEX = "([^;]*)=([^;]*)[;]?";

  /**
   * Parses the JDBC Connection URL.
   * <p></p>
   * The URL format is specified as following :
   * <ul>
   * <li>hostname which contains hosts on which lens server is hosted parsed from authority section</li>
   * <li>port number if specificed in authority part.</li>
   * <li>db name from path</li>
   * <li>session settings from path, separated by ;</li>
   * <li>configuration settings separated by ; from query setting</li>
   * <li>variables setting separated by ; from fragment</li>
   * </ul>
   * <p>
   * Examples :-
   * </p>
   * &lt;code&gt;
   * jdbc:lens://hostname:port/dbname;[optional key value pair of session settings]?
   * [optional configuration settings for connection]#[optional variables to be used in query]
   * &lt;/code&gt;
   *
   * @param uri to be used to connect to lens server
   * @return final list of connection parameters
   * @throws IllegalArgumentException if URI provided is malformed
   */
  public static LensConnectionParams parseUrl(String uri) throws IllegalArgumentException {
    LensConnectionParams params = new LensConnectionParams();

    if (!uri.startsWith(URL_PREFIX)) {
      throw new IllegalArgumentException("Bad URL format");
    }

    if (uri.equalsIgnoreCase(URL_PREFIX)) {
      return params;
    }

    URI jdbcUri = URI.create(uri.substring(URI_JDBC_PREFIX.length()));
    Pattern pattern = Pattern.compile(KEY_VALUE_REGEX);
    // dbname and session settings
    String sessVars = jdbcUri.getPath();
    if ((sessVars != null) && !sessVars.isEmpty()) {
      String dbName = "";
      // removing leading '/' returned by getPath()
      sessVars = sessVars.substring(1);
      if (!sessVars.contains(";")) {
        // only dbname is provided
        dbName = sessVars;
      } else {
        // we have dbname followed by session parameters
        dbName = sessVars.substring(0, sessVars.indexOf(';'));
        sessVars = sessVars.substring(sessVars.indexOf(';') + 1);
        if (sessVars != null) {
          Matcher sessMatcher = pattern.matcher(sessVars);
          while (sessMatcher.find()) {
            params.getSessionVars().put(sessMatcher.group(1), sessMatcher.group(2));
          }
        }
      }
      if (!dbName.isEmpty()) {
        params.setDbName(dbName);
      }
    }

    // parse hive conf settings
    String confStr = jdbcUri.getQuery();
    if (confStr != null) {
      Matcher confMatcher = pattern.matcher(confStr);
      while (confMatcher.find()) {
        params.getLensConfs().put(confMatcher.group(1), confMatcher.group(2));
      }
    }

    // parse hive var settings
    String varStr = jdbcUri.getFragment();
    if (varStr != null) {
      Matcher varMatcher = pattern.matcher(varStr);
      while (varMatcher.find()) {
        params.getLensVars().put(varMatcher.group(1), varMatcher.group(2));
      }
    }
    UriBuilder baseUriBuilder = UriBuilder.fromUri(LensClientConfig.DEFAULT_SERVER_BASE_URL);
    if (jdbcUri.getHost() != null) {
      baseUriBuilder.host(jdbcUri.getHost());
    }
    if (jdbcUri.getPort() != -1) {
      baseUriBuilder.port(jdbcUri.getPort());
    }
    params.setBaseUrl(baseUriBuilder.build().toString());
    return params;
  }

  /** The manifest attributes. */
  private static Attributes manifestAttributes = null;

  /**
   * Load manifest attributes.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static synchronized void loadManifestAttributes() throws IOException {
    if (manifestAttributes != null) {
      return;
    }

    Class<?> clazz = JDBCUtils.class;

    String classContainer = clazz.getProtectionDomain().getCodeSource().getLocation().toString();
    URL manifestUrl = new URL("jar:" + classContainer + "!/META-INF/MANIFEST.MF");
    Manifest manifest = new Manifest(manifestUrl.openStream());
    manifestAttributes = manifest.getMainAttributes();

  }

  /**
   * Fetch manifest attribute.
   *
   * @param attributeName the attribute name
   * @return the string
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static String fetchManifestAttribute(Attributes.Name attributeName) throws IOException {
    loadManifestAttributes();
    return manifestAttributes.getValue(attributeName);
  }

  /**
   * Gets the version.
   *
   * @param tokenPosition the token position
   * @return the version
   */
  static int getVersion(int tokenPosition) {
    int version = -1;
    try {
      String fullVersion = fetchManifestAttribute(Attributes.Name.IMPLEMENTATION_VERSION);
      String[] tokens = fullVersion.split("\\."); //$NON-NLS-1$

      if (tokens != null && tokens.length > 0 && tokens[tokenPosition] != null) {
        version = Integer.parseInt(tokens[tokenPosition]);
      }

    } catch (Exception e) {
      version = -1;
    }
    return version;
  }

  /**
   * Parses the url for property info.
   *
   * @param url  the url
   * @param info the info
   * @return the properties
   * @throws SQLException the SQL exception
   */
  static Properties parseUrlForPropertyInfo(String url, Properties info) throws SQLException {

    Properties urlProperties = (info != null) ? new Properties(info) : new Properties();

    if (url == null || !url.startsWith(URL_PREFIX)) {
      throw new SQLException("Invalid connection url :" + url);
    }

    LensConnectionParams params = parseUrl(url);
    // urlProperties.put(HOST_PROPERTY_KEY, params.getHost());
    // urlProperties.put(PORT_PROPERTY_KEY, params.getPort());
    urlProperties.put(DB_PROPERTY_KEY, params.getDbName());

    return urlProperties;
  }

  /**
   * Gets the SQL type.
   *
   * @param type the type
   * @return the SQL type
   * @throws SQLException the SQL exception
   */
  public static int getSQLType(String type) throws SQLException {

    if ("string".equalsIgnoreCase(type)) {
      return Types.VARCHAR;
    } else if ("varchar".equalsIgnoreCase(type)) {
      return Types.VARCHAR;
    } else if ("char".equalsIgnoreCase(type)) {
      return Types.CHAR;
    } else if ("float".equalsIgnoreCase(type)) {
      return Types.FLOAT;
    } else if ("double".equalsIgnoreCase(type)) {
      return Types.DOUBLE;
    } else if ("boolean".equalsIgnoreCase(type)) {
      return Types.BOOLEAN;
    } else if ("tinyint".equalsIgnoreCase(type)) {
      return Types.TINYINT;
    } else if ("smallint".equalsIgnoreCase(type)) {
      return Types.SMALLINT;
    } else if ("int".equalsIgnoreCase(type)) {
      return Types.INTEGER;
    } else if ("bigint".equalsIgnoreCase(type)) {
      return Types.BIGINT;
    } else if ("date".equalsIgnoreCase(type)) {
      return Types.DATE;
    } else if ("timestamp".equalsIgnoreCase(type)) {
      return Types.TIMESTAMP;
    } else if ("decimal".equalsIgnoreCase(type)) {
      return Types.DECIMAL;
    } else if ("binary".equalsIgnoreCase(type)) {
      return Types.BINARY;
    } else if ("map".equalsIgnoreCase(type)) {
      return Types.JAVA_OBJECT;
    } else if ("array".equalsIgnoreCase(type)) {
      return Types.ARRAY;
    } else if ("struct".equalsIgnoreCase(type)) {
      return Types.STRUCT;
    }
    throw new SQLException("Unrecognized column type: " + type);

  }

  /**
   * Column display size.
   *
   * @param columnType the column type
   * @return the int
   * @throws SQLException the SQL exception
   */
  static int columnDisplaySize(int columnType) throws SQLException {
    // according to hiveTypeToSqlType possible options are:
    switch (columnType) {
    case Types.BOOLEAN:
      return columnPrecision(columnType);
    case Types.CHAR:
    case Types.VARCHAR:
      return columnPrecision(columnType);
    case Types.BINARY:
      return Integer.MAX_VALUE; // hive has no max limit for binary
    case Types.TINYINT:
    case Types.SMALLINT:
    case Types.INTEGER:
    case Types.BIGINT:
      return columnPrecision(columnType) + 1; // allow +/-
    case Types.DATE:
      return 10;
    case Types.TIMESTAMP:
      return columnPrecision(columnType);

    // see http://download.oracle.com/javase/6/docs/api/constant-values.html#java.lang.Float.MAX_EXPONENT
    case Types.FLOAT:
      return 24; // e.g. -(17#).e-###
    // see http://download.oracle.com/javase/6/docs/api/constant-values.html#java.lang.Double.MAX_EXPONENT
    case Types.DOUBLE:
      return 25; // e.g. -(17#).e-####
    case Types.DECIMAL:
      return columnPrecision(columnType) + 2; // '-' sign and '.'
    case Types.JAVA_OBJECT:
    case Types.ARRAY:
    case Types.STRUCT:
      return Integer.MAX_VALUE;
    default:
      throw new SQLException("Invalid column type: " + columnType);
    }
  }

  /**
   * Column precision.
   *
   * @param columnType the column type
   * @return the int
   * @throws SQLException the SQL exception
   */
  static int columnPrecision(int columnType) throws SQLException {
    // according to hiveTypeToSqlType possible options are:
    switch (columnType) {
    case Types.BOOLEAN:
      return 1;
    case Types.CHAR:
    case Types.VARCHAR:
      return Integer.MAX_VALUE; // hive has no max limit for strings
    case Types.BINARY:
      return Integer.MAX_VALUE; // hive has no max limit for binary
    case Types.TINYINT:
      return 3;
    case Types.SMALLINT:
      return 5;
    case Types.INTEGER:
      return 10;
    case Types.BIGINT:
      return 19;
    case Types.FLOAT:
      return 7;
    case Types.DOUBLE:
      return 15;
    case Types.DATE:
      return 10;
    case Types.TIMESTAMP:
      return 29;
    case Types.DECIMAL:
      return 5;
    case Types.JAVA_OBJECT:
    case Types.ARRAY:
    case Types.STRUCT:
      return Integer.MAX_VALUE;
    default:
      throw new SQLException("Invalid column type: " + columnType);
    }
  }

  /**
   * Column scale.
   *
   * @param columnType the column type
   * @return the int
   * @throws SQLException the SQL exception
   */
  static int columnScale(int columnType) throws SQLException {
    // according to hiveTypeToSqlType possible options are:
    switch (columnType) {
    case Types.BOOLEAN:
    case Types.CHAR:
    case Types.VARCHAR:
    case Types.TINYINT:
    case Types.SMALLINT:
    case Types.INTEGER:
    case Types.BIGINT:
    case Types.DATE:
    case Types.BINARY:
      return 0;
    case Types.FLOAT:
      return 7;
    case Types.DOUBLE:
      return 15;
    case Types.TIMESTAMP:
      return 9;
    case Types.DECIMAL:
      return 5;
    case Types.JAVA_OBJECT:
    case Types.ARRAY:
    case Types.STRUCT:
      return 0;
    default:
      throw new SQLException("Invalid column type: " + columnType);
    }
  }

  /**
   * Column class name.
   *
   * @param columnType the column type
   * @return the string
   * @throws SQLException the SQL exception
   */
  static String columnClassName(int columnType) throws SQLException {
    switch (columnType) {
    case Types.BOOLEAN:
      return Boolean.class.getName();
    case Types.CHAR:
    case Types.VARCHAR:
      return String.class.getName();
    case Types.TINYINT:
      return Byte.class.getName();
    case Types.SMALLINT:
      return Short.class.getName();
    case Types.INTEGER:
      return Integer.class.getName();
    case Types.BIGINT:
      return Long.class.getName();
    case Types.DATE:
      return Date.class.getName();
    case Types.FLOAT:
      return Float.class.getName();
    case Types.DOUBLE:
      return Double.class.getName();
    case Types.TIMESTAMP:
      return Timestamp.class.getName();
    case Types.DECIMAL:
      return BigInteger.class.getName();
    case Types.BINARY:
      return byte[].class.getName();
    case Types.JAVA_OBJECT:
    case Types.ARRAY:
    case Types.STRUCT:
      return String.class.getName();
    default:
      throw new SQLException("Invalid column type: " + columnType);
    }
  }

}

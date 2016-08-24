/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

import javax.sql.DataSource;

import org.apache.lens.api.scheduler.SchedulerJobHandle;
import org.apache.lens.api.scheduler.SchedulerJobInstanceHandle;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.error.UnSupportedOpException;

import org.apache.commons.dbcp.*;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class UtilityMethods.
 */
@Slf4j
public final class UtilityMethods {
  private UtilityMethods() {

  }

  /**
   * Merge maps.
   *
   * @param <K>      the key type
   * @param <V>      the value type
   * @param into     the into
   * @param from     the from
   * @param override the override
   */
  public static <K, V> void mergeMaps(Map<K, V> into, Map<K, V> from, boolean override) {
    for (K key : from.keySet()) {
      if (override || !into.containsKey(key)) {
        into.put(key, from.get(key));
      }
    }
  }

  /**
   * Removes the domain.
   *
   * @param username the username
   * @return the string
   */
  public static String removeDomain(String username) {
    if (username != null && username.contains("@")) {
      username = username.substring(0, username.indexOf("@"));
    }
    return username;
  }

  /**
   * Any null.
   *
   * @param args the args
   * @return true, if successful
   */
  public static boolean anyNull(Object... args) {
    for (Object arg : args) {
      if (arg == null) {
        return true;
      }
    }
    return false;
  }

  /**
   * Query database.
   *
   * @param ds        the ds
   * @param querySql  the query sql
   * @param allowNull the allow null
   * @param args      the args
   * @return the string[]
   * @throws SQLException the SQL exception
   */
  public static String[] queryDatabase(DataSource ds, String querySql, final boolean allowNull, Object... args)
    throws SQLException {
    QueryRunner runner = new QueryRunner(ds);
    return runner.query(querySql, new ResultSetHandler<String[]>() {
      @Override
      public String[] handle(ResultSet resultSet) throws SQLException {
        String[] result = new String[resultSet.getMetaData().getColumnCount()];
        if (!resultSet.next()) {
          if (allowNull) {
            return null;
          }
          throw new SQLException("no rows retrieved in query");
        }
        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
          result[i - 1] = resultSet.getString(i);
        }
        if (resultSet.next()) {
          throw new SQLException("more than one row retrieved in query");
        }
        return result;
      }
    }, args);
  }

  /**
   * Gets the data source from conf.
   *
   * @param conf the conf
   * @return the data source from conf
   */
  public static BasicDataSource getDataSourceFromConf(Configuration conf) {
    BasicDataSource tmp = new BasicDataSource();
    tmp.setDriverClassName(
        conf.get(LensConfConstants.SERVER_DB_DRIVER_NAME, LensConfConstants.DEFAULT_SERVER_DB_DRIVER_NAME));
    tmp.setUrl(conf.get(LensConfConstants.SERVER_DB_JDBC_URL, LensConfConstants.DEFAULT_SERVER_DB_JDBC_URL));
    tmp.setUsername(conf.get(LensConfConstants.SERVER_DB_JDBC_USER, LensConfConstants.DEFAULT_SERVER_DB_USER));
    tmp.setPassword(conf.get(LensConfConstants.SERVER_DB_JDBC_PASS, LensConfConstants.DEFAULT_SERVER_DB_PASS));
    tmp.setValidationQuery(
        conf.get(LensConfConstants.SERVER_DB_VALIDATION_QUERY, LensConfConstants.DEFAULT_SERVER_DB_VALIDATION_QUERY));
    tmp.setDefaultAutoCommit(false);
    return tmp;
  }

  public static DataSource getPoolingDataSourceFromConf(Configuration conf) {
    final ConnectionFactory cf = new DriverManagerConnectionFactory(
        conf.get(LensConfConstants.SERVER_DB_JDBC_URL, LensConfConstants.DEFAULT_SERVER_DB_JDBC_URL),
        conf.get(LensConfConstants.SERVER_DB_JDBC_USER, LensConfConstants.DEFAULT_SERVER_DB_USER),
        conf.get(LensConfConstants.SERVER_DB_JDBC_PASS, LensConfConstants.DEFAULT_SERVER_DB_PASS));
    final GenericObjectPool connectionPool = new GenericObjectPool();
    connectionPool.setTestOnBorrow(false);
    connectionPool.setTestOnReturn(false);
    connectionPool.setTestWhileIdle(true);
    new PoolableConnectionFactory(cf, connectionPool, null,
        conf.get(LensConfConstants.SERVER_DB_VALIDATION_QUERY, LensConfConstants.DEFAULT_SERVER_DB_VALIDATION_QUERY),
        false, false).setDefaultAutoCommit(true);
    return new PoolingDataSource(connectionPool);
  }

  /**
   * Conf to string.
   *
   * @param conf the conf
   * @return the string
   */
  public static String confToString(Configuration conf) {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : conf) {
      sb.append(entry.getKey()).append(":").append(entry.getValue()).append("\n");
    }
    return sb.toString();
  }

  /**
   * Pipe input stream to output stream
   *
   * @param is the is
   * @param os the os
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static void pipe(InputStream is, OutputStream os) throws IOException {
    int n;
    byte[] buffer = new byte[4096];
    while ((n = is.read(buffer)) > -1) {
      os.write(buffer, 0, n);
      os.flush();
    }
  }

  /**
   * Generates a md5 hash of a writable object.
   *
   * @param writable
   * @return hash of a writable object
   */
  public static byte[] generateHashOfWritable(Writable writable) {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] lensConfBytes = WritableUtils.toByteArray(writable);
      md.update(lensConfBytes);
      byte[] digest = md.digest();
      return digest;
    } catch (NoSuchAlgorithmException e) {
      log.warn("MD5: No such method error " + writable);
      return null;
    }
  }

  /**
   * @param conf
   * @return
   */
  public static BasicDataSource getDataSourceFromConfForScheduler(Configuration conf) {
    BasicDataSource basicDataSource = getDataSourceFromConf(conf);
    basicDataSource.setDefaultAutoCommit(true);
    return basicDataSource;
  }

  public static SchedulerJobHandle generateSchedulerJobHandle() {
    return new SchedulerJobHandle(UUID.randomUUID());
  }

  public static SchedulerJobInstanceHandle generateSchedulerJobInstanceHandle() {
    return new SchedulerJobInstanceHandle(UUID.randomUUID());
  }
  public static <T extends Enum<T>> T checkAndGetOperation(final String operation, Class<T> enumType,
    T... supportedOperations) throws UnSupportedOpException {
    try {
      T op = Enum.valueOf(enumType, operation.toUpperCase());
      for (T supportedOperation : supportedOperations) {
        if (op.equals(supportedOperation)) {
          return op;
        }
      }
      throw new UnSupportedOpException(supportedOperations);
    } catch (IllegalArgumentException e) {
      throw new UnSupportedOpException(e, supportedOperations);
    }
  }
}

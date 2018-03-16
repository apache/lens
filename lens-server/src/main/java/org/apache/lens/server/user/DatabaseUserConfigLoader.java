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
package org.apache.lens.server.user;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.user.UserConfigLoader;
import org.apache.lens.server.api.user.UserConfigLoaderException;
import org.apache.lens.server.util.UtilityMethods;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.hive.conf.HiveConf;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * The Class DatabaseUserConfigLoader.
 */
public class DatabaseUserConfigLoader implements UserConfigLoader {

  /** The query sql. */
  protected final String querySql;

  /** The keys. */
  protected final String[] keys;

  /** The cache. */
  protected final Cache<String, Map<String, String>> cache;
  private final HiveConf hiveConf;

  /** The ds. */
  protected BasicDataSource ds;

  /**
   * Instantiates a new database user config loader.
   *
   * @param conf the conf
   * @throws UserConfigLoaderException the user config loader exception
   */
  public DatabaseUserConfigLoader(HiveConf conf) throws UserConfigLoaderException {
    this.hiveConf = conf;
    querySql = conf.get(LensConfConstants.USER_RESOLVER_DB_QUERY);
    keys = conf.get(LensConfConstants.USER_RESOLVER_DB_KEYS).split("\\s*,\\s*", -1);
    cache = CacheBuilder.newBuilder()
      .expireAfterWrite(conf.getInt(LensConfConstants.USER_RESOLVER_CACHE_EXPIRY, 2), TimeUnit.HOURS)
      .maximumSize(conf.getInt(LensConfConstants.USER_RESOLVER_CACHE_MAX_SIZE, 100)).build();
  }
  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.user.UserConfigLoader#getUserConfig(java.lang.String)
   */
  @Override
  public Map<String, String> getUserConfig(final String loggedInUser) throws UserConfigLoaderException {
    try {
      return cache.get(loggedInUser, new Callable<Map<String, String>>() {
        @Override
        public Map<String, String> call() throws Exception {
          try {
            final String[] config = queryDatabase(querySql, false, loggedInUser);
            if (config.length != keys.length) {
              throw new UserConfigLoaderException("size of columns retrieved by db query(" + config.length + ") "
                + "is not equal to the number of keys required(" + keys.length + ").");
            }
            return new HashMap<String, String>() {
              {
                for (int i = 0; i < keys.length; i++) {
                  put(keys[i], config[i]);
                }
              }
            };
          } catch (SQLException e) {
            throw new UserConfigLoaderException(e);
          }
        }
      });
    } catch (ExecutionException e) {
      throw new UserConfigLoaderException(e);
    }
  }

  private BasicDataSource refreshDataSource() {
    if (ds == null || ds.isClosed()) {
      ds = UtilityMethods.getDataSourceFromConf(hiveConf);
    }
    return ds;
  }

  String[] queryDatabase(String querySql, boolean allowNull, Object... args) throws SQLException {
    return UtilityMethods.queryDatabase(refreshDataSource(), querySql, allowNull, args);
  }
}

package org.apache.lens.server.user;
/*
 * #%L
 * Grill Server
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.lens.server.api.GrillConfConstants;
import org.apache.lens.server.util.UtilityMethods;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class DatabaseUserConfigLoader extends UserConfigLoader {
  protected final String querySql;
  protected final String[] keys;
  protected final Cache<String, Map<String, String>> cache;
  protected DataSource ds;

  public DatabaseUserConfigLoader(HiveConf conf) throws UserConfigLoaderException {
    super(conf);
    querySql = conf.get(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_DB_QUERY);
    keys = conf.get(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_DB_KEYS).split("\\s*,\\s*", -1);
    ds = getDataSourceFromConf(conf);
    cache = CacheBuilder
      .newBuilder()
      .expireAfterWrite(conf.getInt(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_CACHE_EXPIRY, 2), TimeUnit.HOURS)
      .maximumSize(conf.getInt(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_CACHE_MAX_SIZE, 100)).build();
  }

  public static BasicDataSource getDataSourceFromConf(HiveConf conf) {
    BasicDataSource tmp = new BasicDataSource();
    tmp.setDriverClassName(conf.get(GrillConfConstants.GRILL_SERVER_DB_DRIVER_NAME,
      GrillConfConstants.DEFAULT_SERVER_DB_DRIVER_NAME));
    tmp.setUrl(conf.get(GrillConfConstants.GRILL_SERVER_DB_JDBC_URL,
      GrillConfConstants.DEFAULT_SERVER_DB_JDBC_URL));
    tmp.setUsername(conf.get(GrillConfConstants.GRILL_SERVER_DB_JDBC_USER,
      GrillConfConstants.DEFAULT_SERVER_DB_USER));
    tmp.setPassword(conf.get(GrillConfConstants.GRILL_SERVER_DB_JDBC_PASS,
      GrillConfConstants.DEFAULT_SERVER_DB_PASS));
    return tmp;
  }

  @Override
  public Map<String, String> getUserConfig(final String loggedInUser) throws UserConfigLoaderException {
    try {
      return cache.get(loggedInUser, new Callable<Map<String, String>>() {
        @Override
        public Map<String, String> call() throws Exception {

          try {
            final String[] config = UtilityMethods.queryDatabase(ds, querySql, false, loggedInUser);
            if(config.length != keys.length) {
              throw new UserConfigLoaderException("size of columns retrieved by db query(" + config.length + ") " +
                "is not equal to the number of keys required(" + keys.length + ").");
            }
            return new HashMap<String, String>(){
              {
                for(int i = 0; i < keys.length; i++) {
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
}

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
package org.apache.lens.server.user.usergroup;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.authorization.ADGroupService;
import org.apache.lens.server.api.user.UserGroupConfigLoader;
import org.apache.lens.server.api.user.UserGroupLoaderException;
import org.apache.lens.server.util.UtilityMethods;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.hadoop.hive.conf.HiveConf;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class ADGroupConfigLoader implements UserGroupConfigLoader {

  /** The cache. */
  private final Cache<String, Map<String, String>> cache;

  /** The Constant DATE_TIME_FORMATTER. */
  private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:SS")
    .withZoneUTC();

  /** The expiry hours. */
  private final int expiryHours;

  /** The environment to connect to AD server. picked from conf. */
  private final Hashtable<String, Object> env;


  /** The query sql. */
  private final String querySql;

  /** The keys. */
  private final String[] keys;

  private final HiveConf hiveConf;

  /** The ds. */
  private BasicDataSource ds;

  /** The intermediate delete sql. */
  private final String deleteSql;

  /** The intermediate insert sql. */
  private final String insertSql;

  /** The ldap fields. */
  private final String[] lookupFields;

  /**
   * Instantiates a new LDAP backed database user config loader.
   *
   * @param conf the conf
   * @throws UserGroupLoaderException the user group loader exception
   */
  public ADGroupConfigLoader(final HiveConf conf) throws UserGroupLoaderException {
    this.hiveConf = conf;
    querySql = conf.get(LensConfConstants.USER_GROUP_DB_QUERY);
    deleteSql = conf.get(LensConfConstants.USER_GROUP_DB_DELETE_SQL);
    insertSql = conf.get(LensConfConstants.USER_GROUP_DB_INSERT_SQL);
    keys = conf.get(LensConfConstants.USER_GROUP_DB_KEYS).split("\\s*,\\s*", -1);
    expiryHours = conf.getInt(LensConfConstants.USER_GROUP_CACHE_EXPIRY, 2);
    cache = CacheBuilder.newBuilder().expireAfterWrite(expiryHours, TimeUnit.HOURS)
      .maximumSize(conf.getInt(LensConfConstants.USER_GROUP_CACHE_MAX_SIZE, 100)).build();
    lookupFields = conf.get(LensConfConstants.USER_GROUP_LOOKUP_FIELDS).split("\\s*,\\s*");
    env = new Hashtable<String, Object>() {
      {
        put(LensConfConstants.AD_SERVER_ENDPOINT, conf.get(LensConfConstants.AD_SERVER_ENDPOINT_VALUE));
        put(LensConfConstants.AD_SERVER_ENDPOINT_USER_NAME,
          conf.get(LensConfConstants.AD_SERVER_ENDPOINT_USER_NAME_VALUE));
        put(LensConfConstants.AD_SERVER_ENDPOINT_PWD, conf.get(LensConfConstants.AD_SERVER_ENDPOINT_PWD_VALUE));

      }
    };
  }

  /**
   * Gets the attributes.
   *
   * @param user the user
   * @return the attributes
   */
  public String[] getAttributes(String user) throws IOException, JSONException {

    Map<String, String> res = ADGroupService.getAttributes(formServerUrl(user), lookupFields,
      (String) env.get(LensConfConstants.AD_SERVER_ENDPOINT_USER_NAME),
      (String) env.get(LensConfConstants.AD_SERVER_ENDPOINT_PWD));
    String[] attributes = new String[lookupFields.length];

    for (int i = 0; i < attributes.length; i++) {
      attributes[i] = res.get(lookupFields[i]);
    }
    return attributes;
  }

  private String formServerUrl(String user) {
    return env.get(LensConfConstants.AD_SERVER_ENDPOINT) + user;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.user.DatabaseUserConfigLoader#getUserConfig(java.lang.String)
   */
  @Override
  public Map<String, String> getUserConfig(final String loggedInUser) throws UserGroupLoaderException {
    try {
      Map<String, String> userConfigMap = cache.get(loggedInUser, new Callable<Map<String, String>>() {
        @Override
        public Map<String, String> call() throws Exception {

          try {
            String[] config = queryDatabase(querySql, true, loggedInUser,
              Timestamp.valueOf(DateTime.now().toString(DATE_TIME_FORMATTER)));
            if (config != null && config.length != keys.length) {
              throw new UserGroupLoaderException("size of columns retrieved by db query(" + config.length + ") "
                + "is not equal to the number of keys required(" + keys.length + ").");
            }
            if (config == null) {
              String[] finalConfig = getAttributes(loggedInUser);
              Object[] updateArray = new Object[finalConfig.length + 2];
              for (int i = 0; i < finalConfig.length; i++) {
                updateArray[i + 1] = finalConfig[i];
              }
              updateArray[0] = loggedInUser;
              updateArray[finalConfig.length + 1] = Timestamp.valueOf(DateTime.now().plusHours(expiryHours)
                .toString(DATE_TIME_FORMATTER));
              QueryRunner runner = new QueryRunner(ds);
              runner.update(deleteSql, loggedInUser);
              runner.update(insertSql, updateArray);
              return new HashMap<String, String>() {
                {
                  for (int i = 0; i < keys.length; i++) {
                    put(keys[i], finalConfig[i]);
                  }
                }
              };
            } else {
              return new HashMap<String, String>() {
                {
                  for (int i = 0; i < keys.length; i++) {
                    put(keys[i], config[i]);
                  }
                }
              };
            }

          } catch (SQLException e) {
            throw new UserGroupLoaderException(e);
          }
        }
      });
      return userConfigMap;

    } catch (ExecutionException e) {
      throw new UserGroupLoaderException(e);
    }
  }

  private BasicDataSource refreshDataSource() {
    if (ds == null || ds.isClosed()) {
      ds = UtilityMethods.getDataSourceFromConf(hiveConf);
    }
    return ds;
  }

  private String[] queryDatabase(String querySql, boolean allowNull, Object... args) throws SQLException {
    return UtilityMethods.queryDatabase(refreshDataSource(), querySql, allowNull, args);
  }
}

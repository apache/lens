package com.inmobi.grill.server.user;
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

import com.inmobi.grill.server.api.GrillConfConstants;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.hadoop.hive.conf.HiveConf;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class DatabaseUserConfigLoader extends UserConfigLoader {
  private final String querySql;
  private final String[] keys;
  private DataSource ds;
  public DatabaseUserConfigLoader(HiveConf conf) throws UserConfigLoaderException {
    super(conf);
    String className = conf.get(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_DB_DRIVER_NAME);
    String jdbcUrl = conf.get(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_DB_JDBC_URL);
    String userName = conf.get(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_DB_JDBC_USERNAME);
    String pass = conf.get(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_DB_JDBC_PASSWORD, "");
    querySql = conf.get(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_DB_QUERY);
    keys = conf.get(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_DB_KEYS).split("\\s*,\\s*", -1);
    if(anyNull(className, jdbcUrl, userName, pass, querySql, keys)) {
      throw new UserConfigLoaderException("You need to specify all of the following in conf: ["
        + GrillConfConstants.GRILL_SERVER_USER_RESOLVER_DB_DRIVER_NAME + ", "
        + GrillConfConstants.GRILL_SERVER_USER_RESOLVER_DB_JDBC_URL + ", "
        + GrillConfConstants.GRILL_SERVER_USER_RESOLVER_DB_JDBC_USERNAME + ", "
        + GrillConfConstants.GRILL_SERVER_USER_RESOLVER_DB_JDBC_PASSWORD + ", "
        + GrillConfConstants.GRILL_SERVER_USER_RESOLVER_DB_QUERY + ", "
        + GrillConfConstants.GRILL_SERVER_USER_RESOLVER_DB_KEYS + ", "
      + "]");
    }
    ds = getDataSourceFromConf(conf);
  }

  public static BasicDataSource getDataSourceFromConf(HiveConf conf) {
    BasicDataSource tmp = new BasicDataSource();
    tmp.setDriverClassName(conf.get(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_DB_DRIVER_NAME));
    tmp.setUrl(conf.get(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_DB_JDBC_URL));
    tmp.setUsername(conf.get(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_DB_JDBC_USERNAME));
    tmp.setPassword(conf.get(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_DB_JDBC_PASSWORD, ""));
    return tmp;
  }

  private boolean anyNull(Object... args) {
    for(Object arg: args) {
      if(arg == null) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Map<String, String> getUserConfig(String loggedInUser) throws UserConfigLoaderException {
    QueryRunner runner = new QueryRunner(ds);
    try {
      String[] config = runner.query(querySql, new ResultSetHandler<String[]>() {
        @Override
        public String[] handle(ResultSet resultSet) throws SQLException {
          String[] result = new String[resultSet.getMetaData().getColumnCount()];
          if(!resultSet.next()) {
            throw new SQLException("no rows retrieved in query");
          }
          for(int i=1; i <= resultSet.getMetaData().getColumnCount(); i++) {
            result[i - 1] = resultSet.getString(i);
          }
          if(resultSet.next()) {
            throw new SQLException("more than one row retrieved in query");
          }
          return result;
        }
      }, loggedInUser);
      if(config.length != keys.length) {
        throw new UserConfigLoaderException("size of columns retrieved by db query(" + config.length + ") " +
          "is not equal to the number of keys required(" + keys.length + ").");
      }
      HashMap<String, String> userConfig = new HashMap<String, String>();
      for(int i = 0; i < keys.length; i++) {
        userConfig.put(keys[i], config[i]);
      }
      return userConfig;
    } catch (SQLException e) {
      throw new UserConfigLoaderException(e);
    }
  }
}

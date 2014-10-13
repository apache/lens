package org.apache.lens.server.util;

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

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class UtilityMethods {
  public static <K, V> void mergeMaps(Map<K, V> into, Map<K, V> from, boolean override) {
    for(K key: from.keySet()) {
      if(override || !into.containsKey(key)) {
        into.put(key, from.get(key));
      }
    }
  }
  public static String removeDomain(String username) {
    if(username.contains("@")) {
      username = username.substring(0, username.indexOf("@"));
    }
    return username;
  }
  public static boolean anyNull(Object... args) {
    for(Object arg: args) {
      if(arg == null) {
        return true;
      }
    }
    return false;
  }
  public static String[] queryDatabase(DataSource ds, String querySql, final boolean allowNull, Object... args) throws SQLException {
    QueryRunner runner = new QueryRunner(ds);
    return runner.query(querySql, new ResultSetHandler<String[]>() {
      @Override
      public String[] handle(ResultSet resultSet) throws SQLException {
        String[] result = new String[resultSet.getMetaData().getColumnCount()];
        if(!resultSet.next()) {
          if(allowNull){
            return null;
          }
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
    }, args);
  }
}

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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.util.UtilityMethods;
import com.novell.ldap.*;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.hadoop.hive.conf.HiveConf;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class LDAPBackedDatabaseUserConfigLoader extends DatabaseUserConfigLoader {
  private final Cache<String, String[]> intermediateCache;
  private final Cache<String[], Map<String, String>> cache;
  private final String intermediateQuerySql;
  private final String[] ldapFields;
  private final String intermediateDeleteSql;
  private final String intermediateInsertSql;
  private final static DateTimeFormatter outputFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:SS").withZoneUTC();
  private final int expiryHours;
  private final LDAPAccess access;

  public LDAPBackedDatabaseUserConfigLoader(HiveConf conf) throws UserConfigLoaderException {
    super(conf);
    access = new LDAPAccess(hiveConf);
    expiryHours = conf.getInt(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_CACHE_EXPIRY, 2);
    intermediateQuerySql = conf.get(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_LDAP_INTERMEDIATE_DB_QUERY);
    intermediateDeleteSql = conf.get(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_LDAP_INTERMEDIATE_DB_DELETE_SQL);
    intermediateInsertSql = conf.get(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_LDAP_INTERMEDIATE_DB_INSERT_SQL);
    ldapFields = conf.get(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_LDAP_FIELDS).split("\\s*,\\s*");
    intermediateCache = CacheBuilder
      .newBuilder()
      .expireAfterWrite(expiryHours, TimeUnit.HOURS)
      .maximumSize(conf.getInt(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_CACHE_MAX_SIZE, 100)).build();
    cache = CacheBuilder
      .newBuilder()
      .expireAfterWrite(expiryHours, TimeUnit.HOURS)
      .maximumSize(conf.getInt(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_CACHE_MAX_SIZE, 100)).build();
  }

  @Override
  public Map<String, String> getUserConfig(final String loggedInUser) throws UserConfigLoaderException {
    try {
      final String[] intermediateKey = intermediateCache.get(loggedInUser, new Callable<String[]>() {
        @Override
        public String[] call() throws Exception {
          String[] config = UtilityMethods.queryDatabase(ds, intermediateQuerySql, true,
            loggedInUser, Timestamp.valueOf(DateTime.now().toString(outputFormatter)));
          if(config == null) {
            config = access.getAttributes(loggedInUser, ldapFields);
            Object[] updateArray = new Object[config.length + 2];
            for(int i = 0; i < config.length; i++) {
              updateArray[i + 1] = config[i];
            }
            updateArray[0] = loggedInUser;
            updateArray[config.length + 1] = Timestamp.valueOf(DateTime.now().plusHours(expiryHours).toString(outputFormatter));
            QueryRunner runner = new QueryRunner(ds);
            runner.update(intermediateDeleteSql, loggedInUser);
            runner.update(intermediateInsertSql, updateArray);
          }
          return config;
        }
      });
      return cache.get(intermediateKey, new Callable<Map<String, String>>() {
        @Override
        public Map<String, String> call() throws Exception {
          final String[] argsAsArray = UtilityMethods.queryDatabase(ds, querySql, false, intermediateKey);
          if(argsAsArray.length != keys.length) {
            throw new UserConfigLoaderException("size of columns retrieved by db query(" + argsAsArray.length + ") " +
              "is not equal to the number of keys required(" + keys.length + ").");
          }
          return new HashMap<String, String>(){
            {
              for(int i = 0; i < keys.length; i++) {
                put(keys[i], argsAsArray[i]);
              }
            }
          };
        }
      });
    } catch (ExecutionException e) {
      throw new UserConfigLoaderException(e);
    }
  }
  static class LDAPAccess {
    private final LDAPConnection conn;
    private final String searchBase;
    private final String searchFilter;

    public LDAPAccess(HiveConf conf) {
      searchBase = conf.get(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_LDAP_SEARCH_BASE);
      searchFilter = conf.get(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_LDAP_SEARCH_FILTER);
      conn = new LDAPConnection(new LDAPJSSESecureSocketFactory());
      try {
        conn.connect(conf.get(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_LDAP_HOST),
          conf.getInt(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_LDAP_PORT, -1));
        conn.bind(LDAPConnection.LDAP_V3,
          conf.get(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_LDAP_BIND_DN),
          conf.get(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_LDAP_BIND_PASSWORD).getBytes()
        );
      } catch (LDAPException e) {
        throw new UserConfigLoaderException(e);
      }
    }

    public boolean userExists(String username) throws LDAPException{
      LDAPSearchResults searchResults = conn.search(searchBase, LDAPConnection.SCOPE_ONE, searchFilter + "=" + username, null, false);
      return (searchResults != null && searchResults.hasMore());
    }

    public String[] getAttributes(String username, String... keys) throws LDAPException {
      LDAPSearchResults searchResults = conn.search(searchBase, LDAPConnection.SCOPE_ONE, searchFilter + "=" + username, null, false);
      if (searchResults != null && searchResults.hasMore()) {
        String[] values = new String[keys.length];
        LDAPEntry entry = searchResults.next();
        for(int i = 0; i < keys.length; i++) {
          values[i] = entry.getAttribute(keys[i]) == null ? null : entry.getAttribute(keys[i]).getStringValue();
        }
        return values;
      } else {
        return null;
      }
    }

    public void close() throws LDAPException {
      if (conn != null) {
        conn.disconnect();
      }
    }
  }
}

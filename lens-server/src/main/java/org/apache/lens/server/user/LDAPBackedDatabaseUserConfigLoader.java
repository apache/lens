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

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.user.UserConfigLoaderException;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.hadoop.hive.conf.HiveConf;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * The Class LDAPBackedDatabaseUserConfigLoader.
 */
public class LDAPBackedDatabaseUserConfigLoader extends DatabaseUserConfigLoader {

  /** The intermediate cache. */
  private final Cache<String, String[]> intermediateCache;

  /** The cache. */
  private final Cache<String[], Map<String, String>> cache;

  /** The intermediate query sql. */
  private final String intermediateQuerySql;

  /** The ldap fields. */
  private final String[] ldapFields;

  /** The intermediate delete sql. */
  private final String intermediateDeleteSql;

  /** The intermediate insert sql. */
  private final String intermediateInsertSql;

  /** The Constant DATE_TIME_FORMATTER. */
  private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:SS")
    .withZoneUTC();

  /** The expiry hours. */
  private final int expiryHours;

  /** The search base. */
  private final String searchBase;

  /** The search filter pattern. */
  private final String searchFilterPattern;

  /** The environment to connect to ldap. picked from conf. */
  private final Hashtable<String, Object> env;

  /**
   * Instantiates a new LDAP backed database user config loader.
   *
   * @param conf the conf
   * @throws UserConfigLoaderException the user config loader exception
   */
  public LDAPBackedDatabaseUserConfigLoader(final HiveConf conf) throws UserConfigLoaderException {
    super(conf);
    expiryHours = conf.getInt(LensConfConstants.USER_RESOLVER_CACHE_EXPIRY, 2);
    intermediateQuerySql = conf.get(LensConfConstants.USER_RESOLVER_LDAP_INTERMEDIATE_DB_QUERY);
    intermediateDeleteSql = conf.get(LensConfConstants.USER_RESOLVER_LDAP_INTERMEDIATE_DB_DELETE_SQL);
    intermediateInsertSql = conf.get(LensConfConstants.USER_RESOLVER_LDAP_INTERMEDIATE_DB_INSERT_SQL);
    ldapFields = conf.get(LensConfConstants.USER_RESOLVER_LDAP_FIELDS).split("\\s*,\\s*");
    searchBase = conf.get(LensConfConstants.USER_RESOLVER_LDAP_SEARCH_BASE);
    searchFilterPattern = conf.get(LensConfConstants.USER_RESOLVER_LDAP_SEARCH_FILTER);
    intermediateCache = CacheBuilder.newBuilder().expireAfterWrite(expiryHours, TimeUnit.HOURS)
      .maximumSize(conf.getInt(LensConfConstants.USER_RESOLVER_CACHE_MAX_SIZE, 100)).build();
    cache = CacheBuilder.newBuilder().expireAfterWrite(expiryHours, TimeUnit.HOURS)
      .maximumSize(conf.getInt(LensConfConstants.USER_RESOLVER_CACHE_MAX_SIZE, 100)).build();

    env = new Hashtable<String, Object>() {
      {
        put(Context.SECURITY_AUTHENTICATION, "simple");
        put(Context.SECURITY_PRINCIPAL, conf.get(LensConfConstants.USER_RESOLVER_LDAP_BIND_DN));
        put(Context.SECURITY_CREDENTIALS, conf.get(LensConfConstants.USER_RESOLVER_LDAP_BIND_PASSWORD));
        put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        put(Context.PROVIDER_URL, conf.get(LensConfConstants.USER_RESOLVER_LDAP_URL));
        put("java.naming.ldap.attributes.binary", "objectSID");
      }
    };
  }

  /**
   * Find account by account name.
   *
   * @param accountName the account name
   * @return the search result
   * @throws NamingException the naming exception
   */
  protected SearchResult findAccountByAccountName(String accountName) throws NamingException {
    String searchFilter = String.format(searchFilterPattern, accountName);
    SearchControls searchControls = new SearchControls();
    searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
    InitialLdapContext ctx = new InitialLdapContext(env, null);
    try {
      NamingEnumeration<SearchResult> results = ctx.search(searchBase, searchFilter, searchControls);
      if (!results.hasMoreElements()) {
        throw new UserConfigLoaderException("LDAP Search returned no accounts");
      }
      SearchResult searchResult = results.nextElement();
      if (results.hasMoreElements()) {
        throw new UserConfigLoaderException("More than one account found in ldap search");
      }
      return searchResult;
    } finally {
      ctx.close();
    }
  }

  /**
   * Gets the attributes.
   *
   * @param user the user
   * @return the attributes
   * @throws NamingException the naming exception
   */
  public String[] getAttributes(String user) throws NamingException {
    String[] attributes = new String[ldapFields.length];
    SearchResult sr = findAccountByAccountName(user);
    for (int i = 0; i < attributes.length; i++) {
      Attribute attr = sr.getAttributes().get(ldapFields[i]);
      attributes[i] = (attr == null ? null : attr.get().toString());
    }
    return attributes;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.user.DatabaseUserConfigLoader#getUserConfig(java.lang.String)
   */
  @Override
  public Map<String, String> getUserConfig(final String loggedInUser) throws UserConfigLoaderException {
    try {
      final String[] intermediateKey = intermediateCache.get(loggedInUser, new Callable<String[]>() {
        @Override
        public String[] call() throws Exception {
          String[] config = queryDatabase(intermediateQuerySql, true, loggedInUser,
            Timestamp.valueOf(DateTime.now().toString(DATE_TIME_FORMATTER)));
          if (config == null) {
            config = getAttributes(loggedInUser);
            Object[] updateArray = new Object[config.length + 2];
            for (int i = 0; i < config.length; i++) {
              updateArray[i + 1] = config[i];
            }
            updateArray[0] = loggedInUser;
            updateArray[config.length + 1] = Timestamp.valueOf(DateTime.now().plusHours(expiryHours)
              .toString(DATE_TIME_FORMATTER));
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
          final String[] argsAsArray = queryDatabase(querySql, false, intermediateKey);
          if (argsAsArray.length != keys.length) {
            throw new UserConfigLoaderException("size of columns retrieved by db query(" + argsAsArray.length + ") "
              + "is not equal to the number of keys required(" + keys.length + ").");
          }
          return new HashMap<String, String>() {
            {
              for (int i = 0; i < keys.length; i++) {
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
}

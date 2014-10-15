package org.apache.lens.server.user;

/*
 * #%L
 * Lens Server
 * %%
 * Copyright (C) 2014 Apache Software Foundation
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
import org.apache.commons.dbutils.QueryRunner;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.util.UtilityMethods;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Hashtable;
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
  private final String searchBase;
  private final String searchFilterPattern;
  private final InitialLdapContext ctx;

  public LDAPBackedDatabaseUserConfigLoader(final HiveConf conf) throws UserConfigLoaderException {
    super(conf);
    expiryHours = conf.getInt(LensConfConstants.USER_RESOLVER_CACHE_EXPIRY, 2);
    intermediateQuerySql = conf.get(LensConfConstants.USER_RESOLVER_LDAP_INTERMEDIATE_DB_QUERY);
    intermediateDeleteSql = conf.get(LensConfConstants.USER_RESOLVER_LDAP_INTERMEDIATE_DB_DELETE_SQL);
    intermediateInsertSql = conf.get(LensConfConstants.USER_RESOLVER_LDAP_INTERMEDIATE_DB_INSERT_SQL);
    ldapFields = conf.get(LensConfConstants.USER_RESOLVER_LDAP_FIELDS).split("\\s*,\\s*");
    searchBase = conf.get(LensConfConstants.USER_RESOLVER_LDAP_SEARCH_BASE);
    searchFilterPattern = conf.get(LensConfConstants.USER_RESOLVER_LDAP_SEARCH_FILTER);
    intermediateCache = CacheBuilder
      .newBuilder()
      .expireAfterWrite(expiryHours, TimeUnit.HOURS)
      .maximumSize(conf.getInt(LensConfConstants.USER_RESOLVER_CACHE_MAX_SIZE, 100)).build();
    cache = CacheBuilder
      .newBuilder()
      .expireAfterWrite(expiryHours, TimeUnit.HOURS)
      .maximumSize(conf.getInt(LensConfConstants.USER_RESOLVER_CACHE_MAX_SIZE, 100)).build();

    Hashtable<String, Object> env = new Hashtable<String, Object>(){
      {
        put(Context.SECURITY_AUTHENTICATION, "simple");
        put(Context.SECURITY_PRINCIPAL, conf.get(LensConfConstants.USER_RESOLVER_LDAP_BIND_DN));
        put(Context.SECURITY_CREDENTIALS, conf.get(LensConfConstants.USER_RESOLVER_LDAP_BIND_PASSWORD));
        put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        put(Context.PROVIDER_URL, conf.get(LensConfConstants.USER_RESOLVER_LDAP_URL));
        put("java.naming.ldap.attributes.binary", "objectSID");
      }
    };
    try {
      ctx = new InitialLdapContext(env, null);
    } catch (NamingException e) {
      throw new UserConfigLoaderException(e);
    }
  }

  protected SearchResult findAccountByAccountName(String accountName) throws NamingException {
    String searchFilter = String.format(searchFilterPattern, accountName);
    SearchControls searchControls = new SearchControls();
    searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
    NamingEnumeration<SearchResult> results = ctx.search(searchBase, searchFilter, searchControls);
    if(!results.hasMoreElements()) {
      throw new UserConfigLoaderException("LDAP Search returned no accounts");
    }
    SearchResult searchResult = results.nextElement();
    if(results.hasMoreElements()) {
      throw new UserConfigLoaderException("More than one account found in ldap search");
    }
    return searchResult;
  }
  public String[] getAttributes(String user) throws NamingException {
    String[] attributes = new String[ldapFields.length];
    SearchResult sr = findAccountByAccountName(user);
    for(int i = 0; i < attributes.length; i++) {
      attributes[i] = sr.getAttributes().get(ldapFields[i]).get().toString();
    }
    return attributes;
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
            config = getAttributes(loggedInUser);
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
}

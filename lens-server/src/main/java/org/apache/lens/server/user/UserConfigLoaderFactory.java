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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.lens.server.api.LensConfConstants;

import java.util.Map;

/**
 * A factory for creating UserConfigLoader objects.
 */
public class UserConfigLoaderFactory {

  /** The conf. */
  private static HiveConf conf;

  /** The user config loader. */
  private static UserConfigLoader userConfigLoader;

  /**
   * Inits the.
   *
   * @param c
   *          the c
   */
  public static void init(HiveConf c) {
    conf = c;
    userConfigLoader = initializeUserConfigLoader();
  }

  /**
   * The Enum RESOLVER_TYPE.
   */
  public static enum RESOLVER_TYPE {

    /** The fixed. */
    FIXED,

    /** The propertybased. */
    PROPERTYBASED,

    /** The database. */
    DATABASE,

    /** The ldap backed database. */
    LDAP_BACKED_DATABASE,

    /** The custom. */
    CUSTOM
  }

  /**
   * Initialize user config loader.
   *
   * @return the user config loader
   */
  public static UserConfigLoader initializeUserConfigLoader() {
    String resolverType = conf.get(LensConfConstants.USER_RESOLVER_TYPE);
    if (resolverType == null || resolverType.length() == 0) {
      throw new UserConfigLoaderException("user resolver type not determined. value was not provided in conf");
    }
    for (RESOLVER_TYPE type : RESOLVER_TYPE.values()) {
      if (type.name().equals(resolverType)) {
        return getQueryUserResolver(type);
      }
    }
    throw new UserConfigLoaderException("user resolver type not determined. provided value: " + resolverType);
  }

  /**
   * Gets the query user resolver.
   *
   * @param resolverType
   *          the resolver type
   * @return the query user resolver
   */
  public static UserConfigLoader getQueryUserResolver(RESOLVER_TYPE resolverType) {
    switch (resolverType) {
    case PROPERTYBASED:
      return new PropertyBasedUserConfigLoader(conf);
    case DATABASE:
      return new DatabaseUserConfigLoader(conf);
    case LDAP_BACKED_DATABASE:
      return new LDAPBackedDatabaseUserConfigLoader(conf);
    case CUSTOM:
      return new CustomUserConfigLoader(conf);
    case FIXED:
    default:
      return new FixedUserConfigLoader(conf);
    }
  }

  /**
   * Gets the user config.
   *
   * @param loggedInUser
   *          the logged in user
   * @return the user config
   */
  public static Map<String, String> getUserConfig(String loggedInUser) {
    return userConfigLoader.getUserConfig(loggedInUser);
  }
}

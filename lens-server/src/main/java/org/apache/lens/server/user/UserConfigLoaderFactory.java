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

import static org.apache.lens.server.api.LensConfConstants.USER_RESOLVER_CUSTOM_CLASS;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.user.UserConfigLoader;
import org.apache.lens.server.api.user.UserConfigLoaderException;

import org.apache.hadoop.hive.conf.HiveConf;

import lombok.extern.slf4j.Slf4j;

/**
 * A factory for creating UserConfigLoader objects.
 */
@Slf4j
public final class UserConfigLoaderFactory {
  private UserConfigLoaderFactory() {

  }

  /** The conf. */
  private static HiveConf conf;

  /** The user config loader. */
  private static UserConfigLoader userConfigLoader;

  /**
   * Inits the.
   *
   * @param c the c
   */
  public static void init(HiveConf c) {
    conf = c;
    userConfigLoader = null;
  }

  /**
   * The Enum ResolverType.
   */
  public enum ResolverType {

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

  public static UserConfigLoader getUserConfigLoader() {
    if (userConfigLoader == null) {
      userConfigLoader = initializeUserConfigLoader();
    }
    return userConfigLoader;
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
    for (ResolverType type : ResolverType.values()) {
      if (type.name().equals(resolverType)) {
        return createUserConfigLoader(type);
      }
    }
    throw new UserConfigLoaderException("user resolver type not determined. provided value: " + resolverType);
  }

  /**
   * Gets the query user resolver.
   *
   * @param resolverType the resolver type
   * @return the query user resolver
   */
  public static UserConfigLoader createUserConfigLoader(ResolverType resolverType) {
    switch (resolverType) {
    case PROPERTYBASED:
      return new PropertyBasedUserConfigLoader(conf);
    case DATABASE:
      return new DatabaseUserConfigLoader(conf);
    case LDAP_BACKED_DATABASE:
      return new LDAPBackedDatabaseUserConfigLoader(conf);
    case CUSTOM:
      try {
        return (conf.getClass(USER_RESOLVER_CUSTOM_CLASS, UserConfigLoader.class, UserConfigLoader.class))
          .getConstructor(HiveConf.class).newInstance(conf);
      } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException | InstantiationException e) {
        throw new UserConfigLoaderException(e);
      }
    case FIXED:
    default:
      return new FixedUserConfigLoader(conf);
    }
  }

  /**
   * Gets the user config.
   *
   * @param loggedInUser the logged in user
   * @return the user config
   */
  public static Map<String, String> getUserConfig(String loggedInUser) {
    try {
      Map<String, String> config = getUserConfigLoader().getUserConfig(loggedInUser);
      if (config == null) {
        throw new UserConfigLoaderException("Got null User config for: " + loggedInUser);
      }
      return config;
    } catch (RuntimeException e) {
      log.error("Couldn't get user config for user: " + loggedInUser, e);
      throw e;
    }
  }
}

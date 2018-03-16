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

import static org.apache.lens.server.api.LensConfConstants.*;
import static org.apache.lens.server.api.LensConfConstants.USER_GROUP_CUSTOM_CLASS;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.user.UserConfigLoader;
import org.apache.lens.server.api.user.UserConfigLoaderException;
import org.apache.lens.server.user.FixedUserConfigLoader;

import org.apache.hadoop.hive.conf.HiveConf;

import org.eclipse.persistence.annotations.TimeOfDay;

import lombok.extern.slf4j.Slf4j;

/**
 * A factory for creating UserGroupConfigLoader objects.
 */
@Slf4j
public final class UserGroupLoaderFactory {
  private UserGroupLoaderFactory() {

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
   * The Enum GroupType.
   */

  public enum GroupType {

    /** The adgroup. */
    AD_GROUP,

    /** The custom. */
    CUSTOM,

    /** The fixed. */
    FIXED
  }

  public static UserConfigLoader getUserConfigLoader() {
    if (userConfigLoader == null) {
      userConfigLoader = initializeUserGroupConfigLoader();
    }
    return userConfigLoader;
  }

  /**
   * Initialize user config loader.
   *
   * @return the user config loader
   */
  public static UserConfigLoader initializeUserGroupConfigLoader() {
    String groupType = conf.get(LensConfConstants.USER_GROUP_TYPE);
    if (groupType == null || groupType.length() == 0) {
      throw new UserConfigLoaderException("user group type not determined. value was not provided in conf");
    }
    for (GroupType type : GroupType.values()) {
      if (type.name().equals(groupType)) {
        return createUserGroupConfigLoader(type);
      }
    }
    throw new UserConfigLoaderException("user resolver type not determined. provided value: " + groupType);
  }

  /**
   * Gets the query user resolver.
   *
   * @param groupType the resolver type
   * @return the query user resolver
   */
  public static UserConfigLoader createUserGroupConfigLoader(GroupType groupType) {
    switch (groupType) {
    case AD_GROUP:
    case CUSTOM:
      try {
        return (conf.getClass(USER_GROUP_CUSTOM_CLASS, UserConfigLoader.class, UserConfigLoader.class))
          .getConstructor(HiveConf.class).newInstance(conf);
      } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException | InstantiationException e) {
        throw new UserConfigLoaderException(e);
      }
    case FIXED:
    default:
      return new FixedUserGroupConfigLoader(conf);
    }
  }

  /**
   * Gets the user group grconfig.
   *
   * @param loggedInUser the logged in user
   * @return the user config
   */
  public static Map<String, String> getUserGroupConfig(String loggedInUser) {
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

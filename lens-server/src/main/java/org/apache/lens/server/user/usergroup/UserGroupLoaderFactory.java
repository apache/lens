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

import static org.apache.lens.server.api.LensConfConstants.USER_GROUP_CUSTOM_CLASS;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.user.UserGroupConfigLoader;
import org.apache.lens.server.api.user.UserGroupLoaderException;

import org.apache.hadoop.hive.conf.HiveConf;

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

  /** The user group config loader. */
  private static UserGroupConfigLoader userGroupConfigLoader;

  /**
   * Inits the.
   *
   * @param c the c
   */
  public static void init(HiveConf c) {
    conf = c;
    userGroupConfigLoader = null;
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

  public static UserGroupConfigLoader getUserGroupConfigLoader() {
    if (userGroupConfigLoader == null) {
      userGroupConfigLoader = initializeUserGroupConfigLoader();
    }
    return userGroupConfigLoader;
  }

  /**
   * Initialize user config loader.
   *
   * @return the user config loader
   */
  public static UserGroupConfigLoader initializeUserGroupConfigLoader() {
    String groupType = conf.get(LensConfConstants.USER_GROUP_TYPE);
    if (groupType == null || groupType.length() == 0) {
      throw new UserGroupLoaderException("user group type not determined. value was not provided in conf");
    }
    for (GroupType type : GroupType.values()) {
      if (type.name().equals(groupType)) {
        return createUserGroupConfigLoader(type);
      }
    }
    throw new UserGroupLoaderException("user resolver type not determined. provided value: " + groupType);
  }

  /**
   * Gets the query user resolver.
   *
   * @param groupType the resolver type
   * @return the query user resolver
   */
  public static UserGroupConfigLoader createUserGroupConfigLoader(GroupType groupType) {
    switch (groupType) {
    case AD_GROUP:
      return new ADGroupConfigLoader(conf);
    case CUSTOM:
      try {
        return (conf.getClass(USER_GROUP_CUSTOM_CLASS, UserGroupConfigLoader.class, UserGroupConfigLoader.class))
          .getConstructor(HiveConf.class).newInstance(conf);
      } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException | InstantiationException e) {
        throw new UserGroupLoaderException(e);
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
      Map<String, String> config = getUserGroupConfigLoader().getUserConfig(loggedInUser);
      if (config == null) {
        throw new UserGroupLoaderException("Got null User Group config for: " + loggedInUser);
      }
      return config;
    } catch (RuntimeException e) {
      log.error("Couldn't get user Group config for user: " + loggedInUser, e);
      throw e;
    }
  }
}

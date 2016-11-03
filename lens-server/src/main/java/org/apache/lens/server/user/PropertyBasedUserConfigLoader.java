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

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.user.UserConfigLoader;
import org.apache.lens.server.api.user.UserConfigLoaderException;

import org.apache.hadoop.hive.conf.HiveConf;

import com.google.common.collect.Maps;

/**
 * The Class PropertyBasedUserConfigLoader.
 */
public class PropertyBasedUserConfigLoader implements UserConfigLoader {

  /** The user map. */
  private HashMap<String, Map<String, String>> userMap = Maps.newHashMap();

  /**
   * Instantiates a new property based user config loader.
   *
   * @param conf the conf
   * @throws UserConfigLoaderException the user config loader exception
   */
  public PropertyBasedUserConfigLoader(HiveConf conf) throws UserConfigLoaderException {
    Properties properties = new Properties();
    String filename = conf.get(LensConfConstants.USER_RESOLVER_PROPERTYBASED_FILENAME, null);
    if (filename == null) {
      throw new UserConfigLoaderException("property file path not provided for property based resolver."
        + "Please set property " + LensConfConstants.USER_RESOLVER_PROPERTYBASED_FILENAME);
    }
    try (InputStreamReader reader = new InputStreamReader(new FileInputStream(new File(filename)), "UTF-8")) {
      properties.load(reader);
    } catch (IOException e) {
      throw new UserConfigLoaderException("property file not found. Provided path was: " + filename);
    }
    for (Object o : properties.keySet()) {
      String key = (String) o;
      String[] userAndPropkey = key.split("\\.", 2);
      String user = userAndPropkey[0];
      String propKey = userAndPropkey[1];
      if (!userMap.containsKey(user)) {
        userMap.put(user, new HashMap<String, String>());
      }
      userMap.get(user).put(propKey, properties.getProperty(key));
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.user.UserConfigLoader#getUserConfig(java.lang.String)
   */
  @Override
  public Map<String, String> getUserConfig(String loggedInUser) {
    return userMap.get(loggedInUser) == null ? userMap.get("*") : userMap.get(loggedInUser);
  }
}

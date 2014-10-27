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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * The Class PropertyBasedUserConfigLoader.
 */
public class PropertyBasedUserConfigLoader extends UserConfigLoader {

  /** The user map. */
  private HashMap<String, Map<String, String>> userMap;

  /**
   * Instantiates a new property based user config loader.
   *
   * @param conf
   *          the conf
   * @throws UserConfigLoaderException
   *           the user config loader exception
   */
  public PropertyBasedUserConfigLoader(HiveConf conf) throws UserConfigLoaderException {
    super(conf);
    userMap = new HashMap<String, Map<String, String>>();
    Properties properties = new Properties();
    String filename = hiveConf.get(LensConfConstants.USER_RESOLVER_PROPERTYBASED_FILENAME, null);
    if (filename == null) {
      throw new UserConfigLoaderException("property file path not provided for property based resolver."
          + "Please set property " + LensConfConstants.USER_RESOLVER_PROPERTYBASED_FILENAME);
    }
    try {
      properties.load(new InputStreamReader(new FileInputStream(new File(filename))));
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

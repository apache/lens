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

import java.util.HashMap;
import java.util.Map;

/**
 * The Class FixedUserConfigLoader.
 */
public class FixedUserConfigLoader extends UserConfigLoader {

  /**
   * Instantiates a new fixed user config loader.
   *
   * @param conf
   *          the conf
   */
  public FixedUserConfigLoader(HiveConf conf) {
    super(conf);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.user.UserConfigLoader#getUserConfig(java.lang.String)
   */
  @Override
  public Map<String, String> getUserConfig(String loggedInUser) {
    HashMap<String, String> userConfig = new HashMap<String, String>();
    userConfig.put(LensConfConstants.SESSION_CLUSTER_USER, hiveConf.get(LensConfConstants.USER_RESOLVER_FIXED_VALUE));
    return userConfig;
  }
}

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

import java.util.HashMap;
import java.util.Map;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.user.UserGroupConfigLoader;

import org.apache.hadoop.hive.conf.HiveConf;

import com.google.common.collect.Maps;

public class FixedUserGroupConfigLoader implements UserGroupConfigLoader {

  private final String fixedValue;

  public FixedUserGroupConfigLoader(HiveConf conf) {
    fixedValue = conf.get(LensConfConstants.USER_GROUP_FIXED_VALUE);
  }

  /*
     * (non-Javadoc)
     *
     * @see org.apache.lens.server.user.UserGroupConfigLoader#getUserConfig(java.lang.String)
     */
  @Override
  public Map<String, String> getUserConfig(String loggedInUser) {
    HashMap<String, String> userConfig = Maps.newHashMap();
    userConfig.put(LensConfConstants.SESSION_USER_GROUPS, fixedValue);
    return userConfig;
  }
}


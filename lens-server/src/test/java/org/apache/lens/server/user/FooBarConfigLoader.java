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
import org.apache.lens.server.user.UserConfigLoader;

import java.util.HashMap;
import java.util.Map;

/**
 * The Class FooBarConfigLoader.
 */
public class FooBarConfigLoader extends UserConfigLoader {

  /** The const hash map. */
  public static HashMap<String, String> CONST_HASH_MAP = new HashMap<String, String>() {
    {
      put("key", "value");
    }
  };

  /**
   * Instantiates a new foo bar config loader.
   *
   * @param conf
   *          the conf
   */
  public FooBarConfigLoader(HiveConf conf) {
    super(conf);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.user.UserConfigLoader#getUserConfig(java.lang.String)
   */
  @Override
  public Map<String, String> getUserConfig(String loggedInUser) {
    return CONST_HASH_MAP;
  }
}

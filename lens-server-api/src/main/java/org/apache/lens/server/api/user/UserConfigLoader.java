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
package org.apache.lens.server.api.user;

import java.util.Map;

import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.QueryContext;

import org.apache.hadoop.hive.conf.HiveConf;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class UserConfigLoader. It's initialized once in the server lifetime. After that, t:
 *    1. Gets session configs for the user on each session open. This config applies to the particular session
 *       and is forwarded for all actions. One Use case is to decide driver specific details e.g. priority/queue of
 *       all queries of the user.
 *    2. Provides a pre-submit hook. Just before submission
 */
@Slf4j
public abstract class UserConfigLoader {

  /** The hive conf. */
  protected final HiveConf hiveConf;

  /**
   * Instantiates a new user config loader.
   *
   * @param conf the conf
   */
  protected UserConfigLoader(HiveConf conf) {
    this.hiveConf = conf;
  }

  /**
   * Gets the user config.
   *
   * @param loggedInUser the logged in user
   * @return the user config
   * @throws UserConfigLoaderException the user config loader exception
   */
  public abstract Map<String, String> getUserConfig(String loggedInUser) throws UserConfigLoaderException;

  public void preSubmit(QueryContext ctx) throws LensException {
    log.debug("Pre submit " + ctx + " on " + ctx.getSelectedDriver());
  }
}

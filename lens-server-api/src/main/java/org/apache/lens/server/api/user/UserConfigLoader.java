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

/**
 * The Class UserConfigLoader. It's initialized once in the server lifetime. After that, it's job is to
 * Get session configs for the user on each session open. This config applies to the particular session
 * and is forwarded for all actions. One Use case is to decide driver specific details e.g. priority/queue of
 * all queries of the user.
 */
public interface UserConfigLoader {

  /**
   * Gets the user config.
   *
   * @param loggedInUser the logged in user
   * @return the user config
   * @throws UserConfigLoaderException the user config loader exception
   */
  Map<String, String> getUserConfig(String loggedInUser) throws UserConfigLoaderException;
}

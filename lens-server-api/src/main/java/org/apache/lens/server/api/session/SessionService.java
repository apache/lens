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

package org.apache.lens.server.api.session;

import java.util.List;
import java.util.Map;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.session.SessionPerUserInfo;
import org.apache.lens.api.session.UserSessionInfo;
import org.apache.lens.server.api.error.LensException;

public interface SessionService {

  /** Name of session service */
  String NAME = "session";

  /**
   * Open session.
   *
   * @param username      the username
   * @param password      the password
   * @param database      Set current database to the supplied value
   * @param configuration the configuration
   * @return the lens session handle
   * @throws LensException the lens exception
   */

  LensSessionHandle openSession(String username, String password, String database,
                                Map<String, String> configuration)
    throws LensException;

  /**
   * Restore session from previous instance of lens server.
   *
   * @param sessionHandle the session handle
   * @param userName      the user name
   * @param password      the password
   * @throws LensException the lens exception
   */

  void restoreSession(LensSessionHandle sessionHandle, String userName, String password,
                      Map<String, String> configuration) throws LensException;

  /**
   * Close session.
   *
   * @param sessionHandle the session handle
   * @throws LensException the lens exception
   */

  void closeSession(LensSessionHandle sessionHandle) throws LensException;

  /**
   * Close idle sessions.
   *
   * @throws LensException the lens exception
   */

  void cleanupIdleSessions() throws LensException;
  /**
   * Adds the resource.
   *
   * @param sessionHandle the session handle
   * @param type          the type
   * @param path          the path
   */

  void addResource(LensSessionHandle sessionHandle, String type, String path);

  /**
   * Delete resource.
   *
   * @param sessionHandle the session handle
   * @param type          the type
   * @param path          the path
   */

  void deleteResource(LensSessionHandle sessionHandle, String type, String path);


  /**
   * Gets the all session parameters.
   *
   * @param sessionHandle the sessionid
   * @param verbose       the verbose
   * @param key           the key
   * @return the all session parameters
   * @throws LensException the lens exception
   */
  List<String> getAllSessionParameters(LensSessionHandle sessionHandle, boolean verbose, String key)
    throws LensException;

  /**
   * Sets the session parameter.
   *
   * @param sessionHandle the sessionid
   * @param key           the key
   * @param value         the value
   */
  void setSessionParameter(LensSessionHandle sessionHandle, String key, String value);

  /**
   * Lists resources from the session service
   *
   * @param sessionHandle the sessionid
   * @param type          the resource type, can be null, file or jar
   * @return   Lists resources for a given resource type.
   *           Lists all resources if resource type is null
   */
  List<String> listAllResources(LensSessionHandle sessionHandle, String type);

  /**
   * Returns true if the session is open
   */
  boolean isOpen(LensSessionHandle sessionHandle);

  /**
   *
   * @return a list of all sessions
   */
  List<UserSessionInfo> getSessionInfo();

  /**
   *
   * @return a map of all session per loggedin user
   */
  List<SessionPerUserInfo> getSessionPerUser();
}

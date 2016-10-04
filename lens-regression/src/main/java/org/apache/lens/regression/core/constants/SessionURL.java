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

package org.apache.lens.regression.core.constants;

public class SessionURL {

  private SessionURL() {

  }

  public static final String SESSION_BASE_URL = "/session";
  public static final String SESSION_PARAMS_URL = SESSION_BASE_URL + "/params";
  public static final String SESSION_ADD_RESOURCE_URL = SESSION_BASE_URL + "/resources/add";
  public static final String SESSION_REMOVE_RESOURCE_URL = SESSION_BASE_URL + "/resources/delete";
  public static final String SESSION_LIST_RESOURCE_URL = SESSION_BASE_URL + "/resources/list";
  public static final String SESSIONS_LIST_URL = SESSION_BASE_URL + "/sessions";

}

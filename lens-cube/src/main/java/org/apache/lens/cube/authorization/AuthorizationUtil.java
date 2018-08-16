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
package org.apache.lens.cube.authorization;

import java.util.HashSet;
import java.util.Set;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.authorization.ActionType;
import org.apache.lens.server.api.authorization.Authorizer;
import org.apache.lens.server.api.authorization.LensPrivilegeObject;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.save.exception.PrivilegeException;

import org.apache.hadoop.conf.Configuration;

/*
Helper class for all Authorization needs
*/
public class AuthorizationUtil {

  private AuthorizationUtil(){}

  public static boolean isAuthorized(Authorizer authorizer, String tableName,
    LensPrivilegeObject.LensPrivilegeObjectType privilegeObjectType, ActionType actionType, Configuration configuration)
    throws LensException {
    return isAuthorized(authorizer, tableName, null, privilegeObjectType, actionType, configuration);
  }

  public static boolean isAuthorized(Authorizer authorizer, String tableName, String colName,
    LensPrivilegeObject.LensPrivilegeObjectType privilegeObjectType, ActionType actionType, Configuration configuration)
    throws LensException {
    String user = null;
    Set<String> userGroups = new HashSet<>();
    if (configuration.getBoolean(LensConfConstants.USER_NAME_BASED_AUTHORIZATION,
      LensConfConstants.DEFAULT_USER_NAME_AUTHORIZATION)){
      user = configuration.get(LensConfConstants.SESSION_LOGGEDIN_USER);
    }
    if (configuration.getBoolean(LensConfConstants.USER_GROUPS_BASED_AUTHORIZATION,
      LensConfConstants.DEFAULT_USER_GROUPS_AUTHORIZATION)) {
      userGroups = (Set<String>)
        configuration.getTrimmedStringCollection(LensConfConstants.SESSION_USER_GROUPS);
    }
    LensPrivilegeObject lp = new LensPrivilegeObject(privilegeObjectType, tableName, colName);
    if (!authorizer.authorize(lp, actionType, user, userGroups)) {
      throw new PrivilegeException(privilegeObjectType.toString(), tableName, actionType.toString());
    }
    return true;
  }
}

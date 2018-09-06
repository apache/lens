/*
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
package org.apache.lens.cube.parse;

import java.util.HashSet;
import java.util.Set;

import org.apache.lens.server.api.authorization.ActionType;
import org.apache.lens.server.api.authorization.Authorizer;
import org.apache.lens.server.api.authorization.LensPrivilegeObject;

import lombok.Getter;

public class MockAuthorizer implements Authorizer {

  @Getter
  Set<String> authorizedUserGroups;
  MockAuthorizer(){
    init();
  }

  public void init(){
    this.authorizedUserGroups = new HashSet<>();
    this.authorizedUserGroups.add("lens-auth-test1");
  }
  @Override
  public boolean authorize(LensPrivilegeObject lensPrivilegeObject, ActionType accessType, String user,
    Set<String> userGroups) {
    //check query authorization
    if (lensPrivilegeObject.getTable().equals("basecube") && accessType.equals(ActionType.SELECT)) {
      userGroups.retainAll(getAuthorizedUserGroups());
      return !userGroups.isEmpty();
    }
    // check metastore schema authorization
    if (lensPrivilegeObject.getTable().equals("TestCubeMetastoreClient") && accessType.equals(ActionType.UPDATE)) {
      userGroups.retainAll(getAuthorizedUserGroups());
      return !userGroups.isEmpty();
    }
    return false;
  }
}

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

import java.util.Set;

import org.apache.lens.server.api.authorization.ActionType;
import org.apache.lens.server.api.authorization.Authorizer;
import org.apache.lens.server.api.authorization.LensPrivilegeObject;

import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

// Apache Ranger implementation for Authorization in Lens

@Slf4j
public class RangerLensAuthorizer implements Authorizer {

  @Getter
  private RangerBasePlugin rangerBasePlugin;

  RangerLensAuthorizer() {
    this.init();
  }

  public void init() {
    rangerBasePlugin = new RangerBasePlugin("lens", "lens");
    rangerBasePlugin.setResultProcessor(new RangerDefaultAuditHandler());
    rangerBasePlugin.init();
  }

  @Override
  public boolean authorize(LensPrivilegeObject lensPrivilegeObject, ActionType accessType, String user,
    Set<String> userGroups) {

    log.info("==> Lens Ranger Authorize User : "+ user + "User groups : " + userGroups + " Accesstype : "
      + accessType + "Object : "+ lensPrivilegeObject.getTable());

    RangerLensResource rangerLensResource = getLensResource(lensPrivilegeObject);

    boolean res = false;

    if (rangerLensResource != null) {
      RangerAccessRequest rangerAccessRequest = new RangerAccessRequestImpl(rangerLensResource,
        accessType.toString().toLowerCase(), user, userGroups);
      RangerAccessResult rangerAccessResult = getRangerBasePlugin().isAccessAllowed(rangerAccessRequest);
      res = rangerAccessResult != null && rangerAccessResult.getIsAllowed();
    }

    log.info("<== Lens Ranger Authorize User : "+ user + " User groups : " + userGroups + " Accesstype : "+ accessType
      + " Object : "+ lensPrivilegeObject.getTable() + " Access : "+ res);

    return res;
  }

  private RangerLensResource getLensResource(LensPrivilegeObject lensPrivilegeObject) {

    RangerLensResource lensResource = null;
    switch (lensPrivilegeObject.getObjectType()) {
    case COLUMN:
      lensResource = new RangerLensResource(LensObjectType.COLUMN, lensPrivilegeObject.getTable(),
        lensPrivilegeObject.getColumn());
      break;
    case DATABASE:
      lensResource = new RangerLensResource(LensObjectType.TABLE, lensPrivilegeObject.getTable(), null);
      break;

    case NONE:
    default:
      break;
    }
    return lensResource;
  }

  enum LensObjectType {NONE, TABLE, COLUMN}

  ;
}

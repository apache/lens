/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.api.query.save.exception;

import static org.apache.lens.api.error.LensCommonErrorCode.NOT_AUTHORIZED;

import org.apache.lens.server.api.LensErrorInfo;
import org.apache.lens.server.api.error.LensException;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * The class PrivilegeException. Thrown when the user is
 * not having the required privileges to complete the action.
 */
@EqualsAndHashCode(callSuper = true)
@ToString
public class PrivilegeException extends LensException {

  @Getter
  private final String resourceType;
  @Getter
  private final String resourceIdentifier;
  @Getter
  private final String privilege;

  public PrivilegeException(String resourceType, String resourceIdentifier, String privilege) {
    super(
      new LensErrorInfo(NOT_AUTHORIZED.getValue(), 0, NOT_AUTHORIZED.toString())
      , privilege
      , resourceType
      , resourceIdentifier);
    this.resourceType = resourceType;
    this.resourceIdentifier = resourceIdentifier;
    this.privilege = privilege;
  }
}

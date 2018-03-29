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
package org.apache.lens.server.api.authorization;

import lombok.Data;

@Data
public class LensPrivilegeObject {

  private final LensPrivilegeObjectType objectType;
  private final String cubeOrFactOrDim;
  private final String column;

  public LensPrivilegeObject(LensPrivilegeObjectType objectType, String cubeOrFactOrDim) {
    this(objectType, cubeOrFactOrDim, null);
  }

  public LensPrivilegeObject(LensPrivilegeObjectType objectType, String cubeOrFactOrDim, String column) {
    this.objectType = objectType;
    this.cubeOrFactOrDim = cubeOrFactOrDim;
    this.column = column;
  }

  public enum LensPrivilegeObjectType {
    DATABASE, CUBE, FACT, DIMENSION, DIMENSIONTABLE, STORAGE, COLUMN, NONE, SEGMENTATION
  };

}



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

import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

public class RangerLensResource extends RangerAccessResourceImpl {

  public static final String KEY_TABLE = "table";
  public static final String KEY_COLUMN = "column";

  RangerLensResource(RangerLensAuthorizer.LensObjectType objectType, String cubeOrDimOrFact, String column) {

    switch(objectType) {

    case COLUMN:
      setValue(KEY_TABLE, cubeOrDimOrFact);
      setValue(KEY_COLUMN, column);
      break;

    case TABLE:
      setValue(KEY_TABLE, cubeOrDimOrFact);
      break;

    case NONE:
    default:
      break;
    }
  }
}

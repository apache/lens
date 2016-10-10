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
package org.apache.lens.cube.parse;

import java.util.Set;

interface TrackQueriedCubeFields {

  /**
   * Get queried dim attributes
   *
   * @return set of dim attribute names
   */
  Set<String> getQueriedDimAttrs();

  /**
   * Get queried measures
   *
   * @return set of measure names
   */
  Set<String> getQueriedMsrs();

  /**
   * Get queried expr columns
   *
   * @return set of expr column names
   */
  Set<String> getQueriedExprColumns();

  /**
   * Add queried dim attribute
   *
   * @param attrName attribute name
   */
  void addQueriedDimAttr(String attrName);

  /**
   * Add queried measure
   *
   * @param msrName measure name
   */
  void addQueriedMsr(String msrName);

  /**
   * Add queried expression column
   *
   * @param exprCol expression column name
   */
  void addQueriedExprColumn(String exprCol);
}

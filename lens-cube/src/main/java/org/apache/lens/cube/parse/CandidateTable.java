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

import java.util.Collection;

import org.apache.lens.cube.metadata.AbstractCubeTable;

/**
 * Candidate table interface
 * 
 */
interface CandidateTable {

  /**
   * Get storage string of the base table alias passed
   * 
   * @param alias
   * 
   * @return storage string
   */
  public String getStorageString(String alias);

  /**
   * Get candidate table
   * 
   * @return Candidate fact or dim table
   */
  public AbstractCubeTable getTable();

  /**
   * Get base table of the candidate table
   * 
   * @return Cube or DerivedCube or Dimesions
   */
  public AbstractCubeTable getBaseTable();

  /**
   * Get name of the candidate table
   * 
   * @return name
   */
  public String getName();

  /**
   * Get columns of candidate table
   * 
   * @return set or list of columns
   */
  public Collection<String> getColumns();
}

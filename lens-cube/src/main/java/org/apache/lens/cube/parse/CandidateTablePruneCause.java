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
/*
 *
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
 *
 */
package org.apache.lens.cube.parse;

import java.util.List;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonWriteNullProperties;

/**
 * Contains the cause why a candidate table is not picked for answering the
 * query
 */
@JsonWriteNullProperties(false)
public class CandidateTablePruneCause {

  public enum CubeTableCause {
    // invalid cube table
    INVALID,
    // column not found in cube table
    COLUMN_NOT_FOUND,
    // column not valid in cube table
    COLUMN_NOT_VALID,
    // missing partitions for cube table
    MISSING_PARTITIONS,
    // missing storage tables for cube table
    MISSING_STORAGES,
    // no candidate storges for cube table, storage cause will have why each
    // storage is not a candidate
    NO_CANDIDATE_STORAGES,
    // cube table has more weight
    MORE_WEIGHT,
    // cube table has more partitions
    MORE_PARTITIONS,
    // cube table is an aggregated fact and queried column is not under default
    // aggregate
    MISSING_DEFAULT_AGGREGATE, NO_FACT_UPDATE_PERIODS_FOR_GIVEN_RANGE, NO_COLUMN_PART_OF_A_JOIN_PATH,
    // candidate table tries to get denormalized field from dimension and the
    // referred dimension is invalid.
    INVALID_DENORM_TABLE
  }

  public enum SkipStorageCause {
    // invalid storage table
    INVALID,
    // storage table does not exist
    TABLE_NOT_EXIST,
    // storage has no update periods queried
    MISSING_UPDATE_PERIODS,
    // no candidate update periods, update period cause will have why each
    // update period is not a candidate
    NO_CANDIDATE_PERIODS,
    // storage table has no partitions queried
    NO_PARTITIONS,
    // partition column does not exist
    PART_COL_DOES_NOT_EXIST,
    // storage is not supported by execution engine
    UNSUPPORTED;
  }

  public enum SkipUpdatePeriodCause {
    // invalid update period
    INVALID,
    // Query max interval is more than update period
    QUERY_INTERVAL_BIGGER
  }

  private String cubeTableName;
  // cause for cube table
  private CubeTableCause cause;
  // storage to skip storage cause
  private Map<String, SkipStorageCause> storageCauses;
  // storage to update period to skip cause
  private Map<String, Map<String, SkipUpdatePeriodCause>> updatePeriodCauses;
  // populated only incase of missing partitions cause
  private List<String> missingPartitions;
  // populated only incase of missing update periods cause
  private List<String> missingUpdatePeriods;

  public CandidateTablePruneCause() {
  }

  public CandidateTablePruneCause(String name, CubeTableCause cause) {
    this.cubeTableName = name;
    this.cause = cause;
  }

  public CandidateTablePruneCause(String name, Map<String, SkipStorageCause> storageCauses) {
    this.cubeTableName = name;
    this.storageCauses = storageCauses;
    this.cause = CubeTableCause.NO_CANDIDATE_STORAGES;
  }

  public CandidateTablePruneCause(String name, Map<String, SkipStorageCause> storageCauses,
      Map<String, Map<String, SkipUpdatePeriodCause>> updatePeriodCauses) {
    this.cubeTableName = name;
    this.storageCauses = storageCauses;
    this.cause = CubeTableCause.NO_CANDIDATE_STORAGES;
    this.updatePeriodCauses = updatePeriodCauses;
  }

  /**
   * Get name of table
   * 
   * @return name
   */
  public String getCubeTableName() {
    return cubeTableName;
  }

  /**
   * Set name
   * 
   * @param name
   */
  public void setCubeTableName(String name) {
    this.cubeTableName = name;
  }

  /**
   * @return the cause
   */
  public CubeTableCause getCause() {
    return cause;
  }

  /**
   * @param cause
   *          the cause to set
   */
  public void setCause(CubeTableCause cause) {
    this.cause = cause;
  }

  /**
   * @return the storageCauses
   */
  public Map<String, SkipStorageCause> getStorageCauses() {
    return storageCauses;
  }

  /**
   * @param storageCauses
   *          the storageCauses to set
   */
  public void setStorageCauses(Map<String, SkipStorageCause> storageCauses) {
    this.storageCauses = storageCauses;
  }

  /**
   * @return the updatePeriodCauses
   */
  public Map<String, Map<String, SkipUpdatePeriodCause>> getUpdatePeriodCauses() {
    return updatePeriodCauses;
  }

  /**
   * @param updatePeriodCauses
   *          the updatePeriodCauses to set
   */
  public void setUpdatePeriodCauses(Map<String, Map<String, SkipUpdatePeriodCause>> updatePeriodCauses) {
    this.updatePeriodCauses = updatePeriodCauses;
  }

  /**
   * @return the missingPartitions
   */
  public List<String> getMissingPartitions() {
    return missingPartitions;
  }

  /**
   * @param missingPartitions
   *          the missingPartitions to set
   */
  public void setMissingPartitions(List<String> missingPartitions) {
    this.missingPartitions = missingPartitions;
  }

  /**
   * @return the missingUpdatePeriods
   */
  public List<String> getMissingUpdatePeriods() {
    return missingUpdatePeriods;
  }

  /**
   * @param missingUpdatePeriods
   *          the missingUpdatePeriods to set
   */
  public void setMissingUpdatePeriods(List<String> missingUpdatePeriods) {
    this.missingUpdatePeriods = missingUpdatePeriods;
  }
}

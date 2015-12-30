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

package org.apache.lens.cube.metadata;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;

public class StoragePartitionDesc extends AddPartitionDesc.OnePartitionDesc {

  private static final long serialVersionUID = 1L;

  private Map<String, Date> timePartSpec;
  private Map<String, String> nonTimePartSpec;
  private UpdatePeriod updatePeriod;
  private Map<String, String> fullPartSpec;
  private String cubeTableName;

  public StoragePartitionDesc() {
  }

  public StoragePartitionDesc(String cubeTableName, Map<String, Date> timePartSpec,
    Map<String, String> nonTimePartSpec, UpdatePeriod updatePeriod) {
    this.cubeTableName = cubeTableName;
    this.timePartSpec = timePartSpec;
    this.nonTimePartSpec = nonTimePartSpec;
    this.updatePeriod = updatePeriod;
  }

  /**
   * @return the cubeTableName
   */
  public String getCubeTableName() {
    return cubeTableName;
  }

  /**
   * @param cubeTableName the cubeTableName to set
   */
  public void setCubeTableName(String cubeTableName) {
    this.cubeTableName = cubeTableName;
  }

  /**
   * @deprecated Use getStoragePartSpec
   */
  @Override
  @Deprecated
  public Map<String, String> getPartSpec() {
    return super.getPartSpec();
  }

  public Map<String, String> getStoragePartSpec() {
    if (fullPartSpec == null) {
      fullPartSpec = new HashMap<String, String>();
      for (Map.Entry<String, Date> entry : timePartSpec.entrySet()) {
        fullPartSpec.put(entry.getKey(), updatePeriod.format(entry.getValue()));
      }
      if (nonTimePartSpec != null) {
        fullPartSpec.putAll(nonTimePartSpec);
      }
    }
    return fullPartSpec;
  }

  /**
   * @return the timePartSpec
   */
  public Map<String, Date> getTimePartSpec() {
    return timePartSpec;
  }

  /**
   * @param timePartSpec the timePartSpec to set
   */
  public void setTimePartSpec(Map<String, Date> timePartSpec) {
    this.timePartSpec = timePartSpec;
  }

  /**
   * @return the nonTimePartSpec
   */
  public Map<String, String> getNonTimePartSpec() {
    return nonTimePartSpec;
  }

  /**
   * @param nonTimePartSpec the nonTimePartSpec to set
   */
  public void setNonTimePartSpec(Map<String, String> nonTimePartSpec) {
    this.nonTimePartSpec = nonTimePartSpec;
  }

  /**
   * @return the updatePeriod
   */
  public UpdatePeriod getUpdatePeriod() {
    return updatePeriod;
  }

  /**
   * @param updatePeriod the updatePeriod to set
   */
  public void setUpdatePeriod(UpdatePeriod updatePeriod) {
    this.updatePeriod = updatePeriod;
  }

}

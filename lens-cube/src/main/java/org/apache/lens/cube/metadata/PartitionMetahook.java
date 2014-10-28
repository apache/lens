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

import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;

public interface PartitionMetahook {

  /**
   * Called before calling add partition
   * 
   * @param StoragePartitionDesc
   * @throws HiveException
   */
  public void preAddPartition(StoragePartitionDesc StoragePartitionDesc) throws HiveException;

  /**
   * Called after successfully adding the partition
   * 
   * @param StoragePartitionDesc
   * @throws HiveException
   */
  public void commitAddPartition(StoragePartitionDesc StoragePartitionDesc) throws HiveException;

  /**
   * Called if add partition fails.
   * 
   * @param StoragePartitionDesc
   * @throws HiveException
   */
  public void rollbackAddPartition(StoragePartitionDesc StoragePartitionDesc) throws HiveException;

  /**
   * Called before calling drop partition
   * 
   * @param storageTableName
   * @param partVals
   * @throws HiveException
   */
  public void preDropPartition(String storageTableName, List<String> partVals) throws HiveException;

  /**
   * Called after successfully droping the partition
   * 
   * @param storageTableName
   * @param partVals
   * @throws HiveException
   */
  public void commitDropPartition(String storageTableName, List<String> partVals) throws HiveException;

  /**
   * Called if drop partition fails.
   * 
   * @param storageTableName
   * @param partVals
   */
  public void rollbackDropPartition(String storageTableName, List<String> partVals) throws HiveException;
}

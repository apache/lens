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
import org.apache.hadoop.hive.ql.metadata.Table;

public class HDFSStorage extends Storage {

  public HDFSStorage(String name) {
    super(name, null);
  }

  public HDFSStorage(Table hiveTable) {
    super(hiveTable);
  }

  @Override
  public void preAddPartitions(List<StoragePartitionDesc> addPartitionDesc) throws HiveException {
    // No op

  }

  @Override
  public void commitAddPartitions(List<StoragePartitionDesc> addPartitionDesc) throws HiveException {
    // No op

  }

  @Override
  public void rollbackAddPartitions(List<StoragePartitionDesc> addPartitionDesc) throws HiveException {
    // No op

  }

  @Override
  public void preDropPartition(String storageTableName, List<String> partVals) throws HiveException {
    // No op

  }

  @Override
  public void commitDropPartition(String storageTableName, List<String> partVals) throws HiveException {
    // No op

  }

  @Override
  public void rollbackDropPartition(String storageTableName, List<String> partVals) throws HiveException {
    // No op

  }
}

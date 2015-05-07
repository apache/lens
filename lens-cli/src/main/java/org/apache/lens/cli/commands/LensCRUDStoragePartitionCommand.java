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
package org.apache.lens.cli.commands;

import java.io.IOException;
import java.util.List;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.metastore.XPartition;
import org.apache.lens.api.metastore.XStorageTableElement;

import com.google.common.base.Joiner;

public abstract class LensCRUDStoragePartitionCommand<T> extends LensCRUDCommand<T> {
  public String showAll(String filter) {
    List<String> all = getAll(filter);
    if (all == null || all.isEmpty()) {
      return "No " + getSingleObjectName() + " found" + (filter == null ? "" : " for " + filter);
    }
    return Joiner.on("\n").join(all);
  }

  public String showAllStorages(String tableName) {
    String sep = "";
    StringBuilder sb = new StringBuilder();
    List<String> storages = getAllStorages(tableName);
    if (storages != null) {
      for (String storage : storages) {
        if (!storage.isEmpty()) {
          sb.append(sep).append(storage);
          sep = "\n";
        }
      }
    }
    String ret = sb.toString();
    return ret.isEmpty() ? "No storage found for " + tableName : ret;
  }

  public String addStorage(String tableName, String path) {
    return doAddStorage(tableName, getValidPath(path)).toString().toLowerCase();
  }

  public String getStorage(String tableName, String storage) {
    try {
      return formatJson(mapper.writer(pp).writeValueAsString(readStorage(tableName, storage)));
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public String dropStorage(String tableName, String storageName) {
    return doDropStorage(tableName, storageName).toString().toLowerCase();
  }

  public String dropAllStorages(String tableName) {
    return doDropAllStorages(tableName).toString();
  }

  public String getAllPartitions(String tableName, String storageName, String filter) {
    try {
      return formatJson(mapper.writer(pp).writeValueAsString(readAllPartitions(tableName, storageName, filter)));
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public String addPartition(String tableName, String storageName, String path) {
    return doAddPartition(tableName, storageName, getValidPath(path)).toString().toLowerCase();
  }

  public String addPartitions(String tableName, String storageName, String path) {
    return doAddPartitions(tableName, storageName, getValidPath(path)).toString().toLowerCase();
  }

  public String dropPartitions(String tableName, String storageName, String filter) {
    return doDropPartitions(tableName, storageName, filter).toString().toLowerCase();
  }

  protected abstract List<String> getAll(String filter);

  public abstract List<String> getAllStorages(String name);

  public abstract APIResult doAddStorage(String name, String path);

  protected abstract XStorageTableElement readStorage(String tableName, String storage);

  public abstract APIResult doDropStorage(String tableName, String storageName);

  public abstract APIResult doDropAllStorages(String name);

  protected abstract List<XPartition> readAllPartitions(String tableName, String storageName, String filter);

  protected abstract APIResult doAddPartition(String tableName, String storageName, String path);

  protected abstract APIResult doAddPartitions(String tableName, String storageName, String path);

  protected abstract APIResult doDropPartitions(String tableName, String storageName, String filter);
}

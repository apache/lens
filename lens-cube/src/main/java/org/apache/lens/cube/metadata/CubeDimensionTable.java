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

import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.collect.Sets;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class CubeDimensionTable extends AbstractCubeTable {
  private String dimName; // dimension name the dimtabe belongs to
  private final Map<String, UpdatePeriod> snapshotDumpPeriods = new HashMap<>();

  public CubeDimensionTable(String dimName, String dimTblName, List<FieldSchema> columns, double weight,
    Map<String, UpdatePeriod> snapshotDumpPeriods) {
    this(dimName, dimTblName, columns, weight, snapshotDumpPeriods, new HashMap<String, String>());
  }

  public CubeDimensionTable(String dimName, String dimTblName, List<FieldSchema> columns, double weight,
    Set<String> storages) {
    this(dimName, dimTblName, columns, weight, getSnapshotDumpPeriods(storages), new HashMap<String, String>());
  }

  public CubeDimensionTable(String dimName, String dimTblName, List<FieldSchema> columns, double weight,
    Set<String> storages, Map<String, String> properties) {
    this(dimName, dimTblName, columns, weight, getSnapshotDumpPeriods(storages), properties);
  }

  public CubeDimensionTable(String dimName, String dimTblName, List<FieldSchema> columns, double weight,
    Map<String, UpdatePeriod> snapshotDumpPeriods, Map<String, String> properties) {
    super(dimTblName, columns, properties, weight);
    this.dimName = dimName;
    if (snapshotDumpPeriods != null) {
      this.snapshotDumpPeriods.putAll(snapshotDumpPeriods);
    }
    addProperties();
  }


  private static Map<String, UpdatePeriod> getSnapshotDumpPeriods(Set<String> storages) {
    Map<String, UpdatePeriod> snapshotDumpPeriods = new HashMap<>();
    for (String storage : storages) {
      snapshotDumpPeriods.put(storage, null);
    }
    return snapshotDumpPeriods;
  }

  public CubeDimensionTable(Table tbl) {
    super(tbl);
    this.dimName = getDimName(getName(), getProperties());
    Map<String, UpdatePeriod> dumpPeriods = getDumpPeriods(getName(), getProperties());
    if (dumpPeriods != null) {
      this.snapshotDumpPeriods.putAll(dumpPeriods);
    }
  }

  public Set<String> getPartCols() {
    Set<String> partCols = Sets.newHashSet();
    String partColsStr = getProperties().get(MetastoreUtil.getDimTablePartsKey(getName()));
    if (partColsStr != null) {
      for (String s : StringUtils.split(partColsStr, ",")) {
        if (!StringUtils.isBlank(s)) {
          partCols.add(s);
        }
      }
    }
    return partCols;
  }

  @Override
  public CubeTableType getTableType() {
    return CubeTableType.DIM_TABLE;
  }

  @Override
  protected void addProperties() {
    super.addProperties();
    setDimName(getName(), getProperties(), dimName);
    setSnapshotDumpPeriods(getName(), getProperties(), snapshotDumpPeriods);
  }


  public Map<String, UpdatePeriod> getSnapshotDumpPeriods() {
    return snapshotDumpPeriods;
  }

  public String getDimName() {
    return dimName;
  }

  private static void setSnapshotDumpPeriods(String name, Map<String, String> props,
    Map<String, UpdatePeriod> snapshotDumpPeriods) {
    if (snapshotDumpPeriods != null) {
      props.put(MetastoreUtil.getDimensionStorageListKey(name), MetastoreUtil.getStr(snapshotDumpPeriods.keySet()));
      for (Map.Entry<String, UpdatePeriod> entry : snapshotDumpPeriods.entrySet()) {
        if (entry.getValue() != null) {
          props.put(MetastoreUtil.getDimensionDumpPeriodKey(name, entry.getKey()), entry.getValue().name());
        }
      }
    }
  }

  private static void setDimName(String dimTblName, Map<String, String> props, String dimName) {
    props.put(MetastoreUtil.getDimNameKey(dimTblName), dimName);
  }

  static String getDimName(String dimTblName, Map<String, String> props) {
    return props.get(MetastoreUtil.getDimNameKey(dimTblName));
  }

  private static Map<String, UpdatePeriod> getDumpPeriods(String name, Map<String, String> params) {
    String storagesStr = params.get(MetastoreUtil.getDimensionStorageListKey(name));
    if (!StringUtils.isBlank(storagesStr)) {
      Map<String, UpdatePeriod> dumpPeriods = new HashMap<>();
      for (String storage : StringUtils.split(storagesStr, ",")) {
        String dumpPeriod = params.get(MetastoreUtil.getDimensionDumpPeriodKey(name, storage));
        if (dumpPeriod != null) {
          dumpPeriods.put(storage, UpdatePeriod.valueOf(dumpPeriod));
        } else {
          dumpPeriods.put(storage, null);
        }
      }
      return dumpPeriods;
    }
    return null;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    CubeDimensionTable other = (CubeDimensionTable) obj;

    if (this.getDimName() == null) {
      if (other.getDimName() != null) {
        return false;
      }
    } else {
      if (!this.getDimName().equals(other.getDimName())) {
        return false;
      }
    }
    if (this.getSnapshotDumpPeriods() == null) {
      if (other.getSnapshotDumpPeriods() != null) {
        return false;
      }
    } else {
      if (!this.getSnapshotDumpPeriods().equals(other.getSnapshotDumpPeriods())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Set<String> getStorages() {
    return snapshotDumpPeriods.keySet();
  }

  public boolean hasStorageSnapshots(String storage) {
    return (snapshotDumpPeriods.get(storage) != null);
  }

  /**
   * Alter the dimension name that the table belongs to
   *
   * @param newDimName new dimension name.
   */
  public void alterUberDim(String newDimName) {
    this.dimName = newDimName;
    setDimName(getName(), getProperties(), this.dimName);
  }

  /**
   * Alter snapshot dump period of a storage
   *
   * @param storage Storage name
   * @param period  The new value
   */
  public void alterSnapshotDumpPeriod(@NonNull String storage, UpdatePeriod period) {
    if (snapshotDumpPeriods.containsKey(storage)) {
      log.info("Updating dump period for {} from {} to {}", storage, snapshotDumpPeriods.get(storage), period);
    }

    snapshotDumpPeriods.put(storage, period);
    setSnapshotDumpPeriods(getName(), getProperties(), snapshotDumpPeriods);
  }

  @Override
  public void alterColumn(FieldSchema column) {
    super.alterColumn(column);
  }

  @Override
  public void addColumns(Collection<FieldSchema> columns) {
    super.addColumns(columns);
  }

  void dropStorage(String storage) {
    snapshotDumpPeriods.remove(storage);
    setSnapshotDumpPeriods(getName(), getProperties(), snapshotDumpPeriods);
  }
}

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

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.*;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.TextInputFormat;

import com.google.common.collect.Maps;

/**
 * Storage is Named Interface which would represent the underlying storage of the data.
 */
public abstract class Storage extends AbstractCubeTable implements PartitionMetahook {

  private static final List<FieldSchema> COLUMNS = new ArrayList<FieldSchema>();

  static {
    COLUMNS.add(new FieldSchema("dummy", "string", "dummy column"));
  }

  protected Storage(String name, Map<String, String> properties) {
    super(name, COLUMNS, properties, 0L);
    addProperties();
  }

  public Storage(Table hiveTable) {
    super(hiveTable);
  }

  /**
   * Get the name prefix of the storage
   *
   * @return Name followed by storage separator
   */
  public String getPrefix() {
    return getPrefix(getName());
  }

  @Override
  public CubeTableType getTableType() {
    return CubeTableType.STORAGE;
  }

  @Override
  public Set<String> getStorages() {
    throw new NotImplementedException();
  }

  @Override
  protected void addProperties() {
    super.addProperties();
    getProperties().put(MetastoreUtil.getStorageClassKey(getName()), getClass().getCanonicalName());
  }

  /**
   * Get the name prefix of the storage
   *
   * @param name Name of the storage
   * @return Name followed by storage separator
   */
  public static String getPrefix(String name) {
    return name + StorageConstants.STORGAE_SEPARATOR;
  }

  public static final class LatestInfo {
    Map<String, LatestPartColumnInfo> latestParts = new HashMap<String, LatestPartColumnInfo>();
    Partition part = null;

    void addLatestPartInfo(String partCol, LatestPartColumnInfo partInfo) {
      latestParts.put(partCol, partInfo);
    }

    void setPart(Partition part) {
      this.part = part;
    }
  }

  public static final class LatestPartColumnInfo extends HashMap<String, String> {

    public LatestPartColumnInfo(Map<String, String> partParams) {
      putAll(partParams);
    }

    public Map<String, String> getPartParams(Map<String, String> parentParams) {
      putAll(parentParams);
      return this;
    }
  }

  /**
   * Get the storage table descriptor for the given parent table.
   *
   * @param client The metastore client
   * @param parent Is either Fact or Dimension table
   * @param crtTbl Create table info
   * @return Table describing the storage table
   * @throws HiveException
   */
  public Table getStorageTable(Hive client, Table parent, StorageTableDesc crtTbl) throws HiveException {
    String storageTableName = MetastoreUtil.getStorageTableName(parent.getTableName(), this.getPrefix());
    Table tbl = client.getTable(storageTableName, false);
    if (tbl == null) {
      tbl = client.newTable(storageTableName);
    }
    tbl.getTTable().setSd(new StorageDescriptor(parent.getTTable().getSd()));

    if (crtTbl.getTblProps() != null) {
      tbl.getTTable().getParameters().putAll(crtTbl.getTblProps());
    }

    if (crtTbl.getPartCols() != null) {
      tbl.setPartCols(crtTbl.getPartCols());
    }
    if (crtTbl.getNumBuckets() != -1) {
      tbl.setNumBuckets(crtTbl.getNumBuckets());
    }

    if (!StringUtils.isBlank(crtTbl.getStorageHandler())) {
      tbl.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE,
        crtTbl.getStorageHandler());
    }
    HiveStorageHandler storageHandler = tbl.getStorageHandler();

    if (crtTbl.getSerName() == null) {
      if (storageHandler == null || storageHandler.getSerDeClass() == null) {
        tbl.setSerializationLib(org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
      } else {
        String serDeClassName = storageHandler.getSerDeClass().getName();
        tbl.setSerializationLib(serDeClassName);
      }
    } else {
      // let's validate that the serde exists
      tbl.setSerializationLib(crtTbl.getSerName());
    }

    if (crtTbl.getFieldDelim() != null) {
      tbl.setSerdeParam(serdeConstants.FIELD_DELIM, crtTbl.getFieldDelim());
      tbl.setSerdeParam(serdeConstants.SERIALIZATION_FORMAT, crtTbl.getFieldDelim());
    }
    if (crtTbl.getFieldEscape() != null) {
      tbl.setSerdeParam(serdeConstants.ESCAPE_CHAR, crtTbl.getFieldEscape());
    }

    if (crtTbl.getCollItemDelim() != null) {
      tbl.setSerdeParam(serdeConstants.COLLECTION_DELIM, crtTbl.getCollItemDelim());
    }
    if (crtTbl.getMapKeyDelim() != null) {
      tbl.setSerdeParam(serdeConstants.MAPKEY_DELIM, crtTbl.getMapKeyDelim());
    }
    if (crtTbl.getLineDelim() != null) {
      tbl.setSerdeParam(serdeConstants.LINE_DELIM, crtTbl.getLineDelim());
    }

    if (crtTbl.getSerdeProps() != null) {
      for (Entry<String, String> m : crtTbl.getSerdeProps().entrySet()) {
        tbl.setSerdeParam(m.getKey(), m.getValue());
      }
    }

    if (crtTbl.getBucketCols() != null) {
      tbl.setBucketCols(crtTbl.getBucketCols());
    }
    if (crtTbl.getSortCols() != null) {
      tbl.setSortCols(crtTbl.getSortCols());
    }
    if (crtTbl.getComment() != null) {
      tbl.setProperty("comment", crtTbl.getComment());
    }
    if (crtTbl.getLocation() != null) {
      tbl.setDataLocation(new Path(crtTbl.getLocation()));
    }

    if (crtTbl.getSkewedColNames() != null) {
      tbl.setSkewedColNames(crtTbl.getSkewedColNames());
    }
    if (crtTbl.getSkewedColValues() != null) {
      tbl.setSkewedColValues(crtTbl.getSkewedColValues());
    }

    tbl.setStoredAsSubDirectories(crtTbl.isStoredAsSubDirectories());

    if (crtTbl.getInputFormat() != null) {
      tbl.setInputFormatClass(crtTbl.getInputFormat());
    } else {
      tbl.setInputFormatClass(TextInputFormat.class.getName());
    }
    if (crtTbl.getOutputFormat() != null) {
      tbl.setOutputFormatClass(crtTbl.getOutputFormat());
    } else {
      tbl.setOutputFormatClass(IgnoreKeyTextOutputFormat.class.getName());
    }

    tbl.getTTable().getSd().setInputFormat(tbl.getInputFormatClass().getName());
    tbl.getTTable().getSd().setOutputFormat(tbl.getOutputFormatClass().getName());

    if (crtTbl.isExternal()) {
      tbl.setProperty("EXTERNAL", "TRUE");
      tbl.setTableType(TableType.EXTERNAL_TABLE);
    }
    return tbl;
  }

  /**
   * Add single partition to storage. Just calls #addPartitions.
   * @param client
   * @param addPartitionDesc
   * @param latestInfo
   * @throws HiveException
   */
  public List<Partition> addPartition(Hive client, StoragePartitionDesc addPartitionDesc, LatestInfo latestInfo)
    throws HiveException {
    Map<Map<String, String>, LatestInfo> latestInfos = Maps.newHashMap();
    latestInfos.put(addPartitionDesc.getNonTimePartSpec(), latestInfo);
    return addPartitions(client, addPartitionDesc.getCubeTableName(), addPartitionDesc.getUpdatePeriod(),
      Collections.singletonList(addPartitionDesc), latestInfos);
  }

  /**
   * Add given partitions in the underlying hive table and update latest partition links
   *
   * @param client                hive client instance
   * @param factOrDimTable        fact or dim name
   * @param updatePeriod          update period of partitions.
   * @param storagePartitionDescs all partitions to be added
   * @param latestInfos           new latest info. atleast one partition for the latest value exists for each part
   *                              column
   * @throws HiveException
   */
  public List<Partition> addPartitions(Hive client, String factOrDimTable, UpdatePeriod updatePeriod,
    List<StoragePartitionDesc> storagePartitionDescs,
    Map<Map<String, String>, LatestInfo> latestInfos) throws HiveException {
    preAddPartitions(storagePartitionDescs);
    Map<Map<String, String>, Map<String, Integer>> latestPartIndexForPartCols = Maps.newHashMap();
    boolean success = false;
    try {
      String tableName = MetastoreUtil.getStorageTableName(factOrDimTable, this.getPrefix());
      String dbName = SessionState.get().getCurrentDatabase();
      AddPartitionDesc addParts = new AddPartitionDesc(dbName, tableName, true);
      Table storageTbl = client.getTable(dbName, tableName);
      for (StoragePartitionDesc addPartitionDesc : storagePartitionDescs) {
        String location = null;
        if (addPartitionDesc.getLocation() != null) {
          Path partLocation = new Path(addPartitionDesc.getLocation());
          if (partLocation.isAbsolute()) {
            location = addPartitionDesc.getLocation();
          } else {
            location = new Path(storageTbl.getPath(), partLocation).toString();
          }
        }
        Map<String, String> partParams = addPartitionDesc.getPartParams();
        if (partParams == null) {
          partParams = new HashMap<String, String>();
        }
        partParams.put(MetastoreConstants.PARTITION_UPDATE_PERIOD, addPartitionDesc.getUpdatePeriod().name());
        addParts.addPartition(addPartitionDesc.getStoragePartSpec(), location);
        int curIndex = addParts.getPartitionCount() - 1;
        addParts.getPartition(curIndex).setPartParams(partParams);
        addParts.getPartition(curIndex).setInputFormat(addPartitionDesc.getInputFormat());
        addParts.getPartition(curIndex).setOutputFormat(addPartitionDesc.getOutputFormat());
        addParts.getPartition(curIndex).setNumBuckets(addPartitionDesc.getNumBuckets());
        addParts.getPartition(curIndex).setCols(addPartitionDesc.getCols());
        addParts.getPartition(curIndex).setSerializationLib(addPartitionDesc.getSerializationLib());
        addParts.getPartition(curIndex).setSerdeParams(addPartitionDesc.getSerdeParams());
        addParts.getPartition(curIndex).setBucketCols(addPartitionDesc.getBucketCols());
        addParts.getPartition(curIndex).setSortCols(addPartitionDesc.getSortCols());
        if (latestInfos != null && latestInfos.get(addPartitionDesc.getNonTimePartSpec()) != null) {
          for (Map.Entry<String, LatestPartColumnInfo> entry : latestInfos
            .get(addPartitionDesc.getNonTimePartSpec()).latestParts.entrySet()) {
            if (addPartitionDesc.getTimePartSpec().containsKey(entry.getKey())
              && entry.getValue().get(MetastoreUtil.getLatestPartTimestampKey(entry.getKey())).equals(
                updatePeriod.format(addPartitionDesc.getTimePartSpec().get(entry.getKey())))) {
              if (latestPartIndexForPartCols.get(addPartitionDesc.getNonTimePartSpec()) == null) {
                latestPartIndexForPartCols.put(addPartitionDesc.getNonTimePartSpec(),
                  Maps.<String, Integer>newHashMap());
              }
              latestPartIndexForPartCols.get(addPartitionDesc.getNonTimePartSpec()).put(entry.getKey(), curIndex);
            }
          }
        }
      }
      if (latestInfos != null) {
        for (Map.Entry<Map<String, String>, LatestInfo> entry1 : latestInfos.entrySet()) {
          Map<String, String> nonTimeParts = entry1.getKey();
          LatestInfo latestInfo = entry1.getValue();
          for (Map.Entry<String, LatestPartColumnInfo> entry : latestInfo.latestParts.entrySet()) {
            // symlink this partition to latest
            List<Partition> latest;
            String latestPartCol = entry.getKey();
            try {
              latest = client
                .getPartitionsByFilter(storageTbl, StorageConstants.getLatestPartFilter(latestPartCol, nonTimeParts));
            } catch (Exception e) {
              throw new HiveException("Could not get latest partition", e);
            }
            if (!latest.isEmpty()) {
              client.dropPartition(storageTbl.getTableName(), latest.get(0).getValues(), false);
            }
            if (latestPartIndexForPartCols.get(nonTimeParts).containsKey(latestPartCol)) {
              AddPartitionDesc.OnePartitionDesc latestPartWithFullTimestamp = addParts.getPartition(
                latestPartIndexForPartCols.get(nonTimeParts).get(latestPartCol));
              addParts.addPartition(
                StorageConstants.getLatestPartSpec(latestPartWithFullTimestamp.getPartSpec(), latestPartCol),
                latestPartWithFullTimestamp.getLocation());
              int curIndex = addParts.getPartitionCount() - 1;
              addParts.getPartition(curIndex).setPartParams(entry.getValue().getPartParams(
                latestPartWithFullTimestamp.getPartParams()));
              addParts.getPartition(curIndex).setInputFormat(latestPartWithFullTimestamp.getInputFormat());
              addParts.getPartition(curIndex).setOutputFormat(latestPartWithFullTimestamp.getOutputFormat());
              addParts.getPartition(curIndex).setNumBuckets(latestPartWithFullTimestamp.getNumBuckets());
              addParts.getPartition(curIndex).setCols(latestPartWithFullTimestamp.getCols());
              addParts.getPartition(curIndex).setSerializationLib(latestPartWithFullTimestamp.getSerializationLib());
              addParts.getPartition(curIndex).setSerdeParams(latestPartWithFullTimestamp.getSerdeParams());
              addParts.getPartition(curIndex).setBucketCols(latestPartWithFullTimestamp.getBucketCols());
              addParts.getPartition(curIndex).setSortCols(latestPartWithFullTimestamp.getSortCols());
            }
          }
        }
      }
      List<Partition> partitionsAdded = client.createPartitions(addParts);
      success = true;
      return partitionsAdded;
    } finally {
      if (success) {
        commitAddPartitions(storagePartitionDescs);
      } else {
        rollbackAddPartitions(storagePartitionDescs);
      }
    }
  }

  /**
   * Update existing partition
   * @param client          hive client instance
   * @param fact            fact name
   * @param partition       partition to be updated
   * @throws InvalidOperationException
   * @throws HiveException
   */
  public void updatePartition(Hive client, String fact, Partition partition)
    throws InvalidOperationException, HiveException {
    client.alterPartition(MetastoreUtil.getFactOrDimtableStorageTableName(fact, getName()), partition);
  }

  /**
   * Update existing partitions
   * @param client          hive client instance
   * @param fact            fact name
   * @param partitions      partitions to be updated
   * @throws InvalidOperationException
   * @throws HiveException
   */
  public void updatePartitions(Hive client, String fact, List<Partition> partitions)
    throws InvalidOperationException, HiveException {
    boolean success = false;
    try {
      client.alterPartitions(MetastoreUtil.getFactOrDimtableStorageTableName(fact, getName()), partitions);
      success = true;
    } finally {
      if (success) {
        commitUpdatePartition(partitions);
      } else {
        rollbackUpdatePartition(partitions);
      }
    }
  }

  /**
   * Drop the partition in the underlying hive table and update latest partition link
   *
   * @param client           The metastore client
   * @param storageTableName TableName
   * @param partVals         Partition specification
   * @param updateLatestInfo The latest partition info if it needs update, null if latest should not be updated
   * @param nonTimePartSpec
   * @throws HiveException
   */
  public void dropPartition(Hive client, String storageTableName, List<String> partVals,
    Map<String, LatestInfo> updateLatestInfo, Map<String, String> nonTimePartSpec) throws HiveException {
    preDropPartition(storageTableName, partVals);
    boolean success = false;
    try {
      client.dropPartition(storageTableName, partVals, false);
      String dbName = SessionState.get().getCurrentDatabase();
      Table storageTbl = client.getTable(storageTableName);
      // update latest info
      if (updateLatestInfo != null) {
        for (Entry<String, LatestInfo> entry : updateLatestInfo.entrySet()) {
          String latestPartCol = entry.getKey();
          // symlink this partition to latest
          List<Partition> latestParts;
          try {
            latestParts = client.getPartitionsByFilter(storageTbl,
              StorageConstants.getLatestPartFilter(latestPartCol, nonTimePartSpec));
            MetastoreUtil.filterPartitionsByNonTimeParts(latestParts, nonTimePartSpec, latestPartCol);
          } catch (Exception e) {
            throw new HiveException("Could not get latest partition", e);
          }
          if (!latestParts.isEmpty()) {
            assert latestParts.size() == 1;
            client.dropPartition(storageTbl.getTableName(), latestParts.get(0).getValues(), false);
          }
          LatestInfo latest = entry.getValue();
          if (latest != null && latest.part != null) {
            AddPartitionDesc latestPart = new AddPartitionDesc(dbName, storageTableName, true);
            latestPart.addPartition(StorageConstants.getLatestPartSpec(latest.part.getSpec(), latestPartCol),
              latest.part.getLocation());
            latestPart.getPartition(0).setPartParams(
              latest.latestParts.get(latestPartCol).getPartParams(latest.part.getParameters()));
            latestPart.getPartition(0).setInputFormat(latest.part.getInputFormatClass().getCanonicalName());
            latestPart.getPartition(0).setOutputFormat(latest.part.getOutputFormatClass().getCanonicalName());
            latestPart.getPartition(0).setNumBuckets(latest.part.getBucketCount());
            latestPart.getPartition(0).setCols(latest.part.getCols());
            latestPart.getPartition(0).setSerializationLib(
              latest.part.getTPartition().getSd().getSerdeInfo().getSerializationLib());
            latestPart.getPartition(0).setSerdeParams(
              latest.part.getTPartition().getSd().getSerdeInfo().getParameters());
            latestPart.getPartition(0).setBucketCols(latest.part.getBucketCols());
            latestPart.getPartition(0).setSortCols(latest.part.getSortCols());
            client.createPartitions(latestPart);
          }
        }
      }
      success = true;
    } finally {
      if (success) {
        commitDropPartition(storageTableName, partVals);
      } else {
        rollbackDropPartition(storageTableName, partVals);
      }
    }
  }

  static Storage createInstance(Table tbl) throws HiveException {
    String storageName = tbl.getTableName();
    String storageClassName = tbl.getParameters().get(MetastoreUtil.getStorageClassKey(storageName));
    try {
      Class<?> clazz = Class.forName(storageClassName);
      Constructor<?> constructor = clazz.getConstructor(Table.class);
      return (Storage) constructor.newInstance(tbl);
    } catch (Exception e) {
      throw new HiveException("Could not create storage class" + storageClassName, e);
    }
  }
}

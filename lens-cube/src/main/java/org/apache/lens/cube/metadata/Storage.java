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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * 
 * Storage is Named Interface which would represent the underlying storage of
 * the data.
 * 
 */
public abstract class Storage extends AbstractCubeTable implements PartitionMetahook {

  private static final List<FieldSchema> columns = new ArrayList<FieldSchema>();
  static {
    columns.add(new FieldSchema("dummy", "string", "dummy column"));
  }

  protected Storage(String name, Map<String, String> properties) {
    super(name, columns, properties, 0L);
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
   * @param name
   *          Name of the storage
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

  public static final class LatestPartColumnInfo {
    final Map<String, String> partParams = new HashMap<String, String>();

    public LatestPartColumnInfo(Map<String, String> partParams) {
      this.partParams.putAll(partParams);
    }

    public Map<String, String> getPartParams(Map<String, String> parentParams) {
      partParams.putAll(parentParams);
      return partParams;
    }
  }

  /**
   * Get the storage table descriptor for the given parent table.
   * 
   * @param client
   *          The metastore client
   * @param parent
   *          Is either Fact or Dimension table
   * @param crtTbl
   *          Create table info
   * @return Table describing the storage table
   * 
   * @throws HiveException
   */
  public Table getStorageTable(Hive client, Table parent, StorageTableDesc crtTbl) throws HiveException {
    String storageTableName = MetastoreUtil.getStorageTableName(parent.getTableName(), this.getPrefix());
    Table tbl = client.newTable(storageTableName);
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

    if (crtTbl.getStorageHandler() != null) {
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
      Iterator<Entry<String, String>> iter = crtTbl.getSerdeProps().entrySet().iterator();
      while (iter.hasNext()) {
        Entry<String, String> m = iter.next();
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
   * Add a partition in the underlying hive table and update latest partition
   * links
   * 
   * @param client
   *          The metastore client
   * @param addPartitionDesc
   *          add Partition specification
   * @param latestInfo
   *          The latest partition info, null if latest should not be created
   * 
   * @throws HiveException
   */
  public void addPartition(Hive client, StoragePartitionDesc addPartitionDesc, LatestInfo latestInfo)
      throws HiveException {
    preAddPartition(addPartitionDesc);
    boolean success = false;
    try {
      String tableName = MetastoreUtil.getStorageTableName(addPartitionDesc.getCubeTableName(), this.getPrefix());
      String dbName = SessionState.get().getCurrentDatabase();
      Table storageTbl = client.getTable(dbName, tableName);
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
      AddPartitionDesc addParts = new AddPartitionDesc(dbName, tableName, true);
      addParts.addPartition(addPartitionDesc.getStoragePartSpec(), location);
      addParts.getPartition(0).setPartParams(partParams);
      addParts.getPartition(0).setInputFormat(addPartitionDesc.getInputFormat());
      addParts.getPartition(0).setOutputFormat(addPartitionDesc.getOutputFormat());
      addParts.getPartition(0).setNumBuckets(addPartitionDesc.getNumBuckets());
      addParts.getPartition(0).setCols(addPartitionDesc.getCols());
      addParts.getPartition(0).setSerializationLib(addPartitionDesc.getSerializationLib());
      addParts.getPartition(0).setSerdeParams(addPartitionDesc.getSerdeParams());
      addParts.getPartition(0).setBucketCols(addPartitionDesc.getBucketCols());
      addParts.getPartition(0).setSortCols(addPartitionDesc.getSortCols());
      client.createPartitions(addParts);

      if (latestInfo != null) {
        for (Map.Entry<String, LatestPartColumnInfo> entry : latestInfo.latestParts.entrySet()) {
          // symlink this partition to latest
          List<Partition> latest;
          String latestPartCol = entry.getKey();
          try {
            latest = client.getPartitionsByFilter(storageTbl, StorageConstants.getLatestPartFilter(latestPartCol));
          } catch (Exception e) {
            throw new HiveException("Could not get latest partition", e);
          }
          if (!latest.isEmpty()) {
            client.dropPartition(storageTbl.getTableName(), latest.get(0).getValues(), false);
          }
          AddPartitionDesc latestPart = new AddPartitionDesc(dbName, tableName, true);
          latestPart.addPartition(
              StorageConstants.getLatestPartSpec(addPartitionDesc.getStoragePartSpec(), latestPartCol), location);
          latestPart.getPartition(0).setPartParams(entry.getValue().getPartParams(partParams));
          latestPart.getPartition(0).setInputFormat(addPartitionDesc.getInputFormat());
          latestPart.getPartition(0).setOutputFormat(addPartitionDesc.getOutputFormat());
          latestPart.getPartition(0).setNumBuckets(addPartitionDesc.getNumBuckets());
          latestPart.getPartition(0).setCols(addPartitionDesc.getCols());
          latestPart.getPartition(0).setSerializationLib(addPartitionDesc.getSerializationLib());
          latestPart.getPartition(0).setSerdeParams(addPartitionDesc.getSerdeParams());
          latestPart.getPartition(0).setBucketCols(addPartitionDesc.getBucketCols());
          latestPart.getPartition(0).setSortCols(addPartitionDesc.getSortCols());
          client.createPartitions(latestPart);
        }
      }
      commitAddPartition(addPartitionDesc);
      success = true;
    } finally {
      if (!success) {
        rollbackAddPartition(addPartitionDesc);
      }
    }
  }

  /**
   * Drop the partition in the underlying hive table and update latest partition
   * link
   * 
   * @param client
   *          The metastore client
   * @param storageTableName
   *          TableName
   * @param partSpec
   *          Partition specification
   * @param latestInfo
   *          The latest partition info if it needs update, null if latest
   *          should not be updated
   * 
   * @throws HiveException
   */
  public void dropPartition(Hive client, String storageTableName, List<String> partVals,
      Map<String, LatestInfo> updateLatestInfo) throws HiveException {
    preDropPartition(storageTableName, partVals);
    boolean success = false;
    try {
      client.dropPartition(storageTableName, partVals, false);
      String dbName = SessionState.get().getCurrentDatabase();
      Table storageTbl = client.getTable(storageTableName);
      // update latest info
      for (Map.Entry<String, LatestInfo> entry : updateLatestInfo.entrySet()) {
        String latestPartCol = entry.getKey();
        // symlink this partition to latest
        List<Partition> latestParts;
        try {
          latestParts = client.getPartitionsByFilter(storageTbl, StorageConstants.getLatestPartFilter(latestPartCol));
        } catch (Exception e) {
          throw new HiveException("Could not get latest partition", e);
        }
        if (!latestParts.isEmpty()) {
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
          latestPart.getPartition(0).setSerdeParams(latest.part.getTPartition().getSd().getSerdeInfo().getParameters());
          latestPart.getPartition(0).setBucketCols(latest.part.getBucketCols());
          latestPart.getPartition(0).setSortCols(latest.part.getSortCols());
          client.createPartitions(latestPart);
        }
      }
      commitDropPartition(storageTableName, partVals);
    } finally {
      if (!success) {
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
      Storage storage = (Storage) constructor.newInstance(new Object[] { tbl });
      return storage;
    } catch (Exception e) {
      throw new HiveException("Could not create storage class" + storageClassName, e);
    }
  }
}

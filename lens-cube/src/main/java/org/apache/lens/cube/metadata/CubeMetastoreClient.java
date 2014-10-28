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

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.lens.cube.metadata.Storage.LatestInfo;
import org.apache.lens.cube.metadata.Storage.LatestPartColumnInfo;
import org.apache.thrift.TException;

/**
 * Wrapper class around Hive metastore to do cube metastore operations.
 * 
 */
public class CubeMetastoreClient {
  private final HiveConf config;
  private final boolean enableCaching;

  private CubeMetastoreClient(HiveConf conf) {
    this.config = conf;
    this.enableCaching = conf.getBoolean(MetastoreConstants.METASTORE_ENABLE_CACHING, true);
  }

  // map from table name to Table
  private final Map<String, Table> allHiveTables = new HashMap<String, Table>();
  // map from dimension name to Dimension
  private final Map<String, Dimension> allDims = new HashMap<String, Dimension>();
  // map from cube name to Cube
  private final Map<String, CubeInterface> allCubes = new HashMap<String, CubeInterface>();
  // map from dimtable name to CubeDimensionTable
  private final Map<String, CubeDimensionTable> allDimTables = new HashMap<String, CubeDimensionTable>();
  // map from fact name to fact table
  private final Map<String, CubeFactTable> allFactTables = new HashMap<String, CubeFactTable>();
  // map from storage name to storage
  private final Map<String, Storage> allStorages = new HashMap<String, Storage>();

  // HiveConf to client mapping
  private static final Map<HiveConf, CubeMetastoreClient> clientMapping =
      new HashMap<HiveConf, CubeMetastoreClient>();

  private SchemaGraph schemaGraph;

  /**
   * Get the instance of {@link CubeMetastoreClient} corresponding to
   * {@link HiveConf}
   * 
   * @param conf
   * @return CubeMetastoreClient
   * @throws HiveException
   */
  public static CubeMetastoreClient getInstance(HiveConf conf) throws HiveException {
    if (clientMapping.get(conf) == null) {
      clientMapping.put(conf, new CubeMetastoreClient(conf));
    }
    return clientMapping.get(conf);
  }

  private Hive getClient() throws HiveException {
    return Hive.get(config);
  }

  /**
   * Close the current metastore client
   */
  public static void close() {
    Hive.closeCurrent();
  }

  public void setCurrentDatabase(String currentDatabase) {
    SessionState.get().setCurrentDatabase(currentDatabase);
  }

  public String getCurrentDatabase() {
    return SessionState.get().getCurrentDatabase();
  }

  private void createStorageHiveTable(Table parent, String storage, StorageTableDesc crtTblDesc) throws HiveException {
    try {
      Table tbl = getStorage(storage).getStorageTable(getClient(), parent, crtTblDesc);
      getClient().createTable(tbl);
    } catch (Exception e) {
      throw new HiveException("Exception creating table", e);
    }
  }

  private Table createCubeHiveTable(AbstractCubeTable table) throws HiveException {
    try {
      Table tbl = getClient().newTable(table.getName().toLowerCase());
      tbl.setTableType(TableType.MANAGED_TABLE);
      tbl.getTTable().getSd().setCols(table.getColumns());
      tbl.getTTable().getParameters().putAll(table.getProperties());
      getClient().createTable(tbl);
      return tbl;
    } catch (Exception e) {
      throw new HiveException("Exception creating table", e);
    }
  }

  public void createStorage(Storage storage) throws HiveException {
    createCubeHiveTable(storage);
  }

  /**
   * Create cube in metastore defined by {@link Cube} or {@link DerivedCube}
   * object
   * 
   * @param cube
   *          the {@link Cube} object.
   * @throws HiveException
   */
  public void createCube(CubeInterface cube) throws HiveException {
    createCubeHiveTable((AbstractCubeTable) cube);
  }

  /**
   * Create cube defined by measures and dimensions
   * 
   * @param name
   *          Name of the cube
   * @param measures
   *          Measures of the cube
   * @param dimensions
   *          Dimensions of the cube
   * 
   * @throws HiveException
   */
  public void createCube(String name, Set<CubeMeasure> measures, Set<CubeDimAttribute> dimensions) throws HiveException {
    Cube cube = new Cube(name, measures, dimensions);
    createCube(cube);
  }

  /**
   * Create cube defined by measures, dimensions and properties
   * 
   * @param name
   *          Name of the cube
   * @param measures
   *          Measures of the cube
   * @param dimensions
   *          Dimensions of the cube
   * @param properties
   *          Properties of the cube
   * @throws HiveException
   */
  public void createCube(String name, Set<CubeMeasure> measures, Set<CubeDimAttribute> dimensions,
      Map<String, String> properties) throws HiveException {
    Cube cube = new Cube(name, measures, dimensions, properties);
    createCube(cube);
  }

  /**
   * Create cube defined by measures, dimensions and properties
   * 
   * @param name
   *          Name of the cube
   * @param measures
   *          Measures of the cube
   * @param dimensions
   *          Dimensions of the cube
   * @param properties
   *          Properties of the cube
   * @throws HiveException
   */
  public void createCube(String name, Set<CubeMeasure> measures, Set<CubeDimAttribute> dimensions,
      Set<ExprColumn> expressions, Map<String, String> properties) throws HiveException {
    Cube cube = new Cube(name, measures, dimensions, expressions, properties, 0L);
    createCube(cube);
  }

  /**
   * Create dimension defined by attributes and properties
   * 
   * @param name
   *          Name of the dimension
   * @param attributes
   *          Attributes of the dimension
   * @param properties
   *          Properties of the dimension
   * @param weight
   *          Weight of the dimension
   * 
   * @throws HiveException
   */
  public void createDimension(String name, Set<CubeDimAttribute> attributes, Map<String, String> properties,
      double weight) throws HiveException {
    Dimension dim = new Dimension(name, attributes, properties, weight);
    createDimension(dim);
  }

  /**
   * Create dimension in metastore defined by {@link Dimension} object
   * 
   * @param dim
   *          the {@link Dimension} object.
   * @throws HiveException
   */
  public void createDimension(Dimension dim) throws HiveException {
    createCubeHiveTable(dim);
  }

  /**
   * Create derived cube defined by measures, dimensions and properties
   * 
   * @param parent
   *          Name of the parent cube
   * @param name
   *          Name of the derived cube
   * @param measures
   *          Measures of the derived cube
   * @param dimensions
   *          Dimensions of the derived cube
   * @param properties
   *          Properties of the derived cube
   * @param weight
   *          Weight of the derived cube
   * @throws HiveException
   */
  public void createDerivedCube(String parent, String name, Set<String> measures, Set<String> dimensions,
      Map<String, String> properties, double weight) throws HiveException {
    DerivedCube cube = new DerivedCube(name, measures, dimensions, properties, weight, (Cube) getCube(parent));
    createCube(cube);
  }

  /**
   * Create a cube fact table
   * 
   * @param cubeName
   *          The cube name to which fact belongs to.
   * @param factName
   *          The fact name
   * @param columns
   *          The columns of fact table
   * @param storageAggregatePeriods
   *          Aggregate periods for the storages
   * @param weight
   *          Weight of the cube
   * @param properties
   *          Properties of fact table
   * @param storageTableDescs
   *          Map of storage to its storage table description
   * 
   * @throws HiveException
   */
  public void createCubeFactTable(String cubeName, String factName, List<FieldSchema> columns,
      Map<String, Set<UpdatePeriod>> storageAggregatePeriods, double weight, Map<String, String> properties,
      Map<String, StorageTableDesc> storageTableDescs) throws HiveException {
    CubeFactTable factTable =
        new CubeFactTable(cubeName, factName, columns, storageAggregatePeriods, weight, properties);
    createCubeTable(factTable, storageTableDescs);
  }

  /**
   * Create a cube dimension table
   * 
   * @param dimName
   *          The dimension name to which the dim-table belongs to
   * @param dimTblName
   *          dimension table name
   * @param columns
   *          Columns of the dimension table
   * @param weight
   *          Weight of the dimension table
   * @param dimensionReferences
   *          References to other dimensions
   * @param storages
   *          Storages on which dimension is available without any dumps
   * @param properties
   *          Properties of dimension table
   * @param storageTableDescs
   *          Map of storage to its storage table description
   * 
   * @throws HiveException
   */
  public void createCubeDimensionTable(String dimName, String dimTblName, List<FieldSchema> columns, double weight,
      Set<String> storageNames, Map<String, String> properties, Map<String, StorageTableDesc> storageTableDescs)
      throws HiveException {
    CubeDimensionTable dimTable =
        new CubeDimensionTable(dimName, dimTblName, columns, weight, storageNames, properties);
    createCubeTable(dimTable, storageTableDescs);
  }

  /**
   * Create a cube dimension table
   * 
   * @param dimName
   *          The dimension name to which the dim-table belongs to
   * @param dimTblName
   *          dimension table name
   * @param columns
   *          Columns of the dimension table
   * @param weight
   *          Weight of the dimension table
   * @param dimensionReferences
   *          References to other dimensions
   * @param dumpPeriods
   *          Storage names and their dump periods on which dimension is
   *          available
   * @param properties
   *          properties of dimension table
   * @param storageTableDescs
   *          Map of storage to its storage table description
   * @throws HiveException
   */
  public void createCubeDimensionTable(String dimName, String dimTblName, List<FieldSchema> columns, double weight,
      Map<String, UpdatePeriod> dumpPeriods, Map<String, String> properties,
      Map<String, StorageTableDesc> storageTableDescs) throws HiveException {
    CubeDimensionTable dimTable = new CubeDimensionTable(dimName, dimTblName, columns, weight, dumpPeriods, properties);
    createCubeTable(dimTable, storageTableDescs);
  }

  /**
   * Create cube table defined and create all the corresponding storage tables
   * 
   * @param cubeTable
   *          Can be fact or dimension table
   * @param storageTableDescs
   *          Map of storage to its storage table description
   * @throws HiveException
   */
  public void createCubeTable(AbstractCubeTable cubeTable, Map<String, StorageTableDesc> storageTableDescs)
      throws HiveException {
    // create virtual cube table in metastore
    Table cTable = createCubeHiveTable(cubeTable);

    if (storageTableDescs != null) {
      // create tables for each storage
      for (Map.Entry<String, StorageTableDesc> entry : storageTableDescs.entrySet()) {
        createStorageHiveTable(cTable, entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * Adds storage to fact and creates corresponding storage table
   * 
   * @param fact
   *          The CubeFactTable
   * @param storage
   *          The storage
   * @param updatePeriods
   *          Update periods of the fact on the storage
   * @param storageTableDesc
   *          The storage table description
   * 
   * @throws HiveException
   */
  public void addStorage(CubeFactTable fact, String storage, Set<UpdatePeriod> updatePeriods,
      StorageTableDesc storageTableDesc) throws HiveException {
    fact.addStorage(storage, updatePeriods);
    createStorageHiveTable(getTable(fact.getName()), storage, storageTableDesc);
    alterCubeTable(fact.getName(), getTable(fact.getName()), fact);
    updateFactCache(fact.getName());
  }

  /**
   * Adds storage to dimension and creates corresponding storage table
   * 
   * @param dim
   *          The CubeDimensionTable
   * @param storage
   *          The storage
   * @param dumpPeriod
   *          The dumpPeriod if any, null otherwise
   * @param storageTableDesc
   *          The storage table description
   * 
   * @throws HiveException
   */
  public void addStorage(CubeDimensionTable dim, String storage, UpdatePeriod dumpPeriod,
      StorageTableDesc storageTableDesc) throws HiveException {
    dim.alterSnapshotDumpPeriod(storage, dumpPeriod);
    createStorageHiveTable(getTable(dim.getName()), storage, storageTableDesc);
    alterCubeTable(dim.getName(), getTable(dim.getName()), dim);
    updateDimCache(dim.getName());
  }

  /**
   * Add a partition specified by the storage partition desc on the storage
   * passed.
   * 
   * @param partSpec
   *          The storage partition description
   * @param storage
   *          The storage object
   * 
   * @throws HiveException
   */
  public void addPartition(StoragePartitionDesc partSpec, String storageName) throws HiveException {
    String storageTableName =
        MetastoreUtil.getStorageTableName(partSpec.getCubeTableName(), Storage.getPrefix(storageName));

    getStorage(storageName).addPartition(getClient(), partSpec,
        getLatestInfo(storageTableName, partSpec.getTimePartSpec(), partSpec.getUpdatePeriod()));
  }

  private LatestInfo getLatestInfo(String storageTableName, Map<String, Date> partitionTimestamps,
      UpdatePeriod updatePeriod) throws HiveException {
    Table hiveTable = getHiveTable(storageTableName);
    String timePartColsStr = hiveTable.getTTable().getParameters().get(MetastoreConstants.TIME_PART_COLUMNS);
    if (timePartColsStr != null) {
      LatestInfo latest = new LatestInfo();
      String[] timePartCols = StringUtils.split(timePartColsStr, ',');
      for (String partCol : timePartCols) {
        Date pTimestamp = partitionTimestamps.get(partCol);
        if (pTimestamp == null) {
          continue;
        }
        boolean makeLatest = true;
        Partition part = getLatestPart(storageTableName, partCol);
        Date latestTimestamp = getLatestTimeStamp(part, storageTableName, partCol);
        if (latestTimestamp != null && pTimestamp.before(latestTimestamp)) {
          makeLatest = false;
        }

        if (makeLatest) {
          Map<String, String> latestParams = new HashMap<String, String>();
          latestParams.put(MetastoreUtil.getLatestPartTimestampKey(partCol), updatePeriod.format().format(pTimestamp));
          latest.latestParts.put(partCol, new LatestPartColumnInfo(latestParams));
        }
      }
      return latest;
    } else {
      return null;
    }
  }

  private boolean isLatestPart(Partition part) {
    return part.getValues().contains(StorageConstants.LATEST_PARTITION_VALUE);
  }

  private Date getLatestTimeStamp(Partition part, String storageTableName, String partCol) throws HiveException {
    if (part != null) {
      String latestTimeStampStr = part.getParameters().get(MetastoreUtil.getLatestPartTimestampKey(partCol));
      String latestPartUpdatePeriod = part.getParameters().get(MetastoreConstants.PARTITION_UPDATE_PERIOD);
      UpdatePeriod latestUpdatePeriod = UpdatePeriod.valueOf(latestPartUpdatePeriod.toUpperCase());
      try {
        return latestUpdatePeriod.format().parse(latestTimeStampStr);
      } catch (ParseException e) {
        throw new HiveException(e);
      }
    }
    return null;
  }

  private Date getPartDate(Partition part, int timeColIndex) {
    String partVal = part.getValues().get(timeColIndex);
    String updatePeriodStr = part.getParameters().get(MetastoreConstants.PARTITION_UPDATE_PERIOD);
    Date partDate = null;
    if (updatePeriodStr != null) {
      UpdatePeriod partInterval = UpdatePeriod.valueOf(updatePeriodStr);
      try {
        partDate = partInterval.format().parse(partVal);
      } catch (ParseException e) {
        // ignore
      }
    }
    return partDate;
  }

  private LatestInfo getNextLatest(Table hiveTable, String timeCol, final int timeColIndex) throws HiveException {
    // getClient().getPartitionsByNames(tbl, partNames)
    List<Partition> partitions = getClient().getPartitions(hiveTable);

    // tree set contains partitions with timestamp as value for timeCol, in
    // descending order
    TreeSet<Partition> allPartTimeVals = new TreeSet<Partition>(new Comparator<Partition>() {
      @Override
      public int compare(Partition o1, Partition o2) {
        Date partDate1 = getPartDate(o1, timeColIndex);
        Date partDate2 = getPartDate(o2, timeColIndex);
        if (partDate1 != null && partDate2 == null) {
          return -1;
        } else if (partDate1 == null && partDate2 != null) {
          return 1;
        } else if (partDate1 == null && partDate2 == null) {
          return o2.getTPartition().compareTo(o1.getTPartition());
        } else if (!partDate2.equals(partDate1)) {
          return partDate2.compareTo(partDate1);
        } else {
          return o2.getTPartition().compareTo(o1.getTPartition());
        }
      }
    });
    for (Partition part : partitions) {
      if (!isLatestPart(part)) {
        Date partDate = getPartDate(part, timeColIndex);
        if (partDate != null) {
          allPartTimeVals.add(part);
        }
      }
    }
    Iterator<Partition> it = allPartTimeVals.iterator();
    it.next();
    LatestInfo latest = null;
    if (it.hasNext()) {
      Partition nextLatest = it.next();
      latest = new LatestInfo();
      latest.setPart(nextLatest);
      Map<String, String> latestParams = new HashMap<String, String>();
      String partVal = nextLatest.getValues().get(timeColIndex);
      latestParams.put(MetastoreUtil.getLatestPartTimestampKey(timeCol), partVal);
      latest.addLatestPartInfo(timeCol, new LatestPartColumnInfo(latestParams));
    }
    return latest;
  }

  /**
   * Add a partition specified by the storage partition desc on the storage
   * passed.
   * 
   * @param partSpec
   *          The storage partition description
   * @param storage
   *          The storage object
   * 
   * @throws HiveException
   */
  public void dropPartition(String cubeTableName, String storageName, Map<String, Date> timePartSpec,
      Map<String, String> nonTimePartSpec, UpdatePeriod updatePeriod) throws HiveException {
    String storageTableName = MetastoreUtil.getStorageTableName(cubeTableName, Storage.getPrefix(storageName));
    Table hiveTable = getHiveTable(storageTableName);
    List<FieldSchema> partCols = hiveTable.getPartCols();
    List<String> partColNames = new ArrayList<String>(partCols.size());
    List<String> partVals = new ArrayList<String>(partCols.size());
    for (FieldSchema column : partCols) {
      partColNames.add(column.getName());
      if (timePartSpec.containsKey(column.getName())) {
        partVals.add(updatePeriod.format().format(timePartSpec.get(column.getName())));
      } else if (nonTimePartSpec.containsKey(column.getName())) {
        partVals.add(nonTimePartSpec.get(column.getName()));
      } else {
        throw new HiveException("Invalid partspec, missing value for" + column.getName());
      }
    }
    String timePartColsStr = hiveTable.getTTable().getParameters().get(MetastoreConstants.TIME_PART_COLUMNS);
    Map<String, LatestInfo> latest = new HashMap<String, Storage.LatestInfo>();
    if (timePartColsStr != null) {
      List<String> timePartCols = Arrays.asList(StringUtils.split(timePartColsStr, ','));
      for (String timeCol : timePartSpec.keySet()) {
        if (!timePartCols.contains(timeCol)) {
          throw new HiveException("Not a time partition column:" + timeCol);
        }
        int timeColIndex = partColNames.indexOf(timeCol);
        Partition part = getLatestPart(storageTableName, timeCol);

        // check if partition being dropped is the latest partition
        boolean isLatest = true;
        for (int i = 0; i < partVals.size(); i++) {
          if (i != timeColIndex) {
            if (!part.getValues().get(i).equals(partVals.get(i))) {
              isLatest = false;
              break;
            }
          }
        }
        if (isLatest) {
          Date latestTimestamp = getLatestTimeStamp(part, storageTableName, timeCol);
          Date dropTimestamp;
          try {
            dropTimestamp = updatePeriod.format().parse(updatePeriod.format().format(timePartSpec.get(timeCol)));
          } catch (ParseException e) {
            throw new HiveException(e);
          }
          if (latestTimestamp != null && dropTimestamp.equals(latestTimestamp)) {
            LatestInfo latestInfo = getNextLatest(hiveTable, timeCol, timeColIndex);
            latest.put(timeCol, latestInfo);
          }
        }
      }
    } else {
      if (timePartSpec != null && !timePartSpec.isEmpty()) {
        throw new HiveException("Not time part columns" + timePartSpec.keySet());
      }
    }
    getStorage(storageName).dropPartition(getClient(), storageTableName, partVals, latest);
  }

  private Map<String, String> getPartitionSpec(UpdatePeriod updatePeriod, Map<String, Date> partitionTimestamps) {
    Map<String, String> partSpec = new HashMap<String, String>();
    for (Map.Entry<String, Date> entry : partitionTimestamps.entrySet()) {
      String pval = updatePeriod.format().format(entry.getValue());
      partSpec.put(entry.getKey(), pval);
    }
    return partSpec;
  }

  public boolean tableExists(String cubeName) throws HiveException {
    try {
      return (getClient().getTable(cubeName.toLowerCase(), false) != null);
    } catch (HiveException e) {
      throw new HiveException("Could not check whether table exists", e);
    }
  }

  boolean factPartitionExists(String factName, String storageName, UpdatePeriod updatePeriod,
      Map<String, Date> partitionTimestamp, Map<String, String> partSpec) throws HiveException {
    String storageTableName = MetastoreUtil.getFactStorageTableName(factName, storageName);
    return partitionExists(storageTableName, updatePeriod, partitionTimestamp, partSpec);
  }

  public boolean partitionExists(String storageTableName, UpdatePeriod updatePeriod,
      Map<String, Date> partitionTimestamps) throws HiveException {
    return partitionExists(storageTableName, getPartitionSpec(updatePeriod, partitionTimestamps));
  }

  public boolean partitionExistsByFilter(String storageTableName, String filter) throws HiveException {
    List<Partition> parts;
    try {
      parts = getClient().getPartitionsByFilter(getTable(storageTableName), filter);
    } catch (Exception e) {
      throw new HiveException("Could not find partitions for given filter", e);
    }
    return !(parts.isEmpty());
  }

  public List<Partition> getAllParts(String storageTableName) throws HiveException {
    return getClient().getPartitions(getHiveTable(storageTableName));
  }

  public List<Partition> getPartitionsByFilter(String storageTableName, String filter) throws HiveException {
    try {
      return getClient().getPartitionsByFilter(getTable(storageTableName), filter);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public int getNumPartitionsByFilter(String storageTableName, String filter) throws MetaException,
      NoSuchObjectException, HiveException, TException {
    return getClient().getNumPartitionsByFilter(getTable(storageTableName), filter);
  }

  boolean partitionExists(String storageTableName, UpdatePeriod updatePeriod, Map<String, Date> partitionTimestamps,
      Map<String, String> partSpec) throws HiveException {
    partSpec.putAll(getPartitionSpec(updatePeriod, partitionTimestamps));
    return partitionExists(storageTableName, partSpec);
  }

  private boolean partitionExists(String storageTableName, Map<String, String> partSpec) throws HiveException {
    try {
      Table storageTbl = getTable(storageTableName);
      Partition p = getClient().getPartition(storageTbl, partSpec, false);
      return (p != null && p.getTPartition() != null);
    } catch (HiveException e) {
      throw new HiveException("Could not check whether table exists", e);
    }
  }

  boolean dimPartitionExists(String dimTblName, String storageName, Map<String, Date> partitionTimestamps)
      throws HiveException {
    String storageTableName = MetastoreUtil.getDimStorageTableName(dimTblName, storageName);
    return partitionExists(storageTableName, getDimensionTable(dimTblName).getSnapshotDumpPeriods().get(storageName),
        partitionTimestamps);
  }

  boolean latestPartitionExists(String factName, String storageName, String latestPartCol) throws HiveException {
    String storageTableName = MetastoreUtil.getFactStorageTableName(factName, storageName);
    return partitionExistsByFilter(storageTableName, StorageConstants.getLatestPartFilter(latestPartCol));
  }

  Partition getLatestPart(String storageTableName, String latestPartCol) throws HiveException {
    List<Partition> latestParts =
        getPartitionsByFilter(storageTableName, StorageConstants.getLatestPartFilter(latestPartCol));
    if (latestParts != null && !latestParts.isEmpty()) {
      return latestParts.get(0);
    }
    return null;
  }

  /**
   * Get the hive {@link Table} corresponding to the name
   * 
   * @param tableName
   * @return {@link Table} object
   * @throws HiveException
   */
  public Table getHiveTable(String tableName) throws HiveException {
    return getTable(tableName);
  }

  private Table getTable(String tableName) throws HiveException {
    Table tbl;
    try {
      tbl = allHiveTables.get(tableName.toLowerCase());
      if (tbl == null) {
        tbl = getClient().getTable(tableName.toLowerCase());
        if (enableCaching) {
          allHiveTables.put(tableName.toLowerCase(), tbl);
        }
      }
    } catch (HiveException e) {
      throw new HiveException("Could not get table: " + tableName, e);
    }
    return tbl;
  }

  private Table refreshTable(String tableName) throws HiveException {
    Table tbl;
    try {
      tbl = getClient().getTable(tableName.toLowerCase());
      allHiveTables.put(tableName.toLowerCase(), tbl);
    } catch (HiveException e) {
      throw new HiveException("Could not get table: " + tableName, e);
    }
    return tbl;
  }

  public void dropHiveTable(String table) throws HiveException {
    getClient().dropTable(table);
    allHiveTables.remove(table.toLowerCase());
  }

  /**
   * Is the table name passed a fact table?
   * 
   * @param tableName
   *          table name
   * @return true if it is cube fact, false otherwise
   * @throws HiveException
   */
  public boolean isFactTable(String tableName) throws HiveException {
    Table tbl = getTable(tableName);
    return isFactTable(tbl);
  }

  boolean isFactTable(Table tbl) {
    String tableType = tbl.getParameters().get(MetastoreConstants.TABLE_TYPE_KEY);
    return CubeTableType.FACT.name().equals(tableType);
  }

  boolean isFactTableForCube(Table tbl, String cube) {
    if (isFactTable(tbl)) {
      return CubeFactTable.getCubeName(tbl.getTableName(), tbl.getParameters()).equalsIgnoreCase(cube.toLowerCase());
    }
    return false;
  }

  /**
   * Is the table name passed a dimension table?
   * 
   * @param tableName
   *          table name
   * @return true if it is cube dimension, false otherwise
   * @throws HiveException
   */
  public boolean isDimensionTable(String tableName) throws HiveException {
    Table tbl = getTable(tableName);
    return isDimensionTable(tbl);
  }

  boolean isDimensionTable(Table tbl) throws HiveException {
    String tableType = tbl.getParameters().get(MetastoreConstants.TABLE_TYPE_KEY);
    return CubeTableType.DIM_TABLE.name().equals(tableType);
  }

  /**
   * Is the table name passed a cube?
   * 
   * @param tableName
   *          table name
   * @return true if it is cube, false otherwise
   * @throws HiveException
   */
  public boolean isCube(String tableName) throws HiveException {
    Table tbl = getTable(tableName);
    return isCube(tbl);
  }

  /**
   * Is the table name passed a dimension?
   * 
   * @param tableName
   *          table name
   * @return true if it is dimension, false otherwise
   * 
   * @throws HiveException
   */
  public boolean isDimension(String tableName) throws HiveException {
    Table tbl = getTable(tableName);
    return isDimension(tbl);
  }

  /**
   * Is the hive table a cube table?
   * 
   * @param tbl
   * @return
   * @throws HiveException
   */
  boolean isCube(Table tbl) throws HiveException {
    String tableType = tbl.getParameters().get(MetastoreConstants.TABLE_TYPE_KEY);
    return CubeTableType.CUBE.name().equals(tableType);
  }

  /**
   * Is the hive table a dimension?
   * 
   * @param tbl
   * @return
   * @throws HiveException
   */
  boolean isDimension(Table tbl) throws HiveException {
    String tableType = tbl.getParameters().get(MetastoreConstants.TABLE_TYPE_KEY);
    return CubeTableType.DIMENSION.name().equals(tableType);
  }

  /**
   * Is the table name passed a storage?
   * 
   * @param tableName
   *          table name
   * @return true if it is storage, false otherwise
   * @throws HiveException
   */
  public boolean isStorage(String tableName) throws HiveException {
    Table tbl = getTable(tableName);
    return isStorage(tbl);
  }

  /**
   * Is the hive table a storage
   * 
   * @param tbl
   * @return
   * @throws HiveException
   */
  boolean isStorage(Table tbl) throws HiveException {
    String tableType = tbl.getParameters().get(MetastoreConstants.TABLE_TYPE_KEY);
    return CubeTableType.STORAGE.name().equals(tableType);
  }

  /**
   * Get {@link CubeFactTable} object corresponding to the name
   * 
   * @param tableName
   *          The cube fact name
   * @return Returns CubeFactTable if table name passed is a fact table, null
   *         otherwise
   * @throws HiveException
   */

  public CubeFactTable getFactTable(String tableName) throws HiveException {
    Table tbl = getTable(tableName);
    return getFactTable(tbl);
  }

  private CubeFactTable getFactTable(Table tbl) throws HiveException {
    if (isFactTable(tbl)) {
      return new CubeFactTable(tbl);
    } else {
    }
    return null;
  }

  /**
   * Get {@link CubeDimensionTable} object corresponding to the name
   * 
   * @param tableName
   *          The cube dimension name
   * @return Returns CubeDimensionTable if table name passed is a dimension
   *         table, null otherwise
   * @throws HiveException
   */
  public CubeDimensionTable getDimensionTable(String tableName) throws HiveException {
    CubeDimensionTable dimTable = allDimTables.get(tableName.toLowerCase());
    if (dimTable == null) {
      Table tbl = getTable(tableName);
      if (isDimensionTable(tbl)) {
        dimTable = getDimensionTable(tbl);
        if (enableCaching) {
          allDimTables.put(tableName.toLowerCase(), dimTable);
        }
      }
    }
    return dimTable;
  }

  private CubeDimensionTable getDimensionTable(Table tbl) throws HiveException {
    return new CubeDimensionTable(tbl);
  }

  /**
   * Get {@link Storage} object corresponding to the name
   * 
   * @param storageName
   *          The storage name
   * @return Returns storage if name passed is a storage, null otherwise
   * @throws HiveException
   */
  public Storage getStorage(String storageName) throws HiveException {
    Storage storage = allStorages.get(storageName.toLowerCase());
    if (storage == null) {
      Table tbl = getTable(storageName);
      if (isStorage(tbl)) {
        storage = getStorage(tbl);
        if (enableCaching) {
          allStorages.put(storageName.toLowerCase(), storage);
        }
      }
    }
    return storage;
  }

  private Storage getStorage(Table tbl) throws HiveException {
    return Storage.createInstance(tbl);
  }

  /**
   * Get {@link Cube} object corresponding to the name
   * 
   * @param tableName
   *          The cube name
   * @return Returns cube is table name passed is a cube, null otherwise
   * @throws HiveException
   */
  public CubeInterface getCube(String tableName) throws HiveException {
    CubeInterface cube = allCubes.get(tableName.toLowerCase());
    if (cube == null) {
      Table tbl = getTable(tableName);
      if (isCube(tbl)) {
        cube = getCube(tbl);
        if (enableCaching) {
          allCubes.put(tableName.toLowerCase(), cube);
        }
      }
    }
    return cube;
  }

  /**
   * Get {@link Cube} object corresponding to the name
   * 
   * @param tableName
   *          The cube name
   * @return Returns cube is table name passed is a cube, null otherwise
   * @throws HiveException
   */
  public Dimension getDimension(String tableName) throws HiveException {
    Dimension dim = allDims.get(tableName.toLowerCase());
    if (dim == null) {
      Table tbl = getTable(tableName);
      if (isDimension(tbl)) {
        dim = getDimension(tbl);
        if (enableCaching) {
          allDims.put(tableName.toLowerCase(), dim);
        }
      }
    }
    return dim;
  }

  /**
   * Get {@link Cube} object corresponding to the name
   * 
   * @param tableName
   *          The cube name
   * @return Returns cube is table name passed is a cube, null otherwise
   * @throws HiveException
   */
  public CubeFactTable getCubeFact(String tableName) throws HiveException {
    CubeFactTable fact = allFactTables.get(tableName.toLowerCase());
    if (fact == null) {
      Table tbl = getTable(tableName);
      fact = getFactTable(tbl);
      if (enableCaching) {
        allFactTables.put(tableName.toLowerCase(), fact);
      }
    }
    return fact;
  }

  private CubeInterface getCube(Table tbl) throws HiveException {
    String parentCube = tbl.getParameters().get(MetastoreUtil.getParentCubeNameKey(tbl.getTableName()));
    if (parentCube != null) {
      return new DerivedCube(tbl, (Cube) getCube(parentCube));
    } else {
      return new Cube(tbl);
    }
  }

  private Dimension getDimension(Table tbl) {
    return new Dimension(tbl);
  }

  /**
   * Get all dimension tables in metastore
   * 
   * @return List of dimension tables
   * 
   * @throws HiveException
   */
  public List<CubeDimensionTable> getAllDimensionTables() throws HiveException {

    List<CubeDimensionTable> dimTables = new ArrayList<CubeDimensionTable>();
    try {
      for (String table : getAllHiveTableNames()) {
        CubeDimensionTable dim = getDimensionTable(table);
        if (dim != null) {
          dimTables.add(dim);
        }
      }
    } catch (HiveException e) {
      throw new HiveException("Could not get all dimension tables", e);
    }
    return dimTables;
  }

  /**
   * Get all storages in metastore
   * 
   * @return List of Storage objects
   * @throws HiveException
   */
  public List<Storage> getAllStorages() throws HiveException {
    List<Storage> storages = new ArrayList<Storage>();
    try {
      for (String table : getAllHiveTableNames()) {
        Storage storage = getStorage(table);
        if (storage != null) {
          storages.add(storage);
        }
      }
    } catch (HiveException e) {
      throw new HiveException("Could not get all storages", e);
    }
    return storages;
  }

  /**
   * Get all cubes in metastore
   * 
   * @return List of Cube objects
   * @throws HiveException
   */
  public List<CubeInterface> getAllCubes() throws HiveException {
    List<CubeInterface> cubes = new ArrayList<CubeInterface>();
    try {
      for (String table : getAllHiveTableNames()) {
        CubeInterface cube = getCube(table);
        if (cube != null) {
          cubes.add(cube);
        }
      }
    } catch (HiveException e) {
      throw new HiveException("Could not get all cubes", e);
    }
    return cubes;
  }

  /**
   * Get all cubes in metastore
   * 
   * @return List of Cube objects
   * @throws HiveException
   */
  public List<Dimension> getAllDimensions() throws HiveException {
    List<Dimension> dims = new ArrayList<Dimension>();
    try {
      for (String table : getAllHiveTableNames()) {
        Dimension dim = getDimension(table);
        if (dim != null) {
          dims.add(dim);
        }
      }
    } catch (HiveException e) {
      throw new HiveException("Could not get all dimensions", e);
    }
    return dims;
  }

  /**
   * Get all facts in metastore
   * 
   * @return List of Cube Fact Table objects
   * 
   * @throws HiveException
   */
  public List<CubeFactTable> getAllFacts() throws HiveException {
    List<CubeFactTable> facts = new ArrayList<CubeFactTable>();
    try {
      for (String table : getAllHiveTableNames()) {
        CubeFactTable fact = getCubeFact(table);
        if (fact != null) {
          facts.add(fact);
        }
      }
    } catch (HiveException e) {
      throw new HiveException("Could not get all fact tables", e);
    }
    return facts;
  }

  private List<String> getAllHiveTableNames() throws HiveException {
    return getClient().getAllTables();
  }

  /**
   * Get all fact tables of the cube.
   * 
   * @param cube
   *          Cube object
   * 
   * @return List of fact tables
   * @throws HiveException
   */
  public List<CubeFactTable> getAllFactTables(CubeInterface cube) throws HiveException {
    if (cube instanceof Cube) {
      List<CubeFactTable> cubeFacts = new ArrayList<CubeFactTable>();
      try {
        for (CubeFactTable fact : getAllFacts()) {
          if (fact.getCubeName().equalsIgnoreCase(((Cube) cube).getName())) {
            cubeFacts.add(fact);
          }
        }
      } catch (HiveException e) {
        throw new HiveException("Could not get all fact tables of " + cube, e);
      }
      return cubeFacts;
    } else {
      return getAllFactTables(((DerivedCube) cube).getParent());
    }
  }

  /**
   * Get all derived cubes of the cube.
   * 
   * @param cube
   *          Cube object
   * 
   * @return List of DerivedCube objects
   * @throws HiveException
   */
  public List<DerivedCube> getAllDerivedCubes(CubeInterface cube) throws HiveException {
    List<DerivedCube> dcubes = new ArrayList<DerivedCube>();
    try {
      for (CubeInterface cb : getAllCubes()) {
        if (cb.isDerivedCube() && ((DerivedCube) cb).getParent().getName().equalsIgnoreCase(cube.getName())) {
          dcubes.add((DerivedCube) cb);
        }
      }
    } catch (HiveException e) {
      throw new HiveException("Could not get all derived cubes of " + cube, e);
    }
    return dcubes;
  }

  /**
   * Get all derived cubes of the cube, that have all fields queryable together
   * 
   * @param cube
   *          Cube object
   * 
   * @return List of DerivedCube objects
   * @throws HiveException
   */
  public List<DerivedCube> getAllDerivedQueryableCubes(CubeInterface cube) throws HiveException {
    List<DerivedCube> dcubes = new ArrayList<DerivedCube>();
    try {
      for (CubeInterface cb : getAllCubes()) {
        if (cb.isDerivedCube() && ((DerivedCube) cb).getParent().getName().equalsIgnoreCase(cube.getName())
            && cb.allFieldsQueriable()) {
          dcubes.add((DerivedCube) cb);
        }
      }
    } catch (HiveException e) {
      throw new HiveException("Could not get all derived queryable cubes of " + cube, e);
    }
    return dcubes;
  }

  /**
   * Get all dimension tables of the dimension.
   * 
   * @param dim
   *          Dimension object
   * 
   * @return List of fact tables
   * @throws HiveException
   */
  public List<CubeDimensionTable> getAllDimensionTables(Dimension dim) throws HiveException {
    List<CubeDimensionTable> dimTables = new ArrayList<CubeDimensionTable>();
    try {
      for (CubeDimensionTable dimTbl : getAllDimensionTables()) {
        if (dimTbl.getDimName().equalsIgnoreCase(dim.getName().toLowerCase())) {
          dimTables.add(dimTbl);
        }
      }
    } catch (HiveException e) {
      throw new HiveException("Could not get all dimension tables of " + dim, e);
    }
    return dimTables;
  }

  public List<String> getPartColNames(String tableName) throws HiveException {
    List<String> partColNames = new ArrayList<String>();
    Table tbl = getTable(tableName);
    for (FieldSchema f : tbl.getPartCols()) {
      partColNames.add(f.getName().toLowerCase());
    }
    return partColNames;
  }

  public boolean partColExists(String tableName, String partCol) throws HiveException {
    Table tbl = getTable(tableName);
    for (FieldSchema f : tbl.getPartCols()) {
      if (f.getName().equalsIgnoreCase(partCol)) {
        return true;
      }
    }
    return false;
  }

  public synchronized SchemaGraph getSchemaGraph() throws HiveException {
    if (schemaGraph == null) {
      schemaGraph = new SchemaGraph(this);
    }
    return schemaGraph;
  }

  /**
   * Returns true if columns changed
   * 
   * @param table
   * @param hiveTable
   * @param cubeTable
   * @throws HiveException
   */
  private boolean alterCubeTable(String table, Table hiveTable, AbstractCubeTable cubeTable) throws HiveException {
    hiveTable.getParameters().putAll(cubeTable.getProperties());
    boolean columnsChanged = !(hiveTable.getCols().equals(cubeTable.getColumns()));
    if (columnsChanged) {
      hiveTable.getTTable().getSd().setCols(cubeTable.getColumns());
    }
    hiveTable.getTTable().getParameters().putAll(cubeTable.getProperties());
    try {
      getClient().alterTable(table, hiveTable);
    } catch (InvalidOperationException e) {
      throw new HiveException(e);
    }
    return columnsChanged;
  }

  private void alterHiveTable(String table, Table hiveTable, List<FieldSchema> columns) throws HiveException {
    hiveTable.getTTable().getSd().setCols(columns);
    try {
      getClient().alterTable(table, hiveTable);
    } catch (InvalidOperationException e) {
      throw new HiveException(e);
    }
    if (enableCaching) {
      // refresh the table in cache
      refreshTable(table);
    }
  }

  /**
   * Alter cube specified by the name to new definition
   * 
   * @param cubeName
   *          The cube name to be altered
   * @param cube
   *          The new cube definition {@link Cube} or {@link DerivedCube}
   * @throws HiveException
   * @throws InvalidOperationException
   */
  public void alterCube(String cubeName, CubeInterface cube) throws HiveException {
    Table cubeTbl = getTable(cubeName);
    if (isCube(cubeTbl)) {
      alterCubeTable(cubeName, cubeTbl, (AbstractCubeTable) cube);
      if (enableCaching) {
        allCubes.put(cubeName, getCube(refreshTable(cubeName)));
      }
    } else {
      throw new HiveException(cubeName + " is not a cube");
    }
  }

  /**
   * Alter dimension specified by the dimension name to new definition
   * 
   * @param dimName
   *          The cube name to be altered
   * @param cube
   *          The new dimension definition
   * 
   * @throws HiveException
   * @throws InvalidOperationException
   */
  public void alterDimension(String dimName, Dimension newDim) throws HiveException {
    Table tbl = getTable(dimName);
    if (isDimension(tbl)) {
      alterCubeTable(dimName, tbl, (AbstractCubeTable) newDim);
      if (enableCaching) {
        allDims.put(dimName, getDimension(refreshTable(dimName)));
      }
    } else {
      throw new HiveException(dimName + " is not a dimension");
    }
  }

  /**
   * Alter storage specified by the name to new definition
   * 
   * @param storageName
   *          The storage name to be altered
   * @param storage
   *          The new storage definition
   * @throws HiveException
   * @throws InvalidOperationException
   */
  public void alterStorage(String storageName, Storage storage) throws HiveException {
    Table storageTbl = getTable(storageName);
    if (isStorage(storageTbl)) {
      alterCubeTable(storageName, storageTbl, storage);
      if (enableCaching) {
        allStorages.put(storageName, getStorage(refreshTable(storageName)));
      }
    } else {
      throw new HiveException(storageName + " is not a storage");
    }
  }

  /**
   * Drop a storage
   * 
   * @param storageName
   * @throws HiveException
   */
  public void dropStorage(String storageName) throws HiveException {
    if (isStorage(storageName)) {
      allStorages.remove(storageName.toLowerCase());
      dropHiveTable(storageName);
    } else {
      throw new HiveException(storageName + " is not a storage");
    }
  }

  /**
   * Drop a cube
   * 
   * @param cubeName
   * @throws HiveException
   */
  public void dropCube(String cubeName) throws HiveException {
    if (isCube(cubeName)) {
      allCubes.remove(cubeName.toLowerCase());
      dropHiveTable(cubeName);
    } else {
      throw new HiveException(cubeName + " is not a cube");
    }
  }

  /**
   * Drop a dimension
   * 
   * @param cubeName
   * @throws HiveException
   */
  public void dropDimension(String dimName) throws HiveException {
    if (isDimension(dimName)) {
      allDims.remove(dimName.toLowerCase());
      dropHiveTable(dimName);
    } else {
      throw new HiveException(dimName + " is not a dimension");
    }
  }

  /**
   * Drop a fact with cascade flag
   * 
   * @param factName
   * @param cascade
   *          If true, will drop all the storages of the fact
   * @throws HiveException
   */
  public void dropFact(String factName, boolean cascade) throws HiveException {
    if (isFactTable(factName)) {
      CubeFactTable fact = getFactTable(factName);
      if (cascade) {
        for (String storage : fact.getStorages()) {
          dropStorageFromFact(factName, storage);
        }
      }
      updateFactCache(factName);
      dropHiveTable(factName);
    } else {
      throw new HiveException(factName + " is not a CubeFactTable");
    }
  }

  /**
   * Drop a storage from fact
   * 
   * @param factName
   * @param storage
   * @throws HiveException
   * @throws InvalidOperationException
   */
  public void dropStorageFromFact(String factName, String storage) throws HiveException {
    CubeFactTable cft = getFactTable(factName);
    cft.dropStorage(storage);
    dropHiveTable(MetastoreUtil.getFactStorageTableName(factName, storage));
    alterCubeTable(factName, getTable(factName), cft);
    updateFactCache(factName);
  }

  /**
   * Drop a storage from dimension
   * 
   * @param dimTblName
   * @param storage
   * @throws HiveException
   */
  public void dropStorageFromDim(String dimTblName, String storage) throws HiveException {
    CubeDimensionTable cdt = getDimensionTable(dimTblName);
    cdt.dropStorage(storage);
    dropHiveTable(MetastoreUtil.getDimStorageTableName(dimTblName, storage));
    alterCubeTable(dimTblName, getTable(dimTblName), cdt);
    updateDimCache(dimTblName);
  }

  /**
   * Drop the dimension table
   * 
   * @param dimTblName
   * @param cascade
   *          If true, will drop all the dimension storages
   * 
   * @throws HiveException
   */
  public void dropDimensionTable(String dimTblName, boolean cascade) throws HiveException {
    if (isDimensionTable(dimTblName)) {
      CubeDimensionTable dim = getDimensionTable(dimTblName);
      Set<String> dimStorages = new HashSet<String>();
      dimStorages.addAll(dim.getStorages());
      for (String storage : dimStorages) {
        dropStorageFromDim(dimTblName, storage);
      }
      dropHiveTable(dimTblName);
      allDimTables.remove(dimTblName.toLowerCase());
    } else {
      throw new HiveException(dimTblName + " is not a dimension table");
    }
  }

  /**
   * Alter a cubefact with new definition
   * 
   * @param factTableName
   * @param cubeFactTable
   * @throws HiveException
   * @throws InvalidOperationException
   */
  public void alterCubeFactTable(String factTableName, CubeFactTable cubeFactTable) throws HiveException {
    Table factTbl = getTable(factTableName);
    if (isFactTable(factTbl)) {
      boolean colsChanged = alterCubeTable(factTableName, factTbl, cubeFactTable);
      if (colsChanged) {
        // Change schema of all the storage tables
        for (String storage : cubeFactTable.getStorages()) {
          String storageTableName = MetastoreUtil.getFactStorageTableName(factTableName, storage);
          alterHiveTable(storageTableName, getTable(storageTableName), cubeFactTable.getColumns());
        }
      }
      updateFactCache(factTableName);
    } else {
      throw new HiveException(factTableName + " is not a fact table");
    }
  }

  private void updateFactCache(String factTableName) throws HiveException {
    if (enableCaching) {
      allFactTables.put(factTableName, getFactTable(refreshTable(factTableName)));
    }
  }

  private void updateDimCache(String dimTblName) throws HiveException {
    if (enableCaching) {
      allDimTables.put(dimTblName, getDimensionTable(refreshTable(dimTblName)));
    }
  }

  /**
   * Alter dimension table with new dimension definition
   * 
   * @param dimTableName
   * @param cubeDimensionTable
   * 
   * @throws HiveException
   * @throws InvalidOperationException
   */
  public void alterCubeDimensionTable(String dimTableName, CubeDimensionTable cubeDimensionTable) throws HiveException {
    Table dimTbl = getTable(dimTableName);
    if (isDimensionTable(dimTbl)) {
      boolean colsChanged = alterCubeTable(dimTableName, dimTbl, cubeDimensionTable);
      if (colsChanged) {
        // Change schema of all the storage tables
        for (String storage : cubeDimensionTable.getStorages()) {
          String storageTableName = MetastoreUtil.getDimStorageTableName(dimTableName, storage);
          alterHiveTable(storageTableName, getTable(storageTableName), cubeDimensionTable.getColumns());
        }
      }
      updateDimCache(dimTableName);
    } else {
      throw new HiveException(dimTableName + " is not a dimension table");
    }
  }
}

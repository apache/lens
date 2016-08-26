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

import static org.apache.lens.cube.metadata.DateUtil.resolveDate;
import static org.apache.lens.cube.metadata.MetastoreUtil.*;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.Storage.LatestInfo;
import org.apache.lens.cube.metadata.Storage.LatestPartColumnInfo;
import org.apache.lens.cube.metadata.timeline.PartitionTimeline;
import org.apache.lens.cube.metadata.timeline.PartitionTimelineFactory;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.util.LensUtil;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.thrift.TException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

/**
 * Wrapper class around Hive metastore to do cube metastore operations.
 */
@Slf4j
public class CubeMetastoreClient {
  private final HiveConf config;
  private final boolean enableCaching;

  private CubeMetastoreClient(HiveConf conf) {
    this.config = new HiveConf(conf);
    this.enableCaching = conf.getBoolean(MetastoreConstants.METASTORE_ENABLE_CACHING, true);
  }

  // map from table name to Table
  private final Map<String, Table> allHiveTables = Maps.newConcurrentMap();
  private volatile boolean allTablesPopulated = false;
  // map from dimension name to Dimension
  private final Map<String, Dimension> allDims = Maps.newConcurrentMap();
  private volatile boolean allDimensionsPopulated = false;
  // map from cube name to Cube
  private final Map<String, CubeInterface> allCubes = Maps.newConcurrentMap();
  private volatile boolean allCubesPopulated = false;
  // map from dimtable name to CubeDimensionTable
  private final Map<String, CubeDimensionTable> allDimTables = Maps.newConcurrentMap();
  private volatile boolean allDimTablesPopulated = false;
  // map from fact name to fact table
  private final Map<String, CubeFactTable> allFactTables = Maps.newConcurrentMap();
  private volatile boolean allFactTablesPopulated = false;
  //map from segmentation name to segmentation
  private final Map<String, Segmentation> allSegmentations = Maps.newConcurrentMap();
  private volatile boolean allSegmentationPopulated = false;
  // map from storage name to storage
  private final Map<String, Storage> allStorages = Maps.newConcurrentMap();
  private volatile boolean allStoragesPopulated = false;
  // Partition cache. Inner class since it logically belongs here
  PartitionTimelineCache partitionTimelineCache = new PartitionTimelineCache();
  // dbname to client mapping
  private static final Map<String, CubeMetastoreClient> CLIENT_MAPPING = Maps.newConcurrentMap();
  // Set of all storage table names for which latest partitions exist
  private final Set<String> latestLookupCache = Sets.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

  /** extract storage name from fact and storage table name. String operation */
  private String extractStorageName(CubeFactTable fact, String storageTableName) throws LensException {
    int ind = storageTableName.lastIndexOf(fact.getName());
    if (ind <= 0) {
      throw new LensException("storageTable: " + storageTableName + ", does not belong to fact: " + fact.getName());
    }
    return storageTableName.substring(0, ind - StorageConstants.STORGAE_SEPARATOR.length());
  }

  /**
   * get latest date for timeDimension from all fact-storage tables belonging to the given cube having timeDimension,
   * return the most recent of all.
   * <p></p>
   * latest date for a single fact-storage table for given time dimension is the latest of the latest dates for all its
   * update periods
   *
   * @param cube             Cube to get latest date of
   * @param timeDimension    time dimension
   * @return                 latest date among all facts of cube in timeDimension
   * @throws HiveException
   * @throws LensException
   */
  public Date getLatestDateOfCube(Cube cube, String timeDimension) throws HiveException, LensException {
    String partCol = cube.getPartitionColumnOfTimeDim(timeDimension);
    Date max = new Date(Long.MIN_VALUE);
    boolean updated = false;
    for (CubeFactTable fact : getAllFacts(cube)) {
      for (String storage : fact.getStorages()) {
        for (UpdatePeriod updatePeriod : fact.getUpdatePeriods().get(storage)) {
          PartitionTimeline timeline = partitionTimelineCache.get(fact.getName(), storage, updatePeriod, partCol);
          if (timeline != null) {// this storage table is partitioned by partCol or not.
            Date latest = timeline.getLatestDate();
            if (latest != null && latest.after(max)) {
              max = latest;
              updated = true;
            }
          }
        }
      }
    }
    return updated ? max : null;
  }

  /** clear hive table cache */
  public void clearHiveTableCache() {
    allHiveTables.clear();
  }

  public List<PartitionTimeline> getTimelines(String factName, String storage, String updatePeriodStr,
    String timeDimension)
    throws LensException, HiveException {
    UpdatePeriod updatePeriod = updatePeriodStr == null ? null : UpdatePeriod.valueOf(updatePeriodStr.toUpperCase());
    List<PartitionTimeline> ret = Lists.newArrayList();
    CubeFactTable fact = getCubeFact(factName);
    List<String> keys = Lists.newArrayList();
    if (storage != null) {
      keys.add(storage);
    } else {
      keys.addAll(fact.getStorages());
    }
    String partCol = null;
    if (timeDimension != null) {
      Cube baseCube;
      CubeInterface cube = getCube(fact.getCubeName());
      if (cube instanceof Cube) {
        baseCube = (Cube) cube;
      } else {
        baseCube = ((DerivedCube) cube).getParent();
      }
      partCol = baseCube.getPartitionColumnOfTimeDim(timeDimension);
    }
    for (String key : keys) {
      for (Map.Entry<UpdatePeriod, CaseInsensitiveStringHashMap<PartitionTimeline>> entry : partitionTimelineCache
        .get(factName, key).entrySet()) {
        if (updatePeriod == null || entry.getKey().equals(updatePeriod)) {
          for (Map.Entry<String, PartitionTimeline> entry1 : entry.getValue().entrySet()) {
            if (partCol == null || partCol.equals(entry1.getKey())) {
              ret.add(entry1.getValue());
            }
          }
        }
      }
    }
    return ret;
  }

  public void updatePartition(String fact, String storageName, Partition partition)
    throws HiveException, InvalidOperationException, LensException {
    updatePartitions(fact, storageName, Collections.singletonList(partition));
  }

  public void updatePartitions(String factOrDimtableName, String storageName, List<Partition> partitions)
    throws HiveException, InvalidOperationException, LensException {
    List<Partition> partitionsToAlter = Lists.newArrayList();
    partitionsToAlter.addAll(partitions);
    partitionsToAlter.addAll(getAllLatestPartsEquivalentTo(factOrDimtableName, storageName, partitions));
    getStorage(storageName).updatePartitions(getClient(), factOrDimtableName, partitionsToAlter);
  }

  private List<Partition> getAllLatestPartsEquivalentTo(String factOrDimtableName, String storageName,
    List<Partition> partitions) throws HiveException, LensException {
    if (isFactTable(factOrDimtableName)) {
      return Lists.newArrayList();
    }
    String storageTableName = getFactOrDimtableStorageTableName(factOrDimtableName, storageName);
    Table storageTable = getTable(storageTableName);
    List<String> timePartCols = getTimePartColNamesOfTable(storageTable);
    List<Partition> latestParts = Lists.newArrayList();
    for (Partition partition : partitions) {
      LinkedHashMap<String, String> partSpec = partition.getSpec();
      LinkedHashMap<String, String> timePartSpec = Maps.newLinkedHashMap();
      LinkedHashMap<String, String> nonTimePartSpec = Maps.newLinkedHashMap();
      for (Map.Entry<String, String> entry : partSpec.entrySet()) {
        if (timePartCols.contains(entry.getKey())) {
          timePartSpec.put(entry.getKey(), entry.getValue());
        } else {
          nonTimePartSpec.put(entry.getKey(), entry.getValue());
        }
      }
      for (String timePartCol : timePartCols) {
        Partition latestPart = getLatestPart(storageTableName, timePartCol, nonTimePartSpec);
        if (latestPart != null) {
          LinkedHashMap<String, String> latestPartSpec = latestPart.getSpec();
          latestPartSpec.put(timePartCol, partSpec.get(timePartCol));
          if (partSpec.equals(latestPartSpec)) {
            latestPart.getParameters().putAll(partition.getParameters());
            latestPart.getParameters().put(getLatestPartTimestampKey(timePartCol),
              partSpec.get(timePartCol));
            latestPart.getTPartition().getSd().getSerdeInfo().getParameters().putAll(
              partition.getTPartition().getSd().getSerdeInfo().getParameters());
            latestPart.setLocation(partition.getLocation());
            latestPart.setInputFormatClass(partition.getInputFormatClass());
            latestPart.setOutputFormatClass(partition.getOutputFormatClass().asSubclass(HiveOutputFormat.class));
            latestPart.getTPartition().getSd().getSerdeInfo()
              .setSerializationLib(partition.getTPartition().getSd().getSerdeInfo().getSerializationLib());
            latestParts.add(latestPart);
          }
        }
      }
    }
    return latestParts;
  }

  public boolean isLensQueryableTable(String tableName) {
    try {
      Table table = getTable(tableName);
      String typeProperty = table.getProperty(MetastoreConstants.TABLE_TYPE_KEY);
      if (StringUtils.isBlank(typeProperty)) {
        return false;
      }
      CubeTableType type = CubeTableType.valueOf(typeProperty);
      return type == CubeTableType.CUBE || type == CubeTableType.DIMENSION;
    } catch (LensException e) {
      return false;
    }
  }

  public void verifyStorageExists(AbstractCubeTable cdt, String storage) throws LensException {
    if (cdt.getStorages() == null || !cdt.getStorages().contains(storage)) {
      throw new LensException(LensCubeErrorCode.ENTITY_NOT_FOUND.getLensErrorInfo(), "storage " + storage + " for",
        cdt.getTableType().name().toLowerCase() + " " + cdt.getName());
    }
  }


  /**
   * In-memory storage of {@link PartitionTimeline} objects for each valid
   * storagetable-updateperiod-partitioncolumn tuple. also simultaneously stored in metastore table of the
   * storagetable.
   */
  class PartitionTimelineCache extends CaseInsensitiveStringHashMap<// storage table
    TreeMap<UpdatePeriod,
      CaseInsensitiveStringHashMap<// partition column
        PartitionTimeline>>> {
    /**
     *
     * @param fact      fact
     * @param storage   storage
     * @param partCol   part column
     * @return          true if all the timelines for fact-storage table are empty for all valid update periods.
     * @throws HiveException
     * @throws LensException
     */
    public boolean noPartitionsExist(String fact, String storage, String partCol)
      throws HiveException, LensException {
      if (get(fact, storage) == null) {
        return true;
      }
      for (UpdatePeriod updatePeriod : get(fact, storage).keySet()) {
        PartitionTimeline timeline = get(fact, storage, updatePeriod, partCol);
        if (timeline != null && !timeline.isEmpty()) {
          return false;
        }
      }
      return true;
    }

    /**
     * get all timelines for all update periods and partition columns for the given fact-storage pair. If already loaded
     * in memory, it'll return that. If not, it'll first try to load it from table properties. If not found in table
     * properties, it'll get all partitions, compute timelines in memory, write back all loads timelines to table
     * properties for further usage and return them.
     *
     * @param fact          fact
     * @param storage       storage
     * @return              all timelines for fact-storage pair. Load from properties/all partitions if needed.
     * @throws HiveException
     * @throws LensException
     */
    public TreeMap<UpdatePeriod, CaseInsensitiveStringHashMap<PartitionTimeline>> get(String fact, String storage)
      throws HiveException, LensException {
      // SUSPEND CHECKSTYLE CHECK DoubleCheckedLockingCheck
      String storageTableName = getStorageTableName(fact, Storage.getPrefix(storage));
      if (get(storageTableName) == null) {
        synchronized (this) {
          if (get(storageTableName) == null) {
            Table storageTable = getTable(storageTableName);
            if ("true".equalsIgnoreCase(storageTable.getParameters().get(getPartitionTimelineCachePresenceKey()))) {
              try {
                loadTimelinesFromTableProperties(fact, storage);
              } catch (Exception e) {
                // Ideally this should never come. But since we have another source,
                // let's piggyback on that for loading timeline
                log.error("Error while loading timelines from table properties.", e);
                loadTimelinesFromAllPartitions(fact, storage);
              }
            } else {
              loadTimelinesFromAllPartitions(fact, storage);
            }
          }
        }
        log.info("timeline for {} is: {}", storageTableName, get(storageTableName));
      }
      // return the final value from memory
      return get(storageTableName);
      // RESUME CHECKSTYLE CHECK DoubleCheckedLockingCheck
    }

    private void loadTimelinesFromAllPartitions(String fact, String storage) throws HiveException, LensException {
      // Not found in table properties either, compute from all partitions of the fact-storage table.
      // First make sure all combinations of update period and partition column have an entry even
      // if no partitions exist
      String storageTableName = getStorageTableName(fact, Storage.getPrefix(storage));
      log.info("loading from all partitions: {}", storageTableName);
      Table storageTable = getTable(storageTableName);
      if (getCubeFact(fact).getUpdatePeriods() != null && getCubeFact(fact).getUpdatePeriods().get(
        storage) != null) {
        for (UpdatePeriod updatePeriod : getCubeFact(fact).getUpdatePeriods().get(storage)) {
          for (String partCol : getTimePartColNamesOfTable(storageTable)) {
            ensureEntry(storageTableName, updatePeriod, partCol);
          }
        }
      }
      // Then add all existing partitions for batch addition in respective timelines.
      List<String> timeParts = getTimePartColNamesOfTable(storageTable);
      List<FieldSchema> partCols = storageTable.getPartCols();
      for (Partition partition : getPartitionsByFilter(storageTableName, null)) {
        UpdatePeriod period = deduceUpdatePeriod(partition);
        List<String> values = partition.getValues();
        if (values.contains(StorageConstants.LATEST_PARTITION_VALUE)) {
          log.info("dropping latest partition from fact storage table: {}. Spec: {}", storageTableName,
            partition.getSpec());
          getClient().dropPartition(storageTableName, values, false);
          continue;
        }
        for (int i = 0; i < partCols.size(); i++) {
          if (timeParts.contains(partCols.get(i).getName())) {
            addForBatchAddition(storageTableName, period, partCols.get(i).getName(), values.get(i));
          }
        }
      }
      // commit all batch addition for the storage table,
      // which will in-turn commit all batch additions in all it's timelines.
      commitAllBatchAdditions(storageTableName);
    }

    private void loadTimelinesFromTableProperties(String fact, String storage) throws HiveException, LensException {
      // found in table properties, load from there.
      String storageTableName = getStorageTableName(fact, Storage.getPrefix(storage));
      log.info("loading from table properties: {}", storageTableName);
      for (UpdatePeriod updatePeriod : getCubeFact(fact).getUpdatePeriods().get(storage)) {
        for (String partCol : getTimePartColNamesOfTable(storageTableName)) {
          ensureEntry(storageTableName, updatePeriod, partCol).init(getTable(storageTableName));
        }
      }
    }

    /**
     * Adds given partition(for storageTable, updatePeriod, partitionColum=partition) for batch addition in an
     * appropriate timeline object. Ignore if partition is not valid.
     *
     * @param storageTable      storage table
     * @param updatePeriod      update period
     * @param partitionColumn   partition column
     * @param partition         partition
     */
    public void addForBatchAddition(String storageTable, UpdatePeriod updatePeriod, String partitionColumn,
      String partition) {
      try {
        ensureEntry(storageTable, updatePeriod, partitionColumn).addForBatchAddition(TimePartition.of(updatePeriod,
          partition));
      } catch (LensException e) {
        // to take care of the case where partition name is something like `latest`
        log.error("Couldn't parse partition: {} with update period: {}, skipping.", partition, updatePeriod, e);
      }
    }

    /**
     * helper method for ensuring get(storageTable).get(updatePeriod).get(partitionColumn) gives a non-null object.
     * <p></p>
     * kind of like mkdir -p
     *
     * @param storageTable    storage table
     * @param updatePeriod    update period
     * @param partitionColumn partition column
     * @return timeline if already exists, or puts a new timeline and returns.
     */
    public PartitionTimeline ensureEntry(String storageTable, UpdatePeriod updatePeriod, String partitionColumn) {
      if (get(storageTable) == null) {
        put(storageTable, new TreeMap<UpdatePeriod, CaseInsensitiveStringHashMap<PartitionTimeline>>());
      }
      if (get(storageTable).get(updatePeriod) == null) {
        get(storageTable).put(updatePeriod, new CaseInsensitiveStringHashMap<PartitionTimeline>());
      }
      if (get(storageTable).get(updatePeriod).get(partitionColumn) == null) {
        get(storageTable).get(updatePeriod).put(partitionColumn, PartitionTimelineFactory.get(
          CubeMetastoreClient.this, storageTable, updatePeriod, partitionColumn));
      }
      return get(storageTable).get(updatePeriod).get(partitionColumn);
    }

    /**
     * commit all batch addition for all its timelines.
     *
     * @param storageTable   storage table
     * @throws HiveException
     * @throws LensException
     */
    public void commitAllBatchAdditions(String storageTable) throws HiveException, LensException {
      if (get(storageTable) != null) {
        for (UpdatePeriod updatePeriod : get(storageTable).keySet()) {
          for (String partCol : get(storageTable).get(updatePeriod).keySet()) {
            PartitionTimeline timeline = get(storageTable).get(updatePeriod).get(partCol);
            timeline.commitBatchAdditions();
          }
        }
        alterTablePartitionCache(storageTable);
      }
    }

    /** check partition existence in the appropriate timeline if it exists */
    public boolean partitionTimeExists(String name, String storage, UpdatePeriod period, String partCol, Date partSpec)
      throws HiveException, LensException {
      return get(name, storage, period, partCol) != null
        && get(name, storage, period, partCol).exists(TimePartition.of(period, partSpec));
    }

    /**
     * returns the timeline corresponding to fact-storage table, updatePeriod, partCol. null if doesn't exist, which
     * would only happen if the combination is not valid/supported
     */
    public PartitionTimeline get(String fact, String storage, UpdatePeriod updatePeriod, String partCol)
      throws HiveException, LensException {
      return get(fact, storage) != null && get(fact, storage).get(updatePeriod) != null && get(fact, storage).get(
        updatePeriod).get(partCol) != null ? get(fact, storage).get(updatePeriod).get(partCol) : null;
    }
    /**
     * returns the timeline corresponding to fact-storage table, updatePeriod, partCol. throws exception if not
     * exists, which would most probably mean the combination is incorrect.
     */
    public PartitionTimeline getAndFailFast(String fact, String storage, UpdatePeriod updatePeriod, String partCol)
      throws HiveException, LensException {
      PartitionTimeline timeline = get(fact, storage, updatePeriod, partCol);
      if (timeline == null) {
        throw new LensException(LensCubeErrorCode.TIMELINE_ABSENT.getLensErrorInfo(),
          fact, storage, updatePeriod, partCol);
      }
      return timeline;
    }

    /** update partition timeline cache for addition of time partition */
    public void updateForAddition(String cubeTableName, String storageName, UpdatePeriod updatePeriod,
      Map<String, TreeSet<Date>> timePartSpec) throws HiveException, LensException {
      // fail fast. All part cols mentioned in all partitions should exist.
      for (String partCol : timePartSpec.keySet()) {
        getAndFailFast(cubeTableName, storageName, updatePeriod, partCol);
      }
      for (Map.Entry<String, TreeSet<Date>> entry : timePartSpec.entrySet()) {
        for (Date dt : entry.getValue()) {
          get(cubeTableName, storageName, updatePeriod, entry.getKey()).add(TimePartition.of(updatePeriod, dt));
        }
      }
    }

    /** update partition timeline cache for deletion of time partition */
    public boolean updateForDeletion(String cubeTableName, String storageName, UpdatePeriod updatePeriod,
      Map<String, Date> timePartSpec) throws HiveException, LensException {
      // fail fast. All part cols mentioned in all partitions should exist.
      for (String partCol : timePartSpec.keySet()) {
        getAndFailFast(cubeTableName, storageName, updatePeriod, partCol);
      }
      boolean updated = false;
      for (Map.Entry<String, Date> entry : timePartSpec.entrySet()) {
        TimePartition part = TimePartition.of(updatePeriod, entry.getValue());
        if (!partitionExistsByFilter(cubeTableName, storageName, StorageConstants.getPartFilter(entry.getKey(),
          part.getDateString()))) {
          get(cubeTableName, storageName, updatePeriod, entry.getKey()).drop(part);
          updated = true;
        }
      }
      return updated;
    }
  }


  /**
   * Get the instance of {@link CubeMetastoreClient} corresponding to {@link HiveConf}
   *
   * @param conf                  conf
   * @return                      CubeMetastoreClient instance
   * @throws HiveException
   */
  public static CubeMetastoreClient getInstance(HiveConf conf) throws HiveException {
    String currentdb = SessionState.get().getCurrentDatabase();
    if (CLIENT_MAPPING.get(currentdb) == null) {
      CLIENT_MAPPING.put(currentdb, new CubeMetastoreClient(conf));
    }
    return CLIENT_MAPPING.get(currentdb);
  }

  private Hive getClient() throws HiveException {
    return Hive.get(config);
  }

  /**
   * Get cube metastore client conf
   *
   * @return HiveConf
   */
  public HiveConf getConf() {
    return config;
  }

  /**
   * Close the current metastore client
   */
  public static void close() {
    Hive.closeCurrent();
  }

  private void createOrAlterStorageHiveTable(Table parent, String storage, StorageTableDesc crtTblDesc)
    throws LensException {
    try {
      Table tbl = getStorage(storage).getStorageTable(getClient(), parent, crtTblDesc);
      if (tableExists(tbl.getTableName())) {
        // alter table
        alterHiveTable(tbl.getTableName(), tbl);
      } else {
        getClient().createTable(tbl);
        // do get to update cache
        getTable(tbl.getTableName());
      }
    } catch (HiveException e) {
      throw new LensException("Exception creating table", e);
    }
  }

  private Table createCubeHiveTable(AbstractCubeTable table) throws LensException {
    try {
      Table tbl = getClient().newTable(table.getName().toLowerCase());
      tbl.setTableType(TableType.MANAGED_TABLE);
      tbl.getTTable().getSd().setCols(table.getColumns());
      tbl.getTTable().getParameters().putAll(table.getProperties());
      getClient().createTable(tbl);
      // do get to update cache
      getTable(tbl.getTableName());
      return tbl;
    } catch (Exception e) {
      throw new LensException("Exception creating table", e);
    }
  }

  public void createStorage(Storage storage) throws LensException {
    createCubeHiveTable(storage);
    // do a get to update cache
    getStorage(storage.getName());
  }

  /**
   * Create cube in metastore defined by {@link Cube} or {@link DerivedCube} object
   *
   * @param cube the {@link Cube} object.
   * @throws LensException
   */
  public void createCube(CubeInterface cube) throws LensException {
    createCubeHiveTable((AbstractCubeTable) cube);
    // do a get to update cache
    getCube(cube.getName());
  }

  /**
   * Create cube defined by measures and dimensions
   *
   * @param name       Name of the cube
   * @param measures   Measures of the cube
   * @param dimensions Dimensions of the cube
   * @throws LensException
   */
  public void createCube(String name, Set<CubeMeasure> measures, Set<CubeDimAttribute> dimensions)
    throws LensException {
    Cube cube = new Cube(name, measures, dimensions);
    createCube(cube);
  }

  /**
   * Create cube defined by measures, dimensions and properties
   *
   * @param name       Name of the cube
   * @param measures   Measures of the cube
   * @param dimensions Dimensions of the cube
   * @param properties Properties of the cube
   * @throws LensException
   */
  public void createCube(String name, Set<CubeMeasure> measures, Set<CubeDimAttribute> dimensions,
    Map<String, String> properties) throws LensException {
    Cube cube = new Cube(name, measures, dimensions, properties);
    createCube(cube);
  }

  /**
   * Create cube defined by measures, dimensions and properties
   *
   * @param name       Name of the cube
   * @param measures   Measures of the cube
   * @param dimensions Dimensions of the cube
   * @param properties Properties of the cube
   * @throws LensException
   */
  public void createCube(String name, Set<CubeMeasure> measures, Set<CubeDimAttribute> dimensions,
    Set<ExprColumn> expressions, Map<String, String> properties) throws LensException {
    Cube cube = new Cube(name, measures, dimensions, expressions, null, properties, 0L);
    createCube(cube);
  }

  /**
   * Create cube defined by measures, dimensions and properties
   *
   * @param name        Name of the cube
   * @param measures    Measures of the cube
   * @param dimensions  Dimensions of the cube
   * @param expressions Expressions of the cube
   * @param chains      JoinChains of the cube
   * @param properties  Properties of the cube
   * @throws LensException
   */
  public void createCube(String name, Set<CubeMeasure> measures, Set<CubeDimAttribute> dimensions,
    Set<ExprColumn> expressions, Set<JoinChain> chains, Map<String, String> properties)
    throws LensException {
    Cube cube = new Cube(name, measures, dimensions, expressions, chains, properties, 0L);
    createCube(cube);
  }

  /**
   * Create dimension defined by attributes and properties
   *
   * @param name       Name of the dimension
   * @param attributes Attributes of the dimension
   * @param properties Properties of the dimension
   * @param weight     Weight of the dimension
   * @throws LensException
   */
  public void createDimension(String name, Set<CubeDimAttribute> attributes, Map<String, String> properties,
    double weight) throws LensException {
    Dimension dim = new Dimension(name, attributes, properties, weight);
    createDimension(dim);
  }

  /**
   * Create dimension in metastore defined by {@link Dimension} object
   *
   * @param dim the {@link Dimension} object.
   * @throws LensException
   */
  public void createDimension(Dimension dim) throws LensException {
    createCubeHiveTable(dim);
    // do a get to update cache
    getDimension(dim.getName());
  }

  /**
   * Create derived cube defined by measures, dimensions and properties
   *
   * @param parent     Name of the parent cube
   * @param name       Name of the derived cube
   * @param measures   Measures of the derived cube
   * @param dimensions Dimensions of the derived cube
   * @param properties Properties of the derived cube
   * @param weight     Weight of the derived cube
   * @throws LensException
   */
  public void createDerivedCube(String parent, String name, Set<String> measures, Set<String> dimensions,
    Map<String, String> properties, double weight) throws LensException {
    DerivedCube cube = new DerivedCube(name, measures, dimensions, properties, weight, (Cube) getCube(parent));
    createCube(cube);
  }

  /**
   * Create a cube fact table
   *
   * @param cubeName                The cube name to which fact belongs to.
   * @param factName                The fact name
   * @param columns                 The columns of fact table
   * @param storageAggregatePeriods Aggregate periods for the storages
   * @param weight                  Weight of the cube
   * @param properties              Properties of fact table
   * @param storageTableDescs       Map of storage to its storage table description
   * @throws LensException
   */
  public void createCubeFactTable(String cubeName, String factName, List<FieldSchema> columns,
    Map<String, Set<UpdatePeriod>> storageAggregatePeriods, double weight, Map<String, String> properties,
    Map<String, StorageTableDesc> storageTableDescs) throws LensException {
    CubeFactTable factTable =
      new CubeFactTable(cubeName, factName, columns, storageAggregatePeriods, weight, properties);
    createCubeTable(factTable, storageTableDescs);
    // do a get to update cache
    getCubeFact(factName);
  }

  /**
   *
   * @param baseCubeName             The cube name ot which segmentation belong to
   * @param segmentationName         The segmentation name
   * @param segments             Participating cube segements
   * @param weight                   Weight of segmentation
   * @param properties               Properties of segmentation
   * @throws LensException
   */
  public void createSegmentation(String baseCubeName, String segmentationName, Set<Segment> segments,
                                     double weight, Map<String, String> properties) throws LensException {
    Segmentation cubeSeg =
            new Segmentation(baseCubeName, segmentationName, segments, weight, properties);
    createSegmentation(cubeSeg);
    // do a get to update cache
    getSegmentation(segmentationName);
  }

  /**
   * Create a cube dimension table
   *
   * @param dimName           The dimension name to which the dim-table belongs to
   * @param dimTblName        dimension table name
   * @param columns           Columns of the dimension table
   * @param weight            Weight of the dimension table
   * @param storageNames      Storages on which dimension is available without any dumps
   * @param properties        Properties of dimension table
   * @param storageTableDescs Map of storage to its storage table description
   * @throws LensException
   */
  public void createCubeDimensionTable(String dimName, String dimTblName, List<FieldSchema> columns, double weight,
    Set<String> storageNames, Map<String, String> properties, Map<String, StorageTableDesc> storageTableDescs)
    throws LensException {
    CubeDimensionTable dimTable =
      new CubeDimensionTable(dimName, dimTblName, columns, weight, storageNames, properties);
    createCubeTable(dimTable, storageTableDescs);
    // do a get to update cache
    getDimensionTable(dimTblName);
  }

  /**
   * Create a cube dimension table
   *
   * @param dimName           The dimension name to which the dim-table belongs to
   * @param dimTblName        dimension table name
   * @param columns           Columns of the dimension table
   * @param weight            Weight of the dimension table
   * @param dumpPeriods       Storage names and their dump periods on which dimension is available
   * @param properties        properties of dimension table
   * @param storageTableDescs Map of storage to its storage table description
   * @throws LensException
   */
  public void createCubeDimensionTable(String dimName, String dimTblName, List<FieldSchema> columns, double weight,
    Map<String, UpdatePeriod> dumpPeriods, Map<String, String> properties,
    Map<String, StorageTableDesc> storageTableDescs) throws LensException {
    CubeDimensionTable dimTable = new CubeDimensionTable(dimName, dimTblName, columns, weight, dumpPeriods, properties);
    createCubeTable(dimTable, storageTableDescs);
    // do a get to update cache
    getDimensionTable(dimTblName);
  }

  /**
   * Create cube table defined and create all the corresponding storage tables
   *
   * @param cubeTable         Can be fact or dimension table
   * @param storageTableDescs Map of storage to its storage table description
   * @throws LensException
   */
  public void createCubeTable(AbstractCubeTable cubeTable, Map<String, StorageTableDesc> storageTableDescs)
    throws LensException {
    // create virtual cube table in metastore
    Table cTable = createCubeHiveTable(cubeTable);

    if (storageTableDescs != null) {
      // create tables for each storage
      for (Map.Entry<String, StorageTableDesc> entry : storageTableDescs.entrySet()) {
        createOrAlterStorageHiveTable(cTable, entry.getKey(), entry.getValue());
      }
    }
  }

  public void createSegmentation(Segmentation cubeSeg)
    throws LensException {
    // create virtual cube table in metastore
    createCubeHiveTable(cubeSeg);
  }

  /**
   * Adds storage to fact and creates corresponding storage table
   *
   * @param fact             The CubeFactTable
   * @param storage          The storage
   * @param updatePeriods    Update periods of the fact on the storage
   * @param storageTableDesc The storage table description
   * @throws LensException
   */
  public void addStorage(CubeFactTable fact, String storage, Set<UpdatePeriod> updatePeriods,
    StorageTableDesc storageTableDesc) throws LensException {
    fact.addStorage(storage, updatePeriods);
    createOrAlterStorageHiveTable(getTableWithTypeFailFast(fact.getName(), CubeTableType.FACT),
      storage, storageTableDesc);
    alterCubeTable(fact.getName(), getTableWithTypeFailFast(fact.getName(), CubeTableType.FACT), fact);
    updateFactCache(fact.getName());
  }

  /**
   * Adds storage to dimension and creates corresponding storage table
   *
   * @param dim              The CubeDimensionTable
   * @param storage          The storage
   * @param dumpPeriod       The dumpPeriod if any, null otherwise
   * @param storageTableDesc The storage table description
   * @throws LensException
   */
  public void addStorage(CubeDimensionTable dim, String storage, UpdatePeriod dumpPeriod,
    StorageTableDesc storageTableDesc) throws LensException {
    dim.alterSnapshotDumpPeriod(storage, dumpPeriod);
    createOrAlterStorageHiveTable(getTableWithTypeFailFast(dim.getName(), CubeTableType.DIM_TABLE),
      storage, storageTableDesc);
    alterCubeTable(dim.getName(), getTableWithTypeFailFast(dim.getName(), CubeTableType.DIM_TABLE), dim);
    updateDimCache(dim.getName());
  }

  /**
   * Add a partition specified by the storage partition desc on the storage passed.
   * <p></p>
   * TODO: separate methods for fact and dim partition addition.
   *
   * @param partSpec    The storage partition description
   * @param storageName The storage object
   * @param type
   * @throws HiveException
   */
  public List<Partition> addPartition(StoragePartitionDesc partSpec, String storageName, CubeTableType type)
    throws HiveException, LensException {
    return addPartitions(Collections.singletonList(partSpec), storageName, type);
  }

  /** batch addition */
  public List<Partition> addPartitions(List<StoragePartitionDesc> storagePartitionDescs, String storageName,
    CubeTableType type)
    throws HiveException, LensException {
    List<Partition> partsAdded = Lists.newArrayList();
    for (Map.Entry<String, Map<UpdatePeriod, List<StoragePartitionDesc>>> group : groupPartitionDescs(
      storagePartitionDescs).entrySet()) {
      String factOrDimtable = group.getKey();
      for (Map.Entry<UpdatePeriod, List<StoragePartitionDesc>> entry : group.getValue().entrySet()) {
        partsAdded.addAll(addPartitions(factOrDimtable, storageName, entry.getKey(), entry.getValue(), type));
      }
    }
    return partsAdded;
  }

  private List<Partition> addPartitions(String factOrDimTable, String storageName, UpdatePeriod updatePeriod,
    List<StoragePartitionDesc> storagePartitionDescs, CubeTableType type) throws HiveException, LensException {
    String storageTableName = getStorageTableName(factOrDimTable.trim(),
      Storage.getPrefix(storageName.trim())).toLowerCase();
    if (type == CubeTableType.DIM_TABLE) {
      // Adding partition in dimension table.
      Map<Map<String, String>, LatestInfo> latestInfos = Maps.newHashMap();
      for (Map.Entry<Map<String, String>, List<StoragePartitionDesc>> entry : groupByNonTimePartitions(
        storagePartitionDescs).entrySet()) {
        latestInfos.put(entry.getKey(),
          getDimTableLatestInfo(storageTableName, entry.getKey(), getTimePartSpecs(entry.getValue()), updatePeriod));
      }
      List<Partition> partsAdded =
        getStorage(storageName).addPartitions(getClient(), factOrDimTable, updatePeriod, storagePartitionDescs,
          latestInfos);
      ListIterator<Partition> iter = partsAdded.listIterator();
      while (iter.hasNext()) {
        if (iter.next().getSpec().values().contains(StorageConstants.LATEST_PARTITION_VALUE)) {
          iter.remove();
        }
      }
      latestLookupCache.add(storageTableName);
      return partsAdded;
    } else if (type == CubeTableType.FACT) {
      List<Partition> partsAdded = new ArrayList<>();
      // first update in memory, then add to hive table's partitions. delete is reverse.
      partitionTimelineCache.updateForAddition(factOrDimTable, storageName, updatePeriod,
              getTimePartSpecs(storagePartitionDescs, getStorageTableStartDate(storageTableName, factOrDimTable),
                      getStorageTableEndDate(storageTableName, factOrDimTable)));
      // Adding partition in fact table.
      if (storagePartitionDescs.size() > 0) {
        partsAdded = getStorage(storageName).addPartitions(getClient(), factOrDimTable, updatePeriod,
                storagePartitionDescs, null);
      }
      // update hive table
      alterTablePartitionCache(getStorageTableName(factOrDimTable, Storage.getPrefix(storageName)));
      return partsAdded;
    } else {
      throw new LensException("Can't add partitions to anything other than fact or dimtable");
    }
  }

  private Date getStorageTableStartDate(String storageTable, String factTableName)
    throws LensException {
    List<Date> startDates = getStorageTimes(storageTable, MetastoreUtil.getStoragetableStartTimesKey());
    startDates.add(getFactTable(factTableName).getStartTime());
    return Collections.max(startDates);
  }

  private Date getStorageTableEndDate(String storageTable, String factTableName)
    throws LensException {
    List<Date> endDates = getStorageTimes(storageTable, MetastoreUtil.getStoragetableEndTimesKey());
    endDates.add(getFactTable(factTableName).getEndTime());
    return Collections.min(endDates);
  }

  private Map<String, TreeSet<Date>> getTimePartSpecs(List<StoragePartitionDesc> storagePartitionDescs) {
    Map<String, TreeSet<Date>> timeSpecs = Maps.newHashMap();
    for (StoragePartitionDesc storagePartitionDesc : storagePartitionDescs) {
      for (Map.Entry<String, Date> entry : storagePartitionDesc.getTimePartSpec().entrySet()) {
        if (!timeSpecs.containsKey(entry.getKey())) {
          timeSpecs.put(entry.getKey(), Sets.<Date>newTreeSet());
        }
        timeSpecs.get(entry.getKey()).add(entry.getValue());
      }
    }
    return timeSpecs;
  }


  private Map<String, TreeSet<Date>> getTimePartSpecs(List<StoragePartitionDesc> storagePartitionDescs,
                                                      Date storageStartDate, Date storageEndDate) throws LensException {
    Date now = new Date();
    Map<String, HashSet<Date>> skippedParts = Maps.newHashMap();
    Map<String, TreeSet<Date>> timeSpecs = Maps.newHashMap();
    Iterator<StoragePartitionDesc> itr = storagePartitionDescs.iterator();
    while (itr.hasNext()) {
      StoragePartitionDesc storageDesc = itr.next();
      for (Map.Entry<String, Date> entry : storageDesc.getTimePartSpec().entrySet()) {
        if (!timeSpecs.containsKey(entry.getKey())) {
          timeSpecs.put(entry.getKey(), Sets.<Date>newTreeSet());
        }
        // check whether partition falls between storage table start_time and
        // end_time or d+2, in such case partition is eligible for registration.
        if ((entry.getValue().compareTo(storageStartDate) >= 0 && entry.getValue().compareTo(storageEndDate) < 0)
                && entry.getValue().compareTo(DateUtil.resolveRelativeDate("now +2 days", now)) < 0) {
          timeSpecs.get(entry.getKey()).add(entry.getValue());
        } else {
          if (!skippedParts.containsKey(entry.getKey())) {
            skippedParts.put(entry.getKey(), Sets.newHashSet(entry.getValue()));
          } else {
            skippedParts.get(entry.getKey()).add(entry.getValue());
          }
          itr.remove();
          break;
        }
      }
    }
    if (!skippedParts.isEmpty()) {
      log.info("List of partitions skipped : {}, because they fall before fact start time "
          + "or after end time or they are future partitions", skippedParts);
    }
    return timeSpecs;
  }

  private Map<String, Map<UpdatePeriod, List<StoragePartitionDesc>>> groupPartitionDescs(
    List<StoragePartitionDesc> partitionDescs) {
    Map<String, Map<UpdatePeriod, List<StoragePartitionDesc>>> ret = Maps.newHashMap();
    for (StoragePartitionDesc partitionDesc : partitionDescs) {
      if (ret.get(partitionDesc.getCubeTableName()) == null) {
        ret.put(partitionDesc.getCubeTableName(), Maps.<UpdatePeriod, List<StoragePartitionDesc>>newHashMap());
      }
      if (ret.get(partitionDesc.getCubeTableName()).get(partitionDesc.getUpdatePeriod()) == null) {
        ret.get(partitionDesc.getCubeTableName()).put(partitionDesc.getUpdatePeriod(),
          Lists.<StoragePartitionDesc>newArrayList());
      }
      ret.get(partitionDesc.getCubeTableName()).get(partitionDesc.getUpdatePeriod()).add(partitionDesc);
    }
    return ret;
  }

  /**
   * store back all timelines of given storage table to table properties
   *
   * @param storageTableName  storage table name
   * @throws HiveException
   */
  private void alterTablePartitionCache(String storageTableName) throws HiveException, LensException {
    Table table = getTable(storageTableName);
    Map<String, String> params = table.getParameters();
    if (partitionTimelineCache.get(storageTableName) != null) {
      for (UpdatePeriod updatePeriod : partitionTimelineCache.get(storageTableName).keySet()) {
        for (Map.Entry<String, PartitionTimeline> entry : partitionTimelineCache.get(storageTableName)
          .get(updatePeriod).entrySet()) {
          entry.getValue().updateTableParams(table);
        }
      }
      params.put(getPartitionTimelineCachePresenceKey(), "true");
      alterHiveTable(storageTableName, table);
    }
  }

  /** extract update period from partition properties */
  private UpdatePeriod deduceUpdatePeriod(Partition partition) {
    return UpdatePeriod.valueOf(partition.getParameters().get(MetastoreConstants.PARTITION_UPDATE_PERIOD));
  }

  private LatestInfo getDimTableLatestInfo(String storageTableName, Map<String, String> nonTimeParts,
    Map<String, TreeSet<Date>> timePartSpecs,
    UpdatePeriod updatePeriod) throws HiveException, LensException {
    Table hiveTable = getHiveTable(storageTableName);
    String timePartColsStr = hiveTable.getTTable().getParameters().get(MetastoreConstants.TIME_PART_COLUMNS);
    if (timePartColsStr != null) {
      LatestInfo latest = new LatestInfo();
      String[] timePartCols = StringUtils.split(timePartColsStr, ',');
      for (String partCol : timePartCols) {
        if (!timePartSpecs.containsKey(partCol)) {
          continue;
        }
        boolean makeLatest = true;
        Partition part = getLatestPart(storageTableName, partCol, nonTimeParts);
        Date pTimestamp = timePartSpecs.get(partCol).last();
        Date latestTimestamp = getLatestTimeStampFromPartition(part, partCol);
        if (latestTimestamp != null && pTimestamp.before(latestTimestamp)) {
          makeLatest = false;
        }

        if (makeLatest) {
          Map<String, String> latestParams = LensUtil.getHashMap(getLatestPartTimestampKey(partCol),
            updatePeriod.format(pTimestamp));
          latest.latestParts.put(partCol, new LatestPartColumnInfo(latestParams));
        }
      }
      return latest;
    } else {
      return null;
    }
  }

  private Map<Map<String, String>, List<StoragePartitionDesc>> groupByNonTimePartitions(
    List<StoragePartitionDesc> storagePartitionDescs) {
    Map<Map<String, String>, List<StoragePartitionDesc>> result = Maps.newHashMap();
    for (StoragePartitionDesc storagePartitionDesc : storagePartitionDescs) {
      if (result.get(storagePartitionDesc.getNonTimePartSpec()) == null) {
        result.put(storagePartitionDesc.getNonTimePartSpec(), Lists.<StoragePartitionDesc>newArrayList());
      }
      result.get(storagePartitionDesc.getNonTimePartSpec()).add(storagePartitionDesc);
    }
    return result;
  }

  private boolean isLatestPartOfDimtable(Partition part) {
    return part.getValues().contains(StorageConstants.LATEST_PARTITION_VALUE);
  }

  private Date getPartDate(Partition part, int timeColIndex) {
    String partVal = part.getValues().get(timeColIndex);
    String updatePeriodStr = part.getParameters().get(MetastoreConstants.PARTITION_UPDATE_PERIOD);
    Date partDate = null;
    if (updatePeriodStr != null) {
      UpdatePeriod partInterval = UpdatePeriod.valueOf(updatePeriodStr);
      try {
        partDate = partInterval.parse(partVal);
      } catch (ParseException e) {
        // ignore
      }
    }
    return partDate;
  }

  private LatestInfo getNextLatestOfDimtable(Table hiveTable, String timeCol, final int timeColIndex,
    UpdatePeriod updatePeriod, Map<String, String> nonTimePartSpec)
    throws HiveException {
    // getClient().getPartitionsByNames(tbl, partNames)
    List<Partition> partitions;
    try {
      partitions = getClient().getPartitionsByFilter(hiveTable, StorageConstants.getPartFilter(nonTimePartSpec));
      filterPartitionsByUpdatePeriod(partitions, updatePeriod);
      filterPartitionsByNonTimeParts(partitions, nonTimePartSpec, timeCol);
    } catch (TException e) {
      throw new HiveException(e);
    }

    // tree set contains partitions with timestamp as value for timeCol, in
    // descending order
    TreeSet<Partition> allPartTimeVals = new TreeSet<>(new Comparator<Partition>() {
      @Override
      public int compare(Partition o1, Partition o2) {
        Date partDate1 = getPartDate(o1, timeColIndex);
        Date partDate2 = getPartDate(o2, timeColIndex);
        if (partDate1 != null && partDate2 == null) {
          return -1;
        } else if (partDate1 == null && partDate2 != null) {
          return 1;
        } else if (partDate1 == null) {
          return o2.getTPartition().compareTo(o1.getTPartition());
        } else if (!partDate2.equals(partDate1)) {
          return partDate2.compareTo(partDate1);
        } else {
          return o2.getTPartition().compareTo(o1.getTPartition());
        }
      }
    });
    for (Partition part : partitions) {
      if (!isLatestPartOfDimtable(part)) {
        Date partDate = getPartDate(part, timeColIndex);
        if (partDate != null) {
          allPartTimeVals.add(part);
        }
      }
    }
    Iterator<Partition> it = allPartTimeVals.iterator();
    it.next(); // Skip itself. We have to find next latest.
    LatestInfo latest = null;
    if (it.hasNext()) {
      Partition nextLatest = it.next();
      latest = new LatestInfo();
      latest.setPart(nextLatest);
      Map<String, String> latestParams = LensUtil.getHashMap(getLatestPartTimestampKey(timeCol),
        nextLatest.getValues().get(timeColIndex));
      latest.addLatestPartInfo(timeCol, new LatestPartColumnInfo(latestParams));
    }
    return latest;
  }

  /**
   * Add a partition specified by the storage partition desc on the storage passed.
   *
   * @param cubeTableName   cube fact/dimension table name
   * @param storageName     storage name
   * @param timePartSpec    time partitions
   * @param nonTimePartSpec non time partitions
   * @param updatePeriod    update period of the partition
   * @throws HiveException
   */
  public void dropPartition(String cubeTableName, String storageName, Map<String, Date> timePartSpec,
    Map<String, String> nonTimePartSpec, UpdatePeriod updatePeriod) throws HiveException, LensException {
    String storageTableName = getStorageTableName(cubeTableName.trim(),
      Storage.getPrefix(storageName.trim())).toLowerCase();
    Table hiveTable = getHiveTable(storageTableName);
    List<FieldSchema> partCols = hiveTable.getPartCols();
    List<String> partColNames = new ArrayList<>(partCols.size());
    List<String> partVals = new ArrayList<>(partCols.size());
    for (FieldSchema column : partCols) {
      partColNames.add(column.getName());
      if (timePartSpec.containsKey(column.getName())) {
        partVals.add(updatePeriod.format(timePartSpec.get(column.getName())));
      } else if (nonTimePartSpec.containsKey(column.getName())) {
        partVals.add(nonTimePartSpec.get(column.getName()));
      } else {
        throw new HiveException("Invalid partspec, missing value for" + column.getName());
      }
    }
    if (isDimensionTable(cubeTableName)) {
      String timePartColsStr = hiveTable.getTTable().getParameters().get(MetastoreConstants.TIME_PART_COLUMNS);
      Map<String, LatestInfo> latest = new HashMap<>();
      boolean latestAvailable = false;
      if (timePartColsStr != null) {
        List<String> timePartCols = Arrays.asList(StringUtils.split(timePartColsStr, ','));
        for (String timeCol : timePartSpec.keySet()) {
          if (!timePartCols.contains(timeCol)) {
            throw new HiveException("Not a time partition column:" + timeCol);
          }
          int timeColIndex = partColNames.indexOf(timeCol);
          Partition part = getLatestPart(storageTableName, timeCol, nonTimePartSpec);

          Date latestTimestamp = getLatestTimeStampFromPartition(part, timeCol);
          Date dropTimestamp;
          try {
            dropTimestamp = updatePeriod.parse(updatePeriod.format(timePartSpec.get(timeCol)));
          } catch (ParseException e) {
            throw new HiveException(e);
          }
          // check if partition being dropped is the latest partition
          boolean isLatest = latestTimestamp != null && dropTimestamp.equals(latestTimestamp);
          if (isLatest) {
            for (int i = 0; i < partVals.size(); i++) {
              if (i != timeColIndex) {
                if (!part.getValues().get(i).equals(partVals.get(i))) {
                  isLatest = false;
                  break;
                }
              }
            }
          }
          if (isLatest) {
            LatestInfo latestInfo =
              getNextLatestOfDimtable(hiveTable, timeCol, timeColIndex, updatePeriod, nonTimePartSpec);
            latestAvailable = (latestInfo != null && latestInfo.part != null);
            latest.put(timeCol, latestInfo);
          } else {
            latestAvailable = true;
          }
        }
      } else {
        if (timePartSpec != null && !timePartSpec.isEmpty()) {
          throw new HiveException("Not time part columns" + timePartSpec.keySet());
        }
      }
      getStorage(storageName).dropPartition(getClient(), storageTableName, partVals, latest, nonTimePartSpec);
      if (!latestAvailable) {
        // dropping latest and could not find latest, removing the entry from latest lookup cache
        latestLookupCache.remove(storageTableName);
      }
    } else {
      // dropping fact partition
      getStorage(storageName).dropPartition(getClient(), storageTableName, partVals, null, null);
      if (partitionTimelineCache.updateForDeletion(cubeTableName, storageName, updatePeriod, timePartSpec)) {
        this.alterTablePartitionCache(storageTableName);
      }
    }
  }

  private Map<String, String> getPartitionSpec(UpdatePeriod updatePeriod, Map<String, Date> partitionTimestamps) {
    Map<String, String> partSpec = new HashMap<>();
    for (Map.Entry<String, Date> entry : partitionTimestamps.entrySet()) {
      String pval = updatePeriod.format(entry.getValue());
      partSpec.put(entry.getKey(), pval);
    }
    return partSpec;
  }

  public boolean tableExists(String tblName) throws HiveException {
    try {
      return (getClient().getTable(tblName.toLowerCase(), false) != null);
    } catch (HiveException e) {
      throw new HiveException("Could not check whether table exists", e);
    }
  }

  /** extract storage name and check in timeline cache for existance */
  public boolean factPartitionExists(CubeFactTable fact, FactPartition part, String storageTableName)
    throws HiveException, LensException {
    String storage = extractStorageName(fact, storageTableName);
    return partitionTimelineCache.partitionTimeExists(fact.getName(), storage, part.getPeriod(), part.getPartCol(),
      part.getPartSpec());
  }

  public boolean factPartitionExists(String factName, String storageName, UpdatePeriod updatePeriod,
                                     Map<String, Date> partitionTimestamp,
                                     Map<String, String> partSpec) throws HiveException, LensException {
    String storageTableName = getFactOrDimtableStorageTableName(factName, storageName);
    return partitionExists(storageTableName, updatePeriod, partitionTimestamp, partSpec);
  }

  public boolean partitionExists(String storageTableName, UpdatePeriod updatePeriod,
    Map<String, Date> partitionTimestamps) throws HiveException, LensException {
    return partitionExists(storageTableName, getPartitionSpec(updatePeriod, partitionTimestamps));
  }

  public boolean partitionExistsByFilter(String cubeTableName, String storageName, String filter) throws LensException {
    return partitionExistsByFilter(getStorageTableName(cubeTableName, Storage.getPrefix(storageName)),
      filter);
  }

  public boolean partitionExistsByFilter(String storageTableName, String filter) throws LensException {
    int parts;
    Table tbl;
    try {
      tbl = getTable(storageTableName);
    } catch (Exception e) {
      return false;
    }
    try {
      parts = getClient().getNumPartitionsByFilter(tbl, filter);
    } catch (Exception e) {
      throw new LensException("Could not find partitions for given filter", e);
    }
    return parts > 0;
  }

  public List<Partition> getAllParts(String storageTableName) throws HiveException, LensException {
    return getClient().getPartitions(getHiveTable(storageTableName));
  }

  public Partition getPartitionByFilter(String storageTableName, String filter) throws HiveException {
    List<Partition> parts = getPartitionsByFilter(storageTableName, filter);
    if (parts.size() != 1) {
      throw new HiveException(
        "filter " + filter + " did not result in unique partition. Got " + parts.size() + "partitions");
    }
    return parts.iterator().next();
  }

  public List<Partition> getPartitionsByFilter(String storageTableName, String filter) throws HiveException {
    try {
      return getClient().getPartitionsByFilter(getTable(storageTableName), filter);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  boolean partitionExists(String storageTableName, UpdatePeriod updatePeriod, Map<String, Date> partitionTimestamps,
    Map<String, String> nonTimePartSpec) throws HiveException, LensException {
    HashMap<String, String> partSpec = new HashMap<>(nonTimePartSpec);
    partSpec.putAll(getPartitionSpec(updatePeriod, partitionTimestamps));
    return partitionExists(storageTableName, partSpec);
  }

  private boolean partitionExists(String storageTableName, Map<String, String> partSpec) throws LensException {
    try {
      Table storageTbl = getTable(storageTableName);
      Partition p = getClient().getPartition(storageTbl, partSpec, false);
      return (p != null && p.getTPartition() != null);
    } catch (HiveException e) {
      throw new LensException("Could not check whether table exists", e);
    }
  }

  boolean dimPartitionExists(String dimTblName, String storageName, Map<String, Date> partitionTimestamps)
    throws HiveException, LensException {
    String storageTableName = getFactOrDimtableStorageTableName(dimTblName, storageName);
    return partitionExists(storageTableName, getDimensionTable(dimTblName).getSnapshotDumpPeriods().get(storageName),
            partitionTimestamps);
  }

  boolean latestPartitionExists(String factOrDimTblName, String storageName, String latestPartCol)
    throws HiveException, LensException {
    String storageTableName = getStorageTableName(factOrDimTblName, Storage.getPrefix(storageName));
    if (isDimensionTable(factOrDimTblName)) {
      return dimTableLatestPartitionExists(storageTableName);
    } else {
      return !partitionTimelineCache.noPartitionsExist(factOrDimTblName, storageName, latestPartCol);
    }
  }

  private boolean dimTableLatestPartitionExistsInMetastore(String storageTableName, String latestPartCol)
    throws LensException {
    return partitionExistsByFilter(storageTableName, StorageConstants.getLatestPartFilter(latestPartCol));
  }

  public boolean dimTableLatestPartitionExists(String storageTableName) {
    return latestLookupCache.contains(storageTableName.trim().toLowerCase());
  }

  Partition getLatestPart(String storageTableName, String latestPartCol, Map<String, String> nonTimeParts)
    throws HiveException {
    List<Partition> latestParts =
      getPartitionsByFilter(storageTableName, StorageConstants.getLatestPartFilter(latestPartCol, nonTimeParts));
    if (latestParts != null && !latestParts.isEmpty()) {
      return latestParts.get(0);
    }
    return null;
  }

  /**
   * Get the hive {@link Table} corresponding to the name
   *
   * @param tableName table name
   * @return {@link Table} object corresponding to the name
   * @throws LensException
   */
  public Table getHiveTable(String tableName) throws LensException {
    return getTable(tableName);
  }

  public List<String> getTimePartColNamesOfTable(String storageTableName) throws LensException {
    return getTimePartColNamesOfTable(getTable(storageTableName));
  }

  /** extract from table properties */
  public List<String> getTimePartColNamesOfTable(Table table) {
    List<String> ret = null;
    if (table.getParameters().containsKey(MetastoreConstants.TIME_PART_COLUMNS)) {
      ret = Arrays.asList(StringUtils.split(table.getParameters().get(MetastoreConstants.TIME_PART_COLUMNS), ","));
    }
    return ret == null ? new ArrayList<String>() : ret;
  }
  public Table getTableWithTypeFailFast(String tableName, CubeTableType type) throws LensException {
    return getTableWithType(tableName, type, true);
  }
  public Table getTableWithType(String tableName, CubeTableType type, boolean throwException) throws LensException {
    String typeName = type == null ? "nativetable" : type.name().toLowerCase();
    Table table = getTable(tableName, false);
    if (table == null) {
      if (throwException) {
        throw new LensException(LensCubeErrorCode.ENTITY_NOT_FOUND.getLensErrorInfo(), typeName, tableName);
      } else {
        return null;
      }
    }
    if (type == null && table.getParameters().get(MetastoreConstants.TABLE_TYPE_KEY) != null) {
      if (throwException) {
        throw new LensException(LensCubeErrorCode.ENTITY_NOT_FOUND.getLensErrorInfo(), typeName, tableName);
      } else {
        return null;
      }
    }
    if (type != null) {
      String typeStr = table.getParameters().get(MetastoreConstants.TABLE_TYPE_KEY);
      if (typeStr == null || CubeTableType.valueOf(typeStr.toUpperCase()) != type) {
        if (throwException) {
          throw new LensException(LensCubeErrorCode.ENTITY_NOT_FOUND.getLensErrorInfo(), typeName, tableName);
        } else {
          return null;
        }
      }
    }
    return table;
  }
  public Table getTable(String tableName) throws LensException {
    return getTable(tableName, true);
  }
  public Table getTable(String tableName, boolean throwException) throws LensException {
    Table tbl;
    try {
      tableName = tableName.trim().toLowerCase();
      tbl = allHiveTables.get(tableName);
      if (tbl == null) {
        synchronized (allHiveTables) {
          if (!allHiveTables.containsKey(tableName)) {
            tbl = getClient().getTable(tableName, throwException);
            if (enableCaching && tbl != null) {
              allHiveTables.put(tableName, tbl);
            }
          } else {
            tbl = allHiveTables.get(tableName);
          }
        }
      }
    } catch (HiveException e) {
      throw new LensException("Could not get table: " + tableName, e);
    }
    return tbl;
  }

  private Table refreshTable(String tableName) throws LensException {
    Table tbl;
    try {
      tableName = tableName.trim().toLowerCase();
      tbl = getClient().getTable(tableName);
      allHiveTables.put(tableName, tbl);
    } catch (HiveException e) {
      throw new LensException("Could not get table: " + tableName, e);
    }
    return tbl;
  }

  public void dropHiveTable(String table) throws LensException {
    try {
      getClient().dropTable(table);
    } catch (HiveException e) {
      throw new LensException("Couldn't drop hive table: " + table, e);
    }
    allHiveTables.remove(table.trim().toLowerCase());
  }

  /**
   * Is the table name passed a fact table?
   *
   * @param tableName table name
   * @return true if it is cube fact, false otherwise
   * @throws HiveException
   */
  public boolean isFactTable(String tableName) throws LensException {
    Table tbl = getTable(tableName);
    return isFactTable(tbl);
  }

  boolean isFactTable(Table tbl) {
    String tableType = tbl.getParameters().get(MetastoreConstants.TABLE_TYPE_KEY);
    return CubeTableType.FACT.name().equals(tableType);
  }


  boolean isFactTableForCube(Table tbl, String cube) {
    return isFactTable(tbl) && CubeFactTable.getCubeName(tbl.getTableName(), tbl.getParameters())
      .equalsIgnoreCase(cube.toLowerCase());
  }

  /**
   * Is the table name passed a dimension table?
   *
   * @param tableName table name
   * @return true if it is cube dimension, false otherwise
   * @throws LensException
   */
  public boolean isDimensionTable(String tableName) throws LensException {
    Table tbl = getTable(tableName);
    return isDimensionTable(tbl);
  }

  boolean isDimensionTable(Table tbl) {
    String tableType = tbl.getParameters().get(MetastoreConstants.TABLE_TYPE_KEY);
    return CubeTableType.DIM_TABLE.name().equals(tableType);
  }

  /**
   * Is the table name passed a cube?
   *
   * @param tableName table name
   * @return true if it is cube, false otherwise
   * @throws LensException
   */
  public boolean isCube(String tableName) throws LensException {
    if (allCubesPopulated) {
      if (allCubes.containsKey(tableName.trim().toLowerCase())) {
        return true;
      }
    } else {
      Table tbl = getTable(tableName);
      return isCube(tbl);
    }
    return false;
  }

  /**
   * Is the table name passed a dimension?
   *
   * @param tableName table name
   * @return true if it is dimension, false otherwise
   * @throws LensException
   */
  public boolean isDimension(String tableName) throws LensException {
    if (allDimensionsPopulated) {
      if (allDims.containsKey(tableName.trim().toLowerCase())) {
        return true;
      }
    } else {
      Table tbl = getTable(tableName);
      return isDimension(tbl);
    }
    return false;
  }

  /**
   * Is the hive table a cube table?
   *
   * @param tbl table
   * @return    whether it's a cube table or not
   */
  boolean isCube(Table tbl) {
    String tableType = tbl.getParameters().get(MetastoreConstants.TABLE_TYPE_KEY);
    return CubeTableType.CUBE.name().equals(tableType);
  }

  /**
   * Is the hive table a dimension?
   *
   * @param tbl  table
   * @return     whether the hive table is a dimension or not
   */
  boolean isDimension(Table tbl)  {
    String tableType = tbl.getParameters().get(MetastoreConstants.TABLE_TYPE_KEY);
    return CubeTableType.DIMENSION.name().equals(tableType);
  }

  /**
   * Get {@link CubeFactTable} object corresponding to the name
   *
   * @param tableName The cube fact name
   * @return Returns CubeFactTable if table name passed is a fact table, null otherwise
   * @throws LensException
   */

  public CubeFactTable getFactTable(String tableName) throws LensException {
    return new CubeFactTable(getTableWithTypeFailFast(tableName, CubeTableType.FACT));
  }

  public Segmentation getSegmentationTable(String tableName) throws HiveException, LensException {
    return new Segmentation(getTableWithTypeFailFast(tableName, CubeTableType.SEGMENTATION));
  }

  /**
   * Get {@link CubeDimensionTable} object corresponding to the name
   *
   * @param tableName The cube dimension name
   * @return Returns CubeDimensionTable if table name passed is a dimension table
   * @throws LensException if there is no dimension table with the name
   */
  public CubeDimensionTable getDimensionTable(String tableName) throws LensException {
    return getDimensionTable(tableName, true);
  }
  private CubeDimensionTable getDimensionTable(String tableName, boolean throwException)
    throws LensException {
    tableName = tableName.trim().toLowerCase();
    CubeDimensionTable dimTable = allDimTables.get(tableName);
    if (dimTable == null) {
      synchronized (allDimTables) {
        if (!allDimTables.containsKey(tableName)) {
          Table tbl = getTableWithType(tableName, CubeTableType.DIM_TABLE, throwException);
          dimTable = tbl == null ? null : getDimensionTable(tbl);
          if (enableCaching && dimTable != null) {
            allDimTables.put(tableName, dimTable);
            // update latest partition cache for all storages
            if (!dimTable.getStorages().isEmpty()) {
              for (String storageName : dimTable.getStorages()) {
                if (dimTable.hasStorageSnapshots(storageName)) {
                  String storageTableName = getFactOrDimtableStorageTableName(dimTable.getName(),
                    storageName);
                  if (dimTableLatestPartitionExistsInMetastore(storageTableName,
                    getDimension(dimTable.getDimName()).getTimedDimension())) {
                    latestLookupCache.add(storageTableName.trim().toLowerCase());
                  }
                }
              }
            }
          }
        } else {
          dimTable = allDimTables.get(tableName);
        }
      }
    }
    return dimTable;
  }

  private CubeDimensionTable getDimensionTable(Table tbl) {
    return new CubeDimensionTable(tbl);
  }

  /**
   * Get {@link Storage} object corresponding to the name
   *
   * @param storageName The storage name
   * @return Returns storage if name passed is a storage
   * @throws LensException if there is no storage by the name
   */
  public Storage getStorage(String storageName) throws LensException {
    return getStorage(storageName, true);
  }
  public Storage getStorage(String storageName, boolean throwException) throws LensException {
    storageName = storageName.trim().toLowerCase();
    Storage storage = allStorages.get(storageName);
    if (storage == null) {
      synchronized (allStorages) {
        if (!allStorages.containsKey(storageName)) {
          Table tbl = getTableWithType(storageName, CubeTableType.STORAGE, throwException);
          if (tbl != null) {
            storage = getStorage(tbl);
            if (enableCaching && storage != null) {
              allStorages.put(storageName, storage);
            }
          }
        } else {
          storage = allStorages.get(storageName);
        }
      }
    }
    return storage;
  }

  private Storage getStorage(Table tbl) throws LensException {
    return Storage.createInstance(tbl);
  }

  /**
   * Get {@link Cube} object corresponding to the name
   *
   * @param tableName The cube name
   * @return Returns cube is table name passed is a cube
   * @throws LensException when the table name does not correspond to a cube
   */
  public CubeInterface getCube(String tableName) throws LensException {
    return getCube(tableName, true);
  }

  private CubeInterface getCube(String tableName, boolean throwException) throws LensException {
    if (tableName == null) {
      return null;
    }
    tableName = tableName.trim().toLowerCase();
    CubeInterface cube = allCubes.get(tableName);
    if (cube == null) {
      synchronized (allCubes) {
        if (!allCubes.containsKey(tableName)) {
          Table tbl = getTableWithType(tableName, CubeTableType.CUBE, throwException);
          cube = tbl == null ? null : getCube(tbl);
          if (enableCaching && cube != null) {
            allCubes.put(tableName, cube);
          }
        } else {
          cube = allCubes.get(tableName);
        }
      }
    }
    return cube;
  }

  /**
   * Get {@link Cube} object corresponding to the name
   *
   * @param tableName The cube name
   * @return Returns Dimension if table name passed is a Dimension
   * @throws LensException if the table name passed is not a Dimension
   */
  public Dimension getDimension(String tableName) throws LensException {
    return getDimension(tableName, true);
  }
  private Dimension getDimension(String tableName, boolean throwException) throws LensException {
    if (tableName == null) {
      return null;
    }
    tableName = tableName.trim().toLowerCase();
    Dimension dim = allDims.get(tableName);
    if (dim == null) {
      synchronized (allDims) {
        if (!allDims.containsKey(tableName)) {
          Table tbl = getTableWithType(tableName, CubeTableType.DIMENSION, throwException);
          dim = tbl == null ? null : getDimension(tbl);
          if (enableCaching && dim != null) {
            allDims.put(tableName, dim);
          }
        } else {
          dim = allDims.get(tableName);
        }
      }
    }
    return dim;
  }

  /**
   * Get {@link Cube} object corresponding to the name
   *
   * @param tableName The cube name
   * @return Returns cube is table name passed is a cube
   * @throws LensException if there is no cube by the name
   */
  public CubeFactTable getCubeFact(String tableName) throws LensException {
    return getCubeFact(tableName, true);
  }
  private CubeFactTable getCubeFact(String tableName, boolean throwException) throws LensException {
    tableName = tableName.trim().toLowerCase();
    CubeFactTable fact = allFactTables.get(tableName);
    if (fact == null) {
      synchronized (allFactTables) {
        if (!allFactTables.containsKey(tableName)) {
          Table tbl = getTableWithType(tableName, CubeTableType.FACT, throwException);
          fact = tbl == null ? null : new CubeFactTable(tbl);
          if (enableCaching && fact != null) {
            allFactTables.put(tableName, fact);
          }
        } else {
          fact = allFactTables.get(tableName);
        }
      }
    }
    return fact;
  }

  public Segmentation getSegmentation(String segName) throws LensException {
    return getSegmentation(segName, true);
  }
  public Segmentation getSegmentation(String segName, boolean throwException) throws LensException {
    segName = segName.trim().toLowerCase();
    Segmentation seg = allSegmentations.get(segName);
    if (seg == null) {
      synchronized (allSegmentations) {
        if (!allSegmentations.containsKey(segName)) {
          Table tbl = getTableWithType(segName, CubeTableType.SEGMENTATION, throwException);
          seg = tbl == null ? null : new Segmentation(tbl);
          if (enableCaching && seg != null) {
            allSegmentations.put(segName, seg);
          }
        } else {
          seg = allSegmentations.get(segName);
        }
      }
    }
    return seg;
  }


  private CubeInterface getCube(Table tbl) throws LensException {
    String parentCube = tbl.getParameters().get(getParentCubeNameKey(tbl.getTableName()));
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
   * @throws LensException
   */
  public Collection<CubeDimensionTable> getAllDimensionTables() throws LensException {
    if (!allDimTablesPopulated) {
      List<CubeDimensionTable> dimTables = new ArrayList<>();
      try {
        for (String table : getAllHiveTableNames()) {
          CubeDimensionTable dim = getDimensionTable(table, false);
          if (dim != null) {
            dimTables.add(dim);
          }
        }
      } catch (HiveException e) {
        throw new LensException("Could not get all dimension tables", e);
      }
      allDimTablesPopulated = enableCaching;
      return dimTables;
    } else {
      return allDimTables.values();
    }
  }

  /**
   * Get all storages in metastore
   *
   * @return List of Storage objects
   * @throws LensException
   */
  public Collection<Storage> getAllStorages() throws LensException {
    if (!allStoragesPopulated) {
      List<Storage> storages = new ArrayList<>();
      try {
        for (String table : getAllHiveTableNames()) {
          Storage storage = getStorage(table, false);
          if (storage != null) {
            storages.add(storage);
          }
        }
      } catch (HiveException e) {
        throw new LensException("Could not get all storages", e);
      }
      allStoragesPopulated = enableCaching;
      return storages;
    } else {
      return allStorages.values();
    }
  }

  /**
   * Get all cubes in metastore
   *
   * @return List of Cube objects
   * @throws LensException
   */
  public Collection<CubeInterface> getAllCubes() throws LensException {
    if (!allCubesPopulated) {
      List<CubeInterface> cubes = new ArrayList<>();
      try {
        for (String table : getAllHiveTableNames()) {
          CubeInterface cube = getCube(table, false);
          if (cube != null) {
            cubes.add(cube);
          }
        }
      } catch (HiveException e) {
        throw new LensException("Could not get all cubes", e);
      }
      allCubesPopulated = enableCaching;
      return cubes;
    } else {
      return allCubes.values();
    }
  }

  /**
   * Get all cubes in metastore
   *
   * @return List of Cube objects
   * @throws LensException
   */
  public Collection<Dimension> getAllDimensions() throws LensException {
    if (!allDimensionsPopulated) {
      List<Dimension> dims = new ArrayList<>();
      try {
        for (String table : getAllHiveTableNames()) {
          Dimension dim =  getDimension(table, false);
          if (dim != null) {
            dims.add(dim);
          }
        }
      } catch (HiveException e) {
        throw new LensException("Could not get all dimensions", e);
      }
      allDimensionsPopulated = enableCaching;
      return dims;
    } else {
      return allDims.values();
    }
  }

  /**
   * Get all facts in metastore
   *
   * @return List of Cube Fact Table objects
   * @throws LensException
   */
  public Collection<CubeFactTable> getAllFacts() throws LensException {
    if (!allFactTablesPopulated) {
      List<CubeFactTable> facts = new ArrayList<>();
      try {
        for (String table : getAllHiveTableNames()) {
          CubeFactTable fact = getCubeFact(table, false);
          if (fact != null) {
            facts.add(fact);
          }
        }
      } catch (HiveException e) {
        throw new LensException("Could not get all fact tables", e);
      }
      allFactTablesPopulated = enableCaching;
      return facts;
    } else {
      return allFactTables.values();
    }
  }

  /**
   * Get all segmentations in metastore
   *
   * @return List of segmentation objects
   * @throws LensException
   */
  public Collection<Segmentation> getAllSegmentations() throws LensException {
    if (!allSegmentationPopulated) {
      List<Segmentation> segs = new ArrayList<>();
      try {
        for (String table : getAllHiveTableNames()) {
          Segmentation seg = getSegmentation(table, false);
          if (seg != null) {
            segs.add(seg);
          }
        }
      } catch (HiveException e) {
        throw new LensException("Could not get all fact tables", e);
      }
      allFactTablesPopulated = enableCaching;
      return segs;
    } else {
      return allSegmentations.values();
    }
  }



  private Collection<String> getAllHiveTableNames() throws HiveException, LensException {
    if (!allTablesPopulated) {
      List<String> allTables = getClient().getAllTables();
      for (String tblName : allTables) {
        // getTable call here would add the table to allHiveTables
        getTable(tblName);
      }
      allTablesPopulated = enableCaching;
      return allTables;
    } else {
      return allHiveTables.keySet();
    }
  }

  /**
   * Get all fact tables of the cube.
   *
   * @param cube Cube object
   * @return List of fact tables
   * @throws LensException
   */
  public List<CubeFactTable> getAllFacts(CubeInterface cube) throws LensException {
    String cubeName = null;
    if (cube != null) {
      if (cube instanceof DerivedCube) {
        cube = ((DerivedCube) cube).getParent();
      }
      cubeName = cube.getName();
    }
    List<CubeFactTable> cubeFacts = new ArrayList<>();
    for (CubeFactTable fact : getAllFacts()) {
      if (cubeName == null || fact.getCubeName().equalsIgnoreCase(cubeName)) {
        cubeFacts.add(fact);
      }
    }
    return cubeFacts;
  }

  public List<Segmentation> getAllSegmentations(CubeInterface cube) throws LensException {
    String cubeName = null;
    if (cube != null) {
      if (cube instanceof DerivedCube) {
        cube = ((DerivedCube) cube).getParent();
      }
      cubeName = cube.getName();
    }
    List<Segmentation> cubeSegs = new ArrayList<>();
    for (Segmentation seg : getAllSegmentations()) {
      if (cubeName == null || seg.getBaseCube().equalsIgnoreCase(cubeName)) {
        cubeSegs.add(seg);
      }
    }
    return cubeSegs;
  }


  /**
   * Get all derived cubes of the cube, that have all fields queryable together
   *
   * @param cube Cube object
   * @return List of DerivedCube objects
   * @throws LensException
   */
  public List<DerivedCube> getAllDerivedQueryableCubes(CubeInterface cube) throws LensException {
    List<DerivedCube> dcubes = new ArrayList<>();
    for (CubeInterface cb : getAllCubes()) {
      if (cb.isDerivedCube() && ((DerivedCube) cb).getParent().getName().equalsIgnoreCase(cube.getName())
        && cb.allFieldsQueriable()) {
        dcubes.add((DerivedCube) cb);
      }
    }
    return dcubes;
  }

  /**
   * Get all dimension tables of the dimension.
   *
   * @param dim Dimension object
   * @return List of fact tables
   * @throws LensException
   */
  public List<CubeDimensionTable> getAllDimensionTables(Dimension dim) throws LensException {
    List<CubeDimensionTable> dimTables = new ArrayList<>();
    for (CubeDimensionTable dimTbl : getAllDimensionTables()) {
      if (dim == null || dimTbl.getDimName().equalsIgnoreCase(dim.getName().toLowerCase())) {
        dimTables.add(dimTbl);
      }
    }
    return dimTables;
  }

  public boolean partColExists(String tableName, String partCol) throws LensException {
    Table tbl = getTable(tableName);
    for (FieldSchema f : tbl.getPartCols()) {
      if (f.getName().equalsIgnoreCase(partCol)) {
        return true;
      }
    }
    return false;
  }

  /**
   *
   * @param table     table name
   * @param hiveTable hive table
   * @param cubeTable lens cube table
   * @return true if columns changed in alter
   * @throws LensException
   */
  private boolean alterCubeTable(String table, Table hiveTable, AbstractCubeTable cubeTable) throws LensException {
    hiveTable.getParameters().putAll(cubeTable.getProperties());
    boolean columnsChanged = !(hiveTable.getCols().equals(cubeTable.getColumns()));
    if (columnsChanged) {
      hiveTable.getTTable().getSd().setCols(cubeTable.getColumns());
    }
    hiveTable.getTTable().getParameters().putAll(cubeTable.getProperties());
    try {
      getClient().alterTable(table, hiveTable, null);
    } catch (Exception e) {
      throw new LensException(e);
    }
    return columnsChanged;
  }

  public void pushHiveTable(Table hiveTable) throws HiveException, LensException {
    alterHiveTable(hiveTable.getTableName(), hiveTable);
  }

  public void alterHiveTable(String table, Table hiveTable) throws HiveException, LensException {
    try {
      getClient().alterTable(table, hiveTable, null);
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
   * @param cubeName The cube name to be altered
   * @param cube     The new cube definition {@link Cube} or {@link DerivedCube}
   * @throws HiveException
   */
  public void alterCube(String cubeName, CubeInterface cube) throws HiveException, LensException {
    Table cubeTbl = getTableWithTypeFailFast(cubeName, CubeTableType.CUBE);
    alterCubeTable(cubeName, cubeTbl, (AbstractCubeTable) cube);
    if (enableCaching) {
      allCubes.put(cubeName.trim().toLowerCase(), getCube(refreshTable(cubeName)));
    }
  }

  /**
   * Alter dimension specified by the dimension name to new definition
   *
   * @param dimName The cube name to be altered
   * @param newDim  The new dimension definition
   * @throws HiveException
   */
  public void alterDimension(String dimName, Dimension newDim) throws HiveException, LensException {
    Table tbl = getTableWithTypeFailFast(dimName, CubeTableType.DIMENSION);
    alterCubeTable(dimName, tbl, newDim);
    if (enableCaching) {
      allDims.put(dimName.trim().toLowerCase(), getDimension(refreshTable(dimName)));
    }
  }

  /**
   * Alter storage specified by the name to new definition
   *
   * @param storageName The storage name to be altered
   * @param storage     The new storage definition
   * @throws LensException
   */
  public void alterStorage(String storageName, Storage storage) throws LensException, HiveException {
    Table storageTbl = getTableWithTypeFailFast(storageName, CubeTableType.STORAGE);
    alterCubeTable(storageName, storageTbl, storage);
    if (enableCaching) {
      allStorages.put(storageName.trim().toLowerCase(), getStorage(refreshTable(storageName)));
    }
  }

  /**
   * Drop a storage
   *
   * @param storageName  storage name
   * @throws LensException
   */
  public void dropStorage(String storageName) throws LensException {
    getTableWithTypeFailFast(storageName, CubeTableType.STORAGE);
    allStorages.remove(storageName.trim().toLowerCase());
    dropHiveTable(storageName);
  }

  /**
   * Drop a cube
   *
   * @param cubeName cube name
   * @throws LensException
   */
  public void dropCube(String cubeName) throws LensException {
    getTableWithTypeFailFast(cubeName, CubeTableType.CUBE);
    allCubes.remove(cubeName.trim().toLowerCase());
    dropHiveTable(cubeName);
  }

  /**
   * Drop a dimension
   *
   * @param dimName dimension name to be dropped
   * @throws LensException
   */
  public void dropDimension(String dimName) throws LensException {
    getTableWithTypeFailFast(dimName, CubeTableType.DIMENSION);
    allDims.remove(dimName.trim().toLowerCase());
    dropHiveTable(dimName);
  }

  /**
   * Drop a fact with cascade  flag
   *
   * @param factName fact name
   * @param cascade  If true, will drop all the storages of the fact
   * @throws LensException
   */
  public void dropFact(String factName, boolean cascade) throws LensException {
    getTableWithTypeFailFast(factName, CubeTableType.FACT);
    CubeFactTable fact = getFactTable(factName);
    if (cascade) {
      for (String storage : fact.getStorages()) {
        dropStorageFromFact(factName, storage, false);
      }
    }
    dropHiveTable(factName);
    allFactTables.remove(factName.trim().toLowerCase());
  }


  public void dropSegmentation(String segName) throws LensException {
    getTableWithTypeFailFast(segName, CubeTableType.SEGMENTATION);
    dropHiveTable(segName);
    allSegmentations.remove(segName.trim().toLowerCase());
  }

  /**
   * Drop a storage from fact
   *
   * @param factName fact name
   * @param storage  storage name
   * @throws LensException
   */
  public void dropStorageFromFact(String factName, String storage) throws LensException {
    CubeFactTable cft = getFactTable(factName);
    cft.dropStorage(storage);
    dropHiveTable(getFactOrDimtableStorageTableName(factName, storage));
    alterCubeTable(factName, getTableWithTypeFailFast(factName, CubeTableType.FACT), cft);
    updateFactCache(factName);
  }

  // updateFact will be false when fact is fully dropped
  private void dropStorageFromFact(String factName, String storage, boolean updateFact)
    throws LensException {
    CubeFactTable cft = getFactTable(factName);
    dropHiveTable(getFactOrDimtableStorageTableName(factName, storage));
    if (updateFact) {
      cft.dropStorage(storage);
      alterCubeTable(factName, getTableWithTypeFailFast(factName, CubeTableType.FACT), cft);
      updateFactCache(factName);
    }
  }

  /**
   * Drop a storage from dimension
   *
   * @param dimTblName dim table name
   * @param storage    storage
   * @throws HiveException
   */
  public void dropStorageFromDim(String dimTblName, String storage) throws HiveException, LensException {
    dropStorageFromDim(dimTblName, storage, true);
  }

  // updateDimTbl will be false when dropping dimTbl
  private void dropStorageFromDim(String dimTblName, String storage, boolean updateDimTbl)
    throws LensException {
    CubeDimensionTable cdt = getDimensionTable(dimTblName);
    String storageTableName = getFactOrDimtableStorageTableName(dimTblName, storage);
    dropHiveTable(storageTableName);
    latestLookupCache.remove(storageTableName.trim().toLowerCase());
    if (updateDimTbl) {
      cdt.dropStorage(storage);
      alterCubeTable(dimTblName, getTableWithTypeFailFast(dimTblName, CubeTableType.DIM_TABLE), cdt);
      updateDimCache(dimTblName);
    }
  }

  /**
   * Drop the dimension table
   *
   * @param dimTblName dim table name
   * @param cascade    If true, will drop all the dimension storages
   * @throws HiveException
   */
  public void dropDimensionTable(String dimTblName, boolean cascade) throws LensException {
    getTableWithTypeFailFast(dimTblName, CubeTableType.DIM_TABLE);
    CubeDimensionTable dim = getDimensionTable(dimTblName);
    if (cascade) {
      for (String storage : dim.getStorages()) {
        dropStorageFromDim(dimTblName, storage, false);
      }
    }
    dropHiveTable(dimTblName);
    allDimTables.remove(dimTblName.trim().toLowerCase());
  }

  /**
   * Alter a cubefact with new definition and alter underlying storage tables as well.
   *
   * @param factTableName     fact table name
   * @param cubeFactTable     cube fact table
   * @param storageTableDescs storage table desc objects
   *
   * @throws HiveException
   */
  public void alterCubeFactTable(String factTableName, CubeFactTable cubeFactTable,
                                 Map<String, StorageTableDesc> storageTableDescs,
                                 Map<String, String> props)
    throws HiveException, LensException {
    Table factTbl = getTableWithTypeFailFast(factTableName, CubeTableType.FACT);
    if (!props.isEmpty()) {
      cubeFactTable.getProperties().putAll(props);
    }
    alterCubeTable(factTableName, factTbl, cubeFactTable);
    if (storageTableDescs != null) {
      // create/alter tables for each storage
      for (Map.Entry<String, StorageTableDesc> entry : storageTableDescs.entrySet()) {
        createOrAlterStorageHiveTable(getTable(factTableName), entry.getKey(), entry.getValue());
      }
    }
    updateFactCache(factTableName);
  }

  public void alterSegmentation(String segName, Segmentation seg)
    throws HiveException, LensException {
    getTableWithTypeFailFast(segName, CubeTableType.SEGMENTATION);
    if (!(getSegmentation(segName) == seg)) {
      dropSegmentation(segName);
      createSegmentation(seg);
      updateSegmentationCache(segName);
    }
  }

  private void updateSegmentationCache(String segmentName) throws HiveException, LensException {
    if (enableCaching) {
      allSegmentations.put(segmentName.trim().toLowerCase(), new Segmentation(refreshTable(segmentName)));
    }
  }

  private void updateFactCache(String factTableName) throws LensException {
    if (enableCaching) {
      allFactTables.put(factTableName.trim().toLowerCase(), new CubeFactTable(refreshTable(factTableName)));
    }
  }

  private void updateDimCache(String dimTblName) throws LensException {
    if (enableCaching) {
      allDimTables.put(dimTblName.trim().toLowerCase(), getDimensionTable(refreshTable(dimTblName)));
    }
  }

  /**
   * Alter dimension table with new dimension definition and underlying storage tables as well
   *
   * @param dimTableName         dim table name
   * @param cubeDimensionTable   cube dimention table
   * @throws HiveException
   */
  public void alterCubeDimensionTable(String dimTableName, CubeDimensionTable cubeDimensionTable,
    Map<String, StorageTableDesc> storageTableDescs) throws HiveException, LensException {
    Table dimTbl = getTableWithTypeFailFast(dimTableName, CubeTableType.DIM_TABLE);
    alterCubeTable(dimTableName, dimTbl, cubeDimensionTable);
    if (storageTableDescs != null) {
      // create/alter tables for each storage
      for (Map.Entry<String, StorageTableDesc> entry : storageTableDescs.entrySet()) {
        createOrAlterStorageHiveTable(getTable(dimTableName), entry.getKey(), entry.getValue());
      }
    }
    updateDimCache(dimTableName);
  }

  private List<Date> getStorageTimes(String storageTableName, String timeKey) throws LensException {
    Date now = new Date();
    List<Date> storageTimes = new ArrayList<>();
    String property;
    property = getTable(storageTableName).getProperty(timeKey);
    if (StringUtils.isNotBlank(property)) {
      for (String timeStr : property.split("\\s*,\\s*")) {
        storageTimes.add(resolveDate(timeStr, now));
      }
    }
    return storageTimes;
  }

  /*
   * Storage table is not a candidate for range (t0, tn) :
   * if start_time is after tn; or end date is before t0.
   */
  public boolean isStorageTableCandidateForRange(String storageTableName, Date fromDate, Date toDate)
    throws LensException {
    List<Date> storageEndDates = getStorageTimes(storageTableName, MetastoreUtil.getStoragetableEndTimesKey());
    for(Date endDate : storageEndDates) {
      // endDate is exclusive
      if (endDate.before(fromDate) || endDate.equals(fromDate)) {
        log.debug("from date {} is after validity end time: {}, hence discarding {}",
          fromDate, endDate, storageTableName);
        return false;
      }
    }

    List<Date> storageStartDates = getStorageTimes(storageTableName, MetastoreUtil.getStoragetableStartTimesKey());
    for(Date startDate : storageStartDates) {
      // toDate is exclusive on the range
      if (startDate.after(toDate) || startDate.equals(toDate)) {
        log.debug("to date {} is before validity start time: {}, hence discarding {}",
          toDate, startDate, storageTableName);
        return false;
      }
    }
    return true;
  }

  // Check if partition is valid wrt start and end dates
  public boolean isStorageTablePartitionACandidate(String storageTableName, Date partDate)
    throws LensException {
    List<Date> storageStartDates = getStorageTimes(storageTableName, MetastoreUtil.getStoragetableStartTimesKey());
    for(Date startDate : storageStartDates) {
      if (partDate.before(startDate)) {
        log.info("part date {} is before validity start time: {}, hence discarding {}",
          partDate, startDate, storageTableName);
        return false;
      }
    }

    List<Date> storageEndDates = getStorageTimes(storageTableName, MetastoreUtil.getStoragetableEndTimesKey());
    for(Date endDate : storageEndDates) {
      // end date should be exclusive
      if (partDate.after(endDate) || partDate.equals(endDate)) {
        log.info("part date {} is after validity end time: {}, hence discarding {}",
          partDate, endDate, storageTableName);
        return false;
      }
    }
    return true;
  }

  public boolean isStorageTableCandidateForRange(String storageTableName, String fromDate, String toDate) throws
    HiveException, LensException {
    Date now = new Date();
    return isStorageTableCandidateForRange(storageTableName, resolveDate(fromDate, now), resolveDate(toDate, now));
  }
}

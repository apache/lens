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
package org.apache.lens.server.api.metastore;

import java.util.Date;
import java.util.List;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.metastore.*;
import org.apache.lens.server.api.LensService;
import org.apache.lens.server.api.SessionValidator;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Server api for OLAP Cube Metastore.
 */
public interface CubeMetastoreService extends LensService, SessionValidator {

  /** The constant NAME */
  String NAME = "metastore";

  /**
   * Get current database used by the CubeMetastoreClient
   *
   * @return the current database name
   */
  String getCurrentDatabase(LensSessionHandle sessionid) throws LensException;

  /**
   * Change the current database used by the CubeMetastoreClient
   *
   * @param database
   */
  void setCurrentDatabase(LensSessionHandle sessionid, String database) throws LensException;

  /**
   * Drop a database from cube metastore
   *
   * @param database database name
   * @param cascade  flag indicating if the tables in the database should be dropped as well
   */
  void dropDatabase(LensSessionHandle sessionid, String database, boolean cascade) throws LensException;

  /**
   * Create a database in the metastore
   *
   * @param database database name
   * @param ignore   ignore if database already exists
   */
  void createDatabase(LensSessionHandle sessionid, String database, boolean ignore) throws LensException;

  /**
   * Get names of all databases in this metastore
   *
   * @return list of database names
   */
  List<String> getAllDatabases(LensSessionHandle sessionid) throws LensException;

  /**
   * Create a storage
   *
   * @param sessionid
   * @param storage
   * @throws LensException
   */
  void createStorage(LensSessionHandle sessionid, XStorage storage) throws LensException;

  /**
   * Drop a storage specified by name
   *
   * @param sessionid
   * @param storageName
   * @throws LensException
   */
  void dropStorage(LensSessionHandle sessionid, String storageName) throws LensException;

  /**
   * Alter storage specified by name, with new definition
   *
   * @param sessionid
   * @param storageName
   * @param storage
   * @throws LensException
   */
  void alterStorage(LensSessionHandle sessionid, String storageName, XStorage storage) throws LensException;

  /**
   * Get Storage specified by name
   *
   * @param sessionid
   * @param storageName
   * @throws LensException
   */
  XStorage getStorage(LensSessionHandle sessionid, String storageName) throws LensException;

  /**
   * Get all storage names in current database
   *
   * @param sessionid
   * @return returns list of the storage names
   * @throws LensException
   */
  List<String> getAllStorageNames(LensSessionHandle sessionid) throws LensException;

  /**
   * Get names of all cubes in the current database, includes both base cubes and derived cubes
   *
   * @return list of cube names
   */
  List<String> getAllCubeNames(LensSessionHandle sessionid) throws LensException;

  /**
   * Get names of all base cube names in the current database
   *
   * @return list of cube names
   */
  List<String> getAllBaseCubeNames(LensSessionHandle sessionid) throws LensException;

  /**
   * Get names of all derived cubes in the current database
   *
   * @return list of cube names
   */
  List<String> getAllDerivedCubeNames(LensSessionHandle sessionid) throws LensException;

  /**
   * Get names of all cubes, which can be queried in the current database
   *
   * @param sessionid session id
   * @return list of cube names
   */
  List<String> getAllQueryableCubeNames(LensSessionHandle sessionid) throws LensException;

  /**
   * Get native table for the given name
   *
   * @param sessionid session id
   * @param name      The table name
   * @return {@link XNativeTable} object
   * @throws LensException
   */
  XNativeTable getNativeTable(LensSessionHandle sessionid, String name) throws LensException;

  /**
   * Get names of all native tables
   *
   * @param sessionid session id
   * @param dboption  To get from current or all, the option is ignored if dbname is passed
   * @param dbName    The db name
   * @return list of table names
   */
  List<String> getAllNativeTableNames(LensSessionHandle sessionid,
    String dboption, String dbName) throws LensException;

  /**
   * Create a cube based on JAXB Cube object
   */
  void createCube(LensSessionHandle sessionid, XCube cube) throws LensException;

  /**
   * Get a cube from the metastore
   *
   * @param cubeName
   * @return JAXB Cube object
   */
  XCube getCube(LensSessionHandle sessionid, String cubeName) throws LensException;

  /**
   * Drop a cube from the metastore in the currently deleted database.
   *
   * @param cubeName
   */
  void dropCube(LensSessionHandle sessionid, String cubeName) throws LensException;

  /**
   * Update an existing cube
   *
   * @param cube JAXB Cube object
   */
  void updateCube(LensSessionHandle sessionid, XCube cube) throws LensException;

  /**
   * Create a dimension based on JAXB Dimension object
   */
  void createDimension(LensSessionHandle sessionid, XDimension dimension) throws LensException;

  /**
   * Get a dimension from the metastore
   *
   * @param dimName
   * @return JAXB Dimension object
   */
  XDimension getDimension(LensSessionHandle sessionid, String dimName) throws LensException;

  /**
   * Drop a dimension from the metastore in the currently deleted database.
   *
   * @param sessionid, dimName
   */
  void dropDimension(LensSessionHandle sessionid, String dimName) throws LensException;

  /**
   * Update an existing dimension
   *
   * @param sessionid dimName JAXB Dimension object
   */
  void updateDimension(LensSessionHandle sessionid, String dimName, XDimension dimension) throws LensException;

  /**
   * Get all dimension names in the current session
   *
   * @param sessionid
   * @return List of dimension names as List of string objects
   * @throws LensException
   */
  List<String> getAllDimensionNames(LensSessionHandle sessionid)
    throws LensException;

  /**
   * Create dimension table
   *
   * @param sessionid The sessionid
   * @param xDimTable The dim table definition
   * @throws LensException
   */
  void createDimensionTable(LensSessionHandle sessionid, XDimensionTable xDimTable)
    throws LensException;

  /**
   * Drop a dimension table from the cube metastore
   *
   * @param sessionid
   * @param dimTblName
   * @param cascade
   * @throws LensException
   */
  void dropDimensionTable(LensSessionHandle sessionid, String dimTblName, boolean cascade) throws LensException;

  /**
   * Get the dimension table from metastore
   *
   * @param dimTblName
   * @return The {@link XDimensionTable}
   */
  XDimensionTable getDimensionTable(LensSessionHandle sessionid, String dimTblName) throws LensException;

  /**
   * Update/Alter the dimension table
   *
   * @param sessionid      The sessionid
   * @param dimensionTable The new definition of dimension table
   * @throws LensException
   */
  void updateDimensionTable(LensSessionHandle sessionid, XDimensionTable dimensionTable) throws LensException;

  /**
   * Get all storages of dimension table
   *
   * @param sessionid  The sessionid
   * @param dimTblName The dimension table name
   * @return list of storage names
   * @throws LensException
   */
  List<String> getDimTableStorages(LensSessionHandle sessionid, String dimTblName) throws LensException;

  /**
   * Add a storage to dimension table
   *
   * @param sessionid    The sessionid
   * @param dimTblName   The dimension table name
   * @param storageTable XStorageTableElement with storage name, table desc and dump period, if any.
   * @throws LensException
   */
  void addDimTableStorage(LensSessionHandle sessionid, String dimTblName, XStorageTableElement storageTable)
    throws LensException;

  /**
   * Drop all storages of dimension table. Will drop underlying tables as well.
   *
   * @param sessionid  The sessionid
   * @param dimTblName The dimension table name
   * @throws LensException
   */
  void dropAllStoragesOfDimTable(LensSessionHandle sessionid, String dimTblName) throws LensException;

  /**
   * Get storage table element associated with dimension table for storage name specified
   *
   * @param sessionid   The sessionid
   * @param dimTblName  The dimension table name
   * @param storageName The storage name
   * @return {@link XStorageTableElement}
   * @throws LensException
   */
  XStorageTableElement getStorageOfDim(LensSessionHandle sessionid, String dimTblName, String storageName)
    throws LensException;

  /**
   * Drop storage of dimension table specified by name.
   *
   * @param sessionid  The sessionid
   * @param dimTblName The dimension table name
   * @param storage    The storage name
   * @throws LensException
   */
  void dropStorageOfDimTable(LensSessionHandle sessionid, String dimTblName, String storage) throws LensException;

  /**
   * Get all dimension tables. dimensionName is an optional filter of dimension name.
   * If provided, only the dimension tables belonging to given dimension will be returned
   *
   * @param sessionid
   * @param dimensionName dimension name to be filtered with. Optional
   * @return
   * @throws LensException
   */
  List<String> getAllDimTableNames(LensSessionHandle sessionid, String dimensionName) throws LensException;

  /**
   * Get all partitions of a dimension table in a storage
   *
   * @param sessionid  The sessionid
   * @param dimension The dimension table name
   * @param storageName    The storage name
   * @param filter     The filter for the list of partitions
   * @return list of {@link XPartition}
   * @throws LensException
   */
  XPartitionList getAllPartitionsOfDimTableStorage(LensSessionHandle sessionid, String dimension,
    String storageName, String filter) throws LensException;

  /**
   * Add partition to dimension table on a storage.
   *
   * @param sessionid   The sessionid
   * @param dimTblName  The dimension table name
   * @param storageName The storage name
   * @param partition   {@link XPartition}
   * @throws LensException
   * @return number of partitions added. Either 0 or 1
   */
  int addPartitionToDimStorage(LensSessionHandle sessionid, String dimTblName, String storageName,
    XPartition partition) throws LensException;

  /**
   * Add partitions to dimension table on a storage.
   *
   * @param sessionid   The sessionid
   * @param dimTblName  The dimension table name
   * @param storageName The storage name
   * @param partitions  {@link XPartitionList}
   * @throws LensException
   * @return Number of partitions added
   */
  int addPartitionsToDimStorage(LensSessionHandle sessionid, String dimTblName, String storageName,
    XPartitionList partitions) throws LensException;

  /**
   * Get fact table given by name
   *
   * @param sessionid The sessionid
   * @param fact      The fact table name
   * @return {@link XFact}
   * @throws LensException
   */
  XFact getFactTable(LensSessionHandle sessionid, String fact) throws LensException;

  /**
   * Create fact table
   *
   * @param sessionid The sessionid
   * @param fact      The fact table definition
   * @throws LensException
   */
  void createFactTable(LensSessionHandle sessionid, XFact fact) throws LensException;

  /**
   * Update/Alter fact table
   *
   * @param sessionid The sessionid
   * @param fact      The fact table's new definition
   * @throws LensException
   */
  void updateFactTable(LensSessionHandle sessionid, XFact fact) throws LensException;

  /**
   * Drop fact table.
   *
   * @param sessionid The sessionid
   * @param fact      The fact table name
   * @param cascade   If true, underlying storage tables will also be dropped.
   * @throws LensException
   */
  void dropFactTable(LensSessionHandle sessionid, String fact, boolean cascade) throws LensException;

  /**
   * Get all fact names
   *
   * @param sessionid The sessionid
   * @param cubeName optional filter filter facts by cube name.
   * @return List of fact table names
   * @throws LensException
   */
  List<String> getAllFactNames(LensSessionHandle sessionid, String cubeName) throws LensException;

  /**
   * Get all storages of fact
   *
   * @param sessionid The sessionid
   * @param fact      The fact table name
   * @return List of all storage names on which fact is present
   * @throws LensException
   */
  List<String> getStoragesOfFact(LensSessionHandle sessionid, String fact) throws LensException;

  /**
   * Drop all storages of fact
   *
   * @param sessionid The sessionid
   * @param factName  The fact table name
   * @throws LensException
   */
  void dropAllStoragesOfFact(LensSessionHandle sessionid, String factName) throws LensException;

  /**
   * Get storage table of fact specifed by fact name, storage name
   *
   * @param sessionid   The sessionid
   * @param fact        The fact table name
   * @param storageName The storage name
   * @return
   * @throws LensException
   */
  XStorageTableElement getStorageOfFact(LensSessionHandle sessionid, String fact, String storageName)
    throws LensException;

  /**
   * Add storage to fact table
   *
   * @param sessionid    The sessionid
   * @param fact         The fact table name
   * @param storageTable XStorageTableElement containing storage name, update periods and table description
   * @throws LensException
   */
  void addStorageToFact(LensSessionHandle sessionid, String fact, XStorageTableElement storageTable)
    throws LensException;

  /**
   * Drop storage of fact specified by fact name, storage name
   *
   * @param sessionid The sessionid
   * @param fact      The fact table name
   * @param storage   The storage name
   * @throws LensException
   */
  void dropStorageOfFact(LensSessionHandle sessionid, String fact, String storage) throws LensException;

  /**
   * Get all partitions of fact on a storage
   *
   * @param sessionid The sessionid
   * @param fact      The fact table name
   * @param storageName   The storage name
   * @param filter    The filter for partition listing
   * @return List of {@link XPartition}
   * @throws LensException
   */
  XPartitionList getAllPartitionsOfFactStorage(LensSessionHandle sessionid, String fact,
    String storageName, String filter) throws LensException;

  /**
   * Add partition to fact on a storage
   *
   * @param sessionid   The sessionid
   * @param fact        The fact table name
   * @param storageName The storage name
   * @param partition   {@link XPartition}
   * @throws LensException
   * @return number of partitions added. Either 0 or 1
   */
  int addPartitionToFactStorage(LensSessionHandle sessionid, String fact, String storageName,
    XPartition partition) throws LensException;

  /**
   * Add partitions to fact on a storage
   *
   * @param sessionid   The sessionid
   * @param fact        The fact table name
   * @param storageName The storage name
   * @param partitions  {@link XPartitionList}
   * @throws LensException
   * @return Number of partitions added
   */
  int addPartitionsToFactStorage(LensSessionHandle sessionid, String fact, String storageName,
    XPartitionList partitions) throws LensException;

  /**
   * Drop partition from storage with spec specified as comma separated string
   *
   * @param sessionid     The sessionid
   * @param cubeTableName The cube table name - fact/dimension table name
   * @param storageName   The storage name
   * @param values        The comma separated values for all partition columns
   * @throws LensException
   */
  void dropPartitionFromStorageByValues(LensSessionHandle sessionid,
    String cubeTableName, String storageName, String values) throws LensException;

  /**
   * Drop partition from storage with spec specified by filter
   *
   * @param sessionid     The sessionid
   * @param cubeTableName The cube table name - fact/dimension table name
   * @param storageName   The storage name
   * @param filter        The partition filter - all partitions which obey the filter will be dropped
   * @throws LensException
   */
  void dropPartitionFromStorageByFilter(LensSessionHandle sessionid,
    String cubeTableName, String storageName, String filter) throws LensException;

  void dropPartitionFromStorageByFilter(LensSessionHandle sessionid,
    String cubeTableName, String storageName, String filter, String updatePeriod) throws LensException;

  /**
   * Get flattened columns - all columns of table + all reachable columns
   *
   * @param sessionHandle The session handle
   * @param tableName     The table name - cube name or dimension name
   * @param addChains
   * @return {@link XFlattenedColumns}
   * @throws LensException
   */
  XFlattenedColumns getFlattenedColumns(LensSessionHandle sessionHandle, String tableName, boolean addChains)
    throws LensException;

  /**
   * Get the latest available date upto which data is available for the base cubes, for the time dimension
   *
   * @param sessionid     The session id
   * @param timeDimension time dimension name
   * @param cubeName      The base cube name
   * @return Date
   * @throws LensException
   * @throws HiveException
   */
  Date getLatestDateOfCube(LensSessionHandle sessionid, String cubeName, String timeDimension)
    throws LensException, HiveException;

  List<String> getPartitionTimelines(LensSessionHandle sessionid, String factName, String storage,
    String updatePeriod, String timeDimension) throws LensException, HiveException;

  XJoinChains getAllJoinChains(LensSessionHandle sessionid, String table) throws LensException;

  void updatePartition(LensSessionHandle sessionid, String tblName, String storageName,
    XPartition partition) throws LensException;

  void updatePartitions(LensSessionHandle sessionid, String tblName, String storageName,
    XPartitionList partitions) throws LensException;

  /**
   *
   * @param sessionid         The session id
   * @param cubeSeg           The  segmentation
   * @throws LensException
   */
  void createSegmentation(LensSessionHandle sessionid, XSegmentation cubeSeg) throws LensException;

  /**
   * Create segmentation
   *
   * @param sessionid                    The session id
   * @param segName                      Ssegmentation name
   * @return {@link XSegmentation}
   * @throws LensException
   */
  XSegmentation getSegmentation(LensSessionHandle sessionid, String segName) throws LensException;

  /**
   * Get segmentation given by name
   *
   * @param sessionid        The session id
   * @param cubeSeg          The segmentation
   * @throws LensException
   */

  void updateSegmentation(LensSessionHandle sessionid, XSegmentation cubeSeg) throws LensException;

  /**
   * Update segmentation
   *
   * @param sessionid      The session id
   * @param cubeSegName    Segmentation name
   * @throws LensException
   */
  void dropSegmentation(LensSessionHandle sessionid, String cubeSegName) throws LensException;

  /**
   * Get all segmentations belong to Cube
   *
   * @param sessionid    The session id
   * @param cubeName     The cube Name
   * @return
   * @throws LensException
   */
  List<String> getAllSegmentations(LensSessionHandle sessionid, String cubeName) throws LensException;


  String getSessionUserName(LensSessionHandle sessionid);
}

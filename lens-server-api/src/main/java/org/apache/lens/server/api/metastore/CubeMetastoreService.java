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

import org.apache.lens.api.metastore.*;

import java.util.List;

import org.apache.lens.api.LensException;
import org.apache.lens.api.LensSessionHandle;


public interface CubeMetastoreService {
  /**
   * Get current database used by the CubeMetastoreClient
   *
   * @return the current database name
   */
  public String getCurrentDatabase(LensSessionHandle sessionid) throws LensException;

  /**
   * Change the current database used by the CubeMetastoreClient
   *
   * @param database
   */
  public void setCurrentDatabase(LensSessionHandle sessionid, String database) throws LensException;

  /**
   * Drop a database from cube metastore
   *
   * @param database database name
   * @param cascade flag indicating if the tables in the database should be dropped as well
   */
  public void dropDatabase(LensSessionHandle sessionid, String database, boolean cascade) throws LensException;

  /**
   * Create a database in the metastore
   *
   * @param database database name
   * @param ignore ignore if database already exists
   */
  public void createDatabase(LensSessionHandle sessionid, String database, boolean ignore) throws LensException;

  /**
   * Get names of all databases in this metastore
   * @return list of database names
   */
  public List<String> getAllDatabases(LensSessionHandle sessionid) throws LensException;

  /**
   * Create a storage
   *
   * @param sessionid
   * @param storage
   * @throws LensException
   */
  public void createStorage(LensSessionHandle sessionid, XStorage storage) throws LensException;

  /**
   * Drop a storage specified by name
   *
   * @param sessionid
   * @param storageName
   * @throws LensException
   */
  public void dropStorage(LensSessionHandle sessionid, String storageName) throws LensException;

  /**
   * Alter storage specified by name, with new definition
   *
   * @param sessionid
   * @param storageName
   * @param storage
   * @throws LensException
   */
  public void alterStorage(LensSessionHandle sessionid, String storageName, XStorage storage) throws LensException;

  /**
   * Get Storage specified by name
   *
   * @param sessionid
   * @param storageName
   * @throws LensException
   */
  public XStorage getStorage(LensSessionHandle sessionid, String storageName) throws LensException;

  /**
   * Get all storage names in current database
   *
   * @param sessionid
   *
   * @return returns list of the storage names
   * @throws LensException
   */
  public List<String> getAllStorageNames(LensSessionHandle sessionid) throws LensException;

  /**
   * Get names of all cubes in the current database, includes both base cubes and derived cubes
   *
   * @return list of cube names
   */
  public List<String> getAllCubeNames(LensSessionHandle sessionid) throws LensException;

  /**
   * Get names of all base cube names in the current database
   *
   * @return list of cube names
   */
  public List<String> getAllBaseCubeNames(LensSessionHandle sessionid) throws LensException;

  /**
   * Get names of all derived cubes in the current database
   *
   * @return list of cube names
   */
  public List<String> getAllDerivedCubeNames(LensSessionHandle sessionid) throws LensException;

  /**
   * Get names of all cubes, which can be queried in the current database
   *
   * @param sessionid  session id
   *
   * @return list of cube names
   */
  public List<String> getAllQueryableCubeNames(LensSessionHandle sessionid) throws LensException;

  /**
   * Get native table for the given name
   *
   * @param sessionid  session id
   * @param name The table name
   *
   * @return {@link NativeTable} object
   *
   * @throws LensException
   */
  public NativeTable getNativeTable(LensSessionHandle sessionid, String name) throws LensException;

  /**
   * Get names of all native tables
   *
   * @param sessionid  session id
   * @param dboption To get from current or all, the option is ignored if dbname is passed
   * @param dbName The db name
   *
   * @return list of table names
   */
  public List<String> getAllNativeTableNames(LensSessionHandle sessionid,
      String dboption, String dbName) throws LensException;

  /**
   * Create a cube based on JAXB Cube object
   */
  public void createCube(LensSessionHandle sessionid, XCube cube) throws LensException;

  /**
   * Get a cube from the metastore
   *
   * @param cubeName
   *
   * @return JAXB Cube object
   */
  public XCube getCube(LensSessionHandle sessionid, String cubeName) throws LensException;

  /**
   * Drop a cube from the metastore in the currently deleted database.
   *
   * @param cubeName
   */
  public void dropCube(LensSessionHandle sessionid, String cubeName) throws LensException;

  /**
   * Update an existing cube
   *
   * @param cube JAXB Cube object
   */
  public void updateCube(LensSessionHandle sessionid, XCube cube) throws LensException;

  /**
   * Create a dimension based on JAXB Dimension object
   */
  public void createDimension(LensSessionHandle sessionid, XDimension dimension) throws LensException;

  /**
   * Get a dimension from the metastore
   *
   * @param dimName
   *
   * @return JAXB Dimension object
   */
  public XDimension getDimension(LensSessionHandle sessionid, String dimName) throws LensException;

  /**
   * Drop a dimension from the metastore in the currently deleted database.
   *
   * @param cubeName
   */
  public void dropDimension(LensSessionHandle sessionid, String dimName) throws LensException;

  /**
   * Update an existing dimension
   *
   * @param dim JAXB Dimension object
   */
  public void updateDimension(LensSessionHandle sessionid, String dimName, XDimension dimension) throws LensException;

  /**
   * Get all dimension names in the current session
   *
   * @param sessionid
   *
   * @return List of dimension names as List of string objects
   *
   * @throws LensException
   */
  public List<String> getAllDimensionNames(LensSessionHandle sessionid)
      throws LensException;

  /**
   * Create a cube dimension table
   */
  public void createCubeDimensionTable(LensSessionHandle sessionid, DimensionTable xDimTable, XStorageTables storageTables) throws LensException;

  /**
   * Drop a dimension table from the cube metastore
   *
   * @param sessionid
   * @param dimTblName
   * @param cascade
   *
   * @throws LensException
   */
  public void dropDimensionTable(LensSessionHandle sessionid, String dimTblName, boolean cascade) throws LensException;

  /**
   * Get the dimension table from metastore
   *
   * @param dimTblName
   * @return The {@link DimensionTable}
   */
  public DimensionTable getDimensionTable(LensSessionHandle sessionid, String dimTblName) throws LensException;
  public void updateDimensionTable(LensSessionHandle sessionid, DimensionTable dimensionTable) throws LensException;

  public List<String> getDimTableStorages(LensSessionHandle sessionid, String dimTblName) throws LensException;
  public void createDimTableStorage(LensSessionHandle sessionid, String dimTblName, XStorageTableElement storageTable)
      throws LensException;
  public void dropAllStoragesOfDimTable(LensSessionHandle sessionid, String dimTblName) throws LensException;
  public XStorageTableElement getStorageOfDim(LensSessionHandle sessionid, String dimTblName, String storageName) throws LensException;
  public void dropStorageOfDimTable(LensSessionHandle sessionid, String dimTblName, String storage) throws LensException;
  public List<String> getAllDimTableNames(LensSessionHandle sessionid) throws LensException;

  public List<XPartition> getAllPartitionsOfDimTableStorage(LensSessionHandle sessionid, String dimTblName, String storage, String filter) throws LensException;
  public void addPartitionToDimStorage(LensSessionHandle sessionid, String dimTblName, String storageName, XPartition partition) throws LensException;

  /**
   * Get all facts of cube. Cube can also be a derived cube
   *
   * @param sessionid The session id
   * @param cubeName The cube name
   *
   * @return List of FactTable objects
   *
   * @throws LensException
   */
  public List<FactTable> getAllFactsOfCube(LensSessionHandle sessionid, String cubeName) throws LensException;
  public FactTable getFactTable(LensSessionHandle sessionid, String fact) throws LensException;
  public void createFactTable(LensSessionHandle sessionid, FactTable fact, XStorageTables storageTables) throws LensException;
  public void updateFactTable(LensSessionHandle sessionid, FactTable fact) throws LensException;
  public void dropFactTable(LensSessionHandle sessionid, String fact, boolean cascade) throws LensException;
  public List<String> getAllFactNames(LensSessionHandle sessionid) throws LensException;

  public List<String> getStoragesOfFact(LensSessionHandle sessionid, String fact) throws LensException;
  public void dropAllStoragesOfFact(LensSessionHandle sessionid, String factName) throws LensException;
  public XStorageTableElement getStorageOfFact(LensSessionHandle sessionid, String fact, String storageName) throws LensException;
  public void addStorageToFact(LensSessionHandle sessionid, String fact, XStorageTableElement storageTable) throws LensException;
  public void dropStorageOfFact(LensSessionHandle sessionid, String fact, String storage) throws LensException;

  public List<XPartition> getAllPartitionsOfFactStorage(LensSessionHandle sessionid, String fact, String storage, String filter) throws LensException;
  public void addPartitionToFactStorage(LensSessionHandle sessionid, String fact, String storageName, XPartition partition) throws LensException;

  public void dropPartitionFromStorage(LensSessionHandle sessionid,
      String cubeTableName, String storageName, XTimePartSpec timePartSpec,
      XPartSpec nonTimePartSpec, String updatePeriod) throws LensException;
  public void dropPartitionFromStorageByValues(LensSessionHandle sessionid,
      String cubeTableName, String storageName, String values) throws LensException;
  public void dropPartitionFromStorageByFilter(LensSessionHandle sessionid,
      String cubeTableName, String storageName, String filter) throws LensException;

  public FlattenedColumns getFlattenedColumns(LensSessionHandle sessionHandle, String tableName) throws LensException;

}

package com.inmobi.grill.server.api.metastore;

/*
 * #%L
 * Grill API for server and extensions
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.metastore.*;

import java.util.List;


public interface CubeMetastoreService {
  /**
   * Get current database used by the CubeMetastoreClient
   * 
   * @return the current database name
   */
  public String getCurrentDatabase(GrillSessionHandle sessionid) throws GrillException;

  /**
   * Change the current database used by the CubeMetastoreClient
   * 
   * @param database
   */
  public void setCurrentDatabase(GrillSessionHandle sessionid, String database) throws GrillException;

  /**
   * Drop a database from cube metastore
   * 
   * @param database database name
   * @param cascade flag indicating if the tables in the database should be dropped as well
   */
  public void dropDatabase(GrillSessionHandle sessionid, String database, boolean cascade) throws GrillException;

  /**
   * Create a database in the metastore
   * 
   * @param database database name
   * @param ignore ignore if database already exists
   */
  public void createDatabase(GrillSessionHandle sessionid, String database, boolean ignore) throws GrillException;

  /**
   * Get names of all databases in this metastore
   * @return list of database names
   */
  public List<String> getAllDatabases(GrillSessionHandle sessionid) throws GrillException;

  /**
   * Create a storage
   * 
   * @param sessionid
   * @param storage
   * @throws GrillException
   */
  public void createStorage(GrillSessionHandle sessionid, XStorage storage) throws GrillException;

  /**
   * Drop a storage specified by name
   * 
   * @param sessionid
   * @param storageName
   * @throws GrillException
   */
  public void dropStorage(GrillSessionHandle sessionid, String storageName) throws GrillException;

  /**
   * Alter storage specified by name, with new definition
   * 
   * @param sessionid
   * @param storageName
   * @param storage
   * @throws GrillException
   */
  public void alterStorage(GrillSessionHandle sessionid, String storageName, XStorage storage) throws GrillException;

  /**
   * Get Storage specified by name
   * 
   * @param sessionid
   * @param storageName
   * @throws GrillException
   */
  public XStorage getStorage(GrillSessionHandle sessionid, String storageName) throws GrillException;

  /**
   * Get all storage names in current database
   * 
   * @param sessionid
   * 
   * @return returns list of the storage names
   * @throws GrillException
   */
  public List<String> getAllStorageNames(GrillSessionHandle sessionid) throws GrillException;

  /**
   * Get names of all cubes in the current database, includes both base cubes and derived cubes
   * 
   * @return list of cube names
   */
  public List<String> getAllCubeNames(GrillSessionHandle sessionid) throws GrillException;

  /**
   * Get names of all base cube names in the current database
   * 
   * @return list of cube names
   */
  public List<String> getAllBaseCubeNames(GrillSessionHandle sessionid) throws GrillException;

  /**
   * Get names of all derived cubes in the current database
   * 
   * @return list of cube names
   */
  public List<String> getAllDerivedCubeNames(GrillSessionHandle sessionid) throws GrillException;

  /**
   * Get names of all cubes, which can be queried in the current database
   *
   * @param sessionid Grill session id
   *
   * @return list of cube names
   */
  public List<String> getAllQueryableCubeNames(GrillSessionHandle sessionid) throws GrillException;

  /**
   * Get native table for the given name
   *
   * @param sessionid Grill session id
   * @param name The table name
   *
   * @return {@link NativeTable} object
   *
   * @throws GrillException
   */
  public NativeTable getNativeTable(GrillSessionHandle sessionid, String name) throws GrillException;

  /**
   * Get names of all native tables
   *
   * @param sessionid Grill session id
   * @param dboption To get from current or all, the option is ignored if dbname is passed
   * @param dbName The db name
   *
   * @return list of table names
   */
  public List<String> getAllNativeTableNames(GrillSessionHandle sessionid,
      String dboption, String dbName) throws GrillException;

  /**
   * Create a cube based on JAXB Cube object
   */
  public void createCube(GrillSessionHandle sessionid, XCube cube) throws GrillException;

  /**
   * Get a cube from the metastore
   * 
   * @param cubeName
   * 
   * @return JAXB Cube object
   */
  public XCube getCube(GrillSessionHandle sessionid, String cubeName) throws GrillException;

  /**
   * Drop a cube from the metastore in the currently deleted database.
   * 
   * @param cubeName
   */
  public void dropCube(GrillSessionHandle sessionid, String cubeName) throws GrillException;

  /**
   * Update an existing cube
   * 
   * @param cube JAXB Cube object
   */
  public void updateCube(GrillSessionHandle sessionid, XCube cube) throws GrillException;

  /**
   * Create a dimension based on JAXB Dimension object
   */
  public void createDimension(GrillSessionHandle sessionid, XDimension dimension) throws GrillException;

  /**
   * Get a dimension from the metastore
   *
   * @param dimName
   *
   * @return JAXB Dimension object
   */
  public XDimension getDimension(GrillSessionHandle sessionid, String dimName) throws GrillException;

  /**
   * Drop a dimension from the metastore in the currently deleted database.
   *
   * @param cubeName
   */
  public void dropDimension(GrillSessionHandle sessionid, String dimName) throws GrillException;

  /**
   * Update an existing dimension
   *
   * @param dim JAXB Dimension object
   */
  public void updateDimension(GrillSessionHandle sessionid, String dimName, XDimension dimension) throws GrillException;

  /**
   * Get all dimension names in the current session
   *
   * @param sessionid
   *
   * @return List of dimension names as List of string objects
   *
   * @throws GrillException
   */
  public List<String> getAllDimensionNames(GrillSessionHandle sessionid)
      throws GrillException;

  /**
   * Create a cube dimension table
   */
  public void createCubeDimensionTable(GrillSessionHandle sessionid, DimensionTable xDimTable, XStorageTables storageTables) throws GrillException;

  /**
   * Drop a dimension table from the cube metastore
   *
   * @param sessionid
   * @param dimTblName
   * @param cascade
   *
   * @throws GrillException
   */
  public void dropDimensionTable(GrillSessionHandle sessionid, String dimTblName, boolean cascade) throws GrillException;

  /**
   * Get the dimension table from metastore
   *
   * @param dimTblName
   * @return The {@link DimensionTable}
   */
  public DimensionTable getDimensionTable(GrillSessionHandle sessionid, String dimTblName) throws GrillException;
  public void updateDimensionTable(GrillSessionHandle sessionid, DimensionTable dimensionTable) throws GrillException;

  public List<String> getDimTableStorages(GrillSessionHandle sessionid, String dimTblName) throws GrillException;
  public void createDimTableStorage(GrillSessionHandle sessionid, String dimTblName, XStorageTableElement storageTable)
      throws GrillException;
  public void dropAllStoragesOfDimTable(GrillSessionHandle sessionid, String dimTblName) throws GrillException;
  public XStorageTableElement getStorageOfDim(GrillSessionHandle sessionid, String dimTblName, String storageName) throws GrillException;
  public void dropStorageOfDimTable(GrillSessionHandle sessionid, String dimTblName, String storage) throws GrillException;
  public List<String> getAllDimTableNames(GrillSessionHandle sessionid) throws GrillException;

  public List<XPartition> getAllPartitionsOfDimTableStorage(GrillSessionHandle sessionid, String dimTblName, String storage, String filter) throws GrillException;
  public void addPartitionToDimStorage(GrillSessionHandle sessionid, String dimTblName, String storageName, XPartition partition) throws GrillException;

  /**
   * Get all facts of cube. Cube can also be a derived cube
   *
   * @param sessionid The session id
   * @param cubeName The cube name
   *
   * @return List of FactTable objects
   *
   * @throws GrillException
   */
  public List<FactTable> getAllFactsOfCube(GrillSessionHandle sessionid, String cubeName) throws GrillException;
  public FactTable getFactTable(GrillSessionHandle sessionid, String fact) throws GrillException;
  public void createFactTable(GrillSessionHandle sessionid, FactTable fact, XStorageTables storageTables) throws GrillException;
  public void updateFactTable(GrillSessionHandle sessionid, FactTable fact) throws GrillException;
  public void dropFactTable(GrillSessionHandle sessionid, String fact, boolean cascade) throws GrillException;
  public List<String> getAllFactNames(GrillSessionHandle sessionid) throws GrillException;

  public List<String> getStoragesOfFact(GrillSessionHandle sessionid, String fact) throws GrillException;
  public void dropAllStoragesOfFact(GrillSessionHandle sessionid, String factName) throws GrillException;
  public XStorageTableElement getStorageOfFact(GrillSessionHandle sessionid, String fact, String storageName) throws GrillException;
  public void addStorageToFact(GrillSessionHandle sessionid, String fact, XStorageTableElement storageTable) throws GrillException;
  public void dropStorageOfFact(GrillSessionHandle sessionid, String fact, String storage) throws GrillException;

  public List<XPartition> getAllPartitionsOfFactStorage(GrillSessionHandle sessionid, String fact, String storage, String filter) throws GrillException;
  public void addPartitionToFactStorage(GrillSessionHandle sessionid, String fact, String storageName, XPartition partition) throws GrillException;

  public void dropPartitionFromStorage(GrillSessionHandle sessionid,
      String cubeTableName, String storageName, XTimePartSpec timePartSpec,
      XPartSpec nonTimePartSpec, String updatePeriod) throws GrillException;
  public void dropPartitionFromStorageByValues(GrillSessionHandle sessionid,
      String cubeTableName, String storageName, String values) throws GrillException;
  public void dropPartitionFromStorageByFilter(GrillSessionHandle sessionid,
      String cubeTableName, String storageName, String filter) throws GrillException;
}

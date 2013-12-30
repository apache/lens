package com.inmobi.grill.server.api;

import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.client.api.APIResult;
import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.metastore.model.*;

import java.util.Collection;
import java.util.List;


public interface CubeMetastoreService {
  /**
   * Get current database used by the CubeMetastoreClient
   * @return
   */
  public String getCurrentDatabase(GrillSessionHandle sessionid) throws GrillException;

  /**
   * Change the current database used by the CubeMetastoreClient
   * @param database
   */
  public void setCurrentDatabase(GrillSessionHandle sessionid, String database) throws GrillException;

  /**
   * Drop a database from cube metastore
   * @param database database name
   * @param cascade flag indicating if the tables in the database should be dropped as well
   */
  public void dropDatabase(GrillSessionHandle sessionid, String database, boolean cascade) throws GrillException;

  /**
   * Create a database in the metastore
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
   * Get names of all cubes in the current database
   * @return list of cube names
   */
  public List<String> getAllCubeNames(GrillSessionHandle sessionid) throws GrillException;


  /**
   * Create a cube based on JAXB Cube object
   */
  public void createCube(GrillSessionHandle sessionid, XCube cube) throws GrillException;

  /**
   * Get a cube from the metastore
   * @param cubeName
   * @return JAXB Cube object
   */
  public XCube getCube(GrillSessionHandle sessionid, String cubeName) throws GrillException;

  /**
   * Drop a cube from the metastore in the currently deleted database
   * @param cubeName
   * @param cascade
   */
  public void dropCube(GrillSessionHandle sessionid, String cubeName, boolean cascade) throws GrillException;

  /**
   * Update an existing cube
   * @param cube JAXB Cube object
   */
  public void updateCube(GrillSessionHandle sessionid, XCube cube) throws GrillException;

  /**
   * Create a cube dimension table
   */
  public void createCubeDimensionTable(GrillSessionHandle sessionid, DimensionTable xDimTable) throws GrillException;

  /**
   * Drop a dimension table from the cube metastore
   * @param dimension
   * @param cascade
   * @throws GrillException
   */
  public void dropDimensionTable(GrillSessionHandle sessionid, String dimension, boolean cascade) throws GrillException;

  /**
   * Get the dimension table from metastore
   * @param dimName
   * @return
   */
	public DimensionTable getDimensionTable(GrillSessionHandle sessionid, String dimName) throws GrillException;
	public void updateDimensionTable(GrillSessionHandle sessionid, DimensionTable dimensionTable) throws GrillException;

	public Collection<String> getDimensionStorages(GrillSessionHandle sessionid, String dimension) throws GrillException;
	public void createDimensionStorage(GrillSessionHandle sessionid, String dimName, String updatePeriod, XStorage storageAttr)
	throws GrillException;
	public void dropAllStoragesOfDim(GrillSessionHandle sessionid, String dimName) throws GrillException;
	public XStorage getStorageOfDimension(GrillSessionHandle sessionid, String dimname, String storage) throws GrillException;
	public void dropStorageOfDim(GrillSessionHandle sessionid, String dimName, String storage) throws GrillException;


	public List<FactTable> getAllFactsOfCube(GrillSessionHandle sessionid, String cubeName) throws GrillException;
	public FactTable getFactTable(GrillSessionHandle sessionid, String fact) throws GrillException;
	public void createFactTable(GrillSessionHandle sessionid, FactTable fact) throws GrillException;
	public void updateFactTable(GrillSessionHandle sessionid, FactTable fact) throws GrillException;
	public void dropFactTable(GrillSessionHandle sessionid, String fact, boolean cascade) throws GrillException;
  public List<String> getAllFactNames(GrillSessionHandle sessionid) throws GrillException;

  public List<String> getStoragesOfFact(GrillSessionHandle sessionid, String fact) throws GrillException;
  public void addStorageToFact(GrillSessionHandle sessionid, String fact, FactStorage storage) throws GrillException;
  public void dropStorageOfFact(GrillSessionHandle sessionid, String fact, String storage) throws GrillException;
  public FactStorage getStorageOfFact(GrillSessionHandle sessionid, String fact, String storage) throws GrillException;
  public void alterFactStorageUpdatePeriod(GrillSessionHandle sessionid, String fact, String storage, StorageUpdatePeriodList periods) throws GrillException;

  public List<XPartition> getAllPartitionsOfFactStorage(GrillSessionHandle sessionid, String fact, String storage, String filter) throws GrillException;
  public void addPartitionToFactStorage(GrillSessionHandle sessionid, String fact, String storage, XPartition partition) throws GrillException;
  public void dropPartitionsOfFactStorageByFilter(GrillSessionHandle sessionid, String fact, String storage, String filter) throws GrillException;
  public void dropPartitionOfFactStorageByValue(GrillSessionHandle sessionid, String fact, String storage, String values) throws GrillException;

  public List<XPartition> getAllPartitionsOfDimStorage(GrillSessionHandle sessionid, String dimension, String storage, String filter) throws GrillException;
  public void addPartitionToDimStorage(GrillSessionHandle sessionid, String dimension, String storage, XPartition partition) throws GrillException;
  public void dropPartitionOfDimStorageByFilter(GrillSessionHandle sessionid, String dimension, String storage, String filter) throws GrillException;
  public void dropPartitionOfDimStorageByValue(GrillSessionHandle sessionid, String dimension, String storage, String values) throws GrillException;
}

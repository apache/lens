package com.inmobi.grill.server.api;

import com.inmobi.grill.client.api.APIResult;
import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.metastore.model.*;

import java.util.Collection;
import java.util.List;

public interface CubeMetastoreService extends GrillService {
  /**
   * Get current database used by the CubeMetastoreClient
   * @return
   */
  public String getCurrentDatabase() throws GrillException;

  /**
   * Change the current database used by the CubeMetastoreClient
   * @param database
   */
  public void setCurrentDatabase(String database) throws GrillException;

  /**
   * Drop a database from cube metastore
   * @param database database name
   * @param cascade flag indicating if the tables in the database should be dropped as well
   */
  public void dropDatabase(String database, boolean cascade) throws GrillException;

  /**
   * Create a database in the metastore
   * @param database database name
   * @param ignore ignore if database already exists
   */
  public void createDatabase(String database, boolean ignore) throws GrillException;

  /**
   * Get names of all databases in this metastore
   * @return list of database names
   */
  public List<String> getAllDatabases() throws GrillException;

  /**
   * Get names of all cubes in the current database
   * @return list of cube names
   */
  public List<String> getAllCubeNames() throws GrillException;


  /**
   * Create a cube based on JAXB Cube object
   */
  public void createCube(XCube cube) throws GrillException;

  /**
   * Get a cube from the metastore
   * @param cubeName
   * @return JAXB Cube object
   */
  public XCube getCube(String cubeName) throws GrillException;

  /**
   * Drop a cube from the metastore in the currently deleted database
   * @param cubeName
   * @param cascade
   */
  public void dropCube(String cubeName, boolean cascade) throws GrillException;

  /**
   * Update an existing cube
   * @param cube JAXB Cube object
   */
  public void updateCube(XCube cube) throws GrillException;

  /**
   * Create a cube dimension table
   */
  public void createCubeDimensionTable(DimensionTable xDimTable) throws GrillException;

  /**
   * Drop a dimension table from the cube metastore
   * @param dimension
   * @param cascade
   * @throws GrillException
   */
  public void dropDimensionTable(String dimension, boolean cascade) throws GrillException;

  /**
   * Get the dimension table from metastore
   * @param dimName
   * @return
   */
	public DimensionTable getDimensionTable(String dimName) throws GrillException;
	public void updateDimensionTable(DimensionTable dimensionTable) throws GrillException;

	public Collection<String> getDimensionStorages(String dimension) throws GrillException;
	public void createDimensionStorage(String dimName, String updatePeriod, XStorage storageAttr)
	throws GrillException;
	public void dropAllStoragesOfDim(String dimName) throws GrillException;
	public XStorage getStorageOfDimension(String dimname, String storage) throws GrillException;
	public void dropStorageOfDim(String dimName, String storage) throws GrillException;


	public List<FactTable> getAllFactsOfCube(String cubeName) throws GrillException;
	public FactTable getFactTable(String fact) throws GrillException;
	public void createFactTable(FactTable fact) throws GrillException;
	public void updateFactTable(FactTable fact) throws GrillException;
	public void dropFactTable(String fact, boolean cascade) throws GrillException;
  public List<String> getAllFactNames() throws GrillException;

  public List<String> getStoragesOfFact(String fact) throws GrillException;
  public void addStorageToFact(String fact, FactStorage storage) throws GrillException;
  public void dropStorageOfFact(String fact, String storage) throws GrillException;
  public FactStorage getStorageOfFact(String fact, String storage) throws GrillException;
  public void alterFactStorageUpdatePeriod(String fact, String storage, StorageUpdatePeriodList periods) throws GrillException;

  public List<XPartition> getAllPartitionsOfFactStorage(String fact, String storage, String filter) throws GrillException;
  public void addPartitionToFactStorage(String fact, String storage, XPartition partition) throws GrillException;
  public void dropPartitionsOfFactStorageByFilter(String fact, String storage, String filter) throws GrillException;
  public void dropPartitionOfFactStorageByValue(String fact, String storage, String values) throws GrillException;
}

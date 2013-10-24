package com.inmobi.grill.server.api;

import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.metastore.model.XCube;

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
}

package com.inmobi.grill.server.api;

import com.inmobi.grill.exception.GrillException;

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
}

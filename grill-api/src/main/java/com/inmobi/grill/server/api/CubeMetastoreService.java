package com.inmobi.grill.server.api;

public interface CubeMetastoreService extends GrillService {
  public String getCurrentDatabase();
  public void setCurrentDatabase(String database);
}

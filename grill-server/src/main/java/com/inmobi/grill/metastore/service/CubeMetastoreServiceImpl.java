package com.inmobi.grill.metastore.service;

import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.server.api.CubeMetastoreService;

public class CubeMetastoreServiceImpl implements CubeMetastoreService {

  @Override
  public String getName() {
    return null;
  }

  @Override
  public void start() throws GrillException {
  }

  @Override
  public void stop() throws GrillException {

  }

  public static CubeMetastoreService getInstance() {
    return new CubeMetastoreServiceImpl();
  }

  public  CubeMetastoreServiceImpl() {

  }

  @Override
  public String getCurrentDatabase() {
    // TODO get current database from CubeMetastoreClient
    return "test";
  }

  @Override
  public void setCurrentDatabase(String database) {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}

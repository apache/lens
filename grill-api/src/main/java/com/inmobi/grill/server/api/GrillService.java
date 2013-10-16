package com.inmobi.grill.server.api;

import com.inmobi.grill.exception.GrillException;

public interface GrillService {

  public String getName();

  public void start() throws GrillException;

  public void stop() throws GrillException;
}

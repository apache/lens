package com.inmobi.grill.api;

import com.inmobi.grill.exception.GrillException;

public interface PersistentResultSet extends GrillResultSet {
  public String getOutputPath() throws GrillException;
}

package com.inmobi.grill.server.api;

import com.inmobi.grill.api.GrillResultSet;
import com.inmobi.grill.api.QueryHandle;

public interface QueryHandleWithResultSet {
  public QueryHandle getQueryHandle();
  public GrillResultSet getResultSet();
}

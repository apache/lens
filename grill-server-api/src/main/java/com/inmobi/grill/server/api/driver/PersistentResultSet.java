package com.inmobi.grill.server.api.driver;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.PersistentQueryResult;
import com.inmobi.grill.api.query.QueryResult;

public abstract class PersistentResultSet extends GrillResultSet {
  public abstract String getOutputPath() throws GrillException;
  
  public QueryResult toQueryResult() throws GrillException {
    return new PersistentQueryResult(getOutputPath());
  }
}

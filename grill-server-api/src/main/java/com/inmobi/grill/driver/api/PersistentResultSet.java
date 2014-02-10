package com.inmobi.grill.driver.api;

import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.query.PersistentQueryResult;
import com.inmobi.grill.query.QueryResult;

public abstract class PersistentResultSet extends GrillResultSet {
  public abstract String getOutputPath() throws GrillException;
  
  public QueryResult toQueryResult() throws GrillException {
    return new PersistentQueryResult(getOutputPath());
  }
}

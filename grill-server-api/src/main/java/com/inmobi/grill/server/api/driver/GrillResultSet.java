package com.inmobi.grill.server.api.driver;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.QueryResult;

public abstract class GrillResultSet {
  /**
   * Get the size of the result set
   * 
   * @return The size if available, -1 if not available.
   */
  public abstract int size() throws GrillException;

  /**
   * Get the result set metadata
   * 
   * @return
   */
  public abstract GrillResultSetMetadata getMetadata() throws GrillException;

  public abstract QueryResult toQueryResult() throws GrillException;

}

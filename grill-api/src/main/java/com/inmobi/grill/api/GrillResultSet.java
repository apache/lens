package com.inmobi.grill.api;

import com.inmobi.grill.exception.GrillException;

public interface GrillResultSet {
  /**
   * Get the size of the result set
   * 
   * @return The size if available, -1 if not available.
   */
  public int size() throws GrillException;

  /**
   * Get the result set metadata
   * 
   * @return
   */
  public GrillResultSetMetadata getMetadata() throws GrillException;

}

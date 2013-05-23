package com.inmobi.grill.api;

public interface GrillResultSet {
  /**
   * Get the size of the result set
   * 
   * @return The size if available, -1 if not available.
   */
  public int size();

  /**
   * Get the result set metadata
   * 
   * @return
   */
  public GrillResultSetMetadata getMetadata();

}

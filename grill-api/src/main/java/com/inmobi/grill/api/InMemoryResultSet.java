package com.inmobi.grill.api;

import java.util.List;

import com.inmobi.grill.exception.GrillException;

public interface InMemoryResultSet extends GrillResultSet {

  /**
   * Whether there is another result row available
   * 
   * @return true if next row if available, false otherwise
   * 
   * @throws GrillException
   */
  public boolean hasNext() throws GrillException;

  /**
   * Read the next result row
   * 
   * @return The row as list of object
   * 
   * @throws GrillException
   */
  public List<Object> next() throws GrillException;

  /**
   * Set number of rows to be fetched at time
   * 
   * @param size
   */
  public void setFetchSize(int size) throws GrillException;

}

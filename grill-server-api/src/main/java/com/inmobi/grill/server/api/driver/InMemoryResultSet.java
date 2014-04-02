package com.inmobi.grill.server.api.driver;

import java.util.ArrayList;
import java.util.List;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.InMemoryQueryResult;
import com.inmobi.grill.api.query.QueryResult;
import com.inmobi.grill.api.query.ResultRow;

public abstract class InMemoryResultSet extends GrillResultSet {

  /**
   * Whether there is another result row available
   * 
   * @return true if next row if available, false otherwise
   * 
   * @throws GrillException
   */
  public abstract boolean hasNext() throws GrillException;

  /**
   * Read the next result row
   * 
   * @return The row as list of object
   * 
   * @throws GrillException
   */
  public abstract ResultRow next() throws GrillException;

  /**
   * Set number of rows to be fetched at time
   * 
   * @param size
   */
  public abstract void setFetchSize(int size) throws GrillException;

  public QueryResult toQueryResult() throws GrillException {
    List<ResultRow> rows = new ArrayList<ResultRow>();
    while (hasNext()) {
      rows.add(next());
    }
    return new InMemoryQueryResult(rows);
  }

}

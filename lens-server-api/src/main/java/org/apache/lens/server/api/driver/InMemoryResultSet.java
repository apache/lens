package org.apache.lens.server.api.driver;

import java.util.ArrayList;
import java.util.List;

import org.apache.lens.api.LensException;
import org.apache.lens.api.query.InMemoryQueryResult;
import org.apache.lens.api.query.QueryResult;
import org.apache.lens.api.query.ResultRow;

/**
 * The Class InMemoryResultSet.
 */
public abstract class InMemoryResultSet extends LensResultSet {

  /**
   * Whether there is another result row available.
   *
   * @return true if next row if available, false otherwise
   * @throws LensException
   *           the lens exception
   */
  public abstract boolean hasNext() throws LensException;

  /**
   * Read the next result row.
   *
   * @return The row as list of object
   * @throws LensException
   *           the lens exception
   */
  public abstract ResultRow next() throws LensException;

  /**
   * Set number of rows to be fetched at time
   *
   * @param size
   */
  public abstract void setFetchSize(int size) throws LensException;

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensResultSet#toQueryResult()
   */
  public QueryResult toQueryResult() throws LensException {
    List<ResultRow> rows = new ArrayList<ResultRow>();
    while (hasNext()) {
      rows.add(next());
    }
    return new InMemoryQueryResult(rows);
  }

}

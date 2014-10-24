package org.apache.lens.server.api.driver;

import org.apache.lens.api.LensException;
import org.apache.lens.api.query.PersistentQueryResult;
import org.apache.lens.api.query.QueryResult;

/**
 * The Class PersistentResultSet.
 */
public abstract class PersistentResultSet extends LensResultSet {
  public abstract String getOutputPath() throws LensException;

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensResultSet#toQueryResult()
   */
  public QueryResult toQueryResult() throws LensException {
    return new PersistentQueryResult(getOutputPath(), size());
  }
}

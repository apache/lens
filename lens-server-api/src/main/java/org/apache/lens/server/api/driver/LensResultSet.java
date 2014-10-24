package org.apache.lens.server.api.driver;

import org.apache.lens.api.LensException;
import org.apache.lens.api.query.QueryResult;

/**
 * Result set returned by driver.
 */
public abstract class LensResultSet {

  /**
   * Get the size of the result set.
   *
   * @return The size if available, -1 if not available.
   * @throws LensException
   *           the lens exception
   */
  public abstract int size() throws LensException;

  /**
   * Get the result set metadata
   *
   * @return Returns {@link LensResultSetMetadata}
   */
  public abstract LensResultSetMetadata getMetadata() throws LensException;

  /**
   * Get the corresponding query result object.
   *
   * @return {@link QueryResult}
   * @throws LensException
   *           the lens exception
   */
  public abstract QueryResult toQueryResult() throws LensException;

}

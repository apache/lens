package org.apache.lens.client;

import org.apache.lens.api.query.QueryResult;
import org.apache.lens.api.query.QueryResultSetMetadata;

/**
 * The Class LensClientResultSet.
 */
public class LensClientResultSet {

  /** The result. */
  private final QueryResult result;

  /** The result set metadata. */
  private final QueryResultSetMetadata resultSetMetadata;

  /**
   * Instantiates a new lens client result set.
   *
   * @param result
   *          the result
   * @param resultSetMetaData
   *          the result set meta data
   */
  public LensClientResultSet(QueryResult result, QueryResultSetMetadata resultSetMetaData) {
    this.result = result;
    this.resultSetMetadata = resultSetMetaData;
  }

  public QueryResult getResult() {
    return result;
  }

  public QueryResultSetMetadata getResultSetMetadata() {
    return resultSetMetadata;
  }
}

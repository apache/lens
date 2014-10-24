package org.apache.lens.ml;

import org.apache.lens.api.LensException;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.QueryHandle;

/**
 * Run the model testing query against a Lens server.
 */
public abstract class TestQueryRunner {

  /** The session handle. */
  protected final LensSessionHandle sessionHandle;

  /**
   * Instantiates a new test query runner.
   *
   * @param sessionHandle
   *          the session handle
   */
  public TestQueryRunner(LensSessionHandle sessionHandle) {
    this.sessionHandle = sessionHandle;
  }

  /**
   * Run query.
   *
   * @param query
   *          the query
   * @return the query handle
   * @throws LensException
   *           the lens exception
   */
  public abstract QueryHandle runQuery(String query) throws LensException;
}

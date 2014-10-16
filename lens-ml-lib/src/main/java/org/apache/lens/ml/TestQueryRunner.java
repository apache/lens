package org.apache.lens.ml;

import org.apache.lens.api.LensException;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.QueryHandle;

/**
 * Run the model testing query against a Lens server
 */
public abstract class TestQueryRunner {
  protected final LensSessionHandle sessionHandle;

  public TestQueryRunner(LensSessionHandle sessionHandle) {
    this.sessionHandle = sessionHandle;
  }

  public abstract QueryHandle runQuery(String query) throws LensException;
}

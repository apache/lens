package org.apache.lens.server.api.query;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

/**
 * Query result formatter, if the result is persisted by driver.
 */
public interface PersistedOutputFormatter extends QueryOutputFormatter {

  /**
   * Add result rows from the persisted path.
   *
   * @param persistedPath
   *          the persisted path
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void addRowsFromPersistedPath(Path persistedPath) throws IOException;

}

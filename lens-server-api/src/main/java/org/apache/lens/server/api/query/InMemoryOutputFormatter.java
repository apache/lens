package org.apache.lens.server.api.query;

import java.io.IOException;

import org.apache.lens.api.query.ResultRow;

/**
 * Query result formatter, if the result from driver is in in-memory.
 */
public interface InMemoryOutputFormatter extends QueryOutputFormatter {

  /**
   * Write a row of the result.
   *
   * @param row
   *          {@link ResultRow} object
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void writeRow(ResultRow row) throws IOException;

}

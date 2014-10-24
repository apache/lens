package org.apache.lens.server.api.query;

import java.io.IOException;

import org.apache.lens.server.api.driver.LensResultSetMetadata;

/**
 * The interface for query result formatting
 *
 * This is an abstract interface, user should implement {@link InMemoryOutputFormatter} or
 * {@link PersistedOutputFormatter} for formatting the result.
 */
public interface QueryOutputFormatter {

  /**
   * Initialize the formatter.
   *
   * @param ctx
   *          The {@link QueryContext} object
   * @param metadata
   *          {@link LensResultSetMetadata} object
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void init(QueryContext ctx, LensResultSetMetadata metadata) throws IOException;

  /**
   * Write the header.
   *
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void writeHeader() throws IOException;

  /**
   * Write the footer.
   *
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void writeFooter() throws IOException;

  /**
   * Commit the formatting.
   *
   * This will make the result consumable by user, will be called after all the writes succeed.
   *
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void commit() throws IOException;

  /**
   * Close the formatter. Cleanup any resources.
   *
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void close() throws IOException;

  /**
   * Get final location where formatted output is available
   *
   * @return
   */
  public String getFinalOutputPath();

  /**
   * Get total number of rows in result.
   *
   * @return Total number of rows, return -1, if not known
   */
  public int getNumRows();

  /**
   * Get resultset metadata
   *
   * @return {@link LensResultSetMetadata}
   */
  public LensResultSetMetadata getMetadata();
}

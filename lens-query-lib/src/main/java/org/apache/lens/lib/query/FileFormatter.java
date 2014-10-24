package org.apache.lens.lib.query;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

/**
 * File formatter interface which is wrapped in {@link WrappedFileFormatter}.
 */
public interface FileFormatter {

  /**
   * Setup outputs for file formatter.
   *
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void setupOutputs() throws IOException;

  /**
   * Write the header passed.
   *
   * @param header
   *          the header
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void writeHeader(String header) throws IOException;

  /**
   * Write the footer passed.
   *
   * @param footer
   *          the footer
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void writeFooter(String footer) throws IOException;

  /**
   * Write the row passed.
   *
   * @param row
   *          the row
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void writeRow(String row) throws IOException;

  /**
   * Get the temporary path of the result, if any
   *
   * @return
   */
  public Path getTmpPath();

  /**
   * Get the result encoding, if any
   *
   * @return
   */
  public String getEncoding();
}

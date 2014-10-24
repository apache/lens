package org.apache.lens.server.query;

import org.apache.lens.api.LensException;
import org.apache.lens.server.api.driver.LensResultSetMetadata;
import org.apache.lens.server.api.driver.PersistentResultSet;

/**
 * The Class LensPersistentResult.
 */
public class LensPersistentResult extends PersistentResultSet {

  /** The metadata. */
  private final LensResultSetMetadata metadata;

  /** The output path. */
  private final String outputPath;

  /** The num rows. */
  private final int numRows;

  /**
   * Instantiates a new lens persistent result.
   *
   * @param metadata
   *          the metadata
   * @param outputPath
   *          the output path
   * @param numRows
   *          the num rows
   */
  public LensPersistentResult(LensResultSetMetadata metadata, String outputPath, int numRows) {
    this.metadata = metadata;
    this.outputPath = outputPath;
    this.numRows = numRows;
  }

  @Override
  public String getOutputPath() throws LensException {
    return outputPath;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensResultSet#size()
   */
  @Override
  public int size() throws LensException {
    return numRows;
  }

  @Override
  public LensResultSetMetadata getMetadata() throws LensException {
    return metadata;
  }
}

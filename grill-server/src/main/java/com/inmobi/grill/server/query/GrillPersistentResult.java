package com.inmobi.grill.server.query;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.server.api.driver.GrillResultSetMetadata;
import com.inmobi.grill.server.api.driver.PersistentResultSet;

public class GrillPersistentResult extends PersistentResultSet {

  private final GrillResultSetMetadata metadata;
  private final String outputPath;
  private final int numRows;

  public GrillPersistentResult(GrillResultSetMetadata metadata,
      String outputPath, int numRows) {
    this.metadata = metadata;
    this.outputPath = outputPath;
    this.numRows = numRows;
  }

  @Override
  public String getOutputPath() throws GrillException {
    return outputPath;
  }

  @Override
  public int size() throws GrillException {
    return numRows;
  }

  @Override
  public GrillResultSetMetadata getMetadata() throws GrillException {
    return metadata;
  }
}

package com.inmobi.grill.lib.query;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import com.inmobi.grill.server.api.driver.GrillResultSetMetadata;
import com.inmobi.grill.server.api.query.QueryContext;

public abstract class AbstractFileFormatter extends AbstractOutputFormatter
    implements FileFormatter {

  protected int numRows = 0;
  protected Path finalPath;

  @Override
  public void init(QueryContext ctx, GrillResultSetMetadata metadata) throws IOException {
    super.init(ctx, metadata);
    setupOutputs();
  }

  @Override
  public int getNumRows() {
    return numRows;
  }

  @Override
  public void writeHeader() throws IOException {
    // dummy
  }

  @Override
  public void writeFooter() throws IOException {
    // dummy
  }

  public String getFinalOutputPath() {
    return finalPath.toString();
  }
}

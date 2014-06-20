package com.inmobi.grill.lib.query;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

public interface FileFormatter {

  public void setupOutputs() throws IOException;

  public void writeHeader(String header) throws IOException;

  public void writeFooter(String footer) throws IOException;

  public void writeRow(String row) throws IOException;

  public Path getTmpPath();

  public String getEncoding();
}

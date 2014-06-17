package com.inmobi.grill.lib.query;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;

import com.inmobi.grill.lib.query.GrillFileOutputFormat.GrillRowWriter;
import com.inmobi.grill.server.api.driver.GrillResultSetMetadata;
import com.inmobi.grill.server.api.query.QueryContext;

public abstract class FileFormatter extends AbstractOutputFormatter {

  private Path outputPath;
  protected GrillRowWriter rowWriter;
  private Path finalPath;

  protected void setupOutputs() {
    String pathStr = ctx.getResultSetParentDir();
    if (StringUtils.isBlank(pathStr)) {
      throw new IllegalArgumentException("No output path specified");
    }
    outputPath = new Path(pathStr, ctx.getQueryHandle().toString());
    Path tmpWorkPath = new Path(outputPath + ".tmp");
    try {
      rowWriter = GrillFileOutputFormat.createRecordWriter(getConf(), tmpWorkPath,
          Reporter.NULL);
    } catch (IOException e) {
      throw new IllegalArgumentException("Could not create tmp path");
    }
  }

  @Override
  public void init(QueryContext ctx, GrillResultSetMetadata metadata) {
    super.init(ctx, metadata);
    setupOutputs();
  }

  Text cachedRow;
  protected void writeRow(String row) throws IOException {
    if (cachedRow == null) {
      cachedRow = new Text();
    }
    cachedRow.set(row);
    rowWriter.write(null, cachedRow);
  }

  @Override
  public void commit() throws IOException {
    rowWriter.close(Reporter.NULL);
    if (outputPath != null && rowWriter.getTmpPath() != null) {
      FileSystem fs = outputPath.getFileSystem(getConf());
      finalPath = outputPath;
      if (rowWriter.getExtn() != null) {
        finalPath = new Path(outputPath + rowWriter.getExtn());
      }
      fs.rename(rowWriter.getTmpPath(), finalPath);
      ctx.setResultSetPath(finalPath.makeQualified(fs).toString());
    }
  }

  GrillRowWriter getRowWriter() {
    return rowWriter;
  }
  
  public Path getFinalOutputPath() {
    return finalPath;
  }

  @Override
  public void close() throws IOException {
  }
}

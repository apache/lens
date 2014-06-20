package com.inmobi.grill.lib.query;

/*
 * #%L
 * Grill Query Library
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;

import com.inmobi.grill.lib.query.GrillFileOutputFormat.GrillRowWriter;

public class HadoopFileFormatter extends AbstractFileFormatter {

  private Path outputPath;
  protected GrillRowWriter rowWriter;

  public void setupOutputs() throws IOException {
    String pathStr = ctx.getResultSetParentDir();
    if (StringUtils.isBlank(pathStr)) {
      throw new IllegalArgumentException("No output path specified");
    }
    outputPath = new Path(pathStr, ctx.getQueryHandle().toString());
    Path tmpWorkPath = new Path(outputPath + ".tmp");
    try {
      rowWriter = GrillFileOutputFormat.createRecordWriter(ctx.getConf(), tmpWorkPath,
          Reporter.NULL, ctx.getCompressOutput(), ctx.getOuptutFileExtn(),
          ctx.getResultEncoding());
    } catch (IOException e) {
      throw new IllegalArgumentException("Could not create tmp path");
    }
  }

  public void writeHeader(String header) throws IOException {
    rowWriter.write(null, new Text(header));
  }

  public void writeFooter(String footer) throws IOException {
    rowWriter.write(null, new Text(footer));
  }

  private Text cachedRow;
  public void writeRow(String row) throws IOException {
    if (cachedRow == null) {
      cachedRow = new Text();
    }
    cachedRow.set(row);
    rowWriter.write(null, cachedRow);
    numRows++;
  }

  @Override
  public void commit() throws IOException {
    rowWriter.close(Reporter.NULL);
    if (outputPath != null && rowWriter.getTmpPath() != null) {
      FileSystem fs = outputPath.getFileSystem(ctx.getConf());
      finalPath = outputPath;
      if (rowWriter.getExtn() != null) {
        finalPath = new Path(outputPath + rowWriter.getExtn());
      }
      finalPath = finalPath.makeQualified(fs);
      fs.rename(rowWriter.getTmpPath(), finalPath);
      ctx.setResultSetPath(finalPath.toString());
    }
  }

  @Override
  public void close() throws IOException {
    rowWriter.close(Reporter.NULL);
  }

  @Override
  public Path getTmpPath() {
    return rowWriter.getTmpPath();
  }

  @Override
  public String getEncoding() {
    return rowWriter.getEncoding();
  }
}

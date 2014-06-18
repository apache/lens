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
      finalPath = finalPath.makeQualified(fs);
      fs.rename(rowWriter.getTmpPath(), finalPath);
      ctx.setResultSetPath(finalPath.toString());
    }
  }

  GrillRowWriter getRowWriter() {
    return rowWriter;
  }
  
  public String getFinalOutputPath() {
    return finalPath.toString();
  }

  @Override
  public void close() throws IOException {
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.lib.query;

import java.io.IOException;

import org.apache.lens.lib.query.LensFileOutputFormat.LensRowWriter;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;

import com.google.common.base.Strings;

/**
 * A hadoop file formatter
 * <p></p>
 * This has capability to create output on Hadoop compatible files systems, with hadoop supported compression codecs.
 */
public class HadoopFileFormatter extends AbstractFileFormatter {

  /**
   * The output path.
   */
  private Path outputPath;

  /**
   * The row writer.
   */
  protected LensRowWriter rowWriter;

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.lib.query.FileFormatter#setupOutputs()
   */
  public void setupOutputs() throws IOException {
    String pathStr = ctx.getResultSetParentDir();
    if (StringUtils.isBlank(pathStr)) {
      throw new IllegalArgumentException("No output path specified");
    }
    String outputPathStr = Strings.isNullOrEmpty(ctx.getQueryName()) ? ""
      : LensFileOutputFormat.getValidOutputFileName(ctx.getQueryName()) + "-";
    outputPath = new Path(pathStr, outputPathStr + ctx.getQueryHandle().toString());
    Path tmpWorkPath = new Path(pathStr, ctx.getQueryHandle().toString() + ".tmp");
    try {
      rowWriter = LensFileOutputFormat.createRecordWriter(ctx.getConf(), tmpWorkPath, Reporter.NULL,
        ctx.getCompressOutput(), ctx.getOuptutFileExtn(), ctx.getResultEncoding());
      numRows=0;
    } catch (IOException e) {
      throw new IllegalArgumentException("Could not create tmp path");
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.lib.query.FileFormatter#writeHeader(java.lang.String)
   */
  public void writeHeader(String header) throws IOException {
    rowWriter.write(null, new Text(header));
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.lib.query.FileFormatter#writeFooter(java.lang.String)
   */
  public void writeFooter(String footer) throws IOException {
    rowWriter.write(null, new Text(footer));
  }

  /**
   * The cached row.
   */
  private Text cachedRow;

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.lib.query.FileFormatter#writeRow(java.lang.String)
   */
  public void writeRow(String row) throws IOException {
    if (cachedRow == null) {
      cachedRow = new Text();
    }
    cachedRow.set(row);
    rowWriter.write(null, cachedRow);
    numRows++;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.query.QueryOutputFormatter#commit()
   */
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
      fileSize = fs.getFileStatus(finalPath).getLen();
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.query.QueryOutputFormatter#close()
   */
  @Override
  public void close() throws IOException {
    if (null != rowWriter) {
      rowWriter.close(Reporter.NULL);
    }
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

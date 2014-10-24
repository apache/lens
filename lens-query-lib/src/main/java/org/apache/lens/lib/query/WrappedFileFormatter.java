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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.lens.server.api.driver.LensResultSetMetadata;
import org.apache.lens.server.api.query.QueryContext;

/**
 * Wraps the formatter {@link FileFormatter}, which can have implementations like {@link HadoopFileFormatter} or
 * {@link ZipFileFormatter}.
 */
public abstract class WrappedFileFormatter extends AbstractOutputFormatter {

  /** The formatter. */
  private AbstractFileFormatter formatter;

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(FilePersistentFormatter.class);

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.lib.query.AbstractOutputFormatter#init(org.apache.lens.server.api.query.QueryContext,
   * org.apache.lens.server.api.driver.LensResultSetMetadata)
   */
  public void init(QueryContext ctx, LensResultSetMetadata metadata) throws IOException {
    super.init(ctx, metadata);
    if (ctx.splitResultIntoMultipleFiles()) {
      formatter = new ZipFileFormatter();
    } else {
      formatter = new HadoopFileFormatter();
    }
    formatter.init(ctx, metadata);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryOutputFormatter#writeHeader()
   */
  @Override
  public void writeHeader() throws IOException {
    String header = ctx.getResultHeader();
    if (!StringUtils.isBlank(header)) {
      formatter.writeHeader(header);
    } else {
      formatter.writeHeader(getHeaderFromSerde());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryOutputFormatter#writeFooter()
   */
  @Override
  public void writeFooter() throws IOException {
    String footer = ctx.getResultFooter();
    if (!StringUtils.isBlank(footer)) {
      formatter.writeFooter(footer);
    } else {
      formatter.writeFooter("Total rows:" + getNumRows());
    }
  }

  /**
   * Write row.
   *
   * @param row
   *          the row
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  protected void writeRow(String row) throws IOException {
    formatter.writeRow(row);
  }

  @Override
  public int getNumRows() {
    return formatter.getNumRows();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryOutputFormatter#commit()
   */
  @Override
  public void commit() throws IOException {
    formatter.commit();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryOutputFormatter#close()
   */
  @Override
  public void close() throws IOException {
    if (formatter != null) {
      formatter.close();
    }
  }

  @Override
  public String getFinalOutputPath() {
    return formatter.getFinalOutputPath();
  }

  public Path getTmpPath() {
    return formatter.getTmpPath();
  }

  public String getEncoding() {
    return formatter.getEncoding();
  }
}

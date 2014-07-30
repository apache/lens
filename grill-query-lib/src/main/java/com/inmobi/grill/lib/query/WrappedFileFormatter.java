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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

import com.inmobi.grill.server.api.driver.GrillResultSetMetadata;
import com.inmobi.grill.server.api.query.QueryContext;

/**
 * Wraps the formatter {@link FileFormatter}, which can have implementations
 * like {@link HadoopFileFormatter} or {@link ZipFileFormatter}
 *
 */
public abstract class WrappedFileFormatter extends AbstractOutputFormatter {

  private AbstractFileFormatter formatter;
  public static final Log LOG = LogFactory.getLog(FilePersistentFormatter.class);

  public void init(QueryContext ctx, GrillResultSetMetadata metadata) throws IOException {
    super.init(ctx, metadata);
    if (ctx.splitResultIntoMultipleFiles()) {
      formatter = new ZipFileFormatter();
    } else {
      formatter = new HadoopFileFormatter();
    }
    formatter.init(ctx, metadata);
  }

  @Override
  public void writeHeader() throws IOException {
    String header = ctx.getResultHeader();
    if (!StringUtils.isBlank(header)) {
      formatter.writeHeader(header);
    } else {
      formatter.writeHeader(getHeaderFromSerde());
    }
  }

  @Override
  public void writeFooter() throws IOException {
    String footer = ctx.getResultFooter();
    if (!StringUtils.isBlank(footer)) {
      formatter.writeFooter(footer);
    } else {
      formatter.writeFooter("Total rows:" + getNumRows());
    }
  }

  protected void writeRow(String row) throws IOException {
    formatter.writeRow(row);
  }

  @Override
  public int getNumRows() {
    return formatter.getNumRows();
  }

  @Override
  public void commit() throws IOException {
    formatter.commit();
  }

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

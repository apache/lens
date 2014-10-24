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

import org.apache.hadoop.fs.Path;
import org.apache.lens.server.api.driver.LensResultSetMetadata;
import org.apache.lens.server.api.query.QueryContext;

/**
 * Abstract implementation of {@link FileFormatter}, which gets column details from {@link AbstractOutputFormatter}.
 */
public abstract class AbstractFileFormatter extends AbstractOutputFormatter implements FileFormatter {

  /** The num rows. */
  protected int numRows = 0;

  /** The final path. */
  protected Path finalPath;

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.lib.query.AbstractOutputFormatter#init(org.apache.lens.server.api.query.QueryContext,
   * org.apache.lens.server.api.driver.LensResultSetMetadata)
   */
  @Override
  public void init(QueryContext ctx, LensResultSetMetadata metadata) throws IOException {
    super.init(ctx, metadata);
    setupOutputs();
  }

  @Override
  public int getNumRows() {
    return numRows;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryOutputFormatter#writeHeader()
   */
  @Override
  public void writeHeader() throws IOException {
    // dummy
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryOutputFormatter#writeFooter()
   */
  @Override
  public void writeFooter() throws IOException {
    // dummy
  }

  public String getFinalOutputPath() {
    return finalPath.toString();
  }
}

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
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.lens.server.api.driver.LensResultSetMetadata;
import org.apache.lens.server.api.query.QueryContext;

import org.apache.hadoop.fs.Path;

/**
 * Abstract implementation of {@link FileFormatter}, which gets column details from {@link AbstractOutputFormatter}.
 */
public abstract class AbstractFileFormatter extends AbstractOutputFormatter implements FileFormatter {

  /**
   * The num rows.
   */
  protected Integer numRows;

  /**
   * The file size.
   */
  protected Long fileSize;

  /**
   * The final path.
   */
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
  public Integer getNumRows() {
    return numRows;
  }

  @Override
  public Long getFileSize() {
    return fileSize;
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

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeUTF(metadata.toJson());
    out.writeUTF(finalPath.toString());
    out.writeInt(numRows);
    out.writeLong(fileSize);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    metadata = LensResultSetMetadata.fromJson(in.readUTF());
    finalPath = new Path(in.readUTF());
    numRows = in.readInt();
    fileSize = in.readLong();
  }
}

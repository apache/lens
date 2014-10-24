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
package org.apache.lens.server.query;

import org.apache.lens.api.LensException;
import org.apache.lens.server.api.driver.LensResultSetMetadata;
import org.apache.lens.server.api.driver.PersistentResultSet;

/**
 * The Class LensPersistentResult.
 */
public class LensPersistentResult extends PersistentResultSet {

  /** The metadata. */
  private final LensResultSetMetadata metadata;

  /** The output path. */
  private final String outputPath;

  /** The num rows. */
  private final int numRows;

  /**
   * Instantiates a new lens persistent result.
   *
   * @param metadata
   *          the metadata
   * @param outputPath
   *          the output path
   * @param numRows
   *          the num rows
   */
  public LensPersistentResult(LensResultSetMetadata metadata, String outputPath, int numRows) {
    this.metadata = metadata;
    this.outputPath = outputPath;
    this.numRows = numRows;
  }

  @Override
  public String getOutputPath() throws LensException {
    return outputPath;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensResultSet#size()
   */
  @Override
  public int size() throws LensException {
    return numRows;
  }

  @Override
  public LensResultSetMetadata getMetadata() throws LensException {
    return metadata;
  }
}

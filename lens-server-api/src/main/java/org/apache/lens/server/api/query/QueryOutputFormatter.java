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
package org.apache.lens.server.api.query;

import java.io.Externalizable;
import java.io.IOException;

import org.apache.lens.server.api.driver.LensResultSetMetadata;

/**
 * The interface for query result formatting
 * <p></p>
 * This is an abstract interface, user should implement {@link InMemoryOutputFormatter} or
 * {@link PersistedOutputFormatter} for formatting the result.
 */
public interface QueryOutputFormatter extends Externalizable {

  /**
   * Initialize the formatter.
   *
   * @param ctx      The {@link QueryContext} object
   * @param metadata {@link LensResultSetMetadata} object
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void init(QueryContext ctx, LensResultSetMetadata metadata) throws IOException;

  /**
   * Write the header.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void writeHeader() throws IOException;

  /**
   * Write the footer.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void writeFooter() throws IOException;

  /**
   * Commit the formatting.
   * <p></p>
   * This will make the result consumable by user, will be called after all the writes succeed.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void commit() throws IOException;

  /**
   * Close the formatter. Cleanup any resources.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void close() throws IOException;

  /**
   * Get final location where formatted output is available
   *
   * @return
   */
  String getFinalOutputPath();

  /**
   * Get total number of rows in result.
   *
   * @return Total number of rows, return null, if not known
   */
  Integer getNumRows();

  /**
   * Get size of the resultset file.
   * @return Total number of rows, return null, if not known
   */
  Long getFileSize();

  /**
   * Get resultset metadata
   *
   * @return {@link LensResultSetMetadata}
   */
  LensResultSetMetadata getMetadata();
}

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

import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * File formatter interface which is wrapped in {@link WrappedFileFormatter}.
 */
public interface FileFormatter {

  /**
   * Setup outputs for file formatter.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void setupOutputs() throws IOException;

  /**
   * Write the header passed.
   *
   * @param header the header
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void writeHeader(String header) throws IOException;

  /**
   * Write the footer passed.
   *
   * @param footer the footer
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void writeFooter(String footer) throws IOException;

  /**
   * Write the row passed.
   *
   * @param row the row
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void writeRow(String row) throws IOException;

  /**
   * Get the temporary path of the result, if any
   *
   * @return
   */
  Path getTmpPath();

  /**
   * Get the result encoding, if any
   *
   * @return
   */
  String getEncoding();
}

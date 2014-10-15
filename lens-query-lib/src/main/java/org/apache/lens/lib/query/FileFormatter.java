package org.apache.lens.lib.query;

/*
 * #%L
 * Lens Query Library
 * %%
 * Copyright (C) 2014 Apache Software Foundation
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

import org.apache.hadoop.fs.Path;

/**
 * File formatter interface which is wrapped in {@link WrappedFileFormatter}
 *
 */
public interface FileFormatter {

  /**
   * Setup outputs for file formatter
   * 
   * @throws IOException
   */
  public void setupOutputs() throws IOException;

  /**
   * Write the header passed
   * 
   * @param header
   * @throws IOException
   */
  public void writeHeader(String header) throws IOException;

  /**
   * Write the footer passed
   * 
   * @param footer
   * @throws IOException
   */
  public void writeFooter(String footer) throws IOException;

  /**
   * Write the row passed
   * 
   * @param row
   * @throws IOException
   */
  public void writeRow(String row) throws IOException;

  /**
   * Get the temporary path of the result, if any
   * 
   * @return
   */
  public Path getTmpPath();

  /**
   * Get the result encoding, if any
   * 
   * @return
   */
  public String getEncoding();
}

package org.apache.lens.server.api.query;

/*
 * #%L
 * Grill API for server and extensions
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

import org.apache.hadoop.conf.Configurable;
import org.apache.lens.server.api.driver.GrillResultSetMetadata;


/**
 * The interface for query result formatting
 *
 * This is an abstract interface, user should implement
 * {@link InMemoryOutputFormatter} or {@link PersistedOutputFormatter} for
 * formatting the result.
 */
public interface QueryOutputFormatter {

  /**
   * Initialize the formatter
   *
   * @param ctx The {@link QueryContext} object
   * @param metadata {@link GrillResultSetMetadata} object
   * @throws IOException 
   */
  public void init(QueryContext ctx, GrillResultSetMetadata metadata) throws IOException;

  /**
   * Write the header
   *
   * @throws IOException
   */
  public void writeHeader() throws IOException;

  /**
   * Write the footer
   *
   * @throws IOException
   */
  public void writeFooter() throws IOException;

  /**
   * Commit the formatting. 
   * 
   * This will make the result consumable by user, will be called after
   * all the writes succeed.
   *
   * @throws IOException
   */
  public void commit() throws IOException;

  /**
   * Close the formatter. Cleanup any resources.
   *
   * @throws IOException
   */
  public void close() throws IOException;

  /**
   * Get final location where formatted output is available
   *
   * @return 
   */
  public String getFinalOutputPath();

  /**
   * Get total number of rows in result.
   *
   * @return Total number of rows, return -1, if not known
   */
  public int getNumRows();

  /**
   * Get resultset metadata
   *
   * @return {@link GrillResultSetMetadata}
   */
  public GrillResultSetMetadata getMetadata();
}

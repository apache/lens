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

package org.apache.lens.client.resultset;

import org.apache.lens.client.exceptions.LensClientIOException;

/**
 * This interface represents the query ResultSet that the clients can iterate over
 */
public interface ResultSet{

  /**
   * Returns true if next row is present in result set and moves the cursor to that row.
   *
   * @throws LensClientIOException
   */
  boolean next() throws LensClientIOException;

  /**
   * @return column values for the current row
   */
  String[] getRow() throws LensClientIOException;

  /**
   * Returns the column names for this result set if available otherwise null is returned
   * @throws LensClientIOException
   */
  String[] getColumnNames() throws LensClientIOException;
}

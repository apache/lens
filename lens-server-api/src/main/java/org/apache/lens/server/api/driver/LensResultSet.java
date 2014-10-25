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
package org.apache.lens.server.api.driver;

import org.apache.lens.api.LensException;
import org.apache.lens.api.query.QueryResult;

/**
 * Result set returned by driver.
 */
public abstract class LensResultSet {

  /**
   * Get the size of the result set.
   *
   * @return The size if available, -1 if not available.
   * @throws LensException
   *           the lens exception
   */
  public abstract int size() throws LensException;

  /**
   * Get the result set metadata
   *
   * @return Returns {@link LensResultSetMetadata}
   */
  public abstract LensResultSetMetadata getMetadata() throws LensException;

  /**
   * Get the corresponding query result object.
   *
   * @return {@link QueryResult}
   * @throws LensException
   *           the lens exception
   */
  public abstract QueryResult toQueryResult() throws LensException;

}

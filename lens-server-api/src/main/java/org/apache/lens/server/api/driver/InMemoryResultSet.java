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

import java.util.ArrayList;
import java.util.List;

import org.apache.lens.api.query.InMemoryQueryResult;
import org.apache.lens.api.query.ResultRow;
import org.apache.lens.server.api.error.LensException;

import lombok.Getter;
import lombok.Setter;

/**
 * The Class InMemoryResultSet.
 */
public abstract class InMemoryResultSet extends LensResultSet {

  @Setter
  private boolean fullyAccessed = false;

  @Getter
  private long creationTime = System.currentTimeMillis();;

  @Override
  public boolean canBePurged() {
    return fullyAccessed;
  }

  @Override
  public String getOutputPath() throws LensException {
    return null;
  }

  /**
   * Whether there is another result row available.
   *
   * @return true if next row if available, false otherwise
   * @throws LensException the lens exception
   */
  public abstract boolean hasNext() throws LensException;

  /**
   * Read the next result row.
   *
   * @return The row as list of object
   * @throws LensException the lens exception
   */
  public abstract ResultRow next() throws LensException;

  /**
   * Set number of rows to be fetched at time
   *
   * @param size
   */
  public abstract void setFetchSize(int size) throws LensException;

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.driver.LensResultSet#toQueryResult()
   */
  public InMemoryQueryResult toQueryResult() throws LensException {
    List<ResultRow> rows = new ArrayList<>();
    while (hasNext()) {
      rows.add(next());
    }
    this.setFullyAccessed(true);
    return new InMemoryQueryResult(rows);
  }
  public boolean isHttpResultAvailable() throws LensException {
    return false;
  }
}

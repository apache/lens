package org.apache.lens.server.api.driver;

/*
 * #%L
 * Lens API for server and extensions
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

import java.util.ArrayList;
import java.util.List;

import org.apache.lens.api.LensException;
import org.apache.lens.api.query.InMemoryQueryResult;
import org.apache.lens.api.query.QueryResult;
import org.apache.lens.api.query.ResultRow;


public abstract class InMemoryResultSet extends LensResultSet {

  /**
   * Whether there is another result row available
   *
   * @return true if next row if available, false otherwise
   *
   * @throws LensException
   */
  public abstract boolean hasNext() throws LensException;

  /**
   * Read the next result row
   *
   * @return The row as list of object
   *
   * @throws LensException
   */
  public abstract ResultRow next() throws LensException;

  /**
   * Set number of rows to be fetched at time
   *
   * @param size
   */
  public abstract void setFetchSize(int size) throws LensException;

  public QueryResult toQueryResult() throws LensException {
    List<ResultRow> rows = new ArrayList<ResultRow>();
    while (hasNext()) {
      rows.add(next());
    }
    return new InMemoryQueryResult(rows);
  }

}

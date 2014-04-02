package com.inmobi.grill.server.api.driver;

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

import java.util.ArrayList;
import java.util.List;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.InMemoryQueryResult;
import com.inmobi.grill.api.query.QueryResult;
import com.inmobi.grill.api.query.ResultRow;

public abstract class InMemoryResultSet extends GrillResultSet {

  /**
   * Whether there is another result row available
   * 
   * @return true if next row if available, false otherwise
   * 
   * @throws GrillException
   */
  public abstract boolean hasNext() throws GrillException;

  /**
   * Read the next result row
   * 
   * @return The row as list of object
   * 
   * @throws GrillException
   */
  public abstract ResultRow next() throws GrillException;

  /**
   * Set number of rows to be fetched at time
   * 
   * @param size
   */
  public abstract void setFetchSize(int size) throws GrillException;

  public QueryResult toQueryResult() throws GrillException {
    List<ResultRow> rows = new ArrayList<ResultRow>();
    while (hasNext()) {
      rows.add(next());
    }
    return new InMemoryQueryResult(rows);
  }

}

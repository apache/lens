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
package org.apache.lens.client;

import org.apache.lens.api.query.QueryResult;
import org.apache.lens.api.query.QueryResultSetMetadata;

/**
 * The Class LensClientResultSet.
 */
public class LensClientResultSet {

  /** The result. */
  private final QueryResult result;

  /** The result set metadata. */
  private final QueryResultSetMetadata resultSetMetadata;

  /**
   * Instantiates a new lens client result set.
   *
   * @param result
   *          the result
   * @param resultSetMetaData
   *          the result set meta data
   */
  public LensClientResultSet(QueryResult result, QueryResultSetMetadata resultSetMetaData) {
    this.result = result;
    this.resultSetMetadata = resultSetMetaData;
  }

  public QueryResult getResult() {
    return result;
  }

  public QueryResultSetMetadata getResultSetMetadata() {
    return resultSetMetadata;
  }
}

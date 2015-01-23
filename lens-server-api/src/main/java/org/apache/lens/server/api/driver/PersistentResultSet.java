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
import org.apache.lens.api.query.PersistentQueryResult;
import org.apache.lens.api.query.QueryResult;

/**
 * The Class PersistentResultSet.
 */
public abstract class PersistentResultSet extends LensResultSet {
  public abstract String getOutputPath() throws LensException;

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.driver.LensResultSet#toQueryResult()
   */
  public QueryResult toQueryResult() throws LensException {
    return new PersistentQueryResult(getOutputPath(), size());
  }
}

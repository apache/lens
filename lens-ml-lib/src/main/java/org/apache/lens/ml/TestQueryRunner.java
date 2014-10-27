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
package org.apache.lens.ml;

import org.apache.lens.api.LensException;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.QueryHandle;

/**
 * Run the model testing query against a Lens server.
 */
public abstract class TestQueryRunner {

  /** The session handle. */
  protected final LensSessionHandle sessionHandle;

  /**
   * Instantiates a new test query runner.
   *
   * @param sessionHandle
   *          the session handle
   */
  public TestQueryRunner(LensSessionHandle sessionHandle) {
    this.sessionHandle = sessionHandle;
  }

  /**
   * Run query.
   *
   * @param query
   *          the query
   * @return the query handle
   * @throws LensException
   *           the lens exception
   */
  public abstract QueryHandle runQuery(String query) throws LensException;
}

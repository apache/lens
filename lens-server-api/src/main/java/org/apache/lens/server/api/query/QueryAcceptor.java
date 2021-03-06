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
package org.apache.lens.server.api.query;

import org.apache.lens.api.query.SubmitOp;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;

/**
 * The Interface QueryAcceptor.
 */
public interface QueryAcceptor {

  /**
   * Whether to accept the query or not.
   *
   * @param query    The query
   * @param conf     The configuration of the query
   * @param submitOp the submit op
   * @return null if query should be accepted, rejection cause otherwise
   * @throws LensException the lens exception
   */
  String accept(String query, Configuration conf, SubmitOp submitOp) throws LensException;

}

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
package org.apache.lens.server.api.query.constraint;

import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.collect.EstimatedImmutableQueryCollection;

public interface QueryLaunchingConstraint {

  /**
   * Returns whether this constraint allows candidate query to be launched.
   *
   * @param candidateQuery The query which is the next candidate to be launched.
   * @param launchedQueries Current launched queries
   * @return null if allowed to launch, otherwise a String containing the reason to block launch
   */
  String allowsLaunchOf(final QueryContext candidateQuery, final EstimatedImmutableQueryCollection launchedQueries);
}

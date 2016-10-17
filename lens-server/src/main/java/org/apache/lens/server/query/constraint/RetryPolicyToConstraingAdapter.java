/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.query.constraint;


import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.collect.EstimatedImmutableQueryCollection;
import org.apache.lens.server.api.query.constraint.QueryLaunchingConstraint;
import org.apache.lens.server.api.retry.BackOffRetryHandler;

import lombok.Data;

@Data
public class RetryPolicyToConstraingAdapter implements QueryLaunchingConstraint {
  private final BackOffRetryHandler<QueryContext> constraint;
  @Override
  public String allowsLaunchOf(QueryContext candidateQuery, EstimatedImmutableQueryCollection launchedQueries) {
    if (!constraint.canTryOpNow(candidateQuery)) {
      return "Query will be automatically re-attempted in "
        + (constraint.getOperationNextTime(candidateQuery) - System.currentTimeMillis())/1000 + " seconds";
    }
    return null;
  }
}

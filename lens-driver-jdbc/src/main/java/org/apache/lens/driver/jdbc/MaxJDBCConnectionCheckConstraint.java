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
package org.apache.lens.driver.jdbc;

import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.collect.EstimatedImmutableQueryCollection;
import org.apache.lens.server.api.query.constraint.QueryLaunchingConstraint;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MaxJDBCConnectionCheckConstraint implements QueryLaunchingConstraint {

  private final int poolMaxSize;

  public MaxJDBCConnectionCheckConstraint(final int poolMaxSize) {
    this.poolMaxSize = poolMaxSize;
  }

  @Override
  public String allowsLaunchOf(final QueryContext candidateQuery,
                                EstimatedImmutableQueryCollection launchedQueries) {
    final LensDriver selectedDriver = candidateQuery.getSelectedDriver();
    if (!(selectedDriver instanceof JDBCDriver)) {
      return "driver isn't jdbc driver";
    }
    int runningQueries = ((JDBCDriver) selectedDriver).getQueryContextMap().size();
    if (runningQueries >= poolMaxSize) {
      return runningQueries + "/" + poolMaxSize + " queries running on driver "
        + candidateQuery.getSelectedDriver().getFullyQualifiedName();
    }
    return null;
  }
}


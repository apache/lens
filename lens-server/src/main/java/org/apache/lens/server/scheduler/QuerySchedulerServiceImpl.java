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
package org.apache.lens.server.scheduler;

import org.apache.lens.server.BaseLensService;
import org.apache.lens.server.api.health.HealthStatus;
import org.apache.lens.server.api.scheduler.QuerySchedulerService;

import org.apache.hive.service.cli.CLIService;

/**
 * The Class QuerySchedulerServiceImpl.
 */
public class QuerySchedulerServiceImpl extends BaseLensService implements QuerySchedulerService {

  /**
   * The constant name for scheduler service.
   */
  public static final String NAME = "scheduler";

  /**
   * Instantiates a new query scheduler service impl.
   *
   * @param cliService the cli service
   */
  public QuerySchedulerServiceImpl(CLIService cliService) {
    super(NAME, cliService);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HealthStatus getHealthStatus() {
    return this.getServiceState().equals(STATE.STARTED)
        ? new HealthStatus(true, "Query scheduler service is healthy.")
        : new HealthStatus(false, "Query scheduler service is down.");
  }
}

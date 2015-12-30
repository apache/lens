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
package org.apache.lens.server.quota;

import org.apache.lens.server.BaseLensService;
import org.apache.lens.server.api.health.HealthStatus;
import org.apache.lens.server.api.quota.QuotaService;

import org.apache.hive.service.cli.CLIService;

/**
 * The Class QuotaServiceImpl.
 */
public class QuotaServiceImpl extends BaseLensService implements QuotaService {

  /**
   * The constant name for quota service.
   */
  public static final String NAME = "quota";

  /**
   * Instantiates a new quota service impl.
   *
   * @param cliService the cli service
   */
  public QuotaServiceImpl(CLIService cliService) {
    super(NAME, cliService);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HealthStatus getHealthStatus() {
    return this.getServiceState().equals(STATE.STARTED)
        ? new HealthStatus(true, "Quota service is healthy.")
        : new HealthStatus(false, "Quota service is down.");
  }
}

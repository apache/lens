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
package org.apache.lens.server.query.retry;

import org.apache.lens.server.api.driver.DriverQueryStatus;
import org.apache.lens.server.api.driver.MockDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.cost.FactPartitionBasedQueryCost;
import org.apache.lens.server.api.query.cost.QueryCost;

import org.apache.hadoop.conf.Configuration;

import lombok.Getter;


public class MockDriverForRetries extends MockDriver {
  private int numRetries;
  @Getter
  private String fullyQualifiedName;

  @Override
  public void configure(Configuration conf, String driverType, String driverName) throws LensException {
    this.conf = conf;
    this.fullyQualifiedName = driverType + "/" + driverName;
    this.conf.addResource(getDriverResourcePath("driver-site.xml"));
    this.numRetries = this.conf.getInt("num.retries", 0);
    loadQueryHook();
    loadRetryPolicyDecider();
  }

  private String getDriverProperty(QueryContext ctx, String name) {
    return ctx.getLensConf().getProperty("driver." + this.getFullyQualifiedName() + "." + name);
  }

  @Override
  public void updateStatus(QueryContext context) throws LensException {
    if (context.getFailedAttempts().size() < numRetries) {
      String errorMessage = getDriverProperty(context, "error.message");
      if (errorMessage == null) {
        errorMessage = "Simulated Failure";
      }
      context.getDriverStatus().setState(DriverQueryStatus.DriverQueryState.FAILED);
      context.getDriverStatus().setErrorMessage(errorMessage);
    } else {
      context.getDriverStatus().setState(DriverQueryStatus.DriverQueryState.SUCCESSFUL);
    }
    context.getDriverStatus().setDriverFinishTime(System.currentTimeMillis());
  }

  @Override
  public void executeAsync(QueryContext context) throws LensException {
    super.executeAsync(context);
    context.getDriverStatus().setDriverStartTime(System.currentTimeMillis());
  }

  @Override
  public QueryCost estimate(AbstractQueryContext qctx) throws LensException {
    String cost = qctx.getLensConf().getProperty("driver." + this.getFullyQualifiedName() + ".cost");
    if (cost == null) {
      throw new LensException("Can't run query");
    }
    return new FactPartitionBasedQueryCost(Double.parseDouble(cost));
  }
}

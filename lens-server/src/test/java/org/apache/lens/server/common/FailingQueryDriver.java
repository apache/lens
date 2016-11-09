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

package org.apache.lens.server.common;

import javax.ws.rs.NotFoundException;

import org.apache.lens.server.api.driver.DriverQueryPlan;
import org.apache.lens.server.api.driver.MockDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.cost.FactPartitionBasedQueryCost;
import org.apache.lens.server.api.query.cost.QueryCost;

public class FailingQueryDriver extends MockDriver {

  @Override
  public QueryCost estimate(final AbstractQueryContext ctx) throws LensException {
    if (ctx.getUserQuery().contains("fail")) {
      return new FactPartitionBasedQueryCost(0.0);
    } else {
      throw new LensException("Simulated Estimate Failure");
    }
  }

  @Override
  public DriverQueryPlan explain(AbstractQueryContext explainCtx) throws LensException {
    if (explainCtx.getUserQuery().contains("runtime")) {
      throw new RuntimeException("Runtime exception from query explain");
    }
    if (explainCtx.getUserQuery().contains("webappexception")) {
      throw new NotFoundException("Not found from mock driver");
    }
    return super.explain(explainCtx);
  }

  @Override
  public void executeAsync(final QueryContext ctx) throws LensException {
    // simulate wait for execution.
    if (ctx.getUserQuery().contains("wait")) {
      try {
        // wait for 1 second.
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // ignore interrupted exception
      }
    }
    // simulate autocancel.
    if (ctx.getUserQuery().contains("autocancel")) {
      return;
    }
    throw new LensException("Simulated Launch Failure");
  }

}

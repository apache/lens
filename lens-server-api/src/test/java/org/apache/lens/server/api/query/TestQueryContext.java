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


import static org.testng.Assert.*;

import java.util.List;

import org.apache.lens.api.LensConf;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.driver.MockDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.retry.BackOffRetryHandler;
import org.apache.lens.server.api.retry.FibonacciExponentialBackOffRetryHandler;

import org.apache.hadoop.conf.Configuration;

import org.testng.annotations.Test;

/**
 * Tests for abstract query context
 */
public class TestQueryContext {

  @Test
  public void testUpdateDriverStatusRetries() throws LensException {
    Configuration conf = new Configuration();
    List<LensDriver> drivers = MockQueryContext.getDrivers(conf);
    MockDriver selectedDriver = (MockDriver)drivers.iterator().next();
    MockQueryContext ctx = new MockQueryContext("simulate status retries", new LensConf(), conf, drivers);
    BackOffRetryHandler waitingHandler = new FibonacciExponentialBackOffRetryHandler(10, 10000, 1000);
    BackOffRetryHandler noWaitingHandler = new FibonacciExponentialBackOffRetryHandler(10, 0, 0);
    // do first update
    ctx.updateDriverStatus(waitingHandler);
    assertEquals(selectedDriver.getUpdateCount(), 1);

    // try another update, update should be skipped
    ctx.updateDriverStatus(waitingHandler);
    assertEquals(selectedDriver.getUpdateCount(), 1);

    // update without delays
    ctx.updateDriverStatus(noWaitingHandler);
    ctx.updateDriverStatus(noWaitingHandler);
    ctx.updateDriverStatus(noWaitingHandler);
    ctx.updateDriverStatus(noWaitingHandler);
    assertEquals(selectedDriver.getUpdateCount(), 5);

    // update with delays, update should be skipped
    ctx.updateDriverStatus(waitingHandler);
    assertEquals(selectedDriver.getUpdateCount(), 5);

    // update succeeds now.
    ctx.updateDriverStatus(noWaitingHandler);
    // all next updates should succeed, as retries should be cleared
    ctx.updateDriverStatus(waitingHandler);
    assertEquals(selectedDriver.getUpdateCount(), 7);
  }

  @Test
  public void testUpdateDriverStatusRetriesExhaust() throws LensException {
    Configuration conf = new Configuration();
    List<LensDriver> drivers = MockQueryContext.getDrivers(conf);
    MockQueryContext ctx = new MockQueryContext("simulate status failure", new LensConf(), conf, drivers);
    BackOffRetryHandler waitingHandler = new FibonacciExponentialBackOffRetryHandler(10, 10000, 1000);
    BackOffRetryHandler noWaitingHandler = new FibonacciExponentialBackOffRetryHandler(10, 0, 0);
    for (int i = 0; i < 18; i++) {
      if (i % 2 == 0) {
        ctx.updateDriverStatus(noWaitingHandler);
      } else {
        ctx.updateDriverStatus(waitingHandler);
      }
    }

    // retries should be exhausted and update should fail.
    try {
      ctx.updateDriverStatus(noWaitingHandler);
      fail("Should throw exception");
    } catch (LensException e) {
      // pass
    }
  }
}

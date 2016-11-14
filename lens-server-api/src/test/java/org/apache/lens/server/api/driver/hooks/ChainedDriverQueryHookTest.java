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
/*
 *
 */
package org.apache.lens.server.api.driver.hooks;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.cost.FactPartitionBasedQueryCost;

import org.apache.hadoop.conf.Configuration;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ChainedDriverQueryHookTest {
  private AbstractQueryContext ctx;
  ChainedDriverQueryHook hook;
  private LensDriver driver;

  @BeforeMethod
  public void setUp() throws Exception {
    driver = mock(LensDriver.class);
    Configuration conf = new Configuration();
    when(driver.getConf()).thenReturn(conf);
    ctx = mock(AbstractQueryContext.class);
    conf.set(UserBasedQueryHook.ALLOWED_USERS, "user1,user2");
    conf.set(QueryCostBasedQueryHook.ALLOWED_RANGE_SETS, "[0, 10)");
    conf.set("chain", UserBasedQueryHook.class.getName() + "," + QueryCostBasedQueryHook.class.getName());
    hook = ChainedDriverQueryHook.from(conf, "chain");
    hook.setDriver(driver);
  }

  @DataProvider
  public Object[][] provideData() {
    return new Object[][]{
      {"user1", 0.0, true}, // allowed by both
      {"user3", 0.0, false}, // disallowed by former
      {"user1", 10.0, false}, // disallowed by latter
      {"user3", 11.0, false}, // disallowed by both
    };
  }

  @Test(dataProvider = "provideData")
  public void testChaining(String user, Double cost, boolean allowed) {
    when(ctx.getDriverQueryCost(driver)).thenReturn(new FactPartitionBasedQueryCost(cost));
    when(ctx.getSubmittedUser()).thenReturn(user);
    try {
      hook.preRewrite(ctx);
      hook.postRewrite(ctx);
      hook.preEstimate(ctx);
      hook.postEstimate(ctx);
      assertTrue(allowed);
    } catch (LensException e) {
      assertFalse(allowed);
    }
  }
}

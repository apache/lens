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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.cost.FactPartitionBasedQueryCost;

import org.apache.hadoop.conf.Configuration;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.RangeSet;


public class QueryCostBasedQueryHookTest {

  private QueryCostBasedQueryHook hook = new QueryCostBasedQueryHook();
  private AbstractQueryContext ctx;
  private LensDriver driver;

  @BeforeMethod
  public void setUp() throws Exception {
    driver = mock(LensDriver.class);
    Configuration conf = new Configuration();
    conf.set(QueryCostBasedQueryHook.ALLOWED_RANGE_SETS, "[10, 100]");
    conf.set(QueryCostBasedQueryHook.DISALLOWED_RANGE_SETS, "[40, 50]");
    when(driver.getConf()).thenReturn(conf);
    ctx = mock(AbstractQueryContext.class);
    hook.setDriver(driver);
  }

  @DataProvider
  public Object[][] provideParsingData() {
    return new Object[][]{
      {"(, 5) U [5, 10) U [100, )", new Double[]{0.0, 1.0, 5.0, 100.0}, new Double[]{10.0, 99.0}},
      {"(, )", new Double[]{0.0, 1.0, 10.0, 100.0, 1000000.0}, new Double[]{}},
      {"(5, 10)", new Double[]{6.0, 7.0, 9.9}, new Double[]{2.0, 5.0, 10.0, 20.0}},
      {"[5, 10]", new Double[]{5.0, 6.0, 7.0, 9.9, 10.0}, new Double[]{2.0, 4.9, 10.1, 20.0}},
    };
  }

  @Test(dataProvider = "provideParsingData")
  public void testParse(String rangeString, Double[] includes, Double[] excludes) {
    RangeSet<FactPartitionBasedQueryCost> range = hook.parseRangeSet(rangeString);
    for (Double cost : includes) {
      assertTrue(range.contains(new FactPartitionBasedQueryCost(cost)));
    }
    for (Double cost : excludes) {
      assertFalse(range.contains(new FactPartitionBasedQueryCost(cost)));
    }
  }

  @DataProvider
  public Object[][] provideData() {
    return new Object[][]{
      {5.0, false},
      {10.0, true},
      {30.0, true},
      {40.0, false},
      {45.0, false},
      {50.0, false},
      {51.0, true},
      {99.0, true},
      {100.0, true},
      {101.0, false},
      {1001.0, false},
    };
  }

  @Test(dataProvider = "provideData")
  public void testHook(Double cost, boolean success)
    throws Exception {
    when(ctx.getDriverQueryCost(driver)).thenReturn(new FactPartitionBasedQueryCost(cost));
    try {
      hook.postEstimate(ctx);
      assertTrue(success);
    } catch (Exception e) {
      assertFalse(success);
    }
  }
}

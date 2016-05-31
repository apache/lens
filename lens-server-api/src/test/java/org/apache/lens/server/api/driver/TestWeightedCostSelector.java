/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.api.driver;

import static java.lang.Math.abs;

import static org.apache.lens.server.api.LensConfConstants.DRIVER_WEIGHT;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lens.api.LensConf;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.MockQueryContext;
import org.apache.lens.server.api.query.cost.FactPartitionBasedQueryCost;

import org.apache.hadoop.conf.Configuration;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * The Class TestWeightedCostSelector.
 */
public class TestWeightedCostSelector {

  private WeightedQueryCostDriverSelector selector = new WeightedQueryCostDriverSelector();
  private Configuration conf = new Configuration();
  private LensConf qconf = new LensConf();
  private FactPartitionBasedQueryCost queryCost = new FactPartitionBasedQueryCost(100.0);
  private int numLoop = 1000;           //Number of loops to run to test driver allocation
  private int errorBuffer = 5;          //Error buffer in percentage (change to double for fractional buffers)


  private class TestDriverAllocation {
    double p1, p2, p3;
    int total;
  }

  private static MockQueryContext createMockContext(String query, Configuration conf, LensConf lensConf,
                                                    Map<LensDriver, String> driverQueries) throws LensException {
    MockQueryContext ctx = new MockQueryContext(query, lensConf, conf, driverQueries.keySet());
    ctx.setDriverQueries(driverQueries);
    ctx.estimateCostForDrivers();
    return ctx;
  }

  private TestDriverAllocation driverLoop(MockQueryContext ctx, MockDriver d1, MockDriver d2, MockDriver d3,
                                          MockDriver d4) {
    int c1 = 0;
    int c2 = 0;
    int c3 = 0;

    for (int i = 0; i < numLoop; i++) {
      LensDriver selectedDriver = selector.select(ctx, conf);
      Assert.assertNotEquals(d4, selectedDriver, "Improper driver allocation. Check WeightedQueryCostDriverSelector.");
      if (d1.equals(selectedDriver)) {
        c1++;
      } else {
        if (d2.equals(selectedDriver)) {
          c2++;
        } else {
          if (d3.equals(selectedDriver)) {
            c3++;
          }
        }
      }
    }

    TestDriverAllocation allocation = new TestDriverAllocation();
    allocation.total = c1 + c2 + c3;
    allocation.p1 = c1 * 100.0 / (allocation.total);
    allocation.p2 = c2 * 100.0 / (allocation.total);
    allocation.p3 = c3 * 100.0 / (allocation.total);

    return allocation;
  }

  private TestDriverAllocation testDriverSelector(double r1, double r2, double r3, double r4) throws LensException {

    List<LensDriver> drivers = new ArrayList<LensDriver>();
    Map<LensDriver, String> driverQueries = new HashMap<LensDriver, String>();

    Configuration conf1 = new Configuration();
    Configuration conf2 = new Configuration();
    Configuration conf3 = new Configuration();
    Configuration conf4 = new Configuration();

    //Creating drivers and setting driver ratios
    MockDriver d1 = new MockDriver();
    d1.configure(conf1, null, null);
    if (r1 > 0) {
      conf1.setDouble(DRIVER_WEIGHT, r1);
    }

    MockDriver d2 = new MockDriver();
    d2.configure(conf2, null, null);
    if (r2 > 0) {
      conf2.setDouble(DRIVER_WEIGHT, r2);
    }

    MockDriver d3 = new MockDriver();
    d3.configure(conf3, null, null);
    if (r3 > 0) {
      conf3.setDouble(DRIVER_WEIGHT, r3);
    }

    MockDriver d4 = new MockDriver();
    d4.configure(conf4, null, null);
    if (r4 > 0) {
      conf4.setDouble(DRIVER_WEIGHT, r4);
    }

    drivers.add(d1);
    drivers.add(d2);
    drivers.add(d3);
    drivers.add(d4);

    String query = "test query";
    driverQueries.put(d1, query);
    driverQueries.put(d2, query);
    driverQueries.put(d3, query);
    driverQueries.put(d4, query);

    MockQueryContext ctx = createMockContext(query, conf, qconf, driverQueries);
    ctx.setDriverCost(d4, queryCost);         //Increasing driver 4's query cost

    return driverLoop(ctx, d1, d2, d3, d4);
  }

  private void assertAllocation(String caseString, TestDriverAllocation allocation, double r1, double r2, double r3) {
    if (allocation.total != numLoop) {
      throw new AssertionError(caseString + ": Incomplete driver allocation. Check"
          + " WeightedQueryCostDriverSelector.");
    }

    if (abs(allocation.p1 - r1) > errorBuffer) {
      throw new AssertionError(caseString + ": Driver 1 not properly allocated. Difference by "
          + abs(allocation.p1 - r1) + "." + " Check WeightedQueryCostDriverSelector");
    }

    if (abs(allocation.p2 - r2) > errorBuffer) {
      throw new AssertionError(caseString + ": Driver 2 not properly allocated. Difference by "
          + abs(allocation.p2 - r2) + "." + " Check WeightedQueryCostDriverSelector");
    }

    if (abs(allocation.p3 - r3) > errorBuffer) {
      throw new AssertionError(caseString + ": Driver 3 not properly allocated. Difference by "
          + abs(allocation.p3 - r3) + "." + " Check WeightedQueryCostDriverSelector");
    }
  }

  //4 DRIVERS WITH THEIR WEIGHTS SET (3 are min cost)
  @Test
  public void testCustomWeights() throws LensException {
    double r1 = 30;
    double r2 = 20;
    double r3 = 50;
    double r4 = 100; //Insignificant driver weight due to increased driver cost

    TestDriverAllocation allocation = testDriverSelector(r1, r2, r3, r4);

    assertAllocation("TEST CustomWeights", allocation, r1, r2, r3);
  }


  //4 DRIVERS WITHOUT WEIGHTS SET. (3 are min cost)
  @Test
  public void testNoWeights() throws LensException {

    TestDriverAllocation allocation = testDriverSelector(-1, -1, -1, -1);

    assertAllocation("TEST NoWeights", allocation, 33.33, 33.33, 33.33);
  }

  //2 DRIVERS WITH DIFFERENT WEIGHTS
  @Test
  public void testDifferentWeights() throws LensException {

    int r1 = 10;
    int r2 = 90;

    List<LensDriver> drivers = new ArrayList<LensDriver>();
    Map<LensDriver, String> driverQueries = new HashMap<LensDriver, String>();

    Configuration conf1 = new Configuration();
    Configuration conf2 = new Configuration();

    //Creating drivers and setting driver ratios
    MockDriver d1 = new MockDriver();
    conf1.setDouble(DRIVER_WEIGHT, r1);
    d1.configure(conf1, null, null);

    MockDriver d2 = new MockDriver();
    conf2.setDouble(DRIVER_WEIGHT, r2);
    d2.configure(conf2, null, null);

    drivers.add(d1);
    drivers.add(d2);
    String query = "test query";
    driverQueries.put(d1, query);
    driverQueries.put(d2, query);

    MockQueryContext ctx = createMockContext(query, conf, qconf, driverQueries);

    ctx.setDriverCost(d2, queryCost);

    LensDriver selected = selector.select(ctx, conf);

    Assert.assertEquals(selected, d1, "TEST Different Weights: Improper driver allocation. Check "
        + "WeightedQueryCostDriverSelector.");
  }

  //DEFAULT MIN COST SELECTOR BEHAVIOR
  @Test
  public void testDefaultMinCost() throws LensException {
    List<LensDriver> drivers = new ArrayList<LensDriver>();
    Map<LensDriver, String> driverQueries = new HashMap<LensDriver, String>();

    MockDriver d1 = new MockDriver();
    d1.configure(conf, null, null);
    MockDriver d2 = new MockDriver();
    d2.configure(conf, null, null);
    MockFailDriver fd1 = new MockFailDriver();
    fd1.configure(conf, null, null);
    MockFailDriver fd2 = new MockFailDriver();
    fd2.configure(conf, null, null);

    String message = "TEST DefaultMinCost: Check WeightedQueryCostDriverSelector";

    drivers.add(d1);
    drivers.add(d2);
    String query = "test query";
    driverQueries.put(d1, query);

    MockQueryContext ctx = createMockContext(query, conf, qconf, driverQueries);
    LensDriver selected = selector.select(ctx, conf);

    Assert.assertEquals(d1, selected, message);
    driverQueries.put(d2, query);
    driverQueries.remove(d1);
    ctx = createMockContext(query, conf, qconf, driverQueries);

    selected = selector.select(ctx, conf);
    Assert.assertEquals(d2, selected, message);

    drivers.add(fd1);
    driverQueries.put(fd1, query);

    ctx = createMockContext(query, conf, qconf, driverQueries);
    selected = selector.select(ctx, conf);
    Assert.assertEquals(d2, selected, message);

    drivers.add(fd2);
    driverQueries.put(fd2, query);
    ctx = createMockContext(query, conf, qconf, driverQueries);

    selected = selector.select(ctx, conf);
    Assert.assertEquals(d2, selected, message);

    drivers.clear();
    driverQueries.clear();
    drivers.add(d1);
    drivers.add(fd1);
    driverQueries.put(d1, query);
    ctx = createMockContext(query, conf, qconf, driverQueries);
    selected = selector.select(ctx, conf);
    Assert.assertEquals(d1, selected, message);
  }
}

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
package org.apache.lens.driver.cube;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lens.api.LensConf;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.driver.MinQueryCostSelector;
import org.apache.lens.server.api.driver.MockDriver;
import org.apache.lens.server.api.driver.MockFailDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.MockQueryContext;

import org.apache.hadoop.conf.Configuration;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * The Class TestMinCostSelector.
 */
public class TestMinCostSelector {

  private MockQueryContext createMockContext(String query, Configuration conf, LensConf lensConf,
    Map<LensDriver, String> driverQueries) throws LensException {
    MockQueryContext ctx = new MockQueryContext(query, lensConf, conf, driverQueries.keySet());
    ctx.setDriverQueries(driverQueries);
    ctx.estimateCostForDrivers();
    return ctx;
  }

  private MockQueryContext createMockContext(String query, Configuration conf, LensConf lensConf,
    List<LensDriver> drivers, Map<LensDriver, String> driverQueries) throws LensException {
    MockQueryContext ctx = new MockQueryContext(query, lensConf, conf, driverQueries.keySet());
    ctx.setDriverQueries(driverQueries);
    ctx.estimateCostForDrivers();
    return ctx;
  }

  @Test
  public void testMinCostSelector() throws LensException {
    MinQueryCostSelector selector = new MinQueryCostSelector();
    List<LensDriver> drivers = new ArrayList<LensDriver>();
    Map<LensDriver, String> driverQueries = new HashMap<LensDriver, String>();
    Configuration conf = new Configuration();
    LensConf qconf = new LensConf();

    MockDriver d1 = new MockDriver();
    d1.configure(conf, null, null);
    MockDriver d2 = new MockDriver();
    d2.configure(conf, null, null);
    MockFailDriver fd1 = new MockFailDriver();
    fd1.configure(conf, null, null);
    MockFailDriver fd2 = new MockFailDriver();
    fd2.configure(conf, null, null);

    drivers.add(d1);
    drivers.add(d2);
    String query = "test query";
    driverQueries.put(d1, query);

    MockQueryContext ctx = createMockContext(query, conf, qconf, driverQueries);
    LensDriver selected = selector.select(ctx, conf);

    Assert.assertEquals(d1, selected);
    driverQueries.put(d2, query);
    driverQueries.remove(d1);
    ctx = createMockContext(query, conf, qconf, driverQueries);

    selected = selector.select(ctx, conf);
    Assert.assertEquals(d2, selected);

    drivers.add(fd1);
    driverQueries.put(fd1, query);

    ctx = createMockContext(query, conf, qconf, driverQueries);
    selected = selector.select(ctx, conf);
    Assert.assertEquals(d2, selected);

    drivers.add(fd2);
    driverQueries.put(fd2, query);
    ctx = createMockContext(query, conf, qconf, driverQueries);

    selected = selector.select(ctx, conf);
    Assert.assertEquals(d2, selected);

    drivers.clear();
    driverQueries.clear();
    drivers.add(d1);
    drivers.add(fd1);
    driverQueries.put(d1, query);
    ctx = createMockContext(query, conf, qconf, drivers, driverQueries);
    selected = selector.select(ctx, conf);
    Assert.assertEquals(d1, selected);
  }
}

package com.inmobi.grill.driver.cube;

/*
 * #%L
 * Grill Cube Driver
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.driver.cube.CubeGrillDriver.MinQueryCostSelector;
import com.inmobi.grill.server.api.driver.DriverQueryPlan;
import com.inmobi.grill.server.api.driver.GrillDriver;

public class TestMinCostSelector {

  static class MockFailDriver extends MockDriver {
    public DriverQueryPlan explain(String query, Configuration conf)
        throws GrillException {
      throw new GrillException("failing!");
    }
  }

  @Test
  public void testMinCostSelector() {
    MinQueryCostSelector selector = new MinQueryCostSelector();
    List<GrillDriver> drivers = new ArrayList<GrillDriver>();
    Map<GrillDriver, String> driverQueries = new HashMap<GrillDriver, String>();
    Configuration conf = new Configuration();

    MockDriver d1 = new MockDriver();
    MockDriver d2 = new MockDriver();
    MockFailDriver fd1 = new MockFailDriver();
    MockFailDriver fd2 = new MockFailDriver();
    
    drivers.add(d1);
    drivers.add(d2);
    driverQueries.put(d1, "test query");
    GrillDriver selected = selector.select(drivers, driverQueries, conf);
    Assert.assertEquals(d1, selected);
    driverQueries.put(d2, "test query");
    driverQueries.remove(d1);
    selected = selector.select(drivers, driverQueries, conf);
    Assert.assertEquals(d2, selected);

    drivers.add(fd1);
    driverQueries.put(fd1, "test query");
    selected = selector.select(drivers, driverQueries, conf);
    Assert.assertEquals(d2, selected);

    drivers.add(fd2);
    driverQueries.put(fd2, "test query");
    selected = selector.select(drivers, driverQueries, conf);
    Assert.assertEquals(d2, selected);
  }
}

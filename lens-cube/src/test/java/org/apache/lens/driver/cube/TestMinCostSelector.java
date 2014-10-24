package org.apache.lens.driver.cube;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.lens.api.LensException;
import org.apache.lens.driver.cube.CubeDriver.MinQueryCostSelector;
import org.apache.lens.server.api.driver.DriverQueryPlan;
import org.apache.lens.server.api.driver.LensDriver;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * The Class TestMinCostSelector.
 */
public class TestMinCostSelector {

  /**
   * The Class MockFailDriver.
   */
  static class MockFailDriver extends MockDriver {

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.lens.driver.cube.MockDriver#explain(java.lang.String, org.apache.hadoop.conf.Configuration)
     */
    public DriverQueryPlan explain(String query, Configuration conf) throws LensException {
      throw new LensException("failing!");
    }
  }

  /**
   * Test min cost selector.
   */
  @Test
  public void testMinCostSelector() {
    MinQueryCostSelector selector = new MinQueryCostSelector();
    List<LensDriver> drivers = new ArrayList<LensDriver>();
    Map<LensDriver, String> driverQueries = new HashMap<LensDriver, String>();
    Configuration conf = new Configuration();

    MockDriver d1 = new MockDriver();
    MockDriver d2 = new MockDriver();
    MockFailDriver fd1 = new MockFailDriver();
    MockFailDriver fd2 = new MockFailDriver();

    drivers.add(d1);
    drivers.add(d2);
    driverQueries.put(d1, "test query");
    LensDriver selected = selector.select(drivers, driverQueries, conf);
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

package com.inmobi.grill.driver.cube;

import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.server.api.driver.DriverQueryPlan;
import com.inmobi.grill.server.api.driver.GrillResultSet;

public class TestCubeDriver {

  Configuration conf = new Configuration();
  CubeGrillDriver cubeDriver;

  @BeforeTest
  public void beforeTest() throws Exception {
    cubeDriver = new CubeGrillDriver(conf);
    conf.setInt("mock.driver.test.val", 5);
  }

  @AfterTest
  public void afterTest() throws Exception {
  }

  @Test
  public void testCubeDriver() throws Exception {
    String addQ = "add jar xyz.jar";
    GrillResultSet result = cubeDriver.execute(addQ, conf);
    Assert.assertNotNull(result);
    Assert.assertEquals(((MockDriver)cubeDriver.getDrivers().get(0)).query, addQ);

    String setQ = "set xyz=random";
    result = cubeDriver.execute(setQ, conf);
    Assert.assertNotNull(result);
    Assert.assertEquals(((MockDriver)cubeDriver.getDrivers().get(0)).query, setQ);

    String query = "select name from table";
    DriverQueryPlan plan = cubeDriver.explain(query, conf);
    String planString = plan.getPlan();
    Assert.assertEquals(query, planString);

    // execute async from handle
    cubeDriver.executePrepareAsync(plan.getHandle(), conf);
    Assert.assertEquals(cubeDriver.getStatus(plan.getHandle()).getStatus(),
        QueryStatus.Status.SUCCESSFUL); 
    Assert.assertFalse(cubeDriver.cancelQuery(plan.getHandle()));

    // execute sync from handle
    result = cubeDriver.executePrepare(plan.getHandle(), conf);
    Assert.assertNotNull(result);
    Assert.assertNotNull(result.getMetadata());
    Assert.assertEquals(cubeDriver.getStatus(plan.getHandle()).getStatus(),
        QueryStatus.Status.SUCCESSFUL); 

    cubeDriver.closeQuery(plan.getHandle());

    // getStatus on closed query
    Throwable th = null;
    try {
      cubeDriver.getStatus(plan.getHandle());
    } catch (GrillException e) {
      th = e;
    }
    Assert.assertNotNull(th);

    result = cubeDriver.execute(query, conf);
    Assert.assertNotNull(result);
    Assert.assertNotNull(result.getMetadata());

    QueryHandle handle = cubeDriver.executeAsync(query, conf);
    Assert.assertEquals(cubeDriver.getStatus(handle).getStatus(),
        QueryStatus.Status.SUCCESSFUL); 
    Assert.assertFalse(cubeDriver.cancelQuery(handle));

    cubeDriver.closeQuery(handle);

    th = null;
    try {
      cubeDriver.getStatus(handle);
    } catch (GrillException e) {
      th = e;
    }
    Assert.assertNotNull(th);
  }
}

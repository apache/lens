package com.inmobi.grill.driver.cube;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.grill.api.GrillResultSet;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryPlan;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.exception.GrillException;

public class TestCubeDriver {

  Configuration conf = new Configuration();
  CubeGrillDriver cubeDriver;

  @BeforeTest
  public void beforeTest() throws Exception {
    conf.set(CubeGrillDriver.ENGINE_DRIVER_CLASSES,
        MockDriver.class.getCanonicalName());
    cubeDriver = new CubeGrillDriver(conf);
  }

  @AfterTest
  public void afterTest() throws Exception {
  }

  @Test
  public void testCubeDriver() throws GrillException {
    String query = "select name from table";
    QueryPlan plan = cubeDriver.explain(query, conf);
    String planString = plan.getPlan();
    Assert.assertEquals(query, planString);

    // execute async from handle
    cubeDriver.executePrepareAsync(plan.getHandle(), conf);
    Assert.assertEquals(cubeDriver.getStatus(plan.getHandle()).getStatus(),
        QueryStatus.Status.SUCCESSFUL); 
    Assert.assertFalse(cubeDriver.cancelQuery(plan.getHandle()));

    // execute sync from handle
    GrillResultSet result = cubeDriver.executePrepare(plan.getHandle(), conf);
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

  @Test
  public void testCubeQuery() throws ParseException {
    String q1 = "select name from table";
    Assert.assertFalse(cubeDriver.isCubeQuery(q1));

    String q2 = "cube select name from table";
    Assert.assertTrue(cubeDriver.isCubeQuery(q2));

    q2 = "select * from (cube select name from table) a";
    Assert.assertTrue(cubeDriver.isCubeQuery(q2));

    q2 = "select * from (cube select name from table) a join (cube select name2 from table2) b";
    Assert.assertTrue(cubeDriver.isCubeQuery(q2));

    q2 = "select * from (cube select name from table) a join (select name2 from table2) b";
    Assert.assertTrue(cubeDriver.isCubeQuery(q2));

    q2 = "select * from (cube select name from table union all cube select name2 from table2) u";
    Assert.assertTrue(cubeDriver.isCubeQuery(q2));

    q2 = "select u.* from (select name from table union all cube select name2 from table2) u";
    Assert.assertTrue(cubeDriver.isCubeQuery(q2));
  }
}

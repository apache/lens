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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.lens.api.LensException;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.driver.cube.CubeDriver;
import org.apache.lens.server.api.driver.DriverQueryPlan;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.driver.LensResultSet;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * The Class TestCubeDriver.
 */
public class TestCubeDriver {

  /** The conf. */
  Configuration conf = new Configuration();

  /** The cube driver. */
  CubeDriver cubeDriver;

  /**
   * Before test.
   *
   * @throws Exception
   *           the exception
   */
  @BeforeTest
  public void beforeTest() throws Exception {
    cubeDriver = new CubeDriver(conf);
    conf.setInt("mock.driver.test.val", 5);
  }

  /**
   * After test.
   *
   * @throws Exception
   *           the exception
   */
  @AfterTest
  public void afterTest() throws Exception {
  }

  /**
   * Test cube driver.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testCubeDriver() throws Exception {
    String addQ = "add jar xyz.jar";
    LensResultSet result = cubeDriver.execute(addQ, conf);
    Assert.assertNotNull(result);
    Assert.assertEquals(((MockDriver) cubeDriver.getDrivers().get(0)).query, addQ);

    String setQ = "set xyz=random";
    result = cubeDriver.execute(setQ, conf);
    Assert.assertNotNull(result);
    Assert.assertEquals(((MockDriver) cubeDriver.getDrivers().get(0)).query, setQ);

    String query = "select name from table";
    DriverQueryPlan plan = cubeDriver.explain(query, conf);
    String planString = plan.getPlan();
    Assert.assertEquals(query, planString);

    result = cubeDriver.execute(query, conf);
    Assert.assertNotNull(result);
    Assert.assertNotNull(result.getMetadata());

    QueryHandle handle = cubeDriver.executeAsync(query, conf);
    Assert.assertEquals(cubeDriver.getStatus(handle).getStatus(), QueryStatus.Status.SUCCESSFUL);
    Assert.assertFalse(cubeDriver.cancelQuery(handle));

    cubeDriver.closeQuery(handle);

    Throwable th = null;
    try {
      cubeDriver.getStatus(handle);
    } catch (LensException e) {
      th = e;
    }
    Assert.assertNotNull(th);
  }

  /**
   * Test cube driver read write.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testCubeDriverReadWrite() throws Exception {
    // Test read/write for cube driver
    CubeDriver cubeDriver = new CubeDriver(conf);
    ByteArrayOutputStream driverOut = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(driverOut);
    cubeDriver.writeExternal(out);
    out.close();
    System.out.println(Arrays.toString(driverOut.toByteArray()));

    ByteArrayInputStream driverIn = new ByteArrayInputStream(driverOut.toByteArray());
    conf.setInt("mock.driver.test.val", -1);
    CubeDriver newDriver = new CubeDriver(conf);
    newDriver.readExternal(new ObjectInputStream(driverIn));
    driverIn.close();
    Assert.assertEquals(newDriver.getDrivers().size(), cubeDriver.getDrivers().size());

    for (LensDriver driver : newDriver.getDrivers()) {
      if (driver instanceof MockDriver) {
        MockDriver md = (MockDriver) driver;
        Assert.assertEquals(md.getTestIOVal(), 5);
      }
    }
  }

  /**
   * Test cube driver restart.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testCubeDriverRestart() throws Exception {
    // Test read/write for cube driver
    CubeDriver cubeDriver = new CubeDriver(conf);
    String query = "select name from table";
    QueryHandle handle = cubeDriver.executeAsync(query, conf);

    ByteArrayOutputStream driverOut = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(driverOut);
    cubeDriver.writeExternal(out);
    out.close();
    System.out.println(Arrays.toString(driverOut.toByteArray()));

    ByteArrayInputStream driverIn = new ByteArrayInputStream(driverOut.toByteArray());
    CubeDriver newDriver = new CubeDriver(conf);
    newDriver.readExternal(new ObjectInputStream(driverIn));
    driverIn.close();
    Assert.assertEquals(newDriver.getDrivers().size(), cubeDriver.getDrivers().size());

    Assert.assertEquals(cubeDriver.getStatus(handle).getStatus(), QueryStatus.Status.SUCCESSFUL);
  }

}

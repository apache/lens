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
package org.apache.lens.server;

import org.apache.lens.server.LensApplication;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;

/**
 * The Class TestLensApplication.
 */
@Test(alwaysRun = true, groups = "unit-test")
public class TestLensApplication extends LensJerseyTest {

  /*
   * (non-Javadoc)
   * 
   * @see org.glassfish.jersey.test.JerseyTest#configure()
   */
  @Override
  protected Application configure() {
    return new LensApplication();
  }

  /**
   * Setup.
   *
   * @throws Exception
   *           the exception
   */
  @BeforeTest
  public void setup() throws Exception {
    super.setUp();
  }

  /**
   * Test ws resources loaded.
   *
   * @throws InterruptedException
   *           the interrupted exception
   */
  @Test
  public void testWSResourcesLoaded() throws InterruptedException {
    final WebTarget target = target().path("test");
    final Response response = target.request().get();
    Assert.assertEquals(response.getStatus(), 200);
    Assert.assertEquals(response.readEntity(String.class), "OK");
  }

  @Override
  protected int getTestPort() {
    return 19998;
  }
}

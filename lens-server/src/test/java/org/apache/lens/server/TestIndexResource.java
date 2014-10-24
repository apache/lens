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

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;

import org.apache.lens.server.IndexApp;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * The Class TestIndexResource.
 */
@Test(groups = "unit-test")
public class TestIndexResource extends LensJerseyTest {

  /*
   * (non-Javadoc)
   * 
   * @see org.glassfish.jersey.test.JerseyTest#setUp()
   */
  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.glassfish.jersey.test.JerseyTest#tearDown()
   */
  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.glassfish.jersey.test.JerseyTest#configure()
   */
  @Override
  protected Application configure() {
    return new IndexApp();
  }

  protected int getTestPort() {
    return 8081;
  }

  /**
   * Test client string response.
   */
  @Test
  public void testClientStringResponse() {
    WebTarget target = target().path("index");
    System.out.println("target path:" + target.getUri());
    String s = target().path("index").request().get(String.class);
    Assert.assertEquals("Hello World! from lens", s);
  }
}

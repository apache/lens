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

import static org.testng.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

/**
 * Tests log resource
 */
@Test(groups = "unit-test")
@Slf4j
public class TestLogResource extends LensJerseyTest {

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
    enable(TestProperties.LOG_TRAFFIC);
    enable(TestProperties.DUMP_ENTITY);

    return new ResourceConfig(LogResource.class);
  }

  /**
   * Test response up.
   */
  @Test
  public void testResponseUp() {
    log.info("Testing log resource");
    WebTarget target = target().path("logs");
    String s = target.request().get(String.class);
    assertEquals("Logs resource is up!", s);
  }

  /**
   * Test not found
   */
  @Test
  public void testRandomResponse() {
    log.info("Testing random log");
    WebTarget target = target().path("logs");
    Response response = target.path("random").request().get();
    assertEquals(response.getStatus(), 404);
    final String expectedErrMsg = "Log file for 'random' not found";
    assertEquals(response.readEntity(String.class), expectedErrMsg);
  }

  /**
   * Test log file
   *
   * @throws IOException
   */
  @Test
  public void testLogFile() throws IOException {
    log.info("Testing logfile");
    WebTarget target = target().path("logs");
    Response response = target.path("logresource").request().get();
    assertEquals(response.getStatus(), 200);

    InputStream in = (InputStream) response.getEntity();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      IOUtils.copyBytes(in, bos, new Configuration());
    } finally {
      bos.close();
      in.close();
    }

    String logs = new String(bos.toByteArray());
    assertTrue(logs.contains("Testing logfile"));
  }

}

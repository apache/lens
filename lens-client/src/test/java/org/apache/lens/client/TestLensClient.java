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
package org.apache.lens.client;

import java.net.URI;
import java.util.List;

import javax.ws.rs.core.UriBuilder;

import org.apache.lens.server.LensAllApplicationJerseyTest;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Test(groups="unit-test")
public class TestLensClient extends LensAllApplicationJerseyTest {

  @Override
  protected int getTestPort() {
    return 9999;
  }

  @Override
  protected URI getBaseUri() {
    return UriBuilder.fromUri("http://localhost:9999/lensapi").build();
  }

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }

  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testClient() throws Exception {
    LensClient client = new LensClient();
    Assert.assertEquals(client.getCurrentDatabae(), "default",
        "current database");
    List<String> dbs = client.getAllDatabases();
    Assert.assertEquals(dbs.size(), 1, "no of databases");
    client.createDatabase("testclientdb", true);
    Assert.assertEquals(client.getAllDatabases().size(), 2, " no of databases");
    client.dropDatabase("testclientdb");
    Assert.assertEquals(client.getAllDatabases().size(), 1, " no of databases");
  }
}

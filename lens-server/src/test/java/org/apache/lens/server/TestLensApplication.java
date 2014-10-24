/*
 * #%L
 * Lens Server
 * %%
 * Copyright (C) 2014 Apache Software Foundation
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
package org.apache.lens.server;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.server.LensApplication;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
@Test(alwaysRun=true, groups="unit-test")
public class TestLensApplication extends LensJerseyTest {

  @Override
  protected Application configure() {
    return new LensApplication();
  }

  @BeforeTest
  public void setup() throws Exception {
    super.setUp();
  }

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

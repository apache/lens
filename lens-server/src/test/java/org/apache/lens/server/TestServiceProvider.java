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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.ServiceProvider;
import org.apache.lens.server.api.ServiceProviderFactory;
import org.apache.lens.server.api.events.LensEventService;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * The Class TestServiceProvider.
 */
@Test(groups = "unit-test")
public class TestServiceProvider extends LensAllApplicationJerseyTest {

  /**
   * Test service provider.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testServiceProvider() throws Exception {
    HiveConf conf = LensServerConf.get();
    Class<? extends ServiceProviderFactory> spfClass = conf.getClass(LensConfConstants.SERVICE_PROVIDER_FACTORY, null,
        ServiceProviderFactory.class);

    ServiceProviderFactory spf = spfClass.newInstance();

    ServiceProvider serviceProvider = spf.getServiceProvider();
    Assert.assertNotNull(serviceProvider);
    Assert.assertTrue(serviceProvider instanceof LensServices);

    QueryExecutionService querySvc = (QueryExecutionService) serviceProvider.getService(QueryExecutionService.NAME);
    Assert.assertNotNull(querySvc);

    LensEventService eventSvc = (LensEventService) serviceProvider.getService(LensEventService.NAME);
    Assert.assertNotNull(eventSvc);
  }

  @Override
  protected int getTestPort() {
    return 12121;
  }
}

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
package org.apache.lens.ml;

import javax.ws.rs.core.Application;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.Service;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.ServiceProvider;
import org.apache.lens.server.api.ServiceProviderFactory;
import org.apache.lens.server.ml.MLApp;
import org.apache.lens.server.ml.MLService;
import org.apache.lens.server.ml.MLServiceImpl;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestMLResource extends LensJerseyTest {

  @Override
  protected int getTestPort() {
    return 12345;
  }

  @Override
  protected Application configure() {
    return new MLApp();
  }

  @Override
  protected void configureClient(ClientConfig config) {
    config.register(MultiPartFeature.class);
  }

  private ServiceProvider getServiceProvider() throws Exception {
    HiveConf conf = LensServerConf.get();
    Class<? extends ServiceProviderFactory> spfClass = conf.getClass(LensConfConstants.SERVICE_PROVIDER_FACTORY, null,
        ServiceProviderFactory.class);
    ServiceProviderFactory spf = spfClass.newInstance();
    return spf.getServiceProvider();
  }

  @Test
  public void testStartMLService() throws Exception {
    ServiceProvider serviceProvider = getServiceProvider();
    MLServiceImpl svcImpl = serviceProvider.getService(MLService.NAME);
    Assert.assertEquals(svcImpl.getServiceState(), Service.STATE.STARTED);
  }

}

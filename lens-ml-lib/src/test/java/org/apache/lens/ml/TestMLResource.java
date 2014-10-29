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

import java.util.List;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.Service;
import org.apache.lens.api.StringList;
import org.apache.lens.ml.spark.trainers.DecisionTreeTrainer;
import org.apache.lens.ml.spark.trainers.LogisticRegressionTrainer;
import org.apache.lens.ml.spark.trainers.NaiveBayesTrainer;
import org.apache.lens.ml.spark.trainers.SVMTrainer;
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
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Test
public class TestMLResource extends LensJerseyTest {
  private static final Log LOG = LogFactory.getLog(TestMLResource.class);
  private WebTarget mlTarget;

  public static ServiceProvider getServiceProvider() throws Exception {
    HiveConf conf = LensServerConf.get();
    Class<? extends ServiceProviderFactory> spfClass = conf.getClass(LensConfConstants.SERVICE_PROVIDER_FACTORY, null,
        ServiceProviderFactory.class);
    ServiceProviderFactory spf = spfClass.newInstance();
    return spf.getServiceProvider();
  }

  @Override
  protected int getTestPort() {
    return 8089;
  }

  @Override
  protected Application configure() {
    return new MLApp();
  }

  @Override
  protected void configureClient(ClientConfig config) {
    config.register(MultiPartFeature.class);
  }

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }

  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @BeforeMethod
  public void setMLTarget() {
    mlTarget = target().path("ml");
    LOG.info("## setMLTarget");
  }

  @Test
  public void testStartMLServiceStarted() throws Exception {
    LOG.info("## testStartMLServiceStarted");
    ServiceProvider serviceProvider = getServiceProvider();
    MLServiceImpl svcImpl = serviceProvider.getService(MLService.NAME);
    Assert.assertEquals(svcImpl.getServiceState(), Service.STATE.STARTED);
  }

  @Test
  public void testGetTrainers() throws Exception {
    final WebTarget trainerTarget = mlTarget.path("trainers");
    LOG.info("## testGetTrainers: " + trainerTarget.getUri());
    StringList trainerList = trainerTarget.request(MediaType.APPLICATION_XML).get(StringList.class);
    Assert.assertNotNull(trainerList);

    List<String> trainerNames = trainerList.getElements();
    Assert.assertNotNull(trainerNames);

    Assert.assertTrue(trainerNames.contains(MLUtils.getTrainerName(NaiveBayesTrainer.class)),
        MLUtils.getTrainerName(NaiveBayesTrainer.class));

    Assert.assertTrue(trainerNames.contains(MLUtils.getTrainerName(SVMTrainer.class)),
        MLUtils.getTrainerName(SVMTrainer.class));

    Assert.assertTrue(trainerNames.contains(MLUtils.getTrainerName(LogisticRegressionTrainer.class)),
        MLUtils.getTrainerName(LogisticRegressionTrainer.class));

    Assert.assertTrue(trainerNames.contains(MLUtils.getTrainerName(DecisionTreeTrainer.class)),
        MLUtils.getTrainerName(DecisionTreeTrainer.class));
  }

}

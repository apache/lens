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

import java.io.File;
import java.net.URI;
import java.util.*;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.client.LensConnectionParams;
import org.apache.lens.client.LensMLClient;
import org.apache.lens.ml.spark.algos.DecisionTreeAlgo;
import org.apache.lens.ml.spark.algos.LogisticRegressionAlgo;
import org.apache.lens.ml.spark.algos.NaiveBayesAlgo;
import org.apache.lens.ml.spark.algos.SVMAlgo;
import org.apache.lens.ml.task.MLTask;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.ServiceProvider;
import org.apache.lens.server.api.ServiceProviderFactory;
import org.apache.lens.server.api.session.SessionService;
import org.apache.lens.server.ml.MLApp;
import org.apache.lens.server.ml.MLService;
import org.apache.lens.server.ml.MLServiceImpl;
import org.apache.lens.server.ml.MLServiceResource;
import org.apache.lens.server.query.QueryServiceResource;
import org.apache.lens.server.session.HiveSessionService;
import org.apache.lens.server.session.SessionResource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import org.apache.hive.service.Service;

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
  private static final String TEST_CONN_URL = "http://localhost:8089/lens-server";
  private static final LensConnectionParams LENS_CONNECTION_PARAMS = new LensConnectionParams();

  static {
    LENS_CONNECTION_PARAMS.setBaseUrl(TEST_CONN_URL);
    LENS_CONNECTION_PARAMS.getConf().setUser("foo@localhost");
  }

  private WebTarget mlTarget;
  private LensMLClient mlClient;
  private ServiceProvider serviceProvider;
  private LensSessionHandle sessionHandle;

  public void setServiceProvider() throws Exception {
    HiveConf conf = LensServerConf.get();
    Class<? extends ServiceProviderFactory> spfClass = conf.getClass(LensConfConstants.SERVICE_PROVIDER_FACTORY, null,
      ServiceProviderFactory.class);
    ServiceProviderFactory spf = spfClass.newInstance();
    this.serviceProvider = spf.getServiceProvider();
  }

  @Override
  protected int getTestPort() {
    return 8089;
  }

  @Override
  protected Application configure() {
    return new MLApp(SessionResource.class, QueryServiceResource.class);
  }

  @Override
  protected void configureClient(ClientConfig config) {
    config.register(MultiPartFeature.class);
  }

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
    setServiceProvider();
    HiveSessionService sessionService = serviceProvider.getService(SessionService.NAME);
    this.sessionHandle = sessionService.openSession("foo@localhost", "bar", new HashMap<String, String>());
    mlClient = new LensMLClient(LENS_CONNECTION_PARAMS, sessionHandle);
  }

  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
    mlClient.close();
  }

  @BeforeMethod
  public void setMLTarget() {
    mlTarget = target().path("ml");
  }

  @Test
  public void testStartMLServiceStarted() throws Exception {
    LOG.info("## testStartMLServiceStarted");
    MLServiceImpl svcImpl = serviceProvider.getService(MLService.NAME);
    Assert.assertEquals(svcImpl.getServiceState(), Service.STATE.STARTED);
  }

  @Test
  public void testMLResourceUp() throws Exception {
    String mlUpMsg = mlTarget.request().get(String.class);
    Assert.assertEquals(mlUpMsg, MLServiceResource.ML_UP_MESSAGE);
  }

  @Test
  public void testGetAlgos() throws Exception {
    List<String> algoNames = mlClient.getAlgorithms();
    Assert.assertNotNull(algoNames);

    Assert.assertTrue(algoNames.contains(MLUtils.getAlgoName(NaiveBayesAlgo.class)),
      MLUtils.getAlgoName(NaiveBayesAlgo.class));

    Assert.assertTrue(algoNames.contains(MLUtils.getAlgoName(SVMAlgo.class)),
      MLUtils.getAlgoName(SVMAlgo.class));

    Assert.assertTrue(algoNames.contains(MLUtils.getAlgoName(LogisticRegressionAlgo.class)),
      MLUtils.getAlgoName(LogisticRegressionAlgo.class));

    Assert.assertTrue(algoNames.contains(MLUtils.getAlgoName(DecisionTreeAlgo.class)),
      MLUtils.getAlgoName(DecisionTreeAlgo.class));
  }

  @Test
  public void testGetAlgoParams() throws Exception {
    Map<String, String> params = mlClient.getAlgoParamDescription(MLUtils.getAlgoName(DecisionTreeAlgo.class));
    Assert.assertNotNull(params);
    Assert.assertFalse(params.isEmpty());

    for (String key : params.keySet()) {
      LOG.info("## Param " + key + " help = " + params.get(key));
    }
  }

  @Test
  public void trainAndEval() throws Exception {
    LOG.info("Starting train & eval");
    final String algoName = MLUtils.getAlgoName(NaiveBayesAlgo.class);
    HiveConf conf = new HiveConf();
    String database = "default";
    String tableName = "naivebayes_training_table";
    String sampleDataFilePath = "data/naive_bayes/naive_bayes_train.data";

    File sampleDataFile = new File(sampleDataFilePath);
    URI sampleDataFileURI = sampleDataFile.toURI();

    String labelColumn = "label";
    String[] features = {"feature_1", "feature_2", "feature_3"};
    String outputTable = "naivebayes_eval_table";

    LOG.info("Creating training table from file " + sampleDataFileURI.toString());

    Map<String, String> tableParams = new HashMap<String, String>();
    try {
      ExampleUtils.createTable(conf, database, tableName, sampleDataFileURI.toString(), labelColumn, tableParams,
        features);
    } catch (HiveException exc) {
      exc.printStackTrace();
    }
    MLTask.Builder taskBuilder = new MLTask.Builder();

    taskBuilder.algorithm(algoName).hiveConf(conf).labelColumn(labelColumn).outputTable(outputTable)
      .serverLocation(getBaseUri().toString()).sessionHandle(mlClient.getSessionHandle()).trainingTable(tableName)
      .userName("foo@localhost").password("bar");

    // Add features
    taskBuilder.addFeatureColumn("feature_1").addFeatureColumn("feature_2").addFeatureColumn("feature_3");

    MLTask task = taskBuilder.build();

    LOG.info("Created task " + task.toString());
    task.run();
    Assert.assertEquals(task.getTaskState(), MLTask.State.SUCCESSFUL);

    String firstModelID = task.getModelID();
    String firstReportID = task.getReportID();
    Assert.assertNotNull(firstReportID);
    Assert.assertNotNull(firstModelID);

    taskBuilder = new MLTask.Builder();
    taskBuilder.algorithm(algoName).hiveConf(conf).labelColumn(labelColumn).outputTable(outputTable)
      .serverLocation(getBaseUri().toString()).sessionHandle(mlClient.getSessionHandle()).trainingTable(tableName)
      .userName("foo@localhost").password("bar");
    taskBuilder.addFeatureColumn("feature_1").addFeatureColumn("feature_2").addFeatureColumn("feature_3");

    MLTask anotherTask = taskBuilder.build();

    LOG.info("Created second task " + anotherTask.toString());
    anotherTask.run();

    String secondModelID = anotherTask.getModelID();
    String secondReportID = anotherTask.getReportID();
    Assert.assertNotNull(secondModelID);
    Assert.assertNotNull(secondReportID);

    Hive metastoreClient = Hive.get(conf);
    Table outputHiveTable = metastoreClient.getTable(outputTable);
    List<Partition> partitions = metastoreClient.getPartitions(outputHiveTable);

    Assert.assertNotNull(partitions);

    int i = 0;
    Set<String> partReports = new HashSet<String>();
    for (Partition part : partitions) {
      LOG.info("@@PART#" + i + " " + part.getSpec().toString());
      partReports.add(part.getSpec().get("part_testid"));
    }

    // Verify partitions created for each run
    Assert.assertTrue(partReports.contains(firstReportID), firstReportID + "  first partition not there");
    Assert.assertTrue(partReports.contains(secondReportID), secondReportID + " second partition not there");

    LOG.info("Completed task run");

  }

}

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
import javax.ws.rs.core.UriBuilder;

import org.apache.lens.client.LensClient;
import org.apache.lens.client.LensClientConfig;
import org.apache.lens.client.LensMLClient;
import org.apache.lens.ml.algo.spark.dt.DecisionTreeAlgo;
import org.apache.lens.ml.algo.spark.lr.LogisticRegressionAlgo;
import org.apache.lens.ml.algo.spark.nb.NaiveBayesAlgo;
import org.apache.lens.ml.algo.spark.svm.SVMAlgo;
import org.apache.lens.ml.impl.MLTask;
import org.apache.lens.ml.impl.MLUtils;
import org.apache.lens.ml.server.MLApp;
import org.apache.lens.ml.server.MLServiceResource;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.query.QueryServiceResource;
import org.apache.lens.server.session.SessionResource;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Test
public class TestMLResource extends LensJerseyTest {

  private static final String TEST_DB = "default";

  private WebTarget mlTarget;
  private LensMLClient mlClient;

  @Override
  protected Application configure() {
    return new MLApp(SessionResource.class, QueryServiceResource.class);
  }

  @Override
  protected void configureClient(ClientConfig config) {
    config.register(MultiPartFeature.class);
  }

  @Override
  protected URI getBaseUri() {
    return UriBuilder.fromUri("http://localhost/").port(getTestPort()).path("/lensapi").build();
  }

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
    Hive hive = Hive.get(new HiveConf());
    Database db = new Database();
    db.setName(TEST_DB);
    hive.createDatabase(db, true);
    LensClientConfig lensClientConfig = new LensClientConfig();
    lensClientConfig.setLensDatabase(TEST_DB);
    lensClientConfig.set(LensConfConstants.SERVER_BASE_URL,
        "http://localhost:" + getTestPort() + "/lensapi");
    LensClient client = new LensClient(lensClientConfig);
    mlClient = new LensMLClient(client);
  }

  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
    Hive hive = Hive.get(new HiveConf());

    try {
      hive.dropDatabase(TEST_DB);
    } catch (Exception exc) {
      // Ignore drop db exception
      log.error("Exception while dropping database.", exc);
    }
    mlClient.close();
  }

  @BeforeMethod
  public void setMLTarget() {
    mlTarget = target().path("ml");
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

    Assert.assertTrue(
        algoNames.contains(MLUtils.getAlgoName(NaiveBayesAlgo.class)),
        MLUtils.getAlgoName(NaiveBayesAlgo.class));

    Assert.assertTrue(algoNames.contains(MLUtils.getAlgoName(SVMAlgo.class)),
        MLUtils.getAlgoName(SVMAlgo.class));

    Assert.assertTrue(
        algoNames.contains(MLUtils.getAlgoName(LogisticRegressionAlgo.class)),
        MLUtils.getAlgoName(LogisticRegressionAlgo.class));

    Assert.assertTrue(
        algoNames.contains(MLUtils.getAlgoName(DecisionTreeAlgo.class)),
        MLUtils.getAlgoName(DecisionTreeAlgo.class));
  }

  @Test
  public void testGetAlgoParams() throws Exception {
    Map<String, String> params = mlClient.getAlgoParamDescription(MLUtils
        .getAlgoName(DecisionTreeAlgo.class));
    Assert.assertNotNull(params);
    Assert.assertFalse(params.isEmpty());

    for (String key : params.keySet()) {
      log.info("## Param " + key + " help = " + params.get(key));
    }
  }

  @Test
  public void trainAndEval() throws Exception {
    log.info("Starting train & eval");
    final String algoName = MLUtils.getAlgoName(NaiveBayesAlgo.class);
    HiveConf conf = new HiveConf();
    String tableName = "naivebayes_training_table";
    String sampleDataFilePath = "data/naive_bayes/naive_bayes_train.data";

    File sampleDataFile = new File(sampleDataFilePath);
    URI sampleDataFileURI = sampleDataFile.toURI();

    String labelColumn = "label";
    String[] features = { "feature_1", "feature_2", "feature_3" };
    String outputTable = "naivebayes_eval_table";

    log.info("Creating training table from file "
        + sampleDataFileURI.toString());

    Map<String, String> tableParams = new HashMap<String, String>();
    try {
      ExampleUtils.createTable(conf, TEST_DB, tableName,
          sampleDataFileURI.toString(), labelColumn, tableParams, features);
    } catch (HiveException exc) {
      log.error("Hive exception encountered.", exc);
    }
    MLTask.Builder taskBuilder = new MLTask.Builder();

    taskBuilder.algorithm(algoName).hiveConf(conf).labelColumn(labelColumn)
        .outputTable(outputTable).client(mlClient).trainingTable(tableName);

    // Add features
    taskBuilder.addFeatureColumn("feature_1").addFeatureColumn("feature_2")
        .addFeatureColumn("feature_3");

    MLTask task = taskBuilder.build();

    log.info("Created task " + task.toString());
    task.run();
    Assert.assertEquals(task.getTaskState(), MLTask.State.SUCCESSFUL);

    String firstModelID = task.getModelID();
    String firstReportID = task.getReportID();
    Assert.assertNotNull(firstReportID);
    Assert.assertNotNull(firstModelID);

    taskBuilder = new MLTask.Builder();
    taskBuilder.algorithm(algoName).hiveConf(conf).labelColumn(labelColumn)
        .outputTable(outputTable).client(mlClient).trainingTable(tableName);

    taskBuilder.addFeatureColumn("feature_1").addFeatureColumn("feature_2")
        .addFeatureColumn("feature_3");

    MLTask anotherTask = taskBuilder.build();

    log.info("Created second task " + anotherTask.toString());
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
      log.info("@@PART#" + i + " " + part.getSpec().toString());
      partReports.add(part.getSpec().get("part_testid"));
    }

    // Verify partitions created for each run
    Assert.assertTrue(partReports.contains(firstReportID), firstReportID
        + "  first partition not there");
    Assert.assertTrue(partReports.contains(secondReportID), secondReportID
        + " second partition not there");

    log.info("Completed task run");

  }

}

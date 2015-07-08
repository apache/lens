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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.UriBuilder;

import org.apache.lens.client.LensClient;
import org.apache.lens.client.LensClientConfig;
import org.apache.lens.client.LensMLClient;
import org.apache.lens.ml.algo.spark.dt.DecisionTreeAlgo;
import org.apache.lens.ml.algo.spark.kmeans.KMeansAlgo;
import org.apache.lens.ml.algo.spark.lr.LogisticRegressionAlgo;
import org.apache.lens.ml.algo.spark.nb.NaiveBayesAlgo;
import org.apache.lens.ml.algo.spark.svm.SVMAlgo;
import org.apache.lens.ml.api.*;
import org.apache.lens.ml.server.MLApp;
import org.apache.lens.ml.server.MLServiceResource;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.query.QueryServiceResource;
import org.apache.lens.server.session.SessionResource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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


@Test
public class TestMLServices extends LensJerseyTest {
  private static final Log LOG = LogFactory.getLog(TestMLServices.class);
  private static final String TEST_DB = "default";

  private WebTarget mlTarget;
  private LensMLClient mlClient;

  @Override
  protected int getTestPort() {
    return 10002;
  }

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
    LOG.info("Testing started!");
    super.setUp();
    Hive hive = Hive.get(new HiveConf());
    Database db = new Database();
    db.setName(TEST_DB);
    //hive.createDatabase(db, true);
    LensClientConfig lensClientConfig = new LensClientConfig();
    lensClientConfig.setLensDatabase(TEST_DB);
    lensClientConfig.set(LensConfConstants.SERVER_BASE_URL,
      "http://localhost:" + 10002 + "/lensapi");
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
      LOG.error(exc.getMessage());
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

    LOG.info("Testing registered algorithm.");

    List<Algo> algos = mlClient.getAlgos();

    Assert.assertNotNull(algos);

    ArrayList<String> algoNames = new ArrayList();
    for (Algo algo : algos) {
      algoNames.add(algo.getName());
    }

    Assert.assertTrue(
      algoNames.contains(new NaiveBayesAlgo().getName()),
      new NaiveBayesAlgo().getName());

    Assert.assertTrue(algoNames.contains(new SVMAlgo().getName()),
      new SVMAlgo().getName());

    Assert.assertTrue(
      algoNames.contains(new LogisticRegressionAlgo().getName()),
      new LogisticRegressionAlgo().getName());

    Assert.assertTrue(
      algoNames.contains(new DecisionTreeAlgo().getName()),
      new DecisionTreeAlgo().getName());

    Assert.assertTrue(
      algoNames.contains(new KMeansAlgo().getName()),
      new KMeansAlgo().getName());
  }

  @Test
  public void trainAndEval() throws Exception {
    LOG.info("Starting train and eval");
    HiveConf conf = new HiveConf();

    String sampleDataFilePath = "data/lr/lr_train.data";

    File sampleDataFile = new File(sampleDataFilePath);
    URI sampleDataFileURI = sampleDataFile.toURI();

    ArrayList<Feature> features = new ArrayList();
    Feature feature1 = new Feature("feature1", "description", Feature.Type.Categorical, "feature1");
    Feature feature2 = new Feature("feature2", "description", Feature.Type.Categorical, "feature2");
    Feature feature3 = new Feature("feature3", "description", Feature.Type.Categorical, "feature3");
    Feature label = new Feature("label", "description", Feature.Type.Categorical, "label");

    features.add(feature1);
    features.add(feature2);
    features.add(feature3);

    String tableName = "lr_table_6";

    String labelColumn = "label";
    String[] featureNames = {"feature1", "feature2", "feature3"};
    Map<String, String> tableParams = new HashMap<String, String>();
    try {
      ExampleUtils.createTable(conf, TEST_DB, tableName,
        sampleDataFileURI.toString(), labelColumn, tableParams, featureNames);
    } catch (HiveException exc) {
      LOG.error(exc.getLocalizedMessage());
    }

    mlClient.createDataSet("lr_table_6", "lr_table_6", TEST_DB);


    String modelId =
      mlClient.createModel("lr_model", "spark_logistic_regression", new HashMap<String, String>(), features,
        label, mlClient.getSessionHandle());
    LOG.info("model created with Id: " + modelId);

    Model model = mlClient.getModel(modelId);
    Assert.assertTrue(model != null, "Null model returned after creation");

    String modelInstanceId = mlClient.trainModel(modelId, "lr_table_6", mlClient.getSessionHandle());
    LOG.info("Model Instance created with Id:" + modelInstanceId);

    ModelInstance modelInstance;

    do {
      modelInstance = mlClient.getModelInstance(modelInstanceId);
      Thread.sleep(2000);
    } while (!(modelInstance.getStatus() == Status.COMPLETED || modelInstance.getStatus() == Status.FAILED));

    Assert.assertTrue(modelInstance.getStatus() == Status.COMPLETED, "Training model failed");

    Map<String, String> featureMap = new HashMap();
    featureMap.put("feature1", "1");
    featureMap.put("feature2", "0");
    featureMap.put("feature3", "4");

    String predictedValue = mlClient.predict(modelInstanceId, featureMap);
    LOG.info("Predicting :" + predictedValue);

    Assert.assertTrue(predictedValue.equals("1.0"), "Predicted value incorrect");


    String predictionId = mlClient.predict(modelInstanceId, "lr_table_6", mlClient.getSessionHandle());
    Prediction prediction;
    do {
      prediction = mlClient.getPrediction(predictionId);
      Thread.sleep(2000);
    } while (!(prediction.getStatus() == Status.COMPLETED || prediction.getStatus() == Status.FAILED));

    Assert.assertTrue(prediction != null, "Prediction failed");

    Hive metastoreClient = Hive.get(conf);
    Table outputHiveTableForPrediction = metastoreClient.getTable(prediction.getOutputDataSet());
    List<Partition> predictionPartitions = metastoreClient.getPartitions(outputHiveTableForPrediction);

    Assert.assertNotNull(predictionPartitions);

    String evaluationId = mlClient.evaluate(modelInstanceId, "lr_table_6", mlClient.getSessionHandle());
    LOG.info("Evaluation started with id:" + evaluationId);

    Evaluation evaluation;
    do {
      evaluation = mlClient.getEvaluation(evaluationId);
      Thread.sleep(2000);
    } while (!(evaluation.getStatus() == Status.COMPLETED || evaluation.getStatus() == Status.FAILED));

    Assert.assertTrue(evaluation.getStatus() == Status.COMPLETED, "Evaluating model failed");

    Table outputHiveTableForEvaluation = metastoreClient.getTable(prediction.getOutputDataSet());
    List<Partition> evaluationPartitions = metastoreClient.getPartitions(outputHiveTableForEvaluation);

    Assert.assertNotNull(evaluationPartitions);

    //Testing cancellation

    evaluationId = mlClient.evaluate(modelInstanceId, "lr_table_6", mlClient.getSessionHandle());
    LOG.info("Evaluation started with id:" + evaluationId);
    boolean result = mlClient.cancelEvaluation(evaluationId);

    Assert.assertTrue(result);

    evaluation = mlClient.getEvaluation(evaluationId);
    Assert
      .assertTrue(evaluation.getStatus() == Status.CANCELLED, "Cancellation failed" + evaluation.getStatus().name());

  }


}

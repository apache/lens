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
package org.apache.lens.ml.impl;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.ml.algo.api.Algorithm;
import org.apache.lens.ml.algo.api.MLDriver;
import org.apache.lens.ml.algo.api.TrainedModel;
import org.apache.lens.ml.algo.spark.SparkMLDriver;
import org.apache.lens.ml.api.*;
import org.apache.lens.ml.dao.MetaStoreClient;
import org.apache.lens.ml.dao.MetaStoreClientImpl;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.session.SessionService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.api.java.JavaSparkContext;

public class LensMLImpl implements LensML {

  /**
   * The Constant LOG.
   */
  public static final Log LOG = LogFactory.getLog(LensMLImpl.class);

  /**
   * Check if the predict UDF has been registered for a user
   */
  private final Map<LensSessionHandle, Boolean> predictUdfStatus;

  /**
   * The drivers.
   */
  protected List<MLDriver> drivers;

  /**
   * The metaStoreClient
   */
  MetaStoreClient metaStoreClient;

  Map<String, Algo> algorithms = new HashMap<String, Algo>();

  /**
   * The conf.
   */
  private HiveConf conf;

  /**
   * The spark context.
   */
  private JavaSparkContext sparkContext;

  /**
   * Background thread to periodically check if we need to clear expire status for a session
   */
  private ScheduledExecutorService udfStatusExpirySvc;

  /**
   * Life Cycle manager of Model Instance creation.
   */
  private MLProcessLifeCycleManager mlProcessLifeCycleManager;

  /**
   * Instantiates a new lens ml impl.
   *
   * @param conf the conf
   */
  public LensMLImpl(HiveConf conf) {
    this.conf = conf;
    this.predictUdfStatus = new ConcurrentHashMap<LensSessionHandle, Boolean>();
  }

  public HiveConf getConf() {
    return conf;
  }

  /**
   * Use an existing Spark context. Useful in case of
   *
   * @param jsc JavaSparkContext instance
   */
  public void setSparkContext(JavaSparkContext jsc) {
    this.sparkContext = jsc;
  }

  /**
   * Initialises  LensMLImpl. Registers drives, checks if there are previously interrupted MLprocess.
   *
   * @param hiveConf
   */
  public synchronized void init(HiveConf hiveConf) {
    this.conf = hiveConf;

    // Get all the drivers
    String[] driverClasses = hiveConf.getStrings("lens.ml.drivers");

    if (driverClasses == null || driverClasses.length == 0) {
      throw new RuntimeException("No ML Drivers specified in conf");
    }

    LOG.info("Loading drivers " + Arrays.toString(driverClasses));
    drivers = new ArrayList<MLDriver>(driverClasses.length);

    for (String driverClass : driverClasses) {
      Class<?> cls;
      try {
        cls = Class.forName(driverClass);
      } catch (ClassNotFoundException e) {
        LOG.error("Driver class not found " + driverClass);
        continue;
      }

      if (!MLDriver.class.isAssignableFrom(cls)) {
        LOG.warn("Not a driver class " + driverClass);
        continue;
      }

      try {
        Class<? extends MLDriver> mlDriverClass = (Class<? extends MLDriver>) cls;
        MLDriver driver = mlDriverClass.newInstance();
        driver.init(toLensConf(conf));
        drivers.add(driver);
        LOG.info("Added driver " + driverClass);
      } catch (Exception e) {
        LOG.error("Failed to create driver " + driverClass + " reason: " + e.getMessage(), e);
      }
    }
    if (drivers.isEmpty()) {
      throw new RuntimeException("No ML drivers loaded");
    }

    metaStoreClient = new MetaStoreClientImpl(MLUtils.createMLMetastoreConnectionPool(hiveConf));
    metaStoreClient.init();
    mlProcessLifeCycleManager = new MLProcessLifeCycleManager(conf, metaStoreClient, drivers);

    mlProcessLifeCycleManager.init();

    LOG.info("Inited ML service");
  }

  public synchronized void start() {
    for (MLDriver driver : drivers) {
      try {
        if (driver instanceof SparkMLDriver && sparkContext != null) {
          ((SparkMLDriver) driver).useSparkContext(sparkContext);
        }
        driver.start();
        registerAlgorithms(driver);
      } catch (LensException e) {
        LOG.error("Failed to start driver " + driver, e);
      }
    }

    mlProcessLifeCycleManager.start();

    udfStatusExpirySvc = Executors.newSingleThreadScheduledExecutor();
    udfStatusExpirySvc.scheduleAtFixedRate(new UDFStatusExpiryRunnable(), 60, 60, TimeUnit.SECONDS);

    LOG.info("Started ML service");
  }

  public synchronized void stop() {
    for (MLDriver driver : drivers) {
      try {
        driver.stop();
      } catch (LensException e) {
        LOG.error("Failed to stop driver " + driver, e);
      }
    }
    drivers.clear();
    udfStatusExpirySvc.shutdownNow();

    mlProcessLifeCycleManager.stop();

    LOG.info("Stopped ML service");
  }

  @Override
  public List<Algo> getAlgos() {
    List<Algo> allAlgos = new ArrayList<Algo>();
    allAlgos.addAll(algorithms.values());
    return allAlgos;
  }

  @Override
  public Algo getAlgo(String name) throws LensException {
    return algorithms.get(name);
  }

  @Override
  public void createDataSet(String name, String dataTable, String dataBase) throws LensException {
    createDataSet(new DataSet(name, dataTable, dataBase));
  }

  @Override
  public void createDataSet(DataSet dataSet) throws LensException {
    try {
      metaStoreClient.createDataSet(dataSet);
    } catch (SQLException e) {
      throw new LensException("Error while creating DataSet name " + dataSet.getDsName());
    }
  }

  @Override
  public String createDataSetFromQuery(String name, String query) {
    return null;
  }

  @Override
  public DataSet getDataSet(String name) throws LensException {
    try {
      return metaStoreClient.getDataSet(name);
    } catch (SQLException ex) {
      throw new LensException("Error while reading DataSet. Name: " + name);
    }
  }

  @Override
  public void createModel(String name, String algoName, Map<String, String> algoParams, List<Feature> features,
                          Feature label, LensSessionHandle lensSessionHandle) throws LensException {
    AlgoSpec algoSpec = new AlgoSpec(algoName, algoParams);
    metaStoreClient.createModel(name, algoName, algoSpec, features, label);
  }

  @Override
  public void createModel(Model model) throws LensException {
    metaStoreClient.createModel(model);
  }

  @Override
  public Model getModel(String modelId) throws LensException {
    return metaStoreClient.getModel(modelId);
  }

  @Override
  public String trainModel(String modelId, String dataSetName, LensSessionHandle lensSessionHandle)
    throws LensException {
    String modelInstanceId = metaStoreClient.createModelInstance(new Date(), null, Status.SUBMITTED,
      lensSessionHandle,
      modelId, dataSetName, "", null);
    ModelInstance modelInstance = metaStoreClient.getModelInstance(modelInstanceId);
    mlProcessLifeCycleManager.addProcess(modelInstance);
    return modelInstance.getId();
  }

  @Override
  public ModelInstance getModelInstance(String modelInstanceId) throws LensException {
    ModelInstance modelInstance = (ModelInstance) mlProcessLifeCycleManager.getMLProcess(modelInstanceId);
    if (modelInstance != null) {
      return modelInstance;
    }
    return metaStoreClient.getModelInstance(modelInstanceId);
  }

  @Override
  public boolean cancelModelInstance(String modelInstanceId, LensSessionHandle lensSessionHandle) throws LensException {
    return mlProcessLifeCycleManager.cancelProcess(modelInstanceId, lensSessionHandle);
  }

  @Override
  public List<ModelInstance> getAllModelInstances(String modelId) {
    return null;
  }

  @Override
  public String evaluate(String modelInstanceId, String inputDataSetName, LensSessionHandle lensSessionHandle)
    throws LensException {

    DataSet inputDataSet = getDataSet(inputDataSetName);
    if (inputDataSet == null) {
      throw new LensException("Input DataSet does not exist. Name: " + inputDataSetName);
    }

    String evaluationId = metaStoreClient.createEvaluation(new Date(), null, Status.SUBMITTED, lensSessionHandle,
      modelInstanceId, inputDataSetName);
    Evaluation evaluation = metaStoreClient.getEvaluation(evaluationId);
    mlProcessLifeCycleManager.addProcess(evaluation);
    return evaluation.getId();
  }

  @Override
  public Evaluation getEvaluation(String evalId) throws LensException {
    return metaStoreClient.getEvaluation(evalId);
  }

  @Override
  public boolean cancelEvaluation(String evalId, LensSessionHandle lensSessionHandle) throws LensException {
    return mlProcessLifeCycleManager.cancelProcess(evalId, lensSessionHandle);
  }

  @Override
  public String predict(String modelInstanceId, String dataSetName, LensSessionHandle lensSessionHandle)
    throws LensException {
    String id = UUID.randomUUID().toString();
    DataSet dataSet = getDataSet(dataSetName);
    if (dataSet == null) {
      throw new LensException("DataSet not available: " + dataSetName);
    }
    String outputDataSetName = MLConfConstants.PREDICTION_OUTPUT_TABLE_PREFIX + id.replace("-", "_");
    createDataSet(outputDataSetName, outputDataSetName, dataSet.getDbName());
    String predictionId = metaStoreClient.createPrediction(new Date(), null, Status.SUBMITTED, lensSessionHandle,
      modelInstanceId,
      dataSetName, outputDataSetName);
    Prediction prediction = metaStoreClient.getPrediction(predictionId);
    mlProcessLifeCycleManager.addProcess(prediction);
    return prediction.getId();
  }

  @Override
  public Prediction getPrediction(String predictionId) throws LensException {
    Prediction prediction = (Prediction) mlProcessLifeCycleManager.getMLProcess(predictionId);
    if (prediction != null) {
      return prediction;
    }
    return metaStoreClient.getPrediction(predictionId);
  }

  @Override
  public boolean cancelPrediction(String predictionId, LensSessionHandle lensSessionHandle) throws LensException {
    return mlProcessLifeCycleManager.cancelProcess(predictionId, lensSessionHandle);
  }

  @Override
  public String predict(String modelInstanceId, Map<String, String> featureVector) throws LensException {
    ModelInstance modelInstance;
    Model model;
    try {
      modelInstance = metaStoreClient.getModelInstance(modelInstanceId);
      if (modelInstance == null) {
        throw new LensException("Invalid modelInstance Id.");
      }
      if (modelInstance.getStatus() != Status.COMPLETED) {
        throw new LensException("Prediction is allowed only on modelInstances which has completed training "
          + "successfully. Current modelInstance status : " + modelInstance.getStatus());
      }
      model = metaStoreClient.getModel(modelInstance.getModelId());
    } catch (Exception e) {
      throw new LensException("Error Reading modelInstanceId :" + modelInstanceId, e);
    }

    try {
      TrainedModel trainedModel = ModelLoader
        .loadModel(conf, model.getAlgoSpec().getAlgo(), model.getName(), modelInstance.getId());
      Object trainingResult = trainedModel.predict(featureVector);
      return trainingResult.toString();
    } catch (Exception e) {
      throw new LensException("Error while training model for modelInstanceId: " + modelInstanceId, e);
    }
  }

  @Override
  public void deleteDataSet(String dataSetName) throws LensException {

  }

  @Override
  public void deleteModel(String modelId) throws LensException {

  }

  @Override
  public void deleteModelInstance(String modelInstanceId) throws LensException {

  }

  @Override
  public void deleteEvaluation(String evaluationId) throws LensException {

  }

  @Override
  public void deletePrediction(String predictionId) throws LensException {

  }

  /**
   * Register all available algorithms to cache.
   *
   * @param driver
   */
  void registerAlgorithms(MLDriver driver) {
    for (String algoName : driver.getAlgoNames()) {
      try {
        final Algorithm algorithm = driver.getAlgoInstance(algoName);
        algorithms.put(algoName, new Algo(algorithm.getName(), algorithm.getDescription(), algorithm.getParams()));
      } catch (Exception e) {
        LOG.error("Couldn't register algorithm " + algoName);
      }
    }
  }

  /**
   * To lens conf.
   *
   * @param conf the conf
   * @return the lens conf
   */
  private LensConf toLensConf(HiveConf conf) {
    LensConf lensConf = new LensConf();
    lensConf.getProperties().putAll(conf.getValByRegex(".*"));
    return lensConf;
  }

  /**
   * Gets the algo dir.
   *
   * @param algoName the algo name
   * @return the algo dir
   * @throws java.io.IOException Signals that an I/O exception has occurred.
   */
  private Path getAlgoDir(String algoName) throws IOException {
    String modelSaveBaseDir = conf.get(ModelLoader.MODEL_PATH_BASE_DIR, ModelLoader.MODEL_PATH_BASE_DIR_DEFAULT);
    return new Path(new Path(modelSaveBaseDir), algoName);
  }

  /**
   * Registers predict UDF for a given LensSession
   *
   * @param sessionHandle
   * @param lensQueryRunner
   * @throws LensException
   */
  protected void registerPredictUdf(LensSessionHandle sessionHandle, LensQueryRunner lensQueryRunner)
    throws LensException {
    if (isUdfRegistered(sessionHandle)) {
      // Already registered, nothing to do
      return;
    }

    LOG.info("Registering UDF for session " + sessionHandle.getPublicId().toString());

    String regUdfQuery = "CREATE TEMPORARY FUNCTION " + MLConfConstants.UDF_NAME + " AS '" + HiveMLUDF.class
      .getCanonicalName() + "'";
    lensQueryRunner.setQueryName("register_predict_udf_" + sessionHandle.getPublicId().toString());
    QueryHandle udfQuery = lensQueryRunner.runQuery(regUdfQuery);
    LOG.info("udf query handle is " + udfQuery);
    predictUdfStatus.put(sessionHandle, true);
    LOG.info("Predict UDF registered for session " + sessionHandle.getPublicId().toString());
  }

  /**
   * Checks if predict UDF is registered for a given LensSession
   *
   * @param sessionHandle
   * @return
   */
  protected boolean isUdfRegistered(LensSessionHandle sessionHandle) {
    return predictUdfStatus.containsKey(sessionHandle);
  }

  /**
   * Clears predictUDFStatus for all closed LensSessions.
   */
  private class UDFStatusExpiryRunnable implements Runnable {
    public void run() {
      try {
        SessionService sessionService = (SessionService) MLUtils.getServiceProvider().getService(SessionService.NAME);
        // Clear status of sessions which are closed.
        List<LensSessionHandle> sessions = new ArrayList<LensSessionHandle>(predictUdfStatus.keySet());
        for (LensSessionHandle sessionHandle : sessions) {
          if (!sessionService.isOpen(sessionHandle)) {
            LOG.info("Session closed, removing UDF status: " + sessionHandle);
            predictUdfStatus.remove(sessionHandle);
          }
        }
      } catch (Exception exc) {
        LOG.warn("Error clearing UDF statuses", exc);
      }
    }
  }
}

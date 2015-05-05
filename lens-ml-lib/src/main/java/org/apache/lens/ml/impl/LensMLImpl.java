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
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.api.response.LensResponse;
import org.apache.lens.api.response.NoErrorPayload;
import org.apache.lens.ml.algo.api.MLAlgo;
import org.apache.lens.ml.algo.api.MLDriver;
import org.apache.lens.ml.algo.api.MLModel;
import org.apache.lens.ml.algo.spark.BaseSparkAlgo;
import org.apache.lens.ml.algo.spark.SparkMLDriver;
import org.apache.lens.ml.api.LensML;
import org.apache.lens.ml.api.MLTestReport;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.session.SessionService;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.spark.api.java.JavaSparkContext;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;


/**
 * The Class LensMLImpl.
 */
public class LensMLImpl implements LensML {

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(LensMLImpl.class);

  /** The drivers. */
  protected List<MLDriver> drivers;

  /** The conf. */
  private HiveConf conf;

  /** The spark context. */
  private JavaSparkContext sparkContext;

  /** Check if the predict UDF has been registered for a user */
  private final Map<LensSessionHandle, Boolean> predictUdfStatus;
  /** Background thread to periodically check if we need to clear expire status for a session */
  private ScheduledExecutorService udfStatusExpirySvc;

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

  public List<String> getAlgorithms() {
    List<String> algos = new ArrayList<String>();
    for (MLDriver driver : drivers) {
      algos.addAll(driver.getAlgoNames());
    }
    return algos;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.ml.LensML#getAlgoForName(java.lang.String)
   */
  public MLAlgo getAlgoForName(String algorithm) throws LensException {
    for (MLDriver driver : drivers) {
      if (driver.isAlgoSupported(algorithm)) {
        return driver.getAlgoInstance(algorithm);
      }
    }
    throw new LensException("Algo not supported " + algorithm);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.ml.LensML#train(java.lang.String, java.lang.String, java.lang.String[])
   */
  public String train(String table, String algorithm, String[] args) throws LensException {
    MLAlgo algo = getAlgoForName(algorithm);

    String modelId = UUID.randomUUID().toString();

    LOG.info("Begin training model " + modelId + ", algo=" + algorithm + ", table=" + table + ", params="
      + Arrays.toString(args));

    String database = null;
    if (SessionState.get() != null) {
      database = SessionState.get().getCurrentDatabase();
    } else {
      database = "default";
    }

    MLModel model = algo.train(toLensConf(conf), database, table, modelId, args);

    LOG.info("Done training model: " + modelId);

    model.setCreatedAt(new Date());
    model.setAlgoName(algorithm);

    Path modelLocation = null;
    try {
      modelLocation = persistModel(model);
      LOG.info("Model saved: " + modelId + ", algo: " + algorithm + ", path: " + modelLocation);
      return model.getId();
    } catch (IOException e) {
      throw new LensException("Error saving model " + modelId + " for algo " + algorithm, e);
    }
  }

  /**
   * Gets the algo dir.
   *
   * @param algoName the algo name
   * @return the algo dir
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private Path getAlgoDir(String algoName) throws IOException {
    String modelSaveBaseDir = conf.get(ModelLoader.MODEL_PATH_BASE_DIR, ModelLoader.MODEL_PATH_BASE_DIR_DEFAULT);
    return new Path(new Path(modelSaveBaseDir), algoName);
  }

  /**
   * Persist model.
   *
   * @param model the model
   * @return the path
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private Path persistModel(MLModel model) throws IOException {
    // Get model save path
    Path algoDir = getAlgoDir(model.getAlgoName());
    FileSystem fs = algoDir.getFileSystem(conf);

    if (!fs.exists(algoDir)) {
      fs.mkdirs(algoDir);
    }

    Path modelSavePath = new Path(algoDir, model.getId());
    ObjectOutputStream outputStream = null;

    try {
      outputStream = new ObjectOutputStream(fs.create(modelSavePath, false));
      outputStream.writeObject(model);
      outputStream.flush();
    } catch (IOException io) {
      LOG.error("Error saving model " + model.getId() + " reason: " + io.getMessage());
      throw io;
    } finally {
      IOUtils.closeQuietly(outputStream);
    }
    return modelSavePath;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.ml.LensML#getModels(java.lang.String)
   */
  public List<String> getModels(String algorithm) throws LensException {
    try {
      Path algoDir = getAlgoDir(algorithm);
      FileSystem fs = algoDir.getFileSystem(conf);
      if (!fs.exists(algoDir)) {
        return null;
      }

      List<String> models = new ArrayList<String>();

      for (FileStatus stat : fs.listStatus(algoDir)) {
        models.add(stat.getPath().getName());
      }

      if (models.isEmpty()) {
        return null;
      }

      return models;
    } catch (IOException ioex) {
      throw new LensException(ioex);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.ml.LensML#getModel(java.lang.String, java.lang.String)
   */
  public MLModel getModel(String algorithm, String modelId) throws LensException {
    try {
      return ModelLoader.loadModel(conf, algorithm, modelId);
    } catch (IOException e) {
      throw new LensException(e);
    }
  }

  /**
   * Inits the.
   *
   * @param hiveConf the hive conf
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

    LOG.info("Inited ML service");
  }

  /**
   * Start.
   */
  public synchronized void start() {
    for (MLDriver driver : drivers) {
      try {
        if (driver instanceof SparkMLDriver && sparkContext != null) {
          ((SparkMLDriver) driver).useSparkContext(sparkContext);
        }
        driver.start();
      } catch (LensException e) {
        LOG.error("Failed to start driver " + driver, e);
      }
    }

    udfStatusExpirySvc = Executors.newSingleThreadScheduledExecutor();
    udfStatusExpirySvc.scheduleAtFixedRate(new UDFStatusExpiryRunnable(), 60, 60, TimeUnit.SECONDS);

    LOG.info("Started ML service");
  }

  /**
   * Stop.
   */
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
    LOG.info("Stopped ML service");
  }

  public synchronized HiveConf getHiveConf() {
    return conf;
  }

  /**
   * Clear models.
   */
  public void clearModels() {
    ModelLoader.clearCache();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.ml.LensML#getModelPath(java.lang.String, java.lang.String)
   */
  public String getModelPath(String algorithm, String modelID) {
    return ModelLoader.getModelLocation(conf, algorithm, modelID).toString();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.ml.LensML#testModel(org.apache.lens.api.LensSessionHandle, java.lang.String, java.lang.String,
   * java.lang.String)
   */
  @Override
  public MLTestReport testModel(LensSessionHandle session, String table, String algorithm, String modelID,
    String outputTable) throws LensException {
    return null;
  }

  /**
   * Test a model in embedded mode.
   *
   * @param sessionHandle the session handle
   * @param table         the table
   * @param algorithm     the algorithm
   * @param modelID       the model id
   * @param queryApiUrl   the query api url
   * @return the ML test report
   * @throws LensException the lens exception
   */
  public MLTestReport testModelRemote(LensSessionHandle sessionHandle, String table, String algorithm, String modelID,
    String queryApiUrl, String outputTable) throws LensException {
    return testModel(sessionHandle, table, algorithm, modelID, new RemoteQueryRunner(sessionHandle, queryApiUrl),
      outputTable);
  }

  /**
   * Evaluate a model. Evaluation is done on data selected table from an input table. The model is run as a UDF and its
   * output is inserted into a table with a partition. Each evaluation is given a unique ID. The partition label is
   * associated with this unique ID.
   * <p/>
   * <p>
   * This call also required a query runner. Query runner is responsible for executing the evaluation query against Lens
   * server.
   * </p>
   *
   * @param sessionHandle the session handle
   * @param table         the table
   * @param algorithm     the algorithm
   * @param modelID       the model id
   * @param queryRunner   the query runner
   * @param outputTable   table where test output will be written
   * @return the ML test report
   * @throws LensException the lens exception
   */
  public MLTestReport testModel(final LensSessionHandle sessionHandle, String table, String algorithm, String modelID,
    QueryRunner queryRunner, String outputTable) throws LensException {
    if (sessionHandle == null) {
      throw new NullPointerException("Null session not allowed");
    }
    // check if algorithm exists
    if (!getAlgorithms().contains(algorithm)) {
      throw new LensException("No such algorithm " + algorithm);
    }

    MLModel<?> model;
    try {
      model = ModelLoader.loadModel(conf, algorithm, modelID);
    } catch (IOException e) {
      throw new LensException(e);
    }

    if (model == null) {
      throw new LensException("Model not found: " + modelID + " algorithm=" + algorithm);
    }

    String database = null;

    if (SessionState.get() != null) {
      database = SessionState.get().getCurrentDatabase();
    }

    String testID = UUID.randomUUID().toString().replace("-", "_");
    final String testTable = outputTable;
    final String testResultColumn = "prediction_result";

    // TODO support error metric UDAFs
    TableTestingSpec spec = TableTestingSpec.newBuilder().hiveConf(conf)
      .database(database == null ? "default" : database).inputTable(table).featureColumns(model.getFeatureColumns())
      .outputColumn(testResultColumn).lableColumn(model.getLabelColumn()).algorithm(algorithm).modelID(modelID)
      .outputTable(testTable).testID(testID).build();

    String testQuery = spec.getTestQuery();
    if (testQuery == null) {
      throw new LensException("Invalid test spec. " + "table=" + table + " algorithm=" + algorithm + " modelID="
        + modelID);
    }

    if (!spec.isOutputTableExists()) {
      LOG.info("Output table '" + testTable + "' does not exist for test algorithm = " + algorithm + " modelid="
        + modelID + ", Creating table using query: " + spec.getCreateOutputTableQuery());
      // create the output table
      String createOutputTableQuery = spec.getCreateOutputTableQuery();
      queryRunner.runQuery(createOutputTableQuery);
      LOG.info("Table created " + testTable);
    }

    // Check if ML UDF is registered in this session
    registerPredictUdf(sessionHandle, queryRunner);

    LOG.info("Running evaluation query " + testQuery);
    queryRunner.setQueryName("model_test_" + modelID);
    QueryHandle testQueryHandle = queryRunner.runQuery(testQuery);

    MLTestReport testReport = new MLTestReport();
    testReport.setReportID(testID);
    testReport.setAlgorithm(algorithm);
    testReport.setFeatureColumns(model.getFeatureColumns());
    testReport.setLabelColumn(model.getLabelColumn());
    testReport.setModelID(model.getId());
    testReport.setOutputColumn(testResultColumn);
    testReport.setOutputTable(testTable);
    testReport.setTestTable(table);
    testReport.setQueryID(testQueryHandle.toString());

    // Save test report
    persistTestReport(testReport);
    LOG.info("Saved test report " + testReport.getReportID());
    return testReport;
  }

  /**
   * Persist test report.
   *
   * @param testReport the test report
   * @throws LensException the lens exception
   */
  private void persistTestReport(MLTestReport testReport) throws LensException {
    LOG.info("saving test report " + testReport.getReportID());
    try {
      ModelLoader.saveTestReport(conf, testReport);
      LOG.info("Saved report " + testReport.getReportID());
    } catch (IOException e) {
      LOG.error("Error saving report " + testReport.getReportID() + " reason: " + e.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.ml.LensML#getTestReports(java.lang.String)
   */
  public List<String> getTestReports(String algorithm) throws LensException {
    Path reportBaseDir = new Path(conf.get(ModelLoader.TEST_REPORT_BASE_DIR, ModelLoader.TEST_REPORT_BASE_DIR_DEFAULT));
    FileSystem fs = null;

    try {
      fs = reportBaseDir.getFileSystem(conf);
      if (!fs.exists(reportBaseDir)) {
        return null;
      }

      Path algoDir = new Path(reportBaseDir, algorithm);
      if (!fs.exists(algoDir)) {
        return null;
      }

      List<String> reports = new ArrayList<String>();
      for (FileStatus stat : fs.listStatus(algoDir)) {
        reports.add(stat.getPath().getName());
      }
      return reports;
    } catch (IOException e) {
      LOG.error("Error reading report list for " + algorithm, e);
      return null;
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.ml.LensML#getTestReport(java.lang.String, java.lang.String)
   */
  public MLTestReport getTestReport(String algorithm, String reportID) throws LensException {
    try {
      return ModelLoader.loadReport(conf, algorithm, reportID);
    } catch (IOException e) {
      throw new LensException(e);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.ml.LensML#predict(java.lang.String, java.lang.String, java.lang.Object[])
   */
  public Object predict(String algorithm, String modelID, Object[] features) throws LensException {
    // Load the model instance
    MLModel<?> model = getModel(algorithm, modelID);
    return model.predict(features);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.ml.LensML#deleteModel(java.lang.String, java.lang.String)
   */
  public void deleteModel(String algorithm, String modelID) throws LensException {
    try {
      ModelLoader.deleteModel(conf, algorithm, modelID);
      LOG.info("DELETED model " + modelID + " algorithm=" + algorithm);
    } catch (IOException e) {
      LOG.error(
        "Error deleting model file. algorithm=" + algorithm + " model=" + modelID + " reason: " + e.getMessage(), e);
      throw new LensException("Unable to delete model " + modelID + " for algorithm " + algorithm, e);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.ml.LensML#deleteTestReport(java.lang.String, java.lang.String)
   */
  public void deleteTestReport(String algorithm, String reportID) throws LensException {
    try {
      ModelLoader.deleteTestReport(conf, algorithm, reportID);
      LOG.info("DELETED report=" + reportID + " algorithm=" + algorithm);
    } catch (IOException e) {
      LOG.error("Error deleting report " + reportID + " algorithm=" + algorithm + " reason: " + e.getMessage(), e);
      throw new LensException("Unable to delete report " + reportID + " for algorithm " + algorithm, e);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.ml.LensML#getAlgoParamDescription(java.lang.String)
   */
  public Map<String, String> getAlgoParamDescription(String algorithm) {
    MLAlgo algo = null;
    try {
      algo = getAlgoForName(algorithm);
    } catch (LensException e) {
      LOG.error("Error getting algo description : " + algorithm, e);
      return null;
    }
    if (algo instanceof BaseSparkAlgo) {
      return ((BaseSparkAlgo) algo).getArgUsage();
    }
    return null;
  }

  /**
   * Submit model test query to a remote Lens server.
   */
  class RemoteQueryRunner extends QueryRunner {

    /** The query api url. */
    final String queryApiUrl;

    /**
     * Instantiates a new remote query runner.
     *
     * @param sessionHandle the session handle
     * @param queryApiUrl   the query api url
     */
    public RemoteQueryRunner(LensSessionHandle sessionHandle, String queryApiUrl) {
      super(sessionHandle);
      this.queryApiUrl = queryApiUrl;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lens.ml.TestQueryRunner#runQuery(java.lang.String)
     */
    @Override
    public QueryHandle runQuery(String query) throws LensException {
      // Create jersey client for query endpoint
      Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).build();
      WebTarget target = client.target(queryApiUrl);
      final FormDataMultiPart mp = new FormDataMultiPart();
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), sessionHandle,
        MediaType.APPLICATION_XML_TYPE));
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), query));
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));

      LensConf lensConf = new LensConf();
      lensConf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, false + "");
      lensConf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false + "");
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), lensConf,
        MediaType.APPLICATION_XML_TYPE));

      final QueryHandle handle = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
          new GenericType<LensResponse<QueryHandle, NoErrorPayload>>() {}).getData();

      LensQuery ctx = target.path(handle.toString()).queryParam("sessionid", sessionHandle).request()
        .get(LensQuery.class);

      QueryStatus stat = ctx.getStatus();
      while (!stat.finished()) {
        ctx = target.path(handle.toString()).queryParam("sessionid", sessionHandle).request().get(LensQuery.class);
        stat = ctx.getStatus();
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          throw new LensException(e);
        }
      }

      if (stat.getStatus() != QueryStatus.Status.SUCCESSFUL) {
        throw new LensException("Query failed " + ctx.getQueryHandle().getHandleId() + " reason:"
          + stat.getErrorMessage());
      }

      return ctx.getQueryHandle();
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

  protected void registerPredictUdf(LensSessionHandle sessionHandle, QueryRunner queryRunner) throws LensException {
    if (isUdfRegisterd(sessionHandle)) {
      // Already registered, nothing to do
      return;
    }

    LOG.info("Registering UDF for session " + sessionHandle.getPublicId().toString());

    String regUdfQuery = "CREATE TEMPORARY FUNCTION " + HiveMLUDF.UDF_NAME + " AS '" + HiveMLUDF.class
      .getCanonicalName() + "'";
    queryRunner.setQueryName("register_predict_udf_" + sessionHandle.getPublicId().toString());
    QueryHandle udfQuery = queryRunner.runQuery(regUdfQuery);
    LOG.info("udf query handle is " + udfQuery);
    predictUdfStatus.put(sessionHandle, true);
    LOG.info("Predict UDF registered for session " + sessionHandle.getPublicId().toString());
  }

  protected boolean isUdfRegisterd(LensSessionHandle sessionHandle) {
    return predictUdfStatus.containsKey(sessionHandle);
  }

  /**
   * Periodically check if sessions have been closed, and clear UDF registered status.
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

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

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensException;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.ml.spark.SparkMLDriver;
import org.apache.lens.ml.spark.trainers.BaseSparkTrainer;
import org.apache.lens.server.api.LensConfConstants;
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

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;

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

  /**
   * Instantiates a new lens ml impl.
   *
   * @param conf
   *          the conf
   */
  public LensMLImpl(HiveConf conf) {
    this.conf = conf;
  }

  public HiveConf getConf() {
    return conf;
  }

  /**
   * Use an existing Spark context. Useful in case of
   *
   * @param jsc
   *          JavaSparkContext instance
   */
  public void setSparkContext(JavaSparkContext jsc) {
    this.sparkContext = jsc;
  }

  public List<String> getAlgorithms() {
    List<String> trainers = new ArrayList<String>();
    for (MLDriver driver : drivers) {
      trainers.addAll(driver.getTrainerNames());
    }
    return trainers;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.LensML#getTrainerForName(java.lang.String)
   */
  public MLTrainer getTrainerForName(String algorithm) throws LensException {
    for (MLDriver driver : drivers) {
      if (driver.isTrainerSupported(algorithm)) {
        return driver.getTrainerInstance(algorithm);
      }
    }
    throw new LensException("Trainer not supported " + algorithm);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.LensML#train(java.lang.String, java.lang.String, java.lang.String[])
   */
  public String train(String table, String algorithm, String[] args) throws LensException {
    MLTrainer trainer = getTrainerForName(algorithm);

    String modelId = UUID.randomUUID().toString();

    LOG.info("Begin training model " + modelId + ", trainer=" + algorithm + ", table=" + table + ", params="
        + Arrays.toString(args));

    String database = null;
    if (SessionState.get() != null) {
      database = SessionState.get().getCurrentDatabase();
    } else {
      database = "default";
    }

    MLModel model = trainer.train(toLensConf(conf), database, table, modelId, args);

    LOG.info("Done training model: " + modelId);

    model.setCreatedAt(new Date());
    model.setTrainerName(algorithm);

    Path modelLocation = null;
    try {
      modelLocation = persistModel(model);
      LOG.info("Model saved: " + modelId + ", trainer: " + algorithm + ", path: " + modelLocation);
      return model.getId();
    } catch (IOException e) {
      throw new LensException("Error saving model " + modelId + " for trainer " + algorithm, e);
    }
  }

  /**
   * Gets the trainer dir.
   *
   * @param trainerName
   *          the trainer name
   * @return the trainer dir
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private Path getTrainerDir(String trainerName) throws IOException {
    String modelSaveBaseDir = conf.get(ModelLoader.MODEL_PATH_BASE_DIR, ModelLoader.MODEL_PATH_BASE_DIR_DEFAULT);
    return new Path(new Path(modelSaveBaseDir), trainerName);
  }

  /**
   * Persist model.
   *
   * @param model
   *          the model
   * @return the path
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private Path persistModel(MLModel model) throws IOException {
    // Get model save path
    Path trainerDir = getTrainerDir(model.getTrainerName());
    FileSystem fs = trainerDir.getFileSystem(conf);

    if (!fs.exists(trainerDir)) {
      fs.mkdirs(trainerDir);
    }

    Path modelSavePath = new Path(trainerDir, model.getId());
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
      Path trainerDir = getTrainerDir(algorithm);
      FileSystem fs = trainerDir.getFileSystem(conf);
      if (!fs.exists(trainerDir)) {
        return null;
      }

      List<String> models = new ArrayList<String>();

      for (FileStatus stat : fs.listStatus(trainerDir)) {
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
   * @param hiveConf
   *          the hive conf
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
  public MLTestReport testModel(LensSessionHandle session, String table, String algorithm, String modelID)
      throws LensException {
    return null;
  }

  /**
   * Test a model in embedded mode.
   *
   * @param sessionHandle
   *          the session handle
   * @param table
   *          the table
   * @param algorithm
   *          the algorithm
   * @param modelID
   *          the model id
   * @param queryApiUrl
   *          the query api url
   * @return the ML test report
   * @throws LensException
   *           the lens exception
   */
  public MLTestReport testModelRemote(LensSessionHandle sessionHandle, String table, String algorithm, String modelID,
      String queryApiUrl) throws LensException {
    return testModel(sessionHandle, table, algorithm, modelID, new RemoteQueryRunner(sessionHandle, queryApiUrl));
  }

  /**
   * Test model.
   *
   * @param sessionHandle
   *          the session handle
   * @param table
   *          the table
   * @param algorithm
   *          the algorithm
   * @param modelID
   *          the model id
   * @param queryRunner
   *          the query runner
   * @return the ML test report
   * @throws LensException
   *           the lens exception
   */
  public MLTestReport testModel(LensSessionHandle sessionHandle, String table, String algorithm, String modelID,
      TestQueryRunner queryRunner) throws LensException {
    // check if algorithm exists
    if (!getAlgorithms().contains(algorithm)) {
      throw new LensException("No such algorithm " + algorithm);
    }

    MLModel model;
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
    final String testTable = "ml_test_" + testID;
    final String testResultColumn = "prediction_result";

    // TODO support error metric UDAFs
    TableTestingSpec spec = TableTestingSpec.newBuilder().hiveConf(conf)
        .database(database == null ? "default" : database).table(table).featureColumns(model.getFeatureColumns())
        .outputColumn(testResultColumn).labeColumn(model.getLabelColumn()).algorithm(algorithm).modelID(modelID)
        .outputTable(testTable).build();
    String testQuery = spec.getTestQuery();

    if (testQuery == null) {
      throw new LensException("Invalid test spec. " + "table=" + table + " algorithm=" + algorithm + " modelID="
          + modelID);
    }

    LOG.info("Running test query " + testQuery);
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
   * @param testReport
   *          the test report
   * @throws LensException
   *           the lens exception
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
    MLTrainer trainer = null;
    try {
      trainer = getTrainerForName(algorithm);
    } catch (LensException e) {
      LOG.error("Error getting algo description : " + algorithm, e);
      return null;
    }
    if (trainer instanceof BaseSparkTrainer) {
      return ((BaseSparkTrainer) trainer).getArgUsage();
    }
    return null;
  }

  /**
   * Submit model test query to a remote Lens server.
   */
  class RemoteQueryRunner extends TestQueryRunner {

    /** The query api url. */
    final String queryApiUrl;

    /**
     * Instantiates a new remote query runner.
     *
     * @param sessionHandle
     *          the session handle
     * @param queryApiUrl
     *          the query api url
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

      LensConf LensConf = new LensConf();
      LensConf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, false + "");
      LensConf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false + "");
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), LensConf,
          MediaType.APPLICATION_XML_TYPE));

      final QueryHandle handle = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
          QueryHandle.class);

      LensQuery ctx = target.path(handle.toString()).queryParam("sessionid", sessionHandle).request()
          .get(LensQuery.class);

      QueryStatus stat = ctx.getStatus();
      while (!stat.isFinished()) {
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
   * @param conf
   *          the conf
   * @return the lens conf
   */
  private LensConf toLensConf(HiveConf conf) {
    LensConf LensConf = new LensConf();
    LensConf.getProperties().putAll(conf.getValByRegex(".*"));
    return LensConf;
  }
}

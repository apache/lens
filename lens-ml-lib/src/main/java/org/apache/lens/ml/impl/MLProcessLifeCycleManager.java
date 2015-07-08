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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.ml.algo.api.Algorithm;
import org.apache.lens.ml.algo.api.MLDriver;
import org.apache.lens.ml.algo.api.TrainedModel;
import org.apache.lens.ml.api.*;
import org.apache.lens.ml.dao.MetaStoreClient;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.QueryExecutionService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;

import lombok.Getter;
import lombok.Setter;

/**
 * MLProcessLifeCycleManager class. Responsible for Life Cycle management of a MLProcess i.e. ModelInstance,
 * Prediction, Evaluation.
 */
public class MLProcessLifeCycleManager {

  /**
   * The Constant LOG.
   */
  public static final Log LOG = LogFactory.getLog(MLProcessLifeCycleManager.class);
  /**
   * Prefix for all MLProcess worker threads.
   */
  private static final String ML_PROCESS_LIFECYCLE_THREAD_PREFIX = "MLProcess-";
  /**
   * Check if the predict UDF has been registered for a user
   */
  private final Map<LensSessionHandle, Boolean> predictUdfStatus;
  /**
   * Runnable for thread responsible for purging completed MLProcesses. and killing threads which have exceeded
   * maximum life time.
   */
  ProcessPurger processPurgerRunnable = new ProcessPurger();
  Thread processPurger = new Thread(processPurgerRunnable, "MLProcessPurger");
  /**
   * Runnable for thread responsible for submitting incoming MLProcesses to the Queue.
   */
  MLProcessSubmitter mlProcessSubmitterRunnable = new MLProcessSubmitter();
  Thread mlProcessSubmitter = new Thread(mlProcessSubmitterRunnable, "MLProcessSubmitter");
  /**
   * boolean for checking whether this LifeCycle is running or not.
   */
  boolean stopped;
  /**
   * The meta store client.
   */
  MetaStoreClient metaStoreClient;

  /**
   * All registered drivers.
   */
  List<MLDriver> drivers;

  /**
   * Map for storing MLProcesses which are submitted, or executing. Once a process is finished purger thread will
   * remove it from this Map after configured time or if a process exceeds it's maximum life time.
   */
  ConcurrentMap<String, MLProcessContext> allProcesses = new ConcurrentHashMap<String, MLProcessContext>();
  private HiveConf conf;
  /**
   * All accepted MLProcesses are put into this queue. MLProcessSubmitter thread waits on this queue. It fetches
   * MLProcesses from here starts its execution.
   */
  private BlockingQueue<MLProcessContext> submittedQueue = new LinkedBlockingQueue<MLProcessContext>();
  /**
   * Executor pool for MLProcess worker threads. i.e. EvaluationCreator, ModelCreator, PredictionCreator.
   */
  private ExecutorService executorPool;

  public MLProcessLifeCycleManager(HiveConf conf, MetaStoreClient metaStoreClient, List<MLDriver> drivers) {
    this.conf = conf;
    this.metaStoreClient = metaStoreClient;
    this.predictUdfStatus = new ConcurrentHashMap<LensSessionHandle, Boolean>();
    this.drivers = drivers;
  }

  /**
   * Initializes MLProcess. Restores previous incomplete MLProcesses
   */
  public void init() {

    try {
      for (MLProcess process : metaStoreClient.getIncompleteEvaluations()) {
        MLProcessContext mlProcessContext = new MLProcessContext(process);
        submittedQueue.add(mlProcessContext);
      }
      LOG.info("Restored old incomplete Evaluations.");
    } catch (Exception e) {
      LOG.error("Error while restoring previous incomplete Evaluations.");
    }

    try {
      for (MLProcess process : metaStoreClient.getIncompleteModelInstances()) {
        MLProcessContext mlProcessContext = new MLProcessContext(process);
        submittedQueue.add(mlProcessContext);
        LOG.info("Restored old incomplete ModelInstances.");
      }
    } catch (Exception e) {
      LOG.error("Error while restoring previous incomplete ModelInstance.");
    }

    try {
      for (MLProcess process : metaStoreClient.getIncompletePredictions()) {
        MLProcessContext mlProcessContext = new MLProcessContext(process);
        submittedQueue.add(mlProcessContext);
        LOG.info("Restored old incomplete Predictions.");
      }
    } catch (Exception e) {
      LOG.error("Error while restoring previous incomplete Predictions.");
    }
    LOG.info("Initialized MLProcessLifeCycle");
  }

  /**
   * Starts the MLProcessLifeCycle Manager
   */
  public void start() {
    stopped = false;
    startExecutorPool();
    mlProcessSubmitter.start();
    processPurger.start();
    LOG.info("Started MLProcessLifeCycle");
  }

  /**
   * Stop teh ML Process Life Cycle Manager
   */
  public void stop() {
    executorPool.shutdown();
    stopped = true;
    LOG.info("Stopped MLProcessLifeCycle");
  }

  public MLProcess getMLProcess(String id) {
    if (allProcesses.containsKey(id)) {
      return allProcesses.get(id).getMlProcess();
    }
    return null;
  }

  private void startExecutorPool() {
    int minPoolSize =
      conf.getInt(MLConfConstants.EXECUTOR_POOL_MIN_THREADS, MLConfConstants.DEFAULT_EXECUTOR_POOL_MIN_THREADS);
    int maxPoolSize = conf.getInt(MLConfConstants.EXECUTOR_POOL_MAX_THREADS, MLConfConstants
      .DEFAULT_EXECUTOR_POOL_MAX_THREADS);

    final ThreadFactory defaultFactory = Executors.defaultThreadFactory();
    final AtomicInteger thId = new AtomicInteger();

    ThreadFactory threadFactory = new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread th = defaultFactory.newThread(r);
        th.setName(ML_PROCESS_LIFECYCLE_THREAD_PREFIX + thId.incrementAndGet());
        return th;
      }
    };

    LOG.debug("starting executor pool");
    ThreadPoolExecutor executorPool =
      new ThreadPoolExecutor(minPoolSize, maxPoolSize, MLConfConstants.DEFAULT_CREATOR_POOL_KEEP_ALIVE_MILLIS,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(), threadFactory);
    this.executorPool = executorPool;
  }

  /**
   * Accepts MLProcesses and adds them to submitted queue.
   *
   * @param mlProcess
   * @throws LensException
   */
  public void addProcess(MLProcess mlProcess) throws LensException {
    MLProcessContext mlProcessContext = new MLProcessContext(mlProcess);
    allProcesses.put(mlProcess.getId(), mlProcessContext);
    submittedQueue.add(mlProcessContext);
    LOG.debug("MLProcess submitted, Id: " + mlProcess.getId());
  }

  /**
   * Cancelles the execution of MLProcess by killing the Executor Thread.
   *
   * @param processId
   * @return
   */
  public boolean cancelProcess(String processId) {
    MLProcessContext mlProcessContext = allProcesses.get(processId);
    if (mlProcessContext != null) {
      setProcessStatusAndFinishTime(Status.CANCELLED, mlProcessContext);
    } else {
      return false;
    }
    if (mlProcessContext.getThread() != null) {
      if (!mlProcessContext.getThread().isDone()) {
        try {
          mlProcessContext.getThread().cancel(true);
        } catch (Exception e) {
          LOG.error("Error in cancelling MLProcess, Id: " + processId);
          return false;
        }
        LOG.debug("MLProcess cancelled, Id: " + processId);
      }
    }
    return true;
  }

  /**
   * Sets MLProcess status. Also updates finish time.
   *
   * @param status
   * @param ctx
   */
  public void setProcessStatusAndFinishTime(Status status, MLProcessContext ctx) {
    synchronized (ctx) {
      ctx.getMlProcess().setStatus(status);
      ctx.getMlProcess().setFinishTime(new Date());
    }
  }

  /**
   * Registers the Predict UDF for a given Lens Session.
   *
   * @param sessionHandle
   * @param queryRunner
   * @throws LensException
   */
  protected void registerPredictUdf(LensSessionHandle sessionHandle, QueryRunner queryRunner) throws LensException {
    if (isUdfRegistered(sessionHandle)) {
      // Already registered, nothing to do
      return;
    }

    LOG.info("Registering UDF for session " + sessionHandle.getPublicId().toString());

    String regUdfQuery = "CREATE TEMPORARY FUNCTION " + MLConfConstants.UDF_NAME + " AS '" + HiveMLUDF.class
      .getCanonicalName() + "'";
    queryRunner.setQueryName("register_predict_udf_" + sessionHandle.getPublicId().toString());
    QueryHandle udfQuery = queryRunner.runQuery(regUdfQuery);
    LOG.info("udf query handle is " + udfQuery);
    predictUdfStatus.put(sessionHandle, true);
    LOG.info("Predict UDF registered for session " + sessionHandle.getPublicId().toString());
  }

  protected boolean isUdfRegistered(LensSessionHandle sessionHandle) {
    return predictUdfStatus.containsKey(sessionHandle);
  }

  /**
   * An ML process is finished if it has status among FAILED, COMPLETED or CANCELLED.
   *
   * @param mlProcess
   * @return
   */
  boolean isFinished(MLProcess mlProcess) {
    Status status = mlProcess.getStatus();
    if (status == Status.FAILED || status == Status.COMPLETED || status == Status.CANCELLED) {
      return true;
    }
    return false;
  }

  /**
   * Returns the Algorithm registered in driver for the name.
   *
   * @param name
   * @return
   * @throws LensException
   */
  public Algorithm getAlgoForName(String name) throws LensException {
    for (MLDriver driver : drivers) {
      if (driver.isAlgoSupported(name)) {
        Algorithm algorithm = driver.getAlgoInstance(name);
        algorithm.configure(toLensConf(conf));
        return algorithm;
      }
    }
    throw new LensException("Algorithm not supported " + name);
  }

  private LensConf toLensConf(HiveConf conf) {
    LensConf lensConf = new LensConf();
    lensConf.getProperties().putAll(conf.getValByRegex(".*"));
    return lensConf;
  }

  /**
   * MLProcessContext class
   */
  public class MLProcessContext {
    @Getter
    @Setter
    MLProcess mlProcess;
    @Getter
    @Setter
    Future thread;

    public MLProcessContext(MLProcess mlProcess) {
      this.mlProcess = mlProcess;
    }

  }

  /**
   * MLProcessSubmitter thread. Responsible for starting one of the executor threads based on the type of MLProcess.
   */
  class MLProcessSubmitter implements Runnable {
    @Override
    public void run() {

      LOG.info("Started Submitter Thread.");
      try {
        while (!stopped) {
          MLProcessContext ctx = submittedQueue.take();
          synchronized (ctx) {
            // Only accept the process with SUBMITTED status. they might be cancelled.
            MLProcess mlProcess = ctx.getMlProcess();
            Runnable creatorThread = null;
            if (mlProcess instanceof ModelInstance) {
              creatorThread = new ModelExecutor(ctx.getMlProcess().getId());
            } else if (mlProcess instanceof Evaluation) {
              creatorThread = new EvaluationExecutor(ctx.getMlProcess().getId());
            } else if (mlProcess instanceof Prediction) {
              creatorThread = new PredictionExecutor(ctx.getMlProcess().getId());
            }
            if (creatorThread != null) {
              Future future = executorPool.submit(creatorThread);
              ctx.setThread(future);
            }

          }
        }
      } catch (InterruptedException ex) {
        LOG.error("Submitter has been interrupted, exiting" + ex.getMessage());
        return;
      } catch (Exception e) {
        LOG.error("Error in submitter", e);
      }
      LOG.info("Submitter exited");
    }
  }

  /**
   * Worker Thread for running an Evaluation process. It generates the target Hive Query which uses the predict udf
   * for predicting. Makes sure the outputTable is present, UDF is registered for current session. Finally runs the
   * generated hive query through Lens Server. On successful completion it sets the status of MLProcess to COMPLETED
   * otherwise FAILED.
   */
  private class EvaluationExecutor implements Runnable {
    String evaluationId;

    public EvaluationExecutor(String evaluationId) {
      this.evaluationId = evaluationId;
    }

    @Override
    public void run() {
      MLProcessContext ctx = null;
      try {
        ctx = allProcesses.get(evaluationId);
        if (ctx.getMlProcess().getStatus() == Status.SUBMITTED) {
          ctx.getMlProcess().setStatus(Status.RUNNING);
          String database = null;
          if (SessionState.get() != null) {
            database = SessionState.get().getCurrentDatabase();
          }

          Evaluation evaluation = (Evaluation) ctx.getMlProcess();

          LensSessionHandle sessionHandle = evaluation.getLensSessionHandle();
          if (sessionHandle == null) {
            LOG.error(
              "Error while running evaluation. Id: " + ctx.getMlProcess().getId() + ". Null session not allowed");
            return;
          }

          final String testResultColumn = "prediction_result";

          ModelInstance modelInstance = metaStoreClient.getModelInstance(evaluation.getModeInstanceId());
          Model model = metaStoreClient.getModel(modelInstance.getModelId());


          String outputTableName =
            (MLConfConstants.EVALUATION_OUTPUT_TABLE_PREFIX + evaluation.getId()).replace("-", "_");
          TableTestingSpec spec = TableTestingSpec.newBuilder().hiveConf(conf)
            .database(database == null ? "default" : database).inputTable(evaluation.getInputDataSetName())
            .featureColumns(model.getFeatureSpec())
            .outputColumn(testResultColumn).lableColumn(model.getLabelSpec())
            .algorithm(model.getAlgoSpec().getAlgo().getName()).modelID(model.getId())
            .modelInstanceID(modelInstance.getId())
            .outputTable(outputTableName).testID(evaluationId).build();

          String testQuery = spec.getTestQuery();
          if (testQuery == null) {
            setProcessStatusAndFinishTime(Status.FAILED, ctx);
            LOG.error("Error while running evaluation. Id: " + ctx.getMlProcess().getId());
            return;
          } else {

            DirectQueryRunner queryRunner = new DirectQueryRunner(sessionHandle);
            if (!spec.isOutputTableExists()) {
              try {
                String createOutputTableQuery = spec.getCreateOutputTableQuery();
                LOG.info("Output table '" + outputTableName + "' does not exist for test algorithm = "
                  + model.getAlgoSpec().getAlgo().getName() + " modelId="
                  + model.getId() + " modelInstanceId= " + modelInstance.getId()
                  + ", Creating table using query: " + spec.getCreateOutputTableQuery());
                queryRunner.runQuery(createOutputTableQuery);
              } catch (LensException e) {
                setProcessStatusAndFinishTime(Status.FAILED, ctx);
                LOG.error("Error while running evaluation. Id: " + ctx.getMlProcess().getId()
                  + ". Unable to create output table" + e.getMessage());
                return;
              }

              try {
                LOG.info("Registering UDF for prediction. Id" + ctx.getMlProcess().getId());
                registerPredictUdf(sessionHandle, queryRunner);
              } catch (LensException e) {
                setProcessStatusAndFinishTime(Status.FAILED, ctx);
                LOG.error("Error while running evaluation. Id: " + ctx.getMlProcess().getId()
                  + ". Unable to register predict UDF" + e.getMessage());
                return;
              }

              try {
                LOG.info("Running Prediction UDF" + ctx.getMlProcess().getId());
                queryRunner.runQuery(testQuery);
              } catch (LensException e) {
                setProcessStatusAndFinishTime(Status.FAILED, ctx);
                LOG.error(
                  "Error while running evaluation. Id: " + ctx.getMlProcess().getId() + ". Unable to run predict UDF"
                    + e.getMessage());
                return;
              }
              setProcessStatusAndFinishTime(Status.COMPLETED, ctx);
            }
          }
        }
      } catch (Exception e) {

        if (ctx == null) {
          LOG.info("Error while running evaluation. Id: " + evaluationId);
          return;
        }
        LOG.info("Error while running evaluation. Id: " + evaluationId);
        setProcessStatusAndFinishTime(Status.FAILED, ctx);
      }
      LOG.info("exiting evaluation creator!");

    }
  }

  /**
   * Worker Thread for Creation of Model Instances. It launches the job for training a Model against the inputTable. On
   * successful completion it sets the status of MLProcess to COMPLETED otherwise FAILED.
   */
  private class ModelExecutor implements Runnable {
    String id;

    public ModelExecutor(String id) {
      this.id = id;
    }

    @Override
    public void run() {
      MLProcessContext ctx;
      try {
        ctx = allProcesses.get(id);
      } catch (NullPointerException ex) {
        LOG.error("");
        return;
      }
      try {
        ModelInstance modelInstance = (ModelInstance) ctx.getMlProcess();
        Model model = metaStoreClient.getModel(modelInstance.getModelId());
        DataSet dataSet = metaStoreClient.getDataSet(modelInstance.getDataSet());

        Algorithm algorithm = getAlgoForName(model.getAlgoSpec().getAlgo().getName());
        TrainedModel trainedModel;
        trainedModel = algorithm.train(model, dataSet);


        Path modelLocation = MLUtils.persistModel(trainedModel, model, modelInstance.getId());
        LOG.info("ModelInstance saved: " + modelInstance.getId() + ", algo: " + algorithm + ", path: "
          + modelLocation);

        setProcessStatusAndFinishTime(Status.COMPLETED, ctx);

      } catch (IOException ex) {
        setProcessStatusAndFinishTime(Status.FAILED, ctx);
        LOG.error("Error saving modelInstance ID: " + ctx.getMlProcess().getId());
      } catch (LensException ex) {
        setProcessStatusAndFinishTime(Status.FAILED, ctx);
        LOG.error("Error training modelInstance ID: " + ctx.getMlProcess().getId());
      } catch (Exception e) {
        LOG.error(e.getMessage());
        setProcessStatusAndFinishTime(Status.FAILED, ctx);
      }
    }
  }

  /**
   * Worker Thread for Batch Prediction process. It generates the target Hive Query which uses the predict udf
   * for prediction. Makes sure the outputTable is present, UDF is registered for current session. Finally runs the
   * generated hive query through Lens Server. On successful completion it sets the status of MLProcess to COMPLETED
   * otherwise FAILED.
   */
  private class PredictionExecutor implements Runnable {
    String predictionId;

    public PredictionExecutor(String predictionId) {
      this.predictionId = predictionId;
    }

    @Override
    public void run() {
      MLProcessContext ctx = null;

      try {
        ctx = allProcesses.get(predictionId);

        String database = null;
        if (SessionState.get() != null) {
          database = SessionState.get().getCurrentDatabase();
        }
        Prediction prediction = (Prediction) ctx.getMlProcess();

        LensSessionHandle sessionHandle = prediction.getLensSessionHandle();
        if (sessionHandle == null) {
          LOG.error("Error while running prediction. Id: " + ctx.getMlProcess().getId() + ". Null session not allowed");
          return;
        }

        ModelInstance modelInstance = metaStoreClient.getModelInstance(prediction.getModelInstanceId());
        Model model = metaStoreClient.getModel(modelInstance.getModelId());

        final String testResultColumn = "prediction_result";

        BatchPredictSpec spec = BatchPredictSpec.newBuilder().hiveConf(conf)
          .database(database == null ? "default" : database).inputTable(prediction.getInputDataSet())
          .featureColumns(model.getFeatureSpec())
          .outputColumn(testResultColumn).algorithm(model.getAlgoSpec().getAlgo().getName())
          .modelID(model.getId()).modelInstanceID(modelInstance.getId())
          .outputTable(prediction.getOutputDataSet()).testID(predictionId).build();

        String testQuery = spec.getTestQuery();
        if (testQuery == null) {
          setProcessStatusAndFinishTime(Status.FAILED, ctx);
          LOG.error("Error while running prediction. Id: " + ctx.getMlProcess().getId());
          return;
        } else {
          DirectQueryRunner queryRunner = new DirectQueryRunner(sessionHandle);
          if (!spec.isOutputTableExists()) {
            try {
              String createOutputTableQuery = spec.getCreateOutputTableQuery();
              LOG.info("Output table '" + prediction.getOutputDataSet()
                + "' does not exist for predicting algorithm = " + model.getAlgoSpec().getAlgo().getName()
                + " modelId="
                + model.getId() + " modelInstanceId= " + modelInstance.getId()
                + ", Creating table using query: " + spec.getCreateOutputTableQuery());
              queryRunner.runQuery(createOutputTableQuery);
            } catch (LensException e) {
              setProcessStatusAndFinishTime(Status.FAILED, ctx);
              LOG.error(
                "Error while running prediction. Id: " + ctx.getMlProcess().getId() + "Unable to create output table"
                  + e.getMessage());
              return;
            }

            try {
              LOG.info("Registering UDF for prediction. Id" + ctx.getMlProcess().getId());
              registerPredictUdf(sessionHandle, queryRunner);
            } catch (LensException e) {
              setProcessStatusAndFinishTime(Status.FAILED, ctx);
              LOG.error(
                "Error while running prediction. Id: " + ctx.getMlProcess().getId()
                  + ". Unable to register prediction UDF" + e.getMessage());
              return;
            }

            try {
              LOG.info("Running Prediction UDF" + ctx.getMlProcess().getId());
              queryRunner.runQuery(testQuery);
            } catch (Exception e) {
              setProcessStatusAndFinishTime(Status.FAILED, ctx);
              LOG.error(
                "Error while running prediction. Id: " + ctx.getMlProcess().getId() + ". Unable to run prediction UDF"
                  + e.getMessage());
              return;
            }
            setProcessStatusAndFinishTime(Status.COMPLETED, ctx);
          }
        }
      } catch (Exception e) {
        if (ctx == null) {
          LOG.error("Error while running prediction. Id: " + predictionId + ", " + e.getMessage());
          return;
        }
        setProcessStatusAndFinishTime(Status.FAILED, ctx);
        LOG.error("Error while running prediction. Id: " + predictionId + ", " + e.getMessage());
      }
      LOG.info("exiting prediction creator!");
    }

  }

  /**
   * DirectQueryRunner class which runs query against the same lens server where ML Service is running.
   */
  private class DirectQueryRunner extends QueryRunner {

    /**
     * Instantiates a new direct query runner.
     *
     * @param sessionHandle the session handle
     */
    public DirectQueryRunner(LensSessionHandle sessionHandle) {
      super(sessionHandle);
    }

    /**
     * @param testQuery
     * @return
     * @throws LensException
     */
    @Override
    public QueryHandle runQuery(String testQuery) throws LensException {
      // Run the query in query executions service
      QueryExecutionService queryService;
      try {
        queryService = (QueryExecutionService) MLUtils.getServiceProvider().getService("query");
      } catch (Exception e) {
        throw new LensException("Error while getting Service Provider");
      }

      LensConf queryConf = new LensConf();
      queryConf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, false + "");
      queryConf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false + "");

      QueryHandle testQueryHandle = queryService.executeAsync(sessionHandle, testQuery, queryConf, queryName);

      // Wait for test query to complete
      LensQuery query = queryService.getQuery(sessionHandle, testQueryHandle);

      LOG.info("Submitted query " + testQueryHandle.getHandleId());
      while (!query.getStatus().finished()) {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          throw new LensException(e);
        }

        query = queryService.getQuery(sessionHandle, testQueryHandle);
      }

      if (query.getStatus().getStatus() != QueryStatus.Status.SUCCESSFUL) {
        throw new LensException("Failed to run test query: " + testQueryHandle.getHandleId() + " reason= "
          + query.getStatus().getErrorMessage());
      }

      return testQueryHandle;
    }
  }

  /**
   * Process Purger Thread. Removes processes from in memory cache after MLConfConstants.ML_PROCESS_CACHE_LIFE time.
   * Also kills a process if it exceeds MLConfConstants.ML_PROCESS_MAX_LIFE.
   */
  private class ProcessPurger implements Runnable {
    @Override
    public void run() {
      Set<String> keys = allProcesses.keySet();
      for (String key : keys) {
        MLProcessContext ctx = allProcesses.get(key);
        MLProcess mlProcess = ctx.getMlProcess();
        long maxQueryLife = conf.getLong(MLConfConstants.ML_PROCESS_MAX_LIFE, MLConfConstants
          .DEFAULT_ML_PROCESS_MAX_LIFE);

        if (isFinished(mlProcess)) {
          long cacheLife = conf.getLong(MLConfConstants.ML_PROCESS_CACHE_LIFE, MLConfConstants
            .DEFAULT_ML_PROCESS_CACHE_LIFE);
          if ((new Date().getTime() - mlProcess.getFinishTime().getTime()) > cacheLife) {
            try {
              updateMLProcess(mlProcess);
            } catch (Exception e) {
              LOG.error("Error while persisting MLProcess to meta store, Id: " + mlProcess.getId());
            }
          }
        } else if ((new Date().getTime() - mlProcess.getFinishTime().getTime()) > maxQueryLife) {
          // Kill the thread
          try {
            Future thread = ctx.getThread();
            if (!thread.isDone()) {
              thread.cancel(true);
            }
            mlProcess.setFinishTime(new Date());
            mlProcess.setStatus(Status.FAILED);
            updateMLProcess(mlProcess);
          } catch (LensException e) {
            LOG.error("Error while persisting MLProcess to meta store, Id: " + mlProcess.getId());
          } catch (Exception e) {
            LOG.error("Error while cancelling MLProcess, Id: " + mlProcess.getId());
          }
        }
      }
    }

    void updateMLProcess(MLProcess mlProcess) throws LensException {
      if (mlProcess instanceof Prediction) {
        metaStoreClient.updatePrediction((Prediction) mlProcess);
      } else if (mlProcess instanceof Evaluation) {
        metaStoreClient.updateEvaluation((Evaluation) mlProcess);
      } else if (mlProcess instanceof ModelInstance) {
        metaStoreClient.updateModelInstance((ModelInstance) mlProcess);
      }
    }
  }
}

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
package org.apache.lens.server.ml;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensException;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.ml.*;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.ServiceProvider;
import org.apache.lens.server.api.ServiceProviderFactory;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.CompositeService;
import org.apache.lens.server.ml.MLService;

import java.util.*;

/**
 * The Class MLServiceImpl.
 */
public class MLServiceImpl extends CompositeService implements MLService {

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(LensMLImpl.class);

  /** The ml. */
  private LensMLImpl ml;

  /** The service provider. */
  private ServiceProvider serviceProvider;

  /** The service provider factory. */
  private ServiceProviderFactory serviceProviderFactory;

  /**
   * Instantiates a new ML service impl.
   */
  public MLServiceImpl() {
    this(NAME);
  }

  /**
   * Instantiates a new ML service impl.
   *
   * @param name
   *          the name
   */
  public MLServiceImpl(String name) {
    super(name);
  }

  @Override
  public List<String> getAlgorithms() {
    return ml.getAlgorithms();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.LensML#getTrainerForName(java.lang.String)
   */
  @Override
  public MLTrainer getTrainerForName(String algorithm) throws LensException {
    return ml.getTrainerForName(algorithm);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.LensML#train(java.lang.String, java.lang.String, java.lang.String[])
   */
  @Override
  public String train(String table, String algorithm, String[] args) throws LensException {
    return ml.train(table, algorithm, args);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.LensML#getModels(java.lang.String)
   */
  @Override
  public List<String> getModels(String algorithm) throws LensException {
    return ml.getModels(algorithm);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.LensML#getModel(java.lang.String, java.lang.String)
   */
  @Override
  public MLModel getModel(String algorithm, String modelId) throws LensException {
    return ml.getModel(algorithm, modelId);
  }

  private ServiceProvider getServiceProvider() {
    if (serviceProvider == null) {
      serviceProvider = serviceProviderFactory.getServiceProvider();
    }
    return serviceProvider;
  }

  /**
   * Gets the service provider factory.
   *
   * @param conf
   *          the conf
   * @return the service provider factory
   */
  private ServiceProviderFactory getServiceProviderFactory(HiveConf conf) {
    Class<?> spfClass = conf.getClass(LensConfConstants.SERVICE_PROVIDER_FACTORY, ServiceProviderFactory.class);
    try {
      return (ServiceProviderFactory) spfClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hive.service.CompositeService#init(org.apache.hadoop.hive.conf.HiveConf)
   */
  @Override
  public synchronized void init(HiveConf hiveConf) {
    ml = new LensMLImpl(hiveConf);
    ml.init(hiveConf);
    super.init(hiveConf);
    serviceProviderFactory = getServiceProviderFactory(hiveConf);
    LOG.info("Inited ML service");
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hive.service.CompositeService#start()
   */
  @Override
  public synchronized void start() {
    ml.start();
    super.start();
    LOG.info("Started ML service");
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hive.service.CompositeService#stop()
   */
  @Override
  public synchronized void stop() {
    ml.stop();
    super.stop();
    LOG.info("Stopped ML service");
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
  @Override
  public String getModelPath(String algorithm, String modelID) {
    return ml.getModelPath(algorithm, modelID);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.LensML#testModel(org.apache.lens.api.LensSessionHandle, java.lang.String, java.lang.String,
   * java.lang.String)
   */
  @Override
  public MLTestReport testModel(LensSessionHandle sessionHandle, String table, String algorithm, String modelID)
      throws LensException {

    return ml.testModel(sessionHandle, table, algorithm, modelID, new DirectQueryRunner(sessionHandle));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.LensML#getTestReports(java.lang.String)
   */
  @Override
  public List<String> getTestReports(String algorithm) throws LensException {
    return ml.getTestReports(algorithm);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.LensML#getTestReport(java.lang.String, java.lang.String)
   */
  @Override
  public MLTestReport getTestReport(String algorithm, String reportID) throws LensException {
    return ml.getTestReport(algorithm, reportID);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.LensML#predict(java.lang.String, java.lang.String, java.lang.Object[])
   */
  @Override
  public Object predict(String algorithm, String modelID, Object[] features) throws LensException {
    return ml.predict(algorithm, modelID, features);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.LensML#deleteModel(java.lang.String, java.lang.String)
   */
  @Override
  public void deleteModel(String algorithm, String modelID) throws LensException {
    ml.deleteModel(algorithm, modelID);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.LensML#deleteTestReport(java.lang.String, java.lang.String)
   */
  @Override
  public void deleteTestReport(String algorithm, String reportID) throws LensException {
    ml.deleteTestReport(algorithm, reportID);
  }

  /**
   * Run the test model query directly in the current lens server process.
   */
  private class DirectQueryRunner extends TestQueryRunner {

    /**
     * Instantiates a new direct query runner.
     *
     * @param sessionHandle
     *          the session handle
     */
    public DirectQueryRunner(LensSessionHandle sessionHandle) {
      super(sessionHandle);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.lens.ml.TestQueryRunner#runQuery(java.lang.String)
     */
    @Override
    public QueryHandle runQuery(String testQuery) throws LensException {
      // Run the query in query executions service
      QueryExecutionService queryService = (QueryExecutionService) getServiceProvider().getService("query");

      LensConf queryConf = new LensConf();
      queryConf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, false + "");
      queryConf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false + "");

      QueryHandle testQueryHandle = queryService.executeAsync(sessionHandle, testQuery, queryConf, "ml_test_query");

      // Wait for test query to complete
      LensQuery query = queryService.getQuery(sessionHandle, testQueryHandle);
      LOG.info("Submitted query " + testQueryHandle.getHandleId());
      while (!query.getStatus().isFinished()) {
        try {
          Thread.sleep(500);
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

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.LensML#getAlgoParamDescription(java.lang.String)
   */
  @Override
  public Map<String, String> getAlgoParamDescription(String algorithm) {
    return ml.getAlgoParamDescription(algorithm);
  }
}

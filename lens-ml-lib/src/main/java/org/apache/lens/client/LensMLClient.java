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
package org.apache.lens.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.ml.api.*;
import org.apache.lens.ml.server.MLService;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.ServiceProvider;
import org.apache.lens.server.api.ServiceProviderFactory;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;

public class LensMLClient implements LensML, Closeable {
  private static final Log LOG = LogFactory.getLog(LensMLClient.class);
  private static final HiveConf HIVE_CONF;

  static {
    HIVE_CONF = new HiveConf();
    // Add default config so that we know the service provider implementation
    HIVE_CONF.addResource("lensserver-default.xml");
    HIVE_CONF.addResource("lens-site.xml");
  }

  /**
   * The ml service.
   */
  MLService mlService;
  /**
   * The service provider.
   */
  ServiceProvider serviceProvider;
  /**
   * The service provider factory.
   */
  ServiceProviderFactory serviceProviderFactory;
  /**
   * The client.
   */
  private LensMLJerseyClient client;

  /**
   * Instantiates a new ML service resource.
   */
  public LensMLClient() {

  }

  public LensMLClient(String password) {
    this(new LensClientConfig(), password);
  }

  public LensMLClient(LensClientConfig conf, String password) {
    this(conf, conf.getUser(), password);
  }

  public LensMLClient(String username, String password) {
    this(new LensClientConfig(), username, password);
  }

  public LensMLClient(LensClientConfig conf, String username, String password) {
    this(new LensClient(conf, username, password));
  }

  public LensMLClient(LensClient lensClient) {
    client = new LensMLJerseyClient(lensClient.getConnection(), lensClient.getConnection().getSessionHandle());
    serviceProviderFactory = getServiceProviderFactory(HIVE_CONF);
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
   * @param conf the conf
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

  private MLService getMlService() {
    if (mlService == null) {
      mlService = (MLService) getServiceProvider().getService(MLService.NAME);
    }
    return mlService;
  }

  @Override
  public void close() throws IOException {
    client.close();
  }

  @Override
  public List<Algo> getAlgos() {
    return getMlService().getAlgos();
  }

  public List<String> getAlgoNames() {
    return null;
  }

  @Override
  public Algo getAlgo(String name) throws LensException {
    return null;
  }

  @Override
  public void createDataSet(String name, String dataTable, String dataBase) throws LensException {

    client.createDataSet(name, dataTable, dataBase);
  }

  public void createDataSet(DataSet dataSet) throws LensException {

  }

  public void test() {
    client.test();
  }

  @Override
  public String createDataSetFromQuery(String name, String query) {
    return null;
  }

  @Override
  public DataSet getDataSet(String name) throws LensException {
    return client.getDataSet(name);
  }

  @Override
  public void createModel(String name, String algo, Map<String, String> algoParams, List<Feature> features,
                          Feature label, LensSessionHandle lensSessionHandle) throws LensException {
    APIResult result = client.createModel(name, algo, algoParams, features, label);
  }

  @Override
  public void createModel(Model model) throws LensException {

  }

  @Override
  public Model getModel(String modelId) throws LensException {
    return client.getModel(modelId);
  }

  @Override
  public String trainModel(String modelId, String dataSetName, LensSessionHandle lensSessionHandle)
    throws LensException {
    return client.tranModel(modelId, dataSetName, lensSessionHandle);
  }

  @Override
  public ModelInstance getModelInstance(String modelInstanceId) throws LensException {
    return client.getModelInstance(modelInstanceId);
  }

  @Override
  public List<ModelInstance> getAllModelInstances(String modelId) {
    return null;
  }

  @Override
  public String evaluate(String modelInstanceId, String dataSetName, LensSessionHandle lensSessionHandle)
    throws LensException {
    return client.evaluate(modelInstanceId, dataSetName, lensSessionHandle);
  }

  @Override
  public Evaluation getEvaluation(String evalId) throws LensException {
    return client.getEvaluation(evalId);
  }

  @Override
  public String predict(String modelInstanceId, String dataSetName, LensSessionHandle lensSessionHandle)
    throws LensException {
    return client.predict(modelInstanceId, dataSetName, lensSessionHandle);
  }

  @Override
  public boolean cancelModelInstance(String modelInstanceId, LensSessionHandle lensSessionHandle) throws LensException {
    return client.cancelModelInstance(modelInstanceId, lensSessionHandle);
  }

  @Override
  public boolean cancelEvaluation(String evalId, LensSessionHandle lensSessionHandle) throws LensException {
    return client.cancelEvaluation(evalId, lensSessionHandle);
  }

  @Override
  public boolean cancelPrediction(String predicitonId, LensSessionHandle lensSessionHandle) throws LensException {
    return client.cancelPrediction(predicitonId, lensSessionHandle);
  }

  @Override
  public Prediction getPrediction(String predictionId) throws LensException {
    return client.getPrediction(predictionId);
  }

  @Override
  public String predict(String modelInstanceId, Map<String, String> featureVector) throws LensException {
    return getMlService().predict(modelInstanceId, featureVector);
  }

  @Override
  public void deleteModel(String modelId) throws LensException {
    client.deleteModel(modelId);
  }

  @Override
  public void deleteDataSet(String dataSetName) throws LensException {
    client.deleteDataSet(dataSetName);
  }

  @Override
  public void deleteModelInstance(String modelInstanceId) throws LensException {
    client.deleteModelInstance(modelInstanceId);
  }

  @Override
  public void deleteEvaluation(String evaluationId) throws LensException {
    client.deleteEvaluation(evaluationId);
  }

  @Override
  public void deletePrediction(String predictionId) throws LensException {
    client.deletePrediction(predictionId);
  }

  public LensSessionHandle getSessionHandle() {
    return client.getSessionHandle();
  }
}

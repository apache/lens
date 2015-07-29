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

import java.util.List;
import java.util.Map;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.StringList;
import org.apache.lens.ml.api.*;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.glassfish.jersey.media.multipart.MultiPartFeature;

import lombok.extern.slf4j.Slf4j;

/*
 * Client code to invoke server side ML API
 */

/**
 * The Class LensMLJerseyClient.
 */
@Slf4j
public class LensMLJerseyClient {

  /**
   * The Constant LENS_ML_RESOURCE_PATH.
   */
  public static final String LENS_ML_RESOURCE_PATH = "lens.ml.resource.path";

  /**
   * The Constant DEFAULT_ML_RESOURCE_PATH.
   */
  public static final String DEFAULT_ML_RESOURCE_PATH = "ml";

  /**
   * The Constant LOG.
   */
  public static final Log LOG = LogFactory.getLog(LensMLJerseyClient.class);

  /**
   * The connection.
   */
  private final LensConnection connection;

  private final LensSessionHandle sessionHandle;

  /**
   * Instantiates a new lens ml jersey client.
   *
   * @param connection the connection
   */
  public LensMLJerseyClient(LensConnection connection, String password) {
    this.connection = connection;
    connection.open(password);
    this.sessionHandle = null;
  }

  /**
   * Instantiates a new lens ml jersey client.
   *
   * @param connection the connection
   */
  public LensMLJerseyClient(LensConnection connection, LensSessionHandle sessionHandle) {
    this.connection = connection;
    this.sessionHandle = sessionHandle;
  }

  public void close() {
    try {
      connection.close();
    } catch (Exception exc) {
      LOG.error("Error closing connection", exc);
    }
  }

  protected WebTarget getMLWebTarget() {
    Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).build();
    LensConnectionParams connParams = connection.getLensConnectionParams();
    String baseURI = connParams.getBaseConnectionUrl();
    String mlURI = connParams.getConf().get(LENS_ML_RESOURCE_PATH, DEFAULT_ML_RESOURCE_PATH);
    return client.target(baseURI).path(mlURI);
  }

  public List<String> getAlgoNames() {
    StringList algoNames = getMLWebTarget().path("algonames").request().get(StringList.class);
    return algoNames.getElements() == null ? null : algoNames.getElements();
  }

  public LensSessionHandle getSessionHandle() {
    if (sessionHandle != null) {
      return sessionHandle;
    }
    return connection.getSessionHandle();
  }


  public void test() {

    Evaluation result = getMLWebTarget().path("evaluation")
      .request(MediaType.APPLICATION_XML)
      .get(Evaluation.class);

  }

  public APIResult createDataSet(String dataSetName, String dataTableName, String dataBase) {
    WebTarget target = getMLWebTarget();
    DataSet dataSet = new DataSet(dataSetName, dataTableName, dataBase);
    APIResult result = target.path("dataset")
      .request(MediaType.APPLICATION_XML)
      .post(Entity.xml(dataSet), APIResult.class);
    return result;
  }

  public DataSet getDataSet(String dataSetName) {
    WebTarget target = getMLWebTarget();
    DataSet dataSet = target.path("dataset").queryParam("dataSetName", dataSetName).request(MediaType
      .APPLICATION_XML).get(DataSet.class);
    return dataSet;
  }

  public APIResult createModel(String name, String algo, Map<String, String> algoParams,
                               List<Feature> features, Feature label) {
    Model model = new Model(name, new AlgoSpec(algo, algoParams), features, label);
    WebTarget target = getMLWebTarget();
    APIResult result =
      target.path("models").request(MediaType.APPLICATION_XML).post(Entity.xml(model), APIResult.class);
    return result;
  }

  public Model getModel(String modelName) {
    WebTarget target = getMLWebTarget();
    Model model = target.path("models").queryParam("modelName", modelName).request(MediaType.APPLICATION_XML)
      .get(Model.class);
    return model;
  }

  public String tranModel(String modelId, String dataSetName, LensSessionHandle lensSessionHandle) {
    WebTarget target = getMLWebTarget();
    return target.path("train").queryParam("modelId", modelId).queryParam("dataSetName",
      dataSetName).queryParam("lensSessionHandle", lensSessionHandle.toString()).request(MediaType.APPLICATION_XML)
      .get(String.class);
  }

  ModelInstance getModelInstance(String modelInstanceId) {
    WebTarget target = getMLWebTarget();
    return target.path("modelinstance/" + modelInstanceId).request(MediaType.APPLICATION_XML).get(ModelInstance.class);
  }

  boolean cancelModelInstance(String modelInstanceId, LensSessionHandle lensSessionHandle) {
    WebTarget target = getMLWebTarget();
    return target.path("modelinstance/" + modelInstanceId).queryParam("lensSessionHandle", lensSessionHandle.toString())
      .request(MediaType.APPLICATION_XML).delete(Boolean.class);
  }

  String evaluate(String modelInstanceId, String dataSetName, LensSessionHandle lensSessionHandle) {
    WebTarget target = getMLWebTarget();
    return target.path("evaluate").queryParam("modelInstanceId", modelInstanceId).queryParam("dataSetName",
      dataSetName).queryParam("lensSessionHandle", lensSessionHandle.toString()).request(MediaType.APPLICATION_XML)
      .get(String.class);
  }

  Evaluation getEvaluation(String evalId) {
    WebTarget target = getMLWebTarget();
    return target.path("evaluation/" + evalId).request(MediaType.APPLICATION_XML).get(Evaluation.class);
  }

  boolean cancelEvaluation(String evalId, LensSessionHandle lensSessionHandle) {
    WebTarget target = getMLWebTarget();
    APIResult result = target.path("evaluation/" + evalId).queryParam("lensSessionHandle", lensSessionHandle.toString())
      .request(MediaType.APPLICATION_XML).delete(APIResult.class);
    return result.getStatus() == APIResult.Status.SUCCEEDED;
  }

  String predict(String modelInstanceId, String dataSetName, LensSessionHandle lensSessionHandle) {
    WebTarget target = getMLWebTarget();
    return target.path("predict").queryParam("modelInstanceId", modelInstanceId).queryParam("dataSetName",
      dataSetName).queryParam("lensSessionHandle", lensSessionHandle.toString()).request(MediaType.APPLICATION_XML)
      .get(String.class);
  }

  Prediction getPrediction(String predictionId) {
    WebTarget target = getMLWebTarget();
    return target.path("prediction/" + predictionId).request(MediaType.APPLICATION_XML).get(Prediction.class);
  }

  boolean cancelPrediction(String predictionId, LensSessionHandle lensSessionHandle) {
    WebTarget target = getMLWebTarget();
    return target.path("prediction/" + predictionId).queryParam("lensSessionHandle", lensSessionHandle.toString())
      .request(MediaType.APPLICATION_XML).delete(Boolean.class);

  }

  public void deleteModel(String modelId) throws LensException {
    WebTarget target = getMLWebTarget();
    target.path("model/" + modelId).request(MediaType.APPLICATION_XML).delete();
  }

  public void deleteDataSet(String dataSetName) throws LensException {
    WebTarget target = getMLWebTarget();
  }

  public void deleteModelInstance(String modelInstanceId) throws LensException {
    WebTarget target = getMLWebTarget();
  }

  public void deleteEvaluation(String evaluationId) throws LensException {
    WebTarget target = getMLWebTarget();
  }

  public void deletePrediction(String predictionId) throws LensException {
    WebTarget target = getMLWebTarget();
  }

}

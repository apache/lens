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
package org.apache.lens.ml.api;

import java.util.List;
import java.util.Map;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.server.api.error.LensException;

/**
 * LensML interface to train and evaluate Machine learning models.
 */
public interface LensML {

  /**
   * Name of ML service
   */
  String NAME = "ml";

  /**
   * @return the list of supported Algos.
   */
  List<Algo> getAlgos();

  /**
   * Get Algo given an algo name.
   *
   * @param name of the Algo
   * @return Algo definition
   */
  Algo getAlgo(String name) throws LensException;

  /**
   * Creates a data set from an existing table
   *
   * @param name      the name of the data set
   * @param dataTable the data table
   * @return
   */
  void createDataSet(String name, String dataTable, String dataBase) throws LensException;

  void createDataSet(DataSet dataSet) throws LensException;

  /**
   * Creates a data set from a query
   *
   * @param name  the name of the data set
   * @param query query
   * @return
   */
  String createDataSetFromQuery(String name, String query);

  /**
   * Returns the data set given the name
   *
   * @param name the name of the data set
   * @return
   */
  DataSet getDataSet(String name) throws LensException;

  void deleteDataSet(String dataSetName) throws LensException;

  /**
   * Creates a Model with a chosen Algo and its parameters, feature list and the label.
   *
   * @param name       the name of Model
   * @param algo       the name of the Alog
   * @param algoParams the algo parameters
   * @param features   list of features
   * @param label      the label to use
   * @returns Model id of the created model
   */
  void createModel(String name, String algo, Map<String, String> algoParams,
                   List<Feature> features, Feature label, LensSessionHandle lensSessionHandle) throws LensException;

  void createModel(Model model) throws LensException;

  /**
   * Get Model given a modelId
   *
   * @param modelId the id of the model
   * @return the model
   */
  Model getModel(String modelId) throws LensException;

  void deleteModel(String modelId) throws LensException;

  /**
   * Train a model. This calls returns immediately after triggering the training
   * asynchronously, the readiness of the ModelInstance should be checked based on its status.
   *
   * @param modelId     the model id
   * @param dataSetName data set name to use
   * @return ModelInstance id the handle to the ModelInstance instance
   */
  String trainModel(String modelId, String dataSetName, LensSessionHandle lensSessionHandle) throws LensException;

  /**
   * Get Trained Model
   *
   * @param modelInstanceId the id of the ModelInstance
   * @return Trained model
   */
  ModelInstance getModelInstance(String modelInstanceId) throws LensException;

  /**
   * Cancels the creation of modelInstance.
   *
   * @param modelInstanceId
   * @return true on successful cancellation false otherwise.
   */
  boolean cancelModelInstance(String modelInstanceId, LensSessionHandle lensSessionHandle) throws LensException;

  void deleteModelInstance(String modelInstanceId) throws LensException;


  /**
   * Get the list of ModelInstance for a given model
   *
   * @param modelId the model id
   * @return List of trained models
   */
  List<ModelInstance> getAllModelInstances(String modelId);

  /**
   * Evaluate a ModelInstance. This calls returns immediately after triggering the training
   * asynchronously, the readiness of the Evaluation should be checked based on its status.
   *
   * @param modelInstanceId the trained model id
   * @param dataSetName     the data to use to evaluate
   * @return the evaluationId
   */
  String evaluate(String modelInstanceId, String dataSetName, LensSessionHandle lensSessionHandle) throws LensException;

  /**
   * Get Evaluation
   *
   * @param evalId
   * @return the evaluation
   */
  Evaluation getEvaluation(String evalId) throws LensException;

  void deleteEvaluation(String evaluationId) throws LensException;

  /**
   * Cancels the Evaluation
   *
   * @param evalId
   * @return true on successful cancellation false otherwise.
   */
  boolean cancelEvaluation(String evalId, LensSessionHandle lensSessionHandle) throws LensException;

  /**
   * Batch predicts for a given data set. This calls returns immediately after triggering the prediction
   * asynchronously, the readiness of the prediction should be checked based on its status.
   *
   * @param modelInstanceId
   * @param dataSetName
   * @return prediction id
   */
  String predict(String modelInstanceId, String dataSetName, LensSessionHandle lensSessionHandle) throws LensException;

  /**
   * Get BatchPrediction information
   *
   * @param predictionId
   * @return
   */
  Prediction getPrediction(String predictionId) throws LensException;

  void deletePrediction(String predictionId) throws LensException;


  /**
   * Cancels the Prediction
   *
   * @param predictionId
   * @return true on successful cancellation false otherwise.
   */
  boolean cancelPrediction(String predictionId, LensSessionHandle lensSessionHandle) throws LensException;

  /**
   * Predict for a given feature vector
   *
   * @param modelInstanceId
   * @param featureVector   the key is feature name.
   * @return predicted value
   */
  String predict(String modelInstanceId, Map<String, String> featureVector) throws LensException;

}

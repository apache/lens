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
package org.apache.lens.ml.dao;

import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.ml.api.*;
import org.apache.lens.server.api.error.LensException;

public interface MetaStoreClient {

  void init();

  /**
   * @param dataSet
   * @throws LensException
   */

  void createDataSet(DataSet dataSet) throws SQLException;

  /**
   * Creates a data set
   *
   * @param name
   * @return
   */
  DataSet getDataSet(String name) throws SQLException;

  void deleteDataSet(String dataSetName) throws SQLException;

  /**
   * @param name
   * @param algo
   * @param algoSpec
   * @param features
   * @param label
   * @return same Id if
   * @throws LensException
   */
  void createModel(String name, String algo, AlgoSpec algoSpec,
                   List<Feature> features, Feature label) throws LensException;


  void createModel(Model model) throws LensException;

  /**
   * Retrieves Model
   *
   * @param modelId
   * @return
   * @throws LensException If model not present or meta store error
   */
  Model getModel(String modelId) throws LensException;

  void deleteModel(String modelId) throws SQLException;

  /**
   * creates model instance
   *
   * @param startTime
   * @param finishTime
   * @param status
   * @param lensSessionHandle
   * @param modelId
   * @param dataSet
   * @param path
   * @param evaluationId
   * @return
   * @throws LensException
   */
  String createModelInstance(Date startTime, Date finishTime, Status status, LensSessionHandle lensSessionHandle,
                             String modelId, String dataSet, String path, String evaluationId)
    throws LensException;

  /**
   * Updates the model instance
   *
   * @param modelInstance
   * @throws LensException If modelInstance not already present in DB or meta store error.
   */
  void updateModelInstance(ModelInstance modelInstance) throws LensException;

  void deleteModelInstance(String modelInstanceId) throws SQLException;

  /**
   * Return list of all ModelInstances in meta store having Status other than COMPLETED, FAILED, CANCELLED
   *
   * @return
   * @throws LensException On meta store error.
   */
  List<ModelInstance> getIncompleteModelInstances() throws LensException;

  /**
   * Return list of all Evaluation in meta store having Status other than COMPLETED, FAILED, CANCELLED
   *
   * @return
   * @throws LensException On meta store error.
   */
  List<Evaluation> getIncompleteEvaluations() throws LensException;

  /**
   * Return list of all Prediction in meta store having Status other than COMPLETED, FAILED, CANCELLED
   *
   * @return
   * @throws LensException On meta store error.
   */
  List<Prediction> getIncompletePredictions() throws LensException;

  /**
   * Returns all ModelInstances for modelId
   *
   * @param modelId
   * @return
   * @throws LensException If modelId is not present or meta store error.
   */
  List<ModelInstance> getModelInstances(String modelId) throws SQLException;

  /**
   * Returns all Evaluations for modelInstanceId
   *
   * @param modelInstanceId
   * @return
   * @throws LensException
   */
  List<Evaluation> getEvaluations(String modelInstanceId) throws SQLException;

  void deleteEvaluation(String evaluationId) throws SQLException;

  /**
   * Returns all Prediction for modelInstanceId
   *
   * @param modelInstanceId
   * @return
   * @throws LensException
   */
  List<Prediction> getPredictions(String modelInstanceId) throws SQLException;

  /**
   * @param modelInstanceId
   * @return
   * @throws LensException If modelInstanceId is not present
   */
  ModelInstance getModelInstance(String modelInstanceId) throws LensException;

  /**
   * Creates Prediction
   *
   * @param startTime
   * @param finishTime
   * @param status
   * @param lensSessionHandle
   * @param modelInstanceId
   * @param inputDataSet
   * @param outputDataSet
   * @return predictionId
   * @throws LensException
   */
  String createPrediction(Date startTime, Date finishTime, Status status, LensSessionHandle lensSessionHandle,
                          String modelInstanceId, String inputDataSet, String outputDataSet) throws LensException;

  void deletePrediction(String predictionId) throws SQLException;

  /**
   * gets Prediction
   *
   * @param predictionId
   * @return
   * @throws LensException If prediction Id is not present or meta store error
   */
  Prediction getPrediction(String predictionId) throws LensException;

  /**
   * Updates Evaluation
   *
   * @param evaluation
   * @throws LensException If evaluation not already present in DB or meta store error.
   */
  void updateEvaluation(Evaluation evaluation) throws LensException;

  /**
   * Creates Evaluation
   *
   * @param startTime
   * @param finishTime
   * @param status
   * @param lensSessionHandle
   * @param modelInstanceId
   * @param inputDataSetName
   * @return evaluationId
   * @throws LensException
   */
  String createEvaluation(Date startTime, Date finishTime, Status status, LensSessionHandle lensSessionHandle,
                          String
                            modelInstanceId, String inputDataSetName) throws LensException;

  /**
   * gets evaluation
   *
   * @param evaluationId
   * @return
   * @throws LensException If evaluationId is not present in meta store.
   */
  Evaluation getEvaluation(String evaluationId) throws LensException;

  /**
   * Updates prediction
   *
   * @param prediction
   * @throws LensException If evaluation not already present in DB or meta store error.
   */
  void updatePrediction(Prediction prediction) throws LensException;

}

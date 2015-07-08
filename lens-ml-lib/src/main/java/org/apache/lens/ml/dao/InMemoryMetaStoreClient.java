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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.ml.api.*;
import org.apache.lens.server.api.error.LensException;

public class InMemoryMetaStoreClient implements MetaStoreClient {

  ConcurrentMap<String, Model> modelConcurrentMap = new ConcurrentHashMap<String, Model>();
  ConcurrentMap<String, ModelInstance> modelInstanceConcurrentMap = new ConcurrentHashMap<String, ModelInstance>();
  ConcurrentMap<String, Evaluation> evaluationConcurrentMap = new ConcurrentHashMap<String, Evaluation>();
  ConcurrentMap<String, Prediction> predictionConcurrentMap = new ConcurrentHashMap<String, Prediction>();
  ConcurrentMap<String, DataSet> dataSetConcurrentMap = new ConcurrentHashMap();

  @Override
  public DataSet createDataSet(String name, String dataTable, String dataBase) throws LensException {
    DataSet dataSet = new DataSet(name, dataTable, dataBase);
    dataSetConcurrentMap.put(name, dataSet);
    return dataSet;
  }

  @Override
  public DataSet getDataSet(String name) {
    return dataSetConcurrentMap.get(name);
  }

  @Override
  public Model createModel(String name, String algo, AlgoSpec algoSpec, List<Feature> features, Feature label) throws
    LensException {
    String id = UUID.randomUUID().toString();
    while (modelConcurrentMap.containsKey(id)) {
      id = UUID.randomUUID().toString();
    }

    Model model = new Model(id, name, algoSpec, features, label);
    modelConcurrentMap.put(id, model);
    return model;
  }

  @Override
  public Model getModel(String modelId) throws LensException {
    return modelConcurrentMap.get(modelId);
  }

  @Override
  public ModelInstance createModelInstance(Date startTime, Date finishTime, Status status,
                                           LensSessionHandle lensSessionHandle,
                                           String modelId, String dataSet, String path, String evaluationId)
    throws LensException {
    String id = UUID.randomUUID().toString();
    while (modelInstanceConcurrentMap.containsKey(id)) {
      id = UUID.randomUUID().toString();
    }
    ModelInstance modelInstance = new ModelInstance(id, startTime, finishTime, status, lensSessionHandle, modelId,
      dataSet, path, evaluationId);
    modelInstanceConcurrentMap.put(modelInstance.getId(), modelInstance);
    return modelInstance;
  }

  @Override
  public void updateModelInstance(ModelInstance modelInstance) throws LensException {
    if (modelConcurrentMap.keySet().contains(modelInstance.getId())) {
      ModelInstance modelInstanceDb = modelInstanceConcurrentMap.get(modelInstance.getId());
      modelInstanceDb.setStatus(modelInstance.getStatus());
      modelInstanceDb.setFinishTime(modelInstance.getFinishTime());
    }
  }

  @Override
  public ModelInstance getModelInstance(String modelInstanceId) throws LensException {
    ModelInstance modelInstance = modelInstanceConcurrentMap.get(modelInstanceId);
    return modelInstance;
  }


  public Evaluation getEvaluation(String evaluationId) throws LensException {
    return evaluationConcurrentMap.get(evaluationId);
  }

  @Override
  public void updateEvaluation(Evaluation evaluation) throws LensException {
    if (modelConcurrentMap.keySet().contains(evaluation.getId())) {
      Evaluation evaluationDb = evaluationConcurrentMap.get(evaluation.getId());
      evaluationDb.setStatus(evaluation.getStatus());
      evaluationDb.setFinishTime(evaluation.getFinishTime());
    }
  }

  @Override
  public List<ModelInstance> getIncompleteModelInstances() {
    List<ModelInstance> incompleteModelInstances = new ArrayList<>();
    for (ModelInstance modelInstance : modelInstanceConcurrentMap.values()) {
      if (!(modelInstance.getStatus() == Status.COMPLETED || modelInstance.getStatus() == Status.FAILED || modelInstance
        .getStatus() == Status.CANCELLED)) {
        incompleteModelInstances.add(modelInstance);
      }
    }
    return incompleteModelInstances;
  }

  @Override
  public List<Evaluation> getIncompleteEvaluations() {
    List<Evaluation> incompleteEvaluations = new ArrayList<>();
    for (Evaluation evaluation : evaluationConcurrentMap.values()) {
      if (!(evaluation.getStatus() == Status.COMPLETED || evaluation.getStatus() == Status.FAILED || evaluation
        .getStatus() == Status.CANCELLED)) {
        incompleteEvaluations.add(evaluation);
      }
    }
    return incompleteEvaluations;
  }

  @Override
  public Prediction createPrediction(Date startTime, Date finishTime, Status status,
                                     LensSessionHandle lensSessionHandle,
                                     String modelInstanceId, String inputDataSet, String outputDataSet)
    throws LensException {
    String id = UUID.randomUUID().toString();
    while (modelInstanceConcurrentMap.containsKey(id)) {
      id = UUID.randomUUID().toString();
    }
    Prediction prediction = new Prediction(id, startTime, finishTime, status, lensSessionHandle, modelInstanceId,
      inputDataSet, outputDataSet);
    predictionConcurrentMap.put(prediction.getId(), prediction);
    return prediction;
  }

  @Override
  public Prediction getPrediction(String predictionId) throws LensException {
    return predictionConcurrentMap.get(predictionId);
  }

  @Override
  public void updatePrediction(Prediction prediction) throws LensException {
    if (modelConcurrentMap.keySet().contains(prediction.getId())) {
      Prediction predictionDb = predictionConcurrentMap.get(prediction.getId());
      predictionDb.setStatus(prediction.getStatus());
      predictionDb.setFinishTime(prediction.getFinishTime());
    }
  }

  @Override
  public Evaluation createEvaluation(Date startTime, Date finishTime, Status status,
                                     LensSessionHandle lensSessionHandle,
                                     String modelInstanceId, String inputDataSetName)
    throws LensException {
    String id = UUID.randomUUID().toString();
    while (modelInstanceConcurrentMap.containsKey(id)) {
      id = UUID.randomUUID().toString();
    }
    Evaluation evaluation = new Evaluation(id, startTime, finishTime, status, lensSessionHandle, modelInstanceId,
      inputDataSetName);
    evaluationConcurrentMap.put(evaluation.getId(), evaluation);
    return evaluation;
  }

  @Override
  public List<Prediction> getIncompletePredictions() throws LensException {
    List<Prediction> incompletePrediction = new ArrayList<>();
    for (Prediction prediction : predictionConcurrentMap.values()) {
      if (!(prediction.getStatus() == Status.COMPLETED || prediction.getStatus() == Status.FAILED || prediction
        .getStatus() == Status.CANCELLED)) {
        incompletePrediction.add(prediction);
      }
    }
    return incompletePrediction;
  }

  @Override
  public List<ModelInstance> getModelInstances(String modelId) throws LensException {
    List<ModelInstance> modelInstances = new ArrayList<>();
    for (ModelInstance modelInstance : modelInstanceConcurrentMap.values()) {
      if (modelInstance.getModelId().compareTo(modelId) == 0) {
        modelInstances.add(modelInstance);
      }
    }
    return modelInstances;
  }

  @Override
  public List<Evaluation> getEvaluations(String modelInstanceId) throws LensException {
    List<Evaluation> evaluations = new ArrayList<>();
    for (Evaluation evaluation : evaluationConcurrentMap.values()) {
      if (evaluation.getModeInstanceId().compareTo(modelInstanceId) == 0) {
        evaluations.add(evaluation);
      }
    }
    return evaluations;
  }

  @Override
  public List<Prediction> getPredictions(String modelInstanceId) throws LensException {
    List<Prediction> predictions = new ArrayList<>();
    for (Prediction prediction : predictionConcurrentMap.values()) {
      if (prediction.getModelInstanceId().compareTo(modelInstanceId) == 0) {
        predictions.add(prediction);
      }
    }
    return predictions;
  }
}

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

import org.apache.lens.api.LensException;
import org.apache.lens.api.LensSessionHandle;

import java.util.List;
import java.util.Map;

/**
 * Lens's machine learning interface used by client code as well as Lens ML service.
 */
public interface LensML {

  /** The Constant NAME. */
  public static final String NAME = "ml";

  /**
   * Get list of available machine learning algorithms
   * 
   * @return
   */
  public List<String> getAlgorithms();

  /**
   * Get user friendly information about parameters accepted by the algorithm.
   *
   * @param algorithm
   *          the algorithm
   * @return map of param key to its help message
   */
  public Map<String, String> getAlgoParamDescription(String algorithm);

  /**
   * Get a trainer object instance which could be used to generate a model of the given algorithm.
   *
   * @param algorithm
   *          the algorithm
   * @return the trainer for name
   * @throws LensException
   *           the lens exception
   */
  public MLTrainer getTrainerForName(String algorithm) throws LensException;

  /**
   * Create a model using the given HCatalog table as input. The arguments should contain information needeed to
   * generate the model.
   *
   * @param table
   *          the table
   * @param algorithm
   *          the algorithm
   * @param args
   *          the args
   * @return Unique ID of the model created after training is complete
   * @throws LensException
   *           the lens exception
   */
  public String train(String table, String algorithm, String[] args) throws LensException;

  /**
   * Get model IDs for the given algorithm.
   *
   * @param algorithm
   *          the algorithm
   * @return the models
   * @throws LensException
   *           the lens exception
   */
  public List<String> getModels(String algorithm) throws LensException;

  /**
   * Get a model instance given the algorithm name and model ID.
   *
   * @param algorithm
   *          the algorithm
   * @param modelId
   *          the model id
   * @return the model
   * @throws LensException
   *           the lens exception
   */
  public MLModel getModel(String algorithm, String modelId) throws LensException;

  /**
   * Get the FS location where model instance is saved.
   *
   * @param algorithm
   *          the algorithm
   * @param modelID
   *          the model id
   * @return the model path
   */
  String getModelPath(String algorithm, String modelID);

  /**
   * Evaluate model by running it against test data contained in the given table.
   *
   * @param session
   *          the session
   * @param table
   *          the table
   * @param algorithm
   *          the algorithm
   * @param modelID
   *          the model id
   * @return Test report object containing test output table, and various evaluation metrics
   * @throws LensException
   *           the lens exception
   */
  public MLTestReport testModel(LensSessionHandle session, String table, String algorithm, String modelID)
      throws LensException;

  /**
   * Get test reports for an algorithm.
   *
   * @param algorithm
   *          the algorithm
   * @return the test reports
   * @throws LensException
   *           the lens exception
   */
  public List<String> getTestReports(String algorithm) throws LensException;

  /**
   * Get a test report by ID.
   *
   * @param algorithm
   *          the algorithm
   * @param reportID
   *          the report id
   * @return the test report
   * @throws LensException
   *           the lens exception
   */
  public MLTestReport getTestReport(String algorithm, String reportID) throws LensException;

  /**
   * Online predict call given a model ID, algorithm name and sample feature values.
   *
   * @param algorithm
   *          the algorithm
   * @param modelID
   *          the model id
   * @param features
   *          the features
   * @return prediction result
   * @throws LensException
   *           the lens exception
   */
  public Object predict(String algorithm, String modelID, Object[] features) throws LensException;

  /**
   * Permanently delete a model instance.
   *
   * @param algorithm
   *          the algorithm
   * @param modelID
   *          the model id
   * @throws LensException
   *           the lens exception
   */
  public void deleteModel(String algorithm, String modelID) throws LensException;

  /**
   * Permanently delete a test report instance.
   *
   * @param algorithm
   *          the algorithm
   * @param reportID
   *          the report id
   * @throws LensException
   *           the lens exception
   */
  public void deleteTestReport(String algorithm, String reportID) throws LensException;
}

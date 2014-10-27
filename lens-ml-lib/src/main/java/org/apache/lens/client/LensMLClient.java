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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lens.api.LensException;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.ml.ModelMetadata;
import org.apache.lens.api.ml.TestReport;
import org.apache.lens.ml.LensML;
import org.apache.lens.ml.MLModel;
import org.apache.lens.ml.MLTestReport;
import org.apache.lens.ml.MLTrainer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The Class LensMLClient.
 */
public class LensMLClient implements LensML {

  /** The client. */
  private LensMLJerseyClient client;

  /**
   * Instantiates a new lens ml client.
   *
   * @param clientConf
   *          the client conf
   */
  public LensMLClient(LensConnectionParams clientConf) {
    client = new LensMLJerseyClient(new LensConnection(clientConf));
  }

  /**
   * Get list of available machine learning algorithms
   *
   * @return
   */
  @Override
  public List<String> getAlgorithms() {
    return client.getTrainerNames();
  }

  /**
   * Get user friendly information about parameters accepted by the algorithm.
   *
   * @param algorithm
   *          the algorithm
   * @return map of param key to its help message
   */
  @Override
  public Map<String, String> getAlgoParamDescription(String algorithm) {
    List<String> paramDesc = client.getParamDescriptionOfTrainer(algorithm);
    // convert paramDesc to map
    Map<String, String> paramDescMap = new LinkedHashMap<String, String>();
    for (String str : paramDesc) {
      String[] keyHelp = StringUtils.split(str, ":");
      paramDescMap.put(keyHelp[0].trim(), keyHelp[1].trim());
    }
    return paramDescMap;
  }

  /**
   * Get a trainer object instance which could be used to generate a model of the given algorithm.
   *
   * @param algorithm
   *          the algorithm
   * @return the trainer for name
   * @throws LensException
   *           the lens exception
   */
  @Override
  public MLTrainer getTrainerForName(String algorithm) throws LensException {
    throw new UnsupportedOperationException("MLTrainer cannot be accessed from client");
  }

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
  @Override
  public String train(String table, String algorithm, String[] args) throws LensException {
    Map<String, String> trainParams = new LinkedHashMap<String, String>();
    trainParams.put("table", table);
    for (int i = 0; i < args.length; i += 2) {
      trainParams.put(args[i], args[i + 1]);
    }
    return client.trainModel(algorithm, trainParams);
  }

  /**
   * Get model IDs for the given algorithm.
   *
   * @param algorithm
   *          the algorithm
   * @return the models
   * @throws LensException
   *           the lens exception
   */
  @Override
  public List<String> getModels(String algorithm) throws LensException {
    return client.getModelsForAlgorithm(algorithm);
  }

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
  @Override
  public MLModel getModel(String algorithm, String modelId) throws LensException {
    ModelMetadata metadata = client.getModelMetadata(algorithm, modelId);
    String modelPathURI = metadata.getModelPath();

    ObjectInputStream in = null;
    try {
      URI modelURI = new URI(modelPathURI);
      Path modelPath = new Path(modelURI);
      FileSystem fs = FileSystem.get(modelURI, client.getConf());
      in = new ObjectInputStream(fs.open(modelPath));
      MLModel<?> model = (MLModel) in.readObject();
      return model;
    } catch (IOException e) {
      throw new LensException(e);
    } catch (URISyntaxException e) {
      throw new LensException(e);
    } catch (ClassNotFoundException e) {
      throw new LensException(e);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

  }

  /**
   * Get the FS location where model instance is saved.
   *
   * @param algorithm
   *          the algorithm
   * @param modelID
   *          the model id
   * @return the model path
   */
  @Override
  public String getModelPath(String algorithm, String modelID) {
    ModelMetadata metadata = client.getModelMetadata(algorithm, modelID);
    return metadata.getModelPath();
  }

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
  @Override
  public MLTestReport testModel(LensSessionHandle session, String table, String algorithm, String modelID)
      throws LensException {
    String reportID = client.testModel(table, algorithm, modelID);
    return getTestReport(algorithm, reportID);
  }

  /**
   * Get test reports for an algorithm.
   *
   * @param algorithm
   *          the algorithm
   * @return the test reports
   * @throws LensException
   *           the lens exception
   */
  @Override
  public List<String> getTestReports(String algorithm) throws LensException {
    return client.getTestReportsOfAlgorithm(algorithm);
  }

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
  @Override
  public MLTestReport getTestReport(String algorithm, String reportID) throws LensException {
    TestReport report = client.getTestReport(algorithm, reportID);
    MLTestReport mlTestReport = new MLTestReport();
    mlTestReport.setAlgorithm(report.getAlgorithm());
    mlTestReport.setFeatureColumns(Arrays.asList(report.getFeatureColumns().split("\\,+")));
    mlTestReport.setLensQueryID(report.getQueryID());
    mlTestReport.setLabelColumn(report.getLabelColumn());
    mlTestReport.setModelID(report.getModelID());
    mlTestReport.setOutputColumn(report.getOutputColumn());
    mlTestReport.setPredictionResultColumn(report.getOutputColumn());
    mlTestReport.setQueryID(report.getQueryID());
    mlTestReport.setReportID(report.getReportID());
    mlTestReport.setTestTable(report.getTestTable());
    return mlTestReport;
  }

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
  @Override
  public Object predict(String algorithm, String modelID, Object[] features) throws LensException {
    return getModel(algorithm, modelID).predict(features);
  }

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
  @Override
  public void deleteModel(String algorithm, String modelID) throws LensException {
    client.deleteModel(algorithm, modelID);
  }

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
  @Override
  public void deleteTestReport(String algorithm, String reportID) throws LensException {
    client.deleteTestReport(algorithm, reportID);
  }
}

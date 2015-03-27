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
package org.apache.lens.ml.task;

import java.util.*;

import org.apache.lens.client.LensMLClient;
import org.apache.lens.ml.LensML;
import org.apache.lens.ml.MLTestReport;
import org.apache.lens.ml.MLUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;

import lombok.Getter;
import lombok.ToString;

/**
 * Run a complete cycle of train and test (evaluation) for an ML algorithm
 */
@ToString
public class MLTask implements Runnable {
  private static final Log LOG = LogFactory.getLog(MLTask.class);

  public enum State {
    RUNNING, SUCCESSFUL, FAILED
  }

  @Getter
  private State taskState;

  /**
   * Name of the algo/algorithm.
   */
  @Getter
  private String algorithm;

  /**
   * Name of the table containing training data.
   */
  @Getter
  private String trainingTable;

  /**
   * Name of the table containing test data. Optional, if not provided trainingTable itself is
   * used for testing
   */
  @Getter
  private String testTable;

  /**
   * Training table partition spec
   */
  @Getter
  private String partitionSpec;

  /**
   * Name of the column which is a label for supervised algorithms.
   */
  @Getter
  private String labelColumn;

  /**
   * Names of columns which are features in the training data.
   */
  @Getter
  private List<String> featureColumns;

  /**
   * Configuration for the example.
   */
  @Getter
  private HiveConf configuration;

  private LensML ml;
  private String taskID;

  /**
   * ml client
   */
  @Getter
  private LensMLClient mlClient;

  /**
   * Output table name
   */
  @Getter
  private String outputTable;

  /**
   * Extra params passed to the training algorithm
   */
  @Getter
  private Map<String, String> extraParams;

  @Getter
  private String modelID;

  @Getter
  private String reportID;

  /**
   * Use ExampleTask.Builder to create an instance
   */
  private MLTask() {
    // Use builder to construct the example
    extraParams = new HashMap<String, String>();
    taskID = UUID.randomUUID().toString();
  }

  /**
   * Builder to create an example task
   */
  public static class Builder {
    private MLTask task;

    public Builder() {
      task = new MLTask();
    }

    public Builder trainingTable(String trainingTable) {
      task.trainingTable = trainingTable;
      return this;
    }

    public Builder testTable(String testTable) {
      task.testTable = testTable;
      return this;
    }

    public Builder algorithm(String algorithm) {
      task.algorithm = algorithm;
      return this;
    }

    public Builder labelColumn(String labelColumn) {
      task.labelColumn = labelColumn;
      return this;
    }

    public Builder client(LensMLClient client) {
      task.mlClient = client;
      return this;
    }

    public Builder addFeatureColumn(String featureColumn) {
      if (task.featureColumns == null) {
        task.featureColumns = new ArrayList<String>();
      }
      task.featureColumns.add(featureColumn);
      return this;
    }

    public Builder hiveConf(HiveConf hiveConf) {
      task.configuration = hiveConf;
      return this;
    }



    public Builder extraParam(String param, String value) {
      task.extraParams.put(param, value);
      return this;
    }

    public Builder partitionSpec(String partitionSpec) {
      task.partitionSpec = partitionSpec;
      return this;
    }

    public Builder outputTable(String outputTable) {
      task.outputTable = outputTable;
      return this;
    }

    public MLTask build() {
      MLTask builtTask = task;
      task = null;
      return builtTask;
    }

  }

  @Override
  public void run() {
    taskState = State.RUNNING;
    LOG.info("Starting " + taskID);
    try {
      runTask();
      taskState = State.SUCCESSFUL;
      LOG.info("Complete " + taskID);
    } catch (Exception e) {
      taskState = State.FAILED;
      LOG.info("Error running task " + taskID, e);
    }
  }

  /**
   * Train an ML model, with specified algorithm and input data. Do model evaluation using the evaluation data and print
   * evaluation result
   *
   * @throws Exception
   */
  private void runTask() throws Exception {
    if (mlClient != null) {
      // Connect to a remote Lens server
      ml = mlClient;
      LOG.info("Working in client mode. Lens session handle " + mlClient.getSessionHandle().getPublicId());
    } else {
      // In server mode session handle has to be passed by the user as a request parameter
      ml = MLUtils.getMLService();
      LOG.info("Working in Lens server");
    }

    String[] algoArgs = buildTrainingArgs();
    LOG.info("Starting task " + taskID + " algo args: " + Arrays.toString(algoArgs));

    modelID = ml.train(trainingTable, algorithm, algoArgs);
    printModelMetadata(taskID, modelID);

    LOG.info("Starting test " + taskID);
    testTable = (testTable != null) ? testTable : trainingTable;
    MLTestReport testReport = ml.testModel(mlClient.getSessionHandle(), testTable, algorithm, modelID, outputTable);
    reportID = testReport.getReportID();
    printTestReport(taskID, testReport);
    saveTask();
  }

  // Save task metadata to DB
  private void saveTask() {
    LOG.info("Saving task details to DB");
  }

  private void printTestReport(String exampleID, MLTestReport testReport) {
    StringBuilder builder = new StringBuilder("Example: ").append(exampleID);
    builder.append("\n\t");
    builder.append("EvaluationReport: ").append(testReport.toString());
    System.out.println(builder.toString());
  }

  private String[] buildTrainingArgs() {
    List<String> argList = new ArrayList<String>();
    argList.add("label");
    argList.add(labelColumn);

    // Add all the features
    for (String featureCol : featureColumns) {
      argList.add("feature");
      argList.add(featureCol);
    }

    // Add extra params
    for (String param : extraParams.keySet()) {
      argList.add(param);
      argList.add(extraParams.get(param));
    }

    return argList.toArray(new String[argList.size()]);
  }

  // Get the model instance and print its metadat to stdout
  private void printModelMetadata(String exampleID, String modelID) throws Exception {
    StringBuilder builder = new StringBuilder("Example: ").append(exampleID);
    builder.append("\n\t");
    builder.append("Model: ");
    builder.append(ml.getModel(algorithm, modelID).toString());
    System.out.println(builder.toString());
  }
}

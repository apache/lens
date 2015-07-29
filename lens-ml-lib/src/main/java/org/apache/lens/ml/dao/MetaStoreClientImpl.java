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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import javax.sql.DataSource;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.ml.api.*;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.lang.StringUtils;

import org.codehaus.jettison.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

/**
 * MetaStore implementation for JDBC.
 */
@Slf4j
public class MetaStoreClientImpl implements MetaStoreClient {

  ResultSetHandler<List<Feature>> featureListHandler = new ResultSetHandler<List<Feature>>() {
    public List<Feature> handle(ResultSet rs) throws SQLException {
      List<Feature> featureList = new ArrayList();
      while (rs.next()) {
        String name = rs.getString("featureName");
        String description = rs.getString("description");
        Feature.Type type = Feature.Type.valueOf(rs.getString("valueType"));
        String dataColumn = rs.getString("dataColumn");
        featureList.add(new Feature(name, description, type, dataColumn));
      }
      return featureList;
    }
  };
  ResultSetHandler<Model> modelHandler = new ResultSetHandler<Model>() {
    @Override
    public Model handle(ResultSet resultSet) throws SQLException {
      if (!resultSet.next()) {
        return null;
      }
      String modelName = resultSet.getString("modelName");
      String algoName = resultSet.getString("algoName");
      String lableName = resultSet.getString("labelName");
      String labelDescription = resultSet.getString("labelDescription");
      String lableDataColumn = resultSet.getString("lableDataColumn");
      Feature.Type lableType = Feature.Type.valueOf(resultSet.getString("lableType"));

      Feature lable = new Feature(lableName, labelDescription, lableType, lableDataColumn);
      String algoParamJson = resultSet.getString("algoParams");
      ObjectMapper mapper = new ObjectMapper();
      Map<String, String> alsoParams = null;
      try {
        alsoParams = mapper.readValue(algoParamJson, Map.class);
      } catch (Exception e) {
        throw new SQLException("Error Parsing algoParamJson", e);
      }
      AlgoSpec algoSpec = new AlgoSpec(algoName, alsoParams);
      return new Model(modelName, algoSpec, null, lable);
    }
  };
  ResultSetHandler<List<AlgoParameter>> algoParamListHandler = new ResultSetHandler<List<AlgoParameter>>() {
    public List<AlgoParameter> handle(ResultSet rs) throws SQLException {
      List<AlgoParameter> algoParameterList = new ArrayList<>();
      while (rs.next()) {
        String name = rs.getString("paramName");
        String helpText = rs.getString("helpText");
        String defaultValue = rs.getString("defaultValue");

        algoParameterList.add(new AlgoParameter(name, helpText, defaultValue));
      }

      return algoParameterList;
    }
  };
  ResultSetHandler<List<Evaluation>> evaluationListHandler = new ResultSetHandler<List<Evaluation>>() {
    @Override
    public List<Evaluation> handle(ResultSet rs) throws SQLException {
      List<Evaluation> evaluations = new ArrayList<>();
      if (rs.next()) {
        String id = rs.getString("id");
        Date startTime = rs.getTimestamp("startTime");
        Date finishTime = rs.getTimestamp("finishTime");
        Status status = Status.valueOf(rs.getString("status"));
        LensSessionHandle lensSessionHandle = LensSessionHandle.valueOf(rs.getString("lensSessionHandle"));
        String modelInstanceId = rs.getString("modelInstanceId");
        String inputDataSet = rs.getString("inputDataSetName");

        Evaluation evaluation = new Evaluation(id, startTime, finishTime, status, lensSessionHandle, modelInstanceId,
          inputDataSet);
        evaluations.add(evaluation);
      }
      return evaluations;
    }
  };
  ResultSetHandler<ModelInstance> modelInstanceHandler = new ResultSetHandler<ModelInstance>() {
    public ModelInstance handle(ResultSet rs) throws SQLException {
      if (rs.next()) {
        String id = rs.getString("id");
        Date startTime = rs.getTimestamp("startTime");
        Date finishTime = rs.getTimestamp("finishTime");
        Status status = Status.valueOf(rs.getString("status"));
        LensSessionHandle lensSessionHandle = LensSessionHandle.valueOf(rs.getString("lensSessionHandle"));
        String modelName = rs.getString("modelName");
        String dataSetName = rs.getString("dataSetName");
        String path = rs.getString("path");
        String defaultEvaluationId = rs.getString("defaultEvaluationId");

        return new ModelInstance(id, startTime, finishTime, status, lensSessionHandle, modelName, dataSetName, path,
          defaultEvaluationId);
      }
      return null;
    }
  };
  ResultSetHandler<List<ModelInstance>> modelInstanceListHandler = new ResultSetHandler<List<ModelInstance>>() {
    @Override
    public List<ModelInstance> handle(ResultSet rs) throws SQLException {
      List<ModelInstance> modelInstances = new ArrayList<>();
      if (rs.next()) {
        String id = rs.getString("id");
        Date startTime = rs.getTimestamp("startTime");
        Date finishTime = rs.getTimestamp("finishTime");
        Status status = Status.valueOf(rs.getString("status"));
        LensSessionHandle lensSessionHandle = LensSessionHandle.valueOf(rs.getString("lensSessionHandle"));
        String modelName = rs.getString("modelName");
        String dataSetName = rs.getString("dataSetName");
        String path = rs.getString("path");
        String defaultEvaluationId = rs.getString("defaultEvaluationId");


        ModelInstance modelInstance = new ModelInstance(id, startTime, finishTime, status, lensSessionHandle, modelName,
          dataSetName, path,
          defaultEvaluationId);
        modelInstances.add(modelInstance);
      }
      return modelInstances;
    }
  };
  ResultSetHandler<Prediction> predictionResultSetHandler = new ResultSetHandler<Prediction>() {
    public Prediction handle(ResultSet rs) throws SQLException {
      if (rs.next()) {
        String id = rs.getString("id");
        Date startTime = rs.getTimestamp("startTime");
        Date finishTime = rs.getTimestamp("finishTime");
        Status status = Status.valueOf(rs.getString("status"));
        LensSessionHandle lensSessionHandle = LensSessionHandle.valueOf(rs.getString("lensSessionHandle"));
        String modelInstanceId = rs.getString("modelInstanceId");
        String inputDataSet = rs.getString("inputDataSet");
        String outputDataSet = rs.getString("outputDataSet");

        return new Prediction(id, startTime, finishTime, status, lensSessionHandle, modelInstanceId, inputDataSet,
          outputDataSet);
      }
      return null;
    }
  };
  ResultSetHandler<List<Prediction>> predictionListHandler = new ResultSetHandler<List<Prediction>>() {
    @Override
    public List<Prediction> handle(ResultSet rs) throws SQLException {
      List<Prediction> predictions = new ArrayList<>();
      if (rs.next()) {
        String id = rs.getString("id");
        Date startTime = rs.getTimestamp("startTime");
        Date finishTime = rs.getTimestamp("finishTime");
        Status status = Status.valueOf(rs.getString("status"));
        LensSessionHandle lensSessionHandle = LensSessionHandle.valueOf(rs.getString("lensSessionHandle"));
        String modelInstanceId = rs.getString("modelInstanceId");
        String inputDataSet = rs.getString("inputDataSet");
        String outputDataSet = rs.getString("outputDataSet");

        Prediction prediction = new Prediction(id, startTime, finishTime, status, lensSessionHandle, modelInstanceId,
          inputDataSet,
          outputDataSet);
        predictions.add(prediction);
      }
      return predictions;
    }
  };
  ResultSetHandler<Evaluation> evaluationResultSetHandler = new ResultSetHandler<Evaluation>() {
    public Evaluation handle(ResultSet rs) throws SQLException {
      if (rs.next()) {
        String id = rs.getString("id");
        Date startTime = rs.getTimestamp("startTime");
        Date finishTime = rs.getTimestamp("finishTime");
        Status status = Status.valueOf(rs.getString("status"));
        LensSessionHandle lensSessionHandle = LensSessionHandle.valueOf(rs.getString("lensSessionHandle"));
        String modelInstanceId = rs.getString("modelInstanceId");
        String inputDataSet = rs.getString("inputDataSetName");

        return new Evaluation(id, startTime, finishTime, status, lensSessionHandle, modelInstanceId, inputDataSet);
      }
      return null;
    }
  };
  /**
   * The ds.
   */
  private DataSource ds;

  public MetaStoreClientImpl(DataSource dataSource) {
    this.ds = dataSource;
  }

  public void init() {
    String dataSourceCreateSql = "CREATE TABLE IF NOT EXISTS `datasets` (\n"
      + "  `dsName` varchar(255) NOT NULL,\n"
      + "  `tableName` varchar(255) NOT NULL,\n"
      + "  `dbName` varchar(255) NOT NULL,\n"
      + "  PRIMARY KEY (`dsName`)\n"
      + ");";

    String modelCreateSql = "CREATE TABLE IF NOT EXISTS `models` (\n"
      + "  `modelName` varchar(255) NOT NULL,\n"
      + "  `algoName` varchar(255) NOT NULL,\n"
      + "  `labelName` varchar(255) NOT NULL,\n"
      + "  `labelDescription` varchar(1000) DEFAULT NULL,\n"
      + "  `lableType` varchar(255) NOT NULL,\n"
      + "  `lableDataColumn` varchar(255) DEFAULT NULL,\n"
      + "  `algoParams` text,\n"
      + "  PRIMARY KEY (`modelName`)\n"
      + ");";

    String modelInstanceCreateSql = "CREATE TABLE IF NOT EXISTS `modelInstances` (\n"
      + "  `id` varchar(255) NOT NULL DEFAULT '',\n"
      + "  `startTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
      + "  `finishTime` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',\n"
      + "  `status` enum('SUBMITTED','RUNNING','FAILED','CANCELLED','COMPLETED') DEFAULT NULL,\n"
      + "  `lensSessionHandle` varchar(255) NOT NULL,\n"
      + "  `modelName` varchar(255) DEFAULT NULL,\n"
      + "  `dataSetName` varchar(255) NOT NULL,\n"
      + "  `path` varchar(255) DEFAULT NULL,\n"
      + "  `defaultEvaluationId` varchar(255) DEFAULT NULL,\n"
      + "  PRIMARY KEY (`id`),\n"
      + "  KEY `modelName` (`modelName`),\n"
      + "  KEY `dataSetName` (`dataSetName`),\n"
      + "  CONSTRAINT `modelinstances_ibfk_1` FOREIGN KEY (`modelName`) REFERENCES `models` (`modelName`),\n"
      + "  CONSTRAINT `modelinstances_ibfk_2` FOREIGN KEY (`dataSetName`) REFERENCES `datasets` (`dsName`)\n"
      + ");";

    String evaluationsCreateSql = "CREATE TABLE IF NOT EXISTS `evaluations` (\n"
      + "  `id` varchar(255) NOT NULL DEFAULT '',\n"
      + "  `startTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
      + "  `finishTime` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',\n"
      + "  `status` enum('SUBMITTED','RUNNING','FAILED','CANCELLED','COMPLETED') DEFAULT NULL,\n"
      + "  `lensSessionHandle` varchar(255) NOT NULL,\n"
      + "  `modelInstanceId` varchar(255) DEFAULT NULL,\n"
      + "  `inputDataSetName` varchar(255) DEFAULT NULL,\n"
      + "  PRIMARY KEY (`id`),\n"
      + "  KEY `modelInstanceId` (`modelInstanceId`),\n"
      + "  KEY `inputDataSetName` (`inputDataSetName`),\n"
      + "  CONSTRAINT `evaluations_ibfk_1` FOREIGN KEY (`modelInstanceId`) REFERENCES `modelinstances` (`id`),\n"
      + "  CONSTRAINT `evaluations_ibfk_2` FOREIGN KEY (`inputDataSetName`) REFERENCES `datasets` (`dsName`)\n"
      + ");\n";

    String predictionCreateSql = "CREATE TABLE IF NOT EXISTS `predictions` (\n"
      + "  `id` varchar(255) NOT NULL DEFAULT '',\n"
      + "  `startTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
      + "  `finishTime` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',\n"
      + "  `status` enum('SUBMITTED','RUNNING','FAILED','CANCELLED','COMPLETED') DEFAULT NULL,\n"
      + "  `lensSessionHandle` varchar(255) NOT NULL,\n"
      + "  `modelInstanceId` varchar(255) DEFAULT NULL,\n"
      + "  `inputDataSet` varchar(255) NOT NULL,\n"
      + "  `outputDataSet` varchar(255) NOT NULL,\n"
      + "  PRIMARY KEY (`id`),\n"
      + "  KEY `modelInstanceId` (`modelInstanceId`),\n"
      + "  KEY `inputDataSet` (`inputDataSet`),\n"
      + "  KEY `outputDataSet` (`outputDataSet`),\n"
      + "  CONSTRAINT `predictions_ibfk_1` FOREIGN KEY (`modelInstanceId`) REFERENCES `modelinstances` (`id`),\n"
      + "  CONSTRAINT `predictions_ibfk_2` FOREIGN KEY (`inputDataSet`) REFERENCES `datasets` (`dsName`),\n"
      + "  CONSTRAINT `predictions_ibfk_3` FOREIGN KEY (`outputDataSet`) REFERENCES `datasets` (`dsName`)\n"
      + ") ;";

    String featureCreateSql = "CREATE TABLE IF NOT EXISTS `features` (\n"
      + "  `featureName` varchar(255) NOT NULL,\n"
      + "  `description` varchar(1000) DEFAULT NULL,\n"
      + "  `valueType` enum('Categorical','Continuous') NOT NULL,\n"
      + "  `dataColumn` varchar(255) NOT NULL,\n"
      + "  `modelName` varchar(255) NOT NULL DEFAULT '',\n"
      + "  PRIMARY KEY (`featureName`,`modelName`),\n"
      + "  KEY `modelName` (`modelName`),\n"
      + "  CONSTRAINT `features_ibfk_1` FOREIGN KEY (`modelName`) REFERENCES `models` (`modelName`)\n"
      + ") ;";

    QueryRunner runner = new QueryRunner(ds);
    try {
      runner.update(dataSourceCreateSql);
      runner.update(modelCreateSql);
      runner.update(modelInstanceCreateSql);
      runner.update(predictionCreateSql);
      runner.update(evaluationsCreateSql);
      runner.update(featureCreateSql);
    } catch (SQLException e) {
      log.error("Error creating ML meta store");
    }

  }

  /*
  public List<AlgoParam> getAlgoParams(String modelId) throws LensException{


    QueryRunner runner = new QueryRunner(ds);

    String sql = "select * from algoparams where id = ?";

    try{
      Object[]  runner.query(sql,  modelId);
    } catch(SQLException e){
      log.error("SQL exception while executing query. ", e);
      throw new LensException(e);
    }
  }*/

  @Override
  public void createDataSet(DataSet dataSet) throws SQLException {
    String sql = "insert into datasets (dsName, tableName, dbName) values (?,?,?)";
    DataSet alreadyExisting;
    alreadyExisting = getDataSet(dataSet.getDsName());
    if (alreadyExisting != null) {
      throw new SQLException("Dataset with same name already exists.");
    }

    QueryRunner runner = new QueryRunner(ds);
    runner.update(sql, dataSet.getDsName(), dataSet.getTableName(), dataSet.getDbName());
  }

  @Override
  public DataSet getDataSet(String name) throws SQLException {
    ResultSetHandler<DataSet> dsh = new BeanHandler<DataSet>(DataSet.class);
    String sql = "select * from datasets where dsName = ?";
    QueryRunner runner = new QueryRunner(ds);
    return runner.query(sql, dsh, name);
  }

  public void deleteDataSet(String dataSetName) throws SQLException {
    String sql = "delete from datasets where dsName = ?";
    QueryRunner runner = new QueryRunner(ds);
    runner.update(sql, dataSetName);
  }

  @Override
  public void createModel(String name, String algo, AlgoSpec algoSpec, List<Feature> features, Feature label)
    throws LensException {
    String modelSql = "INSERT INTO models VALUES (?, ?, ?, ?, ?, ?, ?)";
    String featuresSql = "INSERT INTO features VALUES (?, ?, ?, ?, ?)";

    try {
      Connection con = ds.getConnection();
      con.setAutoCommit(false);
      QueryRunner runner = new QueryRunner(ds);

      JSONObject algoParamJson = new JSONObject(algoSpec.getAlgoParams());
      runner.update(con, modelSql, name, algoSpec.getAlgo(), label.getName(), label.getDescription(), label.getType()
        .toString(), label.getDataColumn(), algoParamJson.toString());

      for (Feature feature : features) {
        runner
          .update(con, featuresSql, feature.getName(), feature.getDescription(), feature.getType().toString(), feature
            .getDataColumn(), name);
      }

      DbUtils.commitAndClose(con);
    } catch (SQLException e) {
      throw new LensException("Error while creating Model, Id: " + name, e);
    }

  }

  public void createModel(Model model) throws LensException {
    String modelSql = "INSERT INTO models VALUES (?, ?, ?, ?, ?, ?, ?)";
    String featuresSql = "INSERT INTO features VALUES (?, ?, ?, ?, ?)";

    try {
      Connection con = ds.getConnection();
      con.setAutoCommit(false);
      QueryRunner runner = new QueryRunner(ds);

      JSONObject algoParamJson = new JSONObject(model.getAlgoSpec().getAlgoParams());
      runner.update(con, modelSql, model.getName(), model.getAlgoSpec().getAlgo(), model.getLabelSpec().getName(),
        model.getLabelSpec().getDescription(), model.getLabelSpec().getType().toString(),
        model.getLabelSpec().getDataColumn(),
        algoParamJson.toString());

      for (Feature feature : model.getFeatureSpec()) {
        runner
          .update(con, featuresSql, feature.getName(), feature.getDescription(), feature.getType().toString(), feature
            .getDataColumn(), model.getName());
      }

      DbUtils.commitAndClose(con);
    } catch (SQLException e) {
      throw new LensException("Error while creating Model, Id: " + model.getName(), e);
    }
  }

  @Override
  public Model getModel(String modelName) throws LensException {

    String featureSql = "select * from features where modelName = ?";
    String modelSql = "select * from models where modelName = ? ";
    QueryRunner runner = new QueryRunner(ds);
    try {
      Model model = runner.query(modelSql, modelHandler, modelName);
      List<Feature> featureList = runner.query(featureSql, featureListHandler, modelName);
      model.setFeatureSpec(featureList);
      return model;
    } catch (SQLException e) {
      log.error("SQL exception while executing query. ", e);
      throw new LensException(e);
    }
  }

  public void deleteModel(String modelId) throws SQLException {
    String featureSql = "select * from features where modelName = ?";
    String modelDeleteSql = "delete from models where modelName = ?";
    QueryRunner runner = new QueryRunner(ds);
    List<Feature> featureList = runner.query(featureSql, featureListHandler, modelId);
    if (!featureList.isEmpty()) {
      List<String> features = new ArrayList<String>();
      for (Feature feature : featureList) {
        features.add(feature.getName());
      }
      String featureDeleteSql = "delete from features where modelName = ? and featureName in (";
      String commaDelimitedFeatures = StringUtils.join(features, ",");
      featureDeleteSql = featureDeleteSql.concat(commaDelimitedFeatures).concat(")");
      runner.update(featureDeleteSql, modelId);
      runner.update(modelDeleteSql, modelId);
    }
  }

  @Override
  public String createModelInstance(Date startTime, Date finishTime, Status status,
                                    LensSessionHandle lensSessionHandle, String modelName, String dataSetName,
                                    String path, String defaultEvaluationId) throws LensException {

    String modelInstanceSql = "INSERT INTO modelInstances (id, startTime, finishTime, status, "
      + "lensSessionHandle, modelName, dataSetName, path, defaultEvaluationId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    String modelInstanceId;
    Connection con;
    try {
      QueryRunner runner = new QueryRunner(ds);
      ModelInstance alreadyExisting = null;
      do {
        modelInstanceId = UUID.randomUUID().toString();
        alreadyExisting = getModelInstance(modelInstanceId);
      } while (alreadyExisting != null);

      runner.update(modelInstanceSql, modelInstanceId, startTime, finishTime, status
        .toString(), lensSessionHandle.toString(), modelName, dataSetName, path, defaultEvaluationId);
    } catch (SQLException e) {
      log.error("Error while creating ModelInstance for Model, " + modelName);
      throw new LensException("Error while creating ModelInstance for Model, " + modelName);
    }
    return modelInstanceId;
  }

  public void deleteModelInstance(String modelInstanceId) throws SQLException {
    String sql = "delete from modelInstances where id = ?";
    QueryRunner runner = new QueryRunner(ds);
    runner.update(sql, modelInstanceId);
  }

  @Override
  public void updateModelInstance(ModelInstance modelInstance) throws LensException {
    String modelInstanceSql = "UPDATE modelInstances SET startTime = ?, finishTime = ?, status =?, "
      + "lensSessionHandle = ?, modelName = ?, dataSetName = ?, path = ?, defaultEvaluationId = ? where id = ?";
    try {
      QueryRunner runner = new QueryRunner(ds);
      runner.update(modelInstanceSql, modelInstance.getStartTime(), modelInstance.getFinishTime(), modelInstance
          .getStatus().toString(), modelInstance.getLensSessionHandle().toString(), modelInstance.getModelId(),
        modelInstance.getDataSetName(), modelInstance.getPath(), modelInstance.getDefaultEvaluationId(),
        modelInstance.getId());
    } catch (SQLException sq) {
      throw new LensException("Error while updating Model Instance. Id: " + modelInstance);
    }
  }

  @Override
  public List<ModelInstance> getIncompleteModelInstances() throws LensException {
    return null;
  }

  @Override
  public List<Evaluation> getIncompleteEvaluations() throws LensException {
    return null;
  }

  @Override
  public List<Prediction> getIncompletePredictions() throws LensException {
    return null;
  }

  @Override
  public List<ModelInstance> getModelInstances(String modelId) throws SQLException {
    String sql = "SELECT * FROM modelInstances where modelName = ?";
    QueryRunner runner = new QueryRunner(ds);
    return runner.query(sql, modelInstanceListHandler, modelId);
  }

  @Override
  public List<Evaluation> getEvaluations(String modelInstanceId) throws SQLException {
    String sql = "SELECT * FROM evaluations where modelInstanceId = ?";
    QueryRunner runner = new QueryRunner(ds);
    return runner.query(sql, evaluationListHandler, modelInstanceId);
  }

  @Override
  public List<Prediction> getPredictions(String modelInstanceId) throws SQLException {
    String sql = "SELECT * FROM predictions where modelInstanceId = ?";
    QueryRunner runner = new QueryRunner(ds);
    return runner.query(sql, predictionListHandler, modelInstanceId);
  }

  @Override
  public ModelInstance getModelInstance(String modelInstanceId) throws LensException {
    String modelInstanceGetSql = "SELECT * FROM modelInstances WHERE id = ?";
    QueryRunner runner = new QueryRunner(ds);
    try {
      ModelInstance modelInstance = runner.query(modelInstanceGetSql, modelInstanceHandler, modelInstanceId);
      return modelInstance;
    } catch (Exception e) {
      log.error("Error while reading modelInstance, Id: " + modelInstanceId);
      throw new LensException("Error while reading modelInstance, Id: " + modelInstanceId, e);
    }
  }

  @Override
  public String createPrediction(Date startTime, Date finishTime, Status status,
                                 LensSessionHandle lensSessionHandle, String modelInstanceId, String inputDataSet,
                                 String outputDataSet) throws LensException {
    String predictionInsertSql = "INSERT INTO predictions (id, startTime, finishTime, status, lensSessionHandle,"
      + " modelInstanceId, inputDataSet, outputDataSet) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    String predictionId;
    try {
      QueryRunner runner = new QueryRunner(ds);
      Prediction alreadyExisting = null;
      do {
        predictionId = UUID.randomUUID().toString();
        alreadyExisting = getPrediction(predictionId);
      } while (alreadyExisting != null);

      runner.update(predictionInsertSql, predictionId, startTime, finishTime, status
        .toString(), lensSessionHandle.toString(), modelInstanceId, inputDataSet, outputDataSet);
    } catch (SQLException e) {
      log.error("Error while creating Prediction for ModelInstance, " + modelInstanceId);
      throw new LensException("Error while creating Prediction for ModelInstance, " + modelInstanceId, e);
    }
    return predictionId;
  }

  @Override
  public Prediction getPrediction(String predictionId) throws LensException {
    String modelInstanceGetSql = "SELECT * FROM predictions WHERE id = ?";
    QueryRunner runner = new QueryRunner(ds);
    try {
      Prediction prediction = runner.query(modelInstanceGetSql, predictionResultSetHandler, predictionId);
      return prediction;
    } catch (Exception e) {
      log.error("Error while reading prediction, Id: " + predictionId);
      throw new LensException("Error while reading prediction, Id: " + predictionId, e);
    }
  }

  @Override
  public void updateEvaluation(Evaluation evaluation) throws LensException {
    String evaluationUpdateSql = "UPDATE evaluations SET startTime = ?, finishTime = ?, status =?, "
      + "lensSessionHandle = ?, modelInstanceId = ?, inputDataSetName = ? where id = ?";
    try {
      QueryRunner runner = new QueryRunner(ds);
      runner.update(evaluationUpdateSql, evaluation.getStartTime(), evaluation.getFinishTime(), evaluation
          .getStatus().toString(), evaluation.getLensSessionHandle().toString(), evaluation.getModelInstanceId(),
        evaluation.getInputDataSetName(), evaluation.getId());
    } catch (SQLException sq) {
      throw new LensException("Error while updating Evaluation. Id: " + evaluation.getId());
    }
  }

  @Override
  public String createEvaluation(Date startTime, Date finishTime, Status status,
                                 LensSessionHandle lensSessionHandle, String modelInstanceId,
                                 String inputDataSetName) throws LensException {
    String evaluationInsertSql = "INSERT INTO evaluations (id, startTime, finishTime, status, lensSessionHandle,"
      + " modelInstanceId, inputDataSetName) VALUES (?, ?, ?, ?, ?, ?, ?)";
    String evaluationId;
    try {
      QueryRunner runner = new QueryRunner(ds);
      Evaluation alreadyExisting = null;
      do {
        evaluationId = UUID.randomUUID().toString();
        alreadyExisting = getEvaluation(evaluationId);
      } while (alreadyExisting != null);

      runner.update(evaluationInsertSql, evaluationId, startTime, finishTime, status
        .toString(), lensSessionHandle.toString(), modelInstanceId, inputDataSetName);
    } catch (SQLException e) {
      log.error("Error while creating Evaluation for ModelInstance, " + modelInstanceId);
      throw new LensException("Error while creating Evaluation for ModelInstance, " + modelInstanceId, e);
    }
    return evaluationId;
  }

  public void deleteEvaluation(String evaluationId) throws SQLException {
    String sql = "delete from evaluations where id = ?";
    QueryRunner runner = new QueryRunner(ds);
    runner.update(sql, evaluationId);
  }

  @Override
  public Evaluation getEvaluation(String evaluationId) throws LensException {
    String evaluationGetSql = "SELECT * FROM evaluations WHERE id = ?";
    QueryRunner runner = new QueryRunner(ds);
    try {
      Evaluation evaluation = runner.query(evaluationGetSql, evaluationResultSetHandler, evaluationId);
      return evaluation;
    } catch (Exception e) {
      log.error("Error while reading evaluation, Id: " + evaluationId);
      throw new LensException("Error while reading evaluation, Id: " + evaluationId, e);
    }
  }

  @Override
  public void updatePrediction(Prediction prediction) throws LensException {
    String predictionUpdateSql = "UPDATE predictions SET startTime = ?, finishTime = ?, status =?, "
      + "lensSessionHandle = ?, modelInstanceId = ?, inputDataSet = ?, outputDataSet = ? where id = ?";
    try {
      QueryRunner runner = new QueryRunner(ds);
      runner.update(predictionUpdateSql, prediction.getStartTime(), prediction.getFinishTime(), prediction
          .getStatus().toString(), prediction.getLensSessionHandle().toString(), prediction.getModelInstanceId(),
        prediction.getInputDataSet(), prediction.getOutputDataSet(), prediction.getId());
    } catch (SQLException sq) {
      throw new LensException("Error while updating Prediction. Id: " + prediction.getId());
    }
  }

  public void deletePrediction(String predictionId) throws SQLException {
    String sql = "delete from predictions where id = ?";
    QueryRunner runner = new QueryRunner(ds);
    runner.update(sql, predictionId);
  }

  /**
   * Creates the table.
   *
   * @param sql the sql
   * @throws java.sql.SQLException the SQL exception
   */
  private void createTable(String sql) throws SQLException {
    QueryRunner runner = new QueryRunner(ds);
    runner.update(sql);
  }
}

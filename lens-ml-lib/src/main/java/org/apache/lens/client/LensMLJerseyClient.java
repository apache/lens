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

import org.apache.lens.api.StringList;
import org.apache.lens.api.ml.ModelMetadata;
import org.apache.lens.api.ml.TestReport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.*;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;

/*
 * Client code to invoke server side ML API
 */
/**
 * The Class LensMLJerseyClient.
 */
public class LensMLJerseyClient {

  /** The Constant LENS_ML_RESOURCE_PATH. */
  public static final String LENS_ML_RESOURCE_PATH = "lens.ml.resource.path";

  /** The Constant DEFAULT_ML_RESOURCE_PATH. */
  public static final String DEFAULT_ML_RESOURCE_PATH = "ml";

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(LensMLJerseyClient.class);

  /** The connection. */
  private final LensConnection connection;

  /**
   * Instantiates a new lens ml jersey client.
   *
   * @param connection
   *          the connection
   */
  public LensMLJerseyClient(LensConnection connection) {
    this.connection = connection;
  }

  protected WebTarget getMLWebTarget() {
    Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).build();
    LensConnectionParams connParams = connection.getLensConnectionParams();
    String baseURI = connParams.getBaseConnectionUrl();
    String mlURI = connParams.getConf().get(LENS_ML_RESOURCE_PATH, DEFAULT_ML_RESOURCE_PATH);
    return client.target(baseURI).path(mlURI);
  }

  /**
   * Gets the model metadata.
   *
   * @param algorithm
   *          the algorithm
   * @param modelID
   *          the model id
   * @return the model metadata
   */
  public ModelMetadata getModelMetadata(String algorithm, String modelID) {
    try {
      return getMLWebTarget().path("models").path(algorithm).path(modelID).request().get(ModelMetadata.class);
    } catch (NotFoundException exc) {
      return null;
    }
  }

  /**
   * Delete model.
   *
   * @param algorithm
   *          the algorithm
   * @param modelID
   *          the model id
   */
  public void deleteModel(String algorithm, String modelID) {
    getMLWebTarget().path("models").path(algorithm).path(modelID).request().delete();
  }

  /**
   * Gets the models for algorithm.
   *
   * @param algorithm
   *          the algorithm
   * @return the models for algorithm
   */
  public List<String> getModelsForAlgorithm(String algorithm) {
    try {
      StringList models = getMLWebTarget().path("models").path(algorithm).request().get(StringList.class);
      return models == null ? null : models.getElements();
    } catch (NotFoundException exc) {
      return null;
    }
  }

  public List<String> getTrainerNames() {
    StringList trainerNames = getMLWebTarget().path("trainers").request().get(StringList.class);
    return trainerNames == null ? null : trainerNames.getElements();
  }

  /**
   * Train model.
   *
   * @param algorithm
   *          the algorithm
   * @param params
   *          the params
   * @return the string
   */
  public String trainModel(String algorithm, Map<String, String> params) {
    Form form = new Form();

    for (Map.Entry<String, String> entry : params.entrySet()) {
      form.param(entry.getKey(), entry.getValue());
    }

    return getMLWebTarget().path(algorithm).path("train").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE), String.class);
  }

  /**
   * Test model.
   *
   * @param table
   *          the table
   * @param algorithm
   *          the algorithm
   * @param modelID
   *          the model id
   * @return the string
   */
  public String testModel(String table, String algorithm, String modelID) {
    WebTarget modelTestTarget = getMLWebTarget().path("test").path(table).path(algorithm).path(modelID);

    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), connection
        .getSessionHandle(), MediaType.APPLICATION_XML_TYPE));

    return modelTestTarget.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), String.class);
  }

  /**
   * Gets the test reports of algorithm.
   *
   * @param algorithm
   *          the algorithm
   * @return the test reports of algorithm
   */
  public List<String> getTestReportsOfAlgorithm(String algorithm) {
    try {
      StringList list = getMLWebTarget().path("reports").path(algorithm).request().get(StringList.class);
      return list == null ? null : list.getElements();
    } catch (NotFoundException exc) {
      return null;
    }
  }

  /**
   * Gets the test report.
   *
   * @param algorithm
   *          the algorithm
   * @param reportID
   *          the report id
   * @return the test report
   */
  public TestReport getTestReport(String algorithm, String reportID) {
    try {
      return getMLWebTarget().path("reports").path(algorithm).path(reportID).request().get(TestReport.class);
    } catch (NotFoundException exc) {
      return null;
    }
  }

  /**
   * Delete test report.
   *
   * @param algorithm
   *          the algorithm
   * @param reportID
   *          the report id
   * @return the string
   */
  public String deleteTestReport(String algorithm, String reportID) {
    return getMLWebTarget().path("reports").path(algorithm).path(reportID).request().delete(String.class);
  }

  /**
   * Predict single.
   *
   * @param algorithm
   *          the algorithm
   * @param modelID
   *          the model id
   * @param features
   *          the features
   * @return the string
   */
  public String predictSingle(String algorithm, String modelID, Map<String, String> features) {
    WebTarget target = getMLWebTarget().path("predict").path(algorithm).path(modelID);

    for (Map.Entry<String, String> entry : features.entrySet()) {
      target.queryParam(entry.getKey(), entry.getValue());
    }

    return target.request().get(String.class);
  }

  /**
   * Gets the param description of trainer.
   *
   * @param algorithm
   *          the algorithm
   * @return the param description of trainer
   */
  public List<String> getParamDescriptionOfTrainer(String algorithm) {
    try {
      StringList paramHelp = getMLWebTarget().path("trainers").path(algorithm).request(MediaType.APPLICATION_XML)
          .get(StringList.class);
      return paramHelp.getElements();
    } catch (NotFoundException exc) {
      return null;
    }
  }

  public Configuration getConf() {
    return connection.getLensConnectionParams().getConf();
  }
}

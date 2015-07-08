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

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.StringList;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.ml.api.Evaluation;
import org.apache.lens.ml.api.Feature;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

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

  public String createModel(String name, String algo, Map<String, String> algoParams,
                            List<Feature> features, Feature label) {
    return "";
  }

  public LensSessionHandle getSessionHandle() {
    if (sessionHandle != null) {
      return sessionHandle;
    }
    return connection.getSessionHandle();
  }

  public String createDataSet(String dataSetName, String dataTableName, String dataBase) {
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("dataSetName").build(), dataSetName));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("dataTableName").build(), dataTableName));

    LensAPIResult result = getMLWebTarget().path("dataset")
      .queryParam("dataSetName", dataSetName)
      .queryParam("dataTableName", dataTableName)
      .queryParam("dataBaseName", dataBase)
      .request(MediaType.APPLICATION_XML)
      .post(Entity.xml(dataSetName), LensAPIResult.class);

    LensAPIResult<String> apiResult = getMLWebTarget().path("dataset").request().post(
      Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), LensAPIResult.class);

    return apiResult.getData();

  }

  public void test() {

    Evaluation result = getMLWebTarget().path("evaluation")
      .request(MediaType.APPLICATION_XML)
      .get(Evaluation.class);

  }

}

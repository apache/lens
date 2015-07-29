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
package org.apache.lens.ml.server;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.StringList;
import org.apache.lens.ml.api.*;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.ServiceProvider;
import org.apache.lens.server.api.ServiceProviderFactory;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Machine Learning service.
 */
@Path("/ml")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class MLServiceResource {

  /**
   * The Constant LOG.
   */
  public static final Log LOG = LogFactory.getLog(MLServiceResource.class);
  /**
   * Message indicating if ML service is up
   */
  public static final String ML_UP_MESSAGE = "ML service is up";
  private static final HiveConf HIVE_CONF;

  static {
    HIVE_CONF = new HiveConf();
    // Add default config so that we know the service provider implementation
    HIVE_CONF.addResource("lensserver-default.xml");
    HIVE_CONF.addResource("lens-site.xml");
  }

  /**
   * The ml service.
   */
  MLService mlService;
  /**
   * The service provider.
   */
  ServiceProvider serviceProvider;
  /**
   * The service provider factory.
   */
  ServiceProviderFactory serviceProviderFactory;

  /**
   * Instantiates a new ML service resource.
   */
  public MLServiceResource() {
    serviceProviderFactory = getServiceProviderFactory(HIVE_CONF);
  }

  private ServiceProvider getServiceProvider() {
    if (serviceProvider == null) {
      serviceProvider = serviceProviderFactory.getServiceProvider();
    }
    return serviceProvider;
  }

  /**
   * Gets the service provider factory.
   *
   * @param conf the conf
   * @return the service provider factory
   */
  private ServiceProviderFactory getServiceProviderFactory(HiveConf conf) {
    Class<?> spfClass = conf.getClass(LensConfConstants.SERVICE_PROVIDER_FACTORY, ServiceProviderFactory.class);
    try {
      return (ServiceProviderFactory) spfClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private MLService getMlService() {
    if (mlService == null) {
      mlService = (MLService) getServiceProvider().getService(MLService.NAME);
    }
    return mlService;
  }

  /**
   * Indicates if ML resource is up
   *
   * @return
   */
  @GET
  public String mlResourceUp() {
    return ML_UP_MESSAGE;
  }

  /**
   * Get the list of algos available.
   *
   * @return
   */
  @GET
  @Path("algos")
  public List<Algo> getAlgos() {
    List<Algo> algos = getMlService().getAlgos();
    return algos;
  }

  @GET
  @Path("algonames")
  public StringList getAlgoNames() {
    List<Algo> algos = getMlService().getAlgos();
    ArrayList<String> stringArrayList = new ArrayList();
    for (Algo algo : algos) {
      stringArrayList.add(algo.getName());
    }

    StringList result = new StringList(stringArrayList);
    return result;
  }

  @POST
  @Path("dataset")
  public APIResult createDataSet(DataSet dataSet) throws LensException {
    getMlService().createDataSet(dataSet);
    return new APIResult(APIResult.Status.SUCCEEDED, "");
  }

  @GET
  @Path("dataset")
  public DataSet getDataSet(@QueryParam("dataSetName") String dataSetName) throws LensException {
    return getMlService().getDataSet(dataSetName);
  }

  @POST
  @Path("models")
  public APIResult createModel(Model model) throws LensException {
    getMlService().createModel(model);
    return new APIResult(APIResult.Status.SUCCEEDED, "");
  }

  @GET
  @Path("models")
  public Model getModel(@QueryParam("modelName") String modelName) throws LensException {
    return getMlService().getModel(modelName);
  }

  @GET
  @Path("train")
  public String trainModel(@QueryParam("modelId") String modelId, @QueryParam("dataSetName") String dataSetName,
                           @QueryParam("lensSessionHandle") LensSessionHandle
                             lensSessionHandle)
    throws
    LensException {
    return getMlService().trainModel(modelId, dataSetName, lensSessionHandle);
  }

  @GET
  @Path("modelinstance/{modelInstanceId}")
  public ModelInstance getModelInstance(@PathParam("modelInstanceId") String modelInstanceId) throws LensException {
    return getMlService().getModelInstance(modelInstanceId);
  }

  @DELETE
  @Path("modelinstance/{modelInstanceId}")
  public boolean cancelModelInstance(@PathParam("modelInstanceId") String modelInstanceId,
                                     @QueryParam("lensSessionHandle") LensSessionHandle lensSessionHandle)
    throws LensException {
    return getMlService().cancelModelInstance(modelInstanceId, lensSessionHandle);
  }

  @GET
  @Path("predict")
  public String predict(@QueryParam("modelInstanceId") String modelInstanceId, @QueryParam("dataSetName") String
    dataSetName, @QueryParam("lensSessionHandle") LensSessionHandle lensSessionHandle) throws LensException {
    return getMlService().predict(modelInstanceId, dataSetName, lensSessionHandle);
  }

  @GET
  @Path("prediction/{predictionId}")
  public Prediction getPrediction(@PathParam("predictionId") String predictionId) throws LensException {
    return getMlService().getPrediction(predictionId);
  }

  @DELETE
  @Path("prediction/{predictionId}")
  public boolean cancelPrediction(@PathParam("predictionId") String predictionId,
                                  @QueryParam("lensSessionHandle") LensSessionHandle lensSessionHandle)
    throws LensException {
    return getMlService().cancelPrediction(predictionId, lensSessionHandle);
  }

  @GET
  @Path("evaluate")
  public String evaluate(@QueryParam("modelInstanceId") String modelInstanceId, @QueryParam("dataSetName") String
    dataSetName, @QueryParam("lensSessionHandle") LensSessionHandle lensSessionHandle)
    throws LensException {
    return getMlService().evaluate(modelInstanceId, dataSetName, lensSessionHandle);
  }

  @GET
  @Path("evaluation/{evalId}")
  public Evaluation getEvaluation(@PathParam("evalId") String evalId) throws LensException {
    return getMlService().getEvaluation(evalId);
  }

  @DELETE
  @Path("evaluation/{evalId}")
  public APIResult cancelEvaluation(@PathParam("evalId") String evalId,
                                    @QueryParam("lensSessionHandle") LensSessionHandle lensSessionHandle)
    throws LensException {
    boolean result = getMlService().cancelEvaluation(evalId, lensSessionHandle);
    if (result) {
      return new APIResult(APIResult.Status.SUCCEEDED, "");
    }
    return new APIResult(APIResult.Status.FAILED, "");
  }
}

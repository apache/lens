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

import static org.apache.commons.lang.StringUtils.isBlank;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.*;
import javax.ws.rs.core.*;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.StringList;
import org.apache.lens.ml.algo.api.MLModel;
import org.apache.lens.ml.api.MLTestReport;
import org.apache.lens.ml.api.ModelMetadata;
import org.apache.lens.ml.api.TestReport;
import org.apache.lens.ml.impl.ModelLoader;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.ServiceProvider;
import org.apache.lens.server.api.ServiceProviderFactory;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;

import org.glassfish.jersey.media.multipart.FormDataParam;

import lombok.extern.slf4j.Slf4j;

/**
 * Machine Learning service.
 */
@Path("/ml")
@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
@Slf4j
public class MLServiceResource {

  /** The ml service. */
  MLService mlService;

  /** The service provider. */
  ServiceProvider serviceProvider;

  /** The service provider factory. */
  ServiceProviderFactory serviceProviderFactory;

  private static final HiveConf HIVE_CONF;

  /**
   * Message indicating if ML service is up
   */
  public static final String ML_UP_MESSAGE = "ML service is up";

  static {
    HIVE_CONF = new HiveConf();
    // Add default config so that we know the service provider implementation
    HIVE_CONF.addResource("lensserver-default.xml");
    HIVE_CONF.addResource("lens-site.xml");
  }

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
      mlService = getServiceProvider().getService(MLService.NAME);
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
   * Get a list of algos available
   *
   * @return
   */
  @GET
  @Path("algos")
  public StringList getAlgoNames() {
    List<String> algos = getMlService().getAlgorithms();
    StringList result = new StringList(algos);
    return result;
  }

  /**
   * Gets the human readable param description of an algorithm
   *
   * @param algorithm the algorithm
   * @return the param description
   */
  @GET
  @Path("algos/{algorithm}")
  public StringList getParamDescription(@PathParam("algorithm") String algorithm) {
    Map<String, String> paramDesc = getMlService().getAlgoParamDescription(algorithm);
    if (paramDesc == null) {
      throw new NotFoundException("Param description not found for " + algorithm);
    }

    List<String> descriptions = new ArrayList<String>();
    for (String key : paramDesc.keySet()) {
      descriptions.add(key + " : " + paramDesc.get(key));
    }
    return new StringList(descriptions);
  }

  /**
   * Get model ID list for a given algorithm.
   *
   * @param algorithm algorithm name
   * @return the models for algo
   * @throws LensException the lens exception
   */
  @GET
  @Path("models/{algorithm}")
  public StringList getModelsForAlgo(@PathParam("algorithm") String algorithm) throws LensException {
    List<String> models = getMlService().getModels(algorithm);
    if (models == null || models.isEmpty()) {
      throw new NotFoundException("No models found for algorithm " + algorithm);
    }
    return new StringList(models);
  }

  /**
   * Get metadata of the model given algorithm and model ID.
   *
   * @param algorithm algorithm name
   * @param modelID   model ID
   * @return model metadata
   * @throws LensException the lens exception
   */
  @GET
  @Path("models/{algorithm}/{modelID}")
  public ModelMetadata getModelMetadata(@PathParam("algorithm") String algorithm, @PathParam("modelID") String modelID)
    throws LensException {
    MLModel model = getMlService().getModel(algorithm, modelID);
    if (model == null) {
      throw new NotFoundException("Model not found " + modelID + ", algo=" + algorithm);
    }

    ModelMetadata meta = new ModelMetadata(model.getId(), model.getTable(), model.getAlgoName(), StringUtils.join(
      model.getParams(), ' '), model.getCreatedAt().toString(), getMlService().getModelPath(algorithm, modelID),
      model.getLabelColumn(), StringUtils.join(model.getFeatureColumns(), ","));
    return meta;
  }

  /**
   * Delete a model given model ID and algorithm name.
   *
   * @param algorithm the algorithm
   * @param modelID   the model id
   * @return confirmation text
   * @throws LensException the lens exception
   */
  @DELETE
  @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  @Path("models/{algorithm}/{modelID}")
  public String deleteModel(@PathParam("algorithm") String algorithm, @PathParam("modelID") String modelID)
    throws LensException {
    getMlService().deleteModel(algorithm, modelID);
    return "DELETED model=" + modelID + " algorithm=" + algorithm;
  }

  /**
   * Train a model given an algorithm name and algorithm parameters
   * <p>
   * Following parameters are mandatory and must be passed as part of the form
   * </p>
   * <ol>
   * <li>table - input Hive table to load training data from</li>
   * <li>label - name of the labelled column</li>
   * <li>feature - one entry per feature column. At least one feature column is required</li>
   * </ol>
   * <p></p>
   *
   * @param algorithm algorithm name
   * @param form      form data
   * @return if model is successfully trained, the model ID will be returned
   * @throws LensException the lens exception
   */
  @POST
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Path("{algorithm}/train")
  public String train(@PathParam("algorithm") String algorithm, MultivaluedMap<String, String> form)
    throws LensException {

    // Check if algo is valid
    if (getMlService().getAlgoForName(algorithm) == null) {
      throw new NotFoundException("Algo for algo: " + algorithm + " not found");
    }

    if (isBlank(form.getFirst("table"))) {
      throw new BadRequestException("table parameter is rquired");
    }

    String table = form.getFirst("table");

    if (isBlank(form.getFirst("label"))) {
      throw new BadRequestException("label parameter is required");
    }

    // Check features
    List<String> featureNames = form.get("feature");
    if (featureNames.size() < 1) {
      throw new BadRequestException("At least one feature is required");
    }

    List<String> algoArgs = new ArrayList<String>();
    Set<Map.Entry<String, List<String>>> paramSet = form.entrySet();

    for (Map.Entry<String, List<String>> e : paramSet) {
      String p = e.getKey();
      List<String> values = e.getValue();
      if ("algorithm".equals(p) || "table".equals(p)) {
        continue;
      } else if ("feature".equals(p)) {
        for (String feature : values) {
          algoArgs.add("feature");
          algoArgs.add(feature);
        }
      } else if ("label".equals(p)) {
        algoArgs.add("label");
        algoArgs.add(values.get(0));
      } else {
        algoArgs.add(p);
        algoArgs.add(values.get(0));
      }
    }
    log.info("Training table {} with algo {} params={}", table, algorithm, algoArgs.toString());
    String modelId = getMlService().train(table, algorithm, algoArgs.toArray(new String[]{}));
    log.info("Done training {} modelid = {}", table, modelId);
    return modelId;
  }

  /**
   * Clear model cache (for admin use).
   *
   * @return OK if the cache was cleared
   */
  @DELETE
  @Path("clearModelCache")
  @Produces(MediaType.TEXT_PLAIN)
  public Response clearModelCache() {
    ModelLoader.clearCache();
    log.info("Cleared model cache");
    return Response.ok("Cleared cache", MediaType.TEXT_PLAIN_TYPE).build();
  }

  /**
   * Run a test on a model for an algorithm.
   *
   * @param algorithm algorithm name
   * @param modelID   model ID
   * @param table     Hive table to run test on
   * @param session   Lens session ID. This session ID will be used to run the test query
   * @return Test report ID
   * @throws LensException the lens exception
   */
  @POST
  @Path("test/{table}/{algorithm}/{modelID}")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public String test(@PathParam("algorithm") String algorithm, @PathParam("modelID") String modelID,
    @PathParam("table") String table, @FormDataParam("sessionid") LensSessionHandle session,
    @FormDataParam("outputTable") String outputTable) throws LensException {
    MLTestReport testReport = getMlService().testModel(session, table, algorithm, modelID, outputTable);
    return testReport.getReportID();
  }

  /**
   * Get list of reports for a given algorithm.
   *
   * @param algoritm the algoritm
   * @return the reports for algorithm
   * @throws LensException the lens exception
   */
  @GET
  @Path("reports/{algorithm}")
  public StringList getReportsForAlgorithm(@PathParam("algorithm") String algoritm) throws LensException {
    List<String> reports = getMlService().getTestReports(algoritm);
    if (reports == null || reports.isEmpty()) {
      throw new NotFoundException("No test reports found for " + algoritm);
    }
    return new StringList(reports);
  }

  /**
   * Get a single test report given the algorithm name and report id.
   *
   * @param algorithm the algorithm
   * @param reportID  the report id
   * @return the test report
   * @throws LensException the lens exception
   */
  @GET
  @Path("reports/{algorithm}/{reportID}")
  public TestReport getTestReport(@PathParam("algorithm") String algorithm, @PathParam("reportID") String reportID)
    throws LensException {
    MLTestReport report = getMlService().getTestReport(algorithm, reportID);

    if (report == null) {
      throw new NotFoundException("Test report: " + reportID + " not found for algorithm " + algorithm);
    }

    TestReport result = new TestReport(report.getTestTable(), report.getOutputTable(), report.getOutputColumn(),
      report.getLabelColumn(), StringUtils.join(report.getFeatureColumns(), ","), report.getAlgorithm(),
      report.getModelID(), report.getReportID(), report.getLensQueryID());
    return result;
  }

  /**
   * DELETE a report given the algorithm name and report ID.
   *
   * @param algorithm the algorithm
   * @param reportID  the report id
   * @return the string
   * @throws LensException the lens exception
   */
  @DELETE
  @Path("reports/{algorithm}/{reportID}")
  @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public String deleteTestReport(@PathParam("algorithm") String algorithm, @PathParam("reportID") String reportID)
    throws LensException {
    getMlService().deleteTestReport(algorithm, reportID);
    return "DELETED report=" + reportID + " algorithm=" + algorithm;
  }

  /**
   * Predict.
   *
   * @param algorithm the algorithm
   * @param modelID   the model id
   * @param uriInfo   the uri info
   * @return the string
   * @throws LensException the lens exception
   */
  @GET
  @Path("/predict/{algorithm}/{modelID}")
  @Produces({MediaType.APPLICATION_ATOM_XML, MediaType.APPLICATION_JSON})
  public String predict(@PathParam("algorithm") String algorithm, @PathParam("modelID") String modelID,
    @Context UriInfo uriInfo) throws LensException {
    // Load the model instance
    MLModel<?> model = getMlService().getModel(algorithm, modelID);

    // Get input feature names
    MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
    String[] features = new String[model.getFeatureColumns().size()];
    // Assuming that feature name parameters are same
    int i = 0;
    for (String feature : model.getFeatureColumns()) {
      features[i++] = params.getFirst(feature);
    }

    // TODO needs a 'prediction formatter'
    return getMlService().predict(algorithm, modelID, features).toString();
  }
}

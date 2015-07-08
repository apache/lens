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

import org.apache.lens.api.StringList;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.ml.api.Algo;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.ServiceProvider;
import org.apache.lens.server.api.ServiceProviderFactory;

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

  /**
   *
   * @param dataSetName
   * @param dataTableName
   * @param dataBaseName
   * @return
   */
  @POST
  @Path("dataset")
  public LensAPIResult<String> createDataSet(@QueryParam("dataSetName") String dataSetName,
                                             @QueryParam("dataTableName") String dataTableName,
                                             @QueryParam("dataBaseName")
                                             String dataBaseName) {
    String dataSetId = "";
    try {
      getMlService().createDataSet(dataSetName, dataTableName, dataBaseName);
      return LensAPIResult.composedOf(null, null, null);
    } catch (Exception e) {
      LOG.error("Error creating dataSet " + dataSetName, e);
      return LensAPIResult.composedOf(null, "1.2", dataSetId);
    }
  }

  //@POST
  //@Path("evaluation")
  //public Evaluation evaluation(){
  //    return new Evaluation("modelInstance1", "dataSet1","id", new Date(), new Date(), Status.CANCELLED);
  //}

}

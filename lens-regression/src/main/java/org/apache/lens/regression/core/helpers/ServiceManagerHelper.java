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

package org.apache.lens.regression.core.helpers;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

import javax.ws.rs.client.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import org.apache.lens.regression.core.type.FormBuilder;
import org.apache.lens.regression.core.type.MapBuilder;
import org.apache.lens.regression.util.Util;

import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class ServiceManagerHelper {

  private static final String LENS_BASE_URL = "lens.baseurl";
  private static final String LENS_ADMIN_URL = "lens.adminurl";
  private static final String LENS_USERNAME = "lens.username";
  private static final String LENS_PASSWORD = "lens.password";
  private static final String LENS_SERVER_DIR = "lens.server.dir";
  private static final String LENS_CLIENT_DIR = "lens.client.dir";
  private static final String LENS_SERVER_HDFS_URL = "lens.server.hdfsurl";
  private static final String LENS_CURRENT_DB = "lens.server.currentDB";
  private static final String JOB_CONF_URL = "job.conf.url";
  private static final String START_DATE = "query.start.date";

  protected static String sessionHandleString;
  protected static WebTarget servLens;
  protected Properties configProp = null;

  public ServiceManagerHelper(String envFileName) {
    configProp = Util.getPropertiesObj(envFileName);
  }

  public ServiceManagerHelper() {

  }

  public static WebTarget init() {
    Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).build();
    String baseUri = Util.getProperty(LENS_BASE_URL);
    servLens = client.target(UriBuilder.fromUri(baseUri).build());
    return servLens;
  }

  public static WebTarget getServerLens() {
    return servLens;
  }

  public static URI getServiceURI(String baseUri) {
    return UriBuilder.fromUri(baseUri).build();
  }

  public String getBaseUrl() {
    return configProp.getProperty(LENS_BASE_URL);
  }

  public String getAdminUrl() {
    return configProp.getProperty(LENS_ADMIN_URL);
  }

  public String getUserName() {
    return configProp.getProperty(LENS_USERNAME);
  }

  public String getPassword() {
    return configProp.getProperty(LENS_PASSWORD);
  }

  public String getServerDir() {
    return configProp.getProperty(LENS_SERVER_DIR);
  }

  public String getClientDir() {
    return configProp.getProperty(LENS_CLIENT_DIR);
  }

  public String getServerHdfsUrl() {
    return configProp.getProperty(LENS_SERVER_HDFS_URL);
  }

  public String getSessionHandleString() {
    return sessionHandleString;
  }

  public String getCurrentDB() {
    return configProp.getProperty(LENS_CURRENT_DB);
  }

  public String getJobConfUrl() {
    return configProp.getProperty(JOB_CONF_URL);
  }

  public String getStartDate() {
    return configProp.getProperty(START_DATE);
  }

  public <T> Response exec(String functionName, String path, WebTarget service, FormDataMultiPart headers,
      MapBuilder queryParams, MediaType inputMediaType, String outputMediaType) {
    return exec(functionName, path, service, headers, queryParams, inputMediaType, outputMediaType, null);
  }

  public <T> Response exec(String functionName, String path, WebTarget service, FormDataMultiPart headers,
      MapBuilder queryParams, MediaType inputMediaType) {
    return exec(functionName, path, service, headers, queryParams, inputMediaType, null, null);
  }

  public <T> Response exec(String functionName, String path, WebTarget service, FormDataMultiPart headers,
      MapBuilder queryParams) {
    return exec(functionName, path, service, headers, queryParams, null, null, null);
  }

  public <T> Response exec(String functionName, String path, WebTarget service, FormDataMultiPart headers,
      MapBuilder queryParams, MediaType inputMediaType, String outputMediaType, T inputObject) {

    Response cl = (Response) exec(functionName, path, service, headers, queryParams, inputMediaType, outputMediaType,
        Response.class, inputObject);
    return cl;
  }

  public <T> Object exec(String functionName, String path, WebTarget service, FormDataMultiPart headers,
      MapBuilder queryParams, MediaType inputMediaType, String outputMediaType, Class responseClass, T inputObject) {

    if (outputMediaType == null) {
      outputMediaType = MediaType.APPLICATION_XML;
    }
    if (inputMediaType == null) {
      inputMediaType = MediaType.APPLICATION_XML_TYPE;
    }

    WebTarget builder = service.path(path);
    if (queryParams != null) {
      for (Map.Entry<String, String> queryMapEntry : queryParams.getMap().entrySet()) {
        builder = builder.queryParam(queryMapEntry.getKey(), queryMapEntry.getValue());
      }
    }

    Invocation.Builder build = builder.request(outputMediaType);
    functionName = "exec" + functionName.toUpperCase();

    try {
      Class methodClass = Class.forName(this.getClass().getName());
      Object methodObject = methodClass.newInstance();

      Method method = methodObject.getClass()
          .getMethod(functionName, Invocation.Builder.class, inputMediaType.getClass(), responseClass.getClass(),
              Object.class);
      Object result = method.invoke(methodObject, build, inputMediaType, responseClass, inputObject);
      return result;

    } catch (Exception e) {
      log.error("Exception in exec", e);
      return null;
    }
  }



  public <T> Object execPUT(Invocation.Builder builder, MediaType inputMediaType, Class<?> responseClass,
      T inputObject) {
    return builder.put(Entity.entity(inputObject, inputMediaType), responseClass);
  }

  public <T> Object execGET(Invocation.Builder builder, MediaType inputMediaType, Class<?> responseClass,
      T inputObject) {
    return builder.get(responseClass);
  }

  public <T> Object execPOST(Invocation.Builder builder, MediaType inputMediaType, Class<?> responseClass,
      T inputObject) {
    return builder.post(Entity.entity(inputObject, inputMediaType), responseClass);
  }

  public <T> Object execDELETE(Invocation.Builder builder, MediaType inputMediaType, Class<?> responseClass,
      T inputObject) {
    return builder.delete(responseClass);
  }

  public Response sendForm(String methodName, String url, FormBuilder formData) {
    Response response = this
        .exec(methodName, url, servLens, null, null, MediaType.MULTIPART_FORM_DATA_TYPE, MediaType.APPLICATION_XML,
            formData.getForm());
    return response;
  }

  public Response sendQuery(String methodName, String url, MapBuilder query) {
    Response response = this.exec(methodName, url, servLens, null, query);
    return response;
  }

}

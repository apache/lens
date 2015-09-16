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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

import javax.ws.rs.client.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import javax.xml.bind.JAXBException;

import org.apache.lens.api.APIResult;
import org.apache.lens.regression.core.constants.SessionURL;
import org.apache.lens.regression.core.type.FormBuilder;
import org.apache.lens.regression.core.type.MapBuilder;
import org.apache.lens.regression.util.AssertUtil;
import org.apache.lens.regression.util.Util;
import org.apache.lens.server.api.error.LensException;

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

  protected static String sessionHandleString;
  protected static WebTarget servLens;

  protected String baseUrl;
  protected String adminUrl;
  protected String userName;
  protected String password;
  protected String serverDir;
  protected String clientDir;
  protected String serverHdfsUrl;
  protected String currentDB;

  public ServiceManagerHelper(String envFileName) {
    Properties prop = Util.getPropertiesObj(envFileName);
    this.baseUrl = prop.getProperty(LENS_BASE_URL);
    this.adminUrl = prop.getProperty(LENS_ADMIN_URL);
    this.userName = prop.getProperty(LENS_USERNAME);
    this.password = prop.getProperty(LENS_PASSWORD);
    this.serverDir = prop.getProperty(LENS_SERVER_DIR);
    this.clientDir = prop.getProperty(LENS_CLIENT_DIR);
    this.serverHdfsUrl = prop.getProperty(LENS_SERVER_HDFS_URL);
    this.currentDB = prop.getProperty(LENS_CURRENT_DB);
  }

  public ServiceManagerHelper() {

  }

  public static WebTarget init() {
    Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).build();
    String baseUri = Util.getProperty("lens.baseurl");
    servLens = client.target(UriBuilder.fromUri(baseUri).build());
    return servLens;
  }

  public static WebTarget getServerLens() {
    return servLens;
  }

  public static String getSessionHandle() {
    return sessionHandleString;
  }

  public static URI getServiceURI(String baseUri) {
    return UriBuilder.fromUri(baseUri).build();
  }

  public String getBaseUrl() {
    return baseUrl;
  }

  public String getAdminUrl() {
    return adminUrl;
  }

  public String getUserName() {
    return userName;
  }

  public String getPassword() {
    return password;
  }

  public String getServerDir() {
    return serverDir;
  }

  public String getClientDir() {
    return clientDir;
  }

  public String getServerHdfsUrl() {
    return serverHdfsUrl;
  }

  public String getSessionHandleString() {
    return sessionHandleString;
  }

  public String getCurrentDB() {
    return currentDB;
  }

  public String openSession(String database) throws JAXBException, LensException {
    FormBuilder formData = new FormBuilder();
    formData.add("username", this.getUserName());
    formData.add("password", this.getPassword());
    if (database != null) {
      formData.add("database", database);
    }
    Response response = this.exec("post", SessionURL.SESSION_BASE_URL, ServiceManagerHelper.servLens, null, null,
        MediaType.MULTIPART_FORM_DATA_TYPE, MediaType.APPLICATION_XML, formData.getForm());
    AssertUtil.assertSucceededResponse(response);
    sessionHandleString = response.readEntity(String.class);
    log.info("Session Handle String:{}", sessionHandleString);
    return sessionHandleString;
  }

  public String openSession() throws JAXBException, LensException {
    return openSession(null);
  }

  public void closeSession() throws JAXBException, LensException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = this.exec("delete", SessionURL.SESSION_BASE_URL, ServiceManagerHelper.servLens, null, query);
    APIResult result = response.readEntity(APIResult.class);
    if (result.getStatus() != APIResult.Status.SUCCEEDED) {
      throw new LensException("Status should be SUCCEEDED");
    }
    if (response.getStatus() != 200) {
      throw new LensException("Status code should be 200");
    }
    if (result.getMessage() == null) {
      throw new LensException("Status message is null");
    }
    log.info("Closed Session : {}", sessionHandleString);
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

  public <T> Object exec(String functionName, String path, WebTarget service, FormDataMultiPart headers,
      MapBuilder queryParams, MediaType inputMediaType, String outputMediaType, Class responseClass, T inputObject) {

    Object result;
    WebTarget builder = null;
    String className = this.getClass().getName();

    if (outputMediaType == null) {
      outputMediaType = MediaType.WILDCARD;
    }
    if (inputMediaType == null) {
      inputMediaType = MediaType.WILDCARD_TYPE;
    }

    builder = service.path(path);
    if (queryParams != null) {
      for (Map.Entry<String, String> queryMapEntry : queryParams.getMap().entrySet()) {
        builder = builder.queryParam(queryMapEntry.getKey(), queryMapEntry.getValue());
      }
    }

    Invocation.Builder build = builder.request(outputMediaType);

    functionName = "exec" + functionName.toUpperCase();

    try {
      Class methodClass = Class.forName(className);
      Object methodObject = methodClass.newInstance();

      Method method = methodObject.getClass()
          .getMethod(functionName, Invocation.Builder.class, inputMediaType.getClass(), responseClass.getClass(),
              Object.class);
      result = method.invoke(methodObject, build, inputMediaType, responseClass, inputObject);
      return result;

    } catch (NoSuchMethodException e) {
      return e.getMessage();
    } catch (InstantiationException e) {
      return e.getMessage();
    } catch (IllegalAccessException e) {
      return e.getMessage();
    } catch (InvocationTargetException e) {
      return e.getMessage();
    } catch (ClassNotFoundException e) {
      return e.getMessage();
    } catch (Exception e) {
      return e.getMessage();
    }

  }

  public <T> Response exec(String functionName, String path, WebTarget service, FormDataMultiPart headers,
      MapBuilder queryParams, MediaType inputMediaType, String outputMediaType, T inputObject) {

    Class responseClass = Response.class;
    Response cl = null;
    try {
      cl = (Response) exec(functionName, path, service, headers, queryParams, inputMediaType, outputMediaType,
          responseClass, inputObject);
    } catch (Exception e) {
      System.out.println(e);
    }
    return cl;
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

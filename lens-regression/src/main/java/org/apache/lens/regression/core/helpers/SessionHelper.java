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

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBException;

import org.apache.lens.api.APIResult;
import org.apache.lens.regression.core.type.FormBuilder;
import org.apache.lens.regression.core.type.MapBuilder;
import org.apache.lens.regression.util.AssertUtil;
import org.apache.lens.regression.util.Util;
import org.apache.lens.server.api.error.LensException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SessionHelper extends ServiceManagerHelper {


  public SessionHelper() {
  }

  public SessionHelper(String envFileName) {
    super(envFileName);
  }

  /**
   * Open a New Session
   *
   * @param userName
   * @param password
   * @param database
   * @return the sessionHandle String
   */

  public String openNewSession(String userName, String password, String database) throws JAXBException, LensException {
    FormBuilder formData = new FormBuilder();
    formData.add("username", userName);
    formData.add("password", password);
    if (database != null) {
      formData.add("database", database);
    }
    Response response = this
        .exec("post", "/session", servLens, null, null, MediaType.MULTIPART_FORM_DATA_TYPE, MediaType.APPLICATION_XML,
            formData.getForm());
    AssertUtil.assertSucceededResponse(response);
    String newSessionHandleString = response.readEntity(String.class);
    log.info("Session Handle String:{}", newSessionHandleString);
    return newSessionHandleString;
  }

  public String openNewSession(String userName, String password) throws JAXBException, LensException {
    return openNewSession(userName, password, null);
  }

  /**
   * Close a Session
   *
   * @param sessionHandleString
   */
  public void closeNewSession(String sessionHandleString) throws JAXBException, LensException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = this.exec("delete", "/session", servLens, null, query);

    APIResult result = response.readEntity(APIResult.class);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      throw new LensException("Status should be SUCCEEDED");
    }
    if (response.getStatus() == 200) {
      throw new LensException("Status code should be 200");
    }
    if (result.getMessage() == null) {
      throw new LensException("Status message is null");
    }
    log.info("Closed Session : {}", sessionHandleString);
  }

  /**
   * Set and Validate Session Params
   *
   * @param sessionHandleString
   * @param param
   * @param value
   */
  public void setAndValidateParam(String sessionHandleString, String param, String value) throws Exception {
    boolean success;
    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", sessionHandleString);
    formData.add("key", param);
    formData.add("value", value);
    Response response = this
        .exec("put", "/session/params", servLens, null, null, MediaType.MULTIPART_FORM_DATA_TYPE, null,
            formData.getForm());
    AssertUtil.assertSucceeded(response);
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    query.put("key", param);
    response = this.exec("get", "/session/params", servLens, null, query);
    AssertUtil.assertSucceededResponse(response);
    String responseString = response.readEntity(String.class);
    log.info(responseString);
    HashMap<String, String> map = Util.stringListToMap(responseString);
    if (!map.get(param).equals(value)) {
      throw new LensException("Could not set property");
    }
    log.info("Added property {}={}", param, value);
  }

  public void setAndValidateParam(String param, String value) throws Exception {
    setAndValidateParam(sessionHandleString, param, value);
  }

  public void setAndValidateParam(Map<String, String> map, String sessionHandleString) throws Exception {
    for (Map.Entry<String, String> entry : map.entrySet()) {
      setAndValidateParam(sessionHandleString, entry.getKey(), entry.getValue());
    }
  }

  public void setAndValidateParam(Map<String, String> map) throws Exception {
    setAndValidateParam(map, sessionHandleString);
  }

  /**
   * Add resources to a session
   *
   * @param path
   * @param sessionHandleString
   */
  public void addResourcesJar(String path, String sessionHandleString) throws JAXBException, LensException {
    log.info("Adding Resources {}", path);
    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", sessionHandleString);
    formData.add("type", "jar");
    formData.add("path", path);
    Response response = this
        .exec("put", "/session/resources/add", servLens, null, null, MediaType.MULTIPART_FORM_DATA_TYPE, null,
            formData.getForm());
    log.info("Response : {}", response);
    AssertUtil.assertSucceeded(response);
  }

  public void addResourcesJar(String path) throws JAXBException, LensException {
    addResourcesJar(path, sessionHandleString);
  }

  /**
   * Remove resources from a session
   *
   * @param path
   * @param sessionHandleString
   */
  public void removeResourcesJar(String path, String sessionHandleString) throws JAXBException, LensException {
    log.info("Removing Resources {}", path);
    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", sessionHandleString);
    formData.add("type", "jar");
    formData.add("path", path);
    Response response = this
        .exec("put", "/session/resources/delete", servLens, null, null, MediaType.MULTIPART_FORM_DATA_TYPE, null,
            formData.getForm());
    log.info("Response : {}", response);
    AssertUtil.assertSucceeded(response);
  }

  public void removeResourcesJar(String path) throws JAXBException, LensException {
    removeResourcesJar(path, sessionHandleString);
  }

}

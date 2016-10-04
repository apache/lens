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
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.StringList;
import org.apache.lens.api.session.UserSessionInfo;
import org.apache.lens.regression.core.constants.SessionURL;
import org.apache.lens.regression.core.type.FormBuilder;
import org.apache.lens.regression.core.type.MapBuilder;
import org.apache.lens.regression.util.AssertUtil;
import org.apache.lens.regression.util.Util;
import org.apache.lens.server.api.error.LensException;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

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
   * @param userName
   * @param password
   * @param database
   * @return the sessionHandle String
   */

  public Response openSessionReturnResponse(String userName, String password, String database, String outputMediaType)
    throws LensException {
    FormBuilder formData = new FormBuilder();
    formData.add("username", userName);
    formData.add("password", password);
    if (database != null) {
      formData.add("database", database);
    }
    LensConf conf = new LensConf();
    formData.getForm().bodyPart(
        new FormDataBodyPart(FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(), conf,
            MediaType.APPLICATION_XML_TYPE));
    formData.add("sessionconf", conf.toString(), MediaType.APPLICATION_JSON_TYPE);

    Response response = this.exec("post", SessionURL.SESSION_BASE_URL, servLens, null, null,
        MediaType.MULTIPART_FORM_DATA_TYPE, outputMediaType, formData.getForm());

    return response;
  }

  public String openSession(String database) throws LensException {
    Response response = openSessionReturnResponse(this.getUserName(), this.getPassword(), database,
        MediaType.APPLICATION_XML);
    AssertUtil.assertSucceededResponse(response);
    sessionHandleString = response.readEntity(String.class);
    log.info("Session Handle String:{}", sessionHandleString);
    return sessionHandleString;
  }

  public String openSession() throws LensException {
    return openSession(null);
  }

  public String openSession(String userName, String password, String database, String outputMediaType)
    throws LensException {
    Response response = openSessionReturnResponse(userName, password, database, outputMediaType);
    AssertUtil.assertSucceededResponse(response);
    String newSessionHandleString = response.readEntity(String.class);
    log.info("Session Handle String:{}", newSessionHandleString);
    return newSessionHandleString;
  }

  public String openSession(String userName, String password) throws LensException {
    return openSession(userName, password, null, MediaType.APPLICATION_XML);
  }

  public String openSession(String userName, String password, String database) throws LensException {
    return openSession(userName, password, database, MediaType.APPLICATION_XML);
  }

  /**
   * Close a Session
   * @param sessionHandleString
   */

  public void closeSession(String sessionHandleString, String outputMediaType) throws LensException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = this.exec("delete", SessionURL.SESSION_BASE_URL, servLens, null, query, null,
        outputMediaType, null);
    AssertUtil.assertSucceededResult(response);
    log.info("Closed Session : {}", sessionHandleString);
  }

  public void closeSession(String sessionHandleString) throws LensException {
    closeSession(sessionHandleString, null);
  }

  public void closeSession() throws LensException {
    closeSession(sessionHandleString, null);
  }

  /**
   * Set and Validate Session Params
   *
   * @param sessionHandleString
   * @param param
   * @param value
   */

  public void setAndValidateParam(String sessionHandleString, String param, String value) throws Exception {

    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", sessionHandleString);
    formData.add("key", param);
    formData.add("value", value);
    Response response = this.exec("put", SessionURL.SESSION_PARAMS_URL, servLens, null, null,
        MediaType.MULTIPART_FORM_DATA_TYPE, null, formData.getForm());
    AssertUtil.assertSucceededResponse(response);

    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    query.put("key", param);
    response = this.exec("get", SessionURL.SESSION_PARAMS_URL, servLens, null, query);
    AssertUtil.assertSucceededResponse(response);
    StringList strList = response.readEntity(new GenericType<StringList>(StringList.class));
    HashMap<String, String> map = Util.stringListToMap(strList);

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
  public void addResourcesJar(String path, String sessionHandleString) throws LensException {
    log.info("Adding Resources {}", path);
    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", sessionHandleString);
    formData.add("type", "jar");
    formData.add("path", path);
    Response response = this.exec("put", SessionURL.SESSION_ADD_RESOURCE_URL, servLens, null, null,
        MediaType.MULTIPART_FORM_DATA_TYPE, null, formData.getForm());
    log.info("Response : {}", response);
    AssertUtil.assertSucceededResult(response);
  }

  public void addResourcesJar(String path) throws  LensException {
    addResourcesJar(path, sessionHandleString);
  }

  /**
   * Remove resources from a session
   *
   * @param path
   * @param sessionHandleString
   */
  public void removeResourcesJar(String path, String sessionHandleString) throws LensException {
    log.info("Removing Resources {}", path);
    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", sessionHandleString);
    formData.add("type", "jar");
    formData.add("path", path);
    Response response = this.exec("put", SessionURL.SESSION_REMOVE_RESOURCE_URL, servLens, null, null,
        MediaType.MULTIPART_FORM_DATA_TYPE, null,  formData.getForm());
    log.info("Response : {}", response);
    AssertUtil.assertSucceededResult(response);
  }

  public void removeResourcesJar(String path) throws LensException {
    removeResourcesJar(path, sessionHandleString);
  }

  public String getSessionParam(String sessionHandleString, String param) throws Exception {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString, "key", param);
    Response response = this.exec("get", SessionURL.SESSION_PARAMS_URL, servLens, null, query);
    AssertUtil.assertSucceededResponse(response);
    StringList strList = response.readEntity(new GenericType<StringList>(StringList.class));
    HashMap<String, String> map = Util.stringListToMap(strList);
    return map.get(param);
  }

  public String getSessionParam(String param) throws Exception {
    return getSessionParam(sessionHandleString, param);
  }

  public List<UserSessionInfo> getSessionList() throws Exception {
    Response response = this.exec("get", SessionURL.SESSIONS_LIST_URL, servLens, null, null);
    AssertUtil.assertSucceededResponse(response);
    List<UserSessionInfo> sessionInfoList = response.readEntity(new GenericType<List<UserSessionInfo>>(){});
    return sessionInfoList;
  }

}

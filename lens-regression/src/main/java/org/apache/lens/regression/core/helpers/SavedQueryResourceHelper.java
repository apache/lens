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

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.save.*;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.regression.core.type.FormBuilder;
import org.apache.lens.regression.core.type.MapBuilder;
import org.apache.lens.regression.util.AssertUtil;
import org.apache.lens.server.api.error.LensException;

import org.codehaus.jackson.map.ObjectMapper;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SavedQueryResourceHelper extends ServiceManagerHelper {

  public static final String SAVED_QUERY_BASE_URL = "/queryapi/savedqueries";

  public SavedQueryResourceHelper() {
  }

  public SavedQueryResourceHelper(String envFileName) {
    super(envFileName);
  }


  public ListResponse listSavedQueries(String start, String count, String sessionHandleString) throws JAXBException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString, "start", start, "count", count);
    Response response = this.exec("get", SAVED_QUERY_BASE_URL, servLens, null, query);
    AssertUtil.assertSucceededResponse(response);
    return response.readEntity(ListResponse.class);
  }

  public ListResponse getSavedQueries(String start, String count) throws JAXBException {
    return listSavedQueries(start, count, sessionHandleString);
  }

  public ResourceModifiedResponse createSavedQuery(String query, String name, List<Parameter> paramList,
      String sessionHandleString, MediaType inputMediaType) throws JAXBException, IOException {

    MapBuilder map = new MapBuilder("sessionid", sessionHandleString);

    SavedQuery savedQuery = new SavedQuery();
    savedQuery.setQuery(query);
    savedQuery.setName(name);
    savedQuery.setParameters(paramList);
    savedQuery.setDescription("description");

    Response response = null;

    if (inputMediaType!=null && inputMediaType.equals(MediaType.APPLICATION_XML_TYPE)) {

      Marshaller m = JAXBContext.newInstance(SavedQuery.class).createMarshaller();
      StringWriter xmlStringWriter = new StringWriter();
      m.marshal(savedQuery, xmlStringWriter);
      response = this.exec("post", SAVED_QUERY_BASE_URL, servLens, null, map, inputMediaType,
          MediaType.APPLICATION_XML, xmlStringWriter.toString());

    } else {
      String json = "{\"savedQuery\":" + (new ObjectMapper().writeValueAsString(savedQuery))  + "}";
      response = this.exec("post", SAVED_QUERY_BASE_URL, servLens, null, map, inputMediaType,
          MediaType.APPLICATION_XML, json);
    }

    AssertUtil.assertCreated(response);
    response.getStringHeaders().putSingle(HttpHeaders.CONTENT_TYPE, "application/xml");
    return response.readEntity(ResourceModifiedResponse.class);
  }

  public Parameter getParameter(String name, ParameterDataType dataType, String defaultValue,
    ParameterCollectionType collectionType){

    Parameter p = new Parameter(name);
    p.setDataType(dataType);
    p.setCollectionType(collectionType);
    p.setDisplayName("param display name");

    if (collectionType.equals(ParameterCollectionType.MULTIPLE)){
      p.setDefaultValue(defaultValue.split(","));
    }else {
      p.setDefaultValue(new String[]{defaultValue});
    }
    return p;
  }

  public ResourceModifiedResponse createSavedQuery(String query, String name, List<Parameter> paramList)
    throws JAXBException, IOException {
    return createSavedQuery(query, name, paramList, sessionHandleString, MediaType.APPLICATION_JSON_TYPE);
  }

  public ResourceModifiedResponse createSavedQuery(String query, String name, List<Parameter> paramList, String session)
    throws JAXBException, IOException {
    return createSavedQuery(query, name, paramList, session, MediaType.APPLICATION_JSON_TYPE);
  }


  public SavedQuery getSavedQuery(Long queryId, String sessionHandleString) throws JAXBException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString, "id", Long.toString(queryId));
    Response response = this.exec("get", SAVED_QUERY_BASE_URL + "/" + queryId , servLens, null, query);
    AssertUtil.assertSucceededResponse(response);
    return response.readEntity(SavedQuery.class);
  }

  public SavedQuery getSavedQuery(Long queryId) throws JAXBException, LensException {
    return getSavedQuery(queryId, sessionHandleString);
  }

  public ResourceModifiedResponse deleteSavedQuery(Long queryId, String sessionHandleString) throws JAXBException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString, "id", Long.toString(queryId));
    Response response = this.exec("delete", SAVED_QUERY_BASE_URL + "/" + queryId , servLens, null, query);
    AssertUtil.assertSucceededResponse(response);
    return response.readEntity(ResourceModifiedResponse.class);
  }

  public void deleteAllSavedQueries() throws JAXBException {
    ListResponse savedQueryList = listSavedQueries("0", "100", sessionHandleString);
    if (savedQueryList.getResoures() != null){
      for(SavedQuery s: savedQueryList.getResoures()){
        deleteSavedQuery(s.getId(), sessionHandleString);
      }
    }
  }

  public ResourceModifiedResponse updateSavedQuery(Long queryId, String sessionHandleString, SavedQuery savedQuery)
    throws JAXBException, IOException {

    MapBuilder map = new MapBuilder("sessionid", sessionHandleString);
    String json = "{\"savedQuery\":" + (new ObjectMapper().writeValueAsString(savedQuery))  + "}";
    Response response = this.exec("put", SAVED_QUERY_BASE_URL + "/" + Long.toString(queryId), servLens, null, map,
        MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_XML, json);
    AssertUtil.assertCreated(response);
    response.getStringHeaders().putSingle(HttpHeaders.CONTENT_TYPE, "application/xml");
    return response.readEntity(ResourceModifiedResponse.class);
  }

  public LensAPIResult<QueryHandle> runSavedQuery(Long queryId, String sessionHandleString, String conf,
    HashMap<String, String> params) throws JAXBException {

    MapBuilder map = new MapBuilder(params);
    FormBuilder formData = new FormBuilder("sessionid", sessionHandleString);

    if (conf == null) {
      conf = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><conf />";
    }
    formData.getForm().bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf")
        .build(), conf, MediaType.APPLICATION_XML_TYPE));

    Response response = this.exec("post", SAVED_QUERY_BASE_URL + "/" + queryId , servLens, null, map,
        MediaType.MULTIPART_FORM_DATA_TYPE, MediaType.APPLICATION_XML, formData.getForm());

    AssertUtil.assertSucceededResponse(response);
    return response.readEntity(new GenericType<LensAPIResult<QueryHandle>>(){});
  }

  public LensAPIResult<QueryHandle> runSavedQuery(Long queryId, String sessionHandleString,
    HashMap<String, String> params) throws JAXBException {
    return runSavedQuery(queryId, sessionHandleString, null, params);
  }

  public LensAPIResult<QueryHandle> runSavedQuery(Long queryId, HashMap<String, String> params) throws JAXBException {
    return runSavedQuery(queryId, sessionHandleString, null, params);
  }

  public ParameterParserResponse getSavedQueryParameter(String query, String sessionHandleString) throws JAXBException {

    MapBuilder map = new MapBuilder("sessionid", sessionHandleString);
    FormBuilder formData = new FormBuilder("query", query);

    Response response = this.exec("post", SAVED_QUERY_BASE_URL + "/parameters", servLens, null, map,
        MediaType.MULTIPART_FORM_DATA_TYPE, MediaType.APPLICATION_XML, formData.getForm());
    AssertUtil.assertSucceededResponse(response);
    response.getStringHeaders().putSingle(HttpHeaders.CONTENT_TYPE, "application/xml");
    return response.readEntity(ParameterParserResponse.class);
  }

}

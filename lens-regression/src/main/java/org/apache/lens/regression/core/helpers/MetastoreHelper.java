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

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import javax.xml.bind.JAXBException;

import org.apache.lens.api.DateTime;
import org.apache.lens.api.StringList;
import org.apache.lens.api.metastore.*;
import org.apache.lens.regression.core.constants.MetastoreURL;
import org.apache.lens.regression.core.type.FormBuilder;
import org.apache.lens.regression.core.type.MapBuilder;
import org.apache.lens.regression.util.AssertUtil;
import org.apache.lens.regression.util.Util;
import org.apache.lens.server.api.error.LensException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetastoreHelper extends ServiceManagerHelper {


  public MetastoreHelper() {
  }

  public MetastoreHelper(String envFileName) {
    super(envFileName);
  }

  /**
   * Set Current Database for a Session
   *
   * @param sessionHandleString
   * @param currentDBName
   */

  public void setCurrentDatabase(String sessionHandleString, String currentDBName) throws JAXBException, LensException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = this.exec("put", MetastoreURL.METASTORE_DATABASES_CURRENT_URL, servLens, null, query,
        MediaType.APPLICATION_XML_TYPE, null, currentDBName);
    AssertUtil.assertSucceededResponse(response);
    response = this.exec("get", MetastoreURL.METASTORE_DATABASES_CURRENT_URL, servLens, null, query);
    String responseString = response.readEntity(String.class);
    AssertUtil.assertSucceededResponse(response);
    log.info("Response: {}", responseString.trim());
    if (!responseString.trim().equals(currentDBName)) {
      throw new LensException("Could not set database");
    }
    log.info("Set Current database to {}", currentDBName);
  }

  public void setCurrentDatabase(String currentDBName) throws JAXBException, LensException {
    setCurrentDatabase(sessionHandleString, currentDBName);
  }

  /**
   * Get Current Database for a Session
   *
   * @param sessionHandleString
   * @return the current DB Name
   */

  public String getCurrentDatabase(String sessionHandleString) throws JAXBException, LensException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = this.exec("get", MetastoreURL.METASTORE_DATABASES_CURRENT_URL, servLens, null, query,
        MediaType.APPLICATION_XML_TYPE, null);
    AssertUtil.assertSucceededResponse(response);
    return response.readEntity(String.class);
  }

  public String getCurrentDatabase() throws JAXBException, LensException {
    return getCurrentDatabase(sessionHandleString);
  }

  /**
   * Get list of databases
   *
   * @param sessionHandleString
   * @return List of all the Databases
   */

  public StringList listDatabases(String sessionHandleString) throws JAXBException, LensException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = this
        .exec("get", MetastoreURL.METASTORE_DATABASES_URL, servLens, null, query, MediaType.APPLICATION_XML_TYPE, null);
    AssertUtil.assertSucceededResponse(response);
    StringList responseString = response.readEntity(StringList.class);
    return responseString;
  }

  public StringList listDatabases() throws JAXBException, LensException {
    return listDatabases(sessionHandleString);
  }

  /**
   * Create a database
   *
   * @param dbName
   * @param sessionHandleString
   */

  public void createDatabase(String dbName, String sessionHandleString) throws JAXBException, LensException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = this
        .exec("post", MetastoreURL.METASTORE_DATABASES_URL, servLens, null, query, MediaType.APPLICATION_XML_TYPE, null,
            dbName);
    AssertUtil.assertSucceededResult(response);
  }

  public void createDatabase(String dbName) throws JAXBException, LensException {
    createDatabase(dbName, sessionHandleString);
  }

  /**
   * Drop a DB
   *
   * @param dbName
   * @param sessionHandleString
   */
  public void dropDatabase(String dbName, String sessionHandleString) throws JAXBException, LensException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    query.put("cascade", "true");
    Response response = this.exec("delete", MetastoreURL.METASTORE_DATABASES_URL + "/" + dbName, servLens, null, query,
        MediaType.APPLICATION_XML_TYPE, null);
    AssertUtil.assertSucceededResult(response);
  }

  public void dropDatabase(String dbName) throws JAXBException, LensException {
    dropDatabase(dbName, sessionHandleString);
  }

  public void createStorage(XStorage storage, String sessionHandleString) throws Exception {

    String storageString = Util.convertObjectToXml(storage, XStorage.class, "createXStorage");
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);

    Response response = this
        .exec("post", MetastoreURL.METASTORE_STORAGES_URL, servLens, null, query, MediaType.APPLICATION_XML_TYPE, null,
            storageString);
    AssertUtil.assertSucceededResult(response);
  }

  public void createStorage(XStorage storage) throws Exception {
    createStorage(storage, sessionHandleString);
  }

  public void dropStorage(String storageName, String sessionHandleString) throws JAXBException, LensException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = this
        .exec("delete", MetastoreURL.METASTORE_STORAGES_URL + "/" + storageName, servLens, null, query,
            MediaType.APPLICATION_XML_TYPE, null);
    AssertUtil.assertSucceededResult(response);
  }

  public void dropStorage(String storageName) throws JAXBException, LensException {
    dropStorage(storageName, sessionHandleString);
  }

  public StringList listStorages(String sessionHandleString) throws JAXBException, LensException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = this.exec("get", MetastoreURL.METASTORE_STORAGES_URL, servLens, null, query);
    AssertUtil.assertSucceededResponse(response);
    StringList cubeList = response.readEntity(StringList.class);
    return cubeList;
  }

  public StringList listStorages() throws JAXBException, LensException {
    return listStorages(sessionHandleString);
  }

  public void createCube(XCube cube, String sessionHandleString) throws Exception {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    String cubeString = Util.convertObjectToXml(cube, XCube.class, "createXCube");
    Response response = this
        .exec("post", MetastoreURL.METASTORE_CUBES_URL, servLens, null, query, MediaType.APPLICATION_XML_TYPE, null,
            cubeString);
    AssertUtil.assertSucceededResult(response);
  }

  public void createCube(XCube cube) throws Exception {
    createCube(cube, sessionHandleString);
  }

  public void createFacts(XFactTable facts, String sessionHandleString) throws Exception {
    String factString = Util.convertObjectToXml(facts, XFactTable.class, "createXFactTable");

    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", sessionHandleString);
    formData.add("fact", factString);

    Response response = this
        .exec("post", MetastoreURL.METASTORE_FACTS_URL, servLens, null, null, MediaType.MULTIPART_FORM_DATA_TYPE,
            MediaType.APPLICATION_XML, formData.getForm());
    AssertUtil.assertSucceededResult(response);
  }

  public void createFacts(XFactTable facts) throws Exception {
    createFacts(facts, sessionHandleString);
  }

  public StringList listCubes(String type, String sessionHandleString) throws Exception {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    if (type != null) {
      query.put("type", type);
    }
    Response response = this.exec("get", MetastoreURL.METASTORE_CUBES_URL, servLens, null, query);
    AssertUtil.assertSucceededResponse(response);
    StringList cubeList = response.readEntity(StringList.class);
    return cubeList;
  }

  public StringList listCubes(String type) throws Exception {
    return listCubes(type, sessionHandleString);
  }

  public StringList listCubes() throws Exception {
    return listCubes(null);
  }

  public XCube getCube(String cubeName, String sessionHandleString)
    throws InstantiationException, IllegalAccessException, JAXBException, LensException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = this.exec("get", MetastoreURL.METASTORE_CUBES_URL + "/" + cubeName, servLens, null, query,
        MediaType.APPLICATION_XML_TYPE);
    AssertUtil.assertSucceededResponse(response);
    String responseString = response.readEntity(String.class);
    log.info(responseString);
    XCube cube = (XCube) Util.extractObject(responseString, XCube.class);
    return cube;
  }

  public XCube getCube(String cubeName)
    throws InstantiationException, IllegalAccessException, JAXBException, LensException {
    return getCube(cubeName, sessionHandleString);
  }

  public XFactTable getFact(String factName, String sessionHandleString)
    throws InstantiationException, IllegalAccessException, JAXBException, LensException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = this.exec("get", MetastoreURL.METASTORE_FACTS_URL + "/" + factName, servLens, null, query,
        MediaType.APPLICATION_XML_TYPE);
    AssertUtil.assertSucceededResponse(response);
    String responseString = response.readEntity(String.class);
    log.info(responseString);
    XFactTable fact = (XFactTable) Util.extractObject(responseString, XFactTable.class);
    return fact;
  }

  public XFactTable getFact(String factName)
    throws InstantiationException, IllegalAccessException, JAXBException, LensException {
    return getFact(factName, sessionHandleString);
  }

  public XDimension getDimension(String dimName, String sessionHandleString)
    throws InstantiationException, IllegalAccessException, JAXBException, LensException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = this.exec("get", MetastoreURL.METASTORE_DIMENSIONS_URL + "/" + dimName, servLens, null, query,
        MediaType.APPLICATION_XML_TYPE);
    AssertUtil.assertSucceededResponse(response);
    String responseString = response.readEntity(String.class);
    log.info(responseString);
    XDimension dim = (XDimension) Util.extractObject(responseString, XDimension.class);
    return dim;
  }

  public XDimension getDimension(String dimName)
    throws InstantiationException, IllegalAccessException, JAXBException, LensException {
    return getDimension(dimName, sessionHandleString);
  }

  public XDimensionTable getDimensionTable(String dimName, String sessionHandleString)
    throws InstantiationException, IllegalAccessException, JAXBException, LensException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = this.exec("get", MetastoreURL.METASTORE_DIMTABLES_URL + "/" + dimName, servLens, null, query,
        MediaType.APPLICATION_XML_TYPE);
    AssertUtil.assertSucceededResponse(response);
    String responseString = response.readEntity(String.class);
    log.info(responseString);
    XDimensionTable dim = (XDimensionTable) Util.extractObject(responseString, XDimensionTable.class);
    return dim;
  }

  public XDimensionTable getDimensionTable(String dimName)
    throws InstantiationException, IllegalAccessException, JAXBException, LensException {
    return getDimensionTable(dimName, sessionHandleString);
  }

  public void updateCube(XCube cube, String cubeName, String sessionHandleString) throws Exception {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    String cubeString = Util.convertObjectToXml(cube, XCube.class, "createXCube");
    Response response = this.exec("put", MetastoreURL.METASTORE_CUBES_URL + "/" + cubeName, servLens, null, query,
        MediaType.APPLICATION_XML_TYPE, null, cubeString);
    AssertUtil.assertSucceededResult(response);
  }

  public void updateCube(XCube cube, String cubeName) throws Exception {
    updateCube(cube, cubeName, sessionHandleString);
  }

  public void dropCube(String cubeName, String sessionHandleString) throws JAXBException, LensException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = this.exec("delete", MetastoreURL.METASTORE_CUBES_URL + "/" + cubeName, servLens, null, query,
        MediaType.APPLICATION_XML_TYPE, null);
    AssertUtil.assertSucceededResult(response);
  }

  public void dropCube(String cubeName) throws JAXBException, LensException {
    dropCube(cubeName, sessionHandleString);
  }

  public void createDimTable(XDimensionTable dt, String sessionHandleString) throws Exception {
    String dimTable = Util.convertObjectToXml(dt, XDimensionTable.class, "createDimensionTable");

    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", sessionHandleString);
    formData.add("dimensionTable", dimTable);

    Response response = this
        .exec("post", MetastoreURL.METASTORE_DIMTABLES_URL, servLens, null, null, MediaType.MULTIPART_FORM_DATA_TYPE,
            MediaType.APPLICATION_XML, formData.getForm());
    AssertUtil.assertSucceededResult(response);
  }

  public void createDimTable(XDimensionTable dt) throws Exception {
    createDimTable(dt, sessionHandleString);
  }

  public void getLatestDate(String sessionHandleString) throws Exception {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    query.put("timeDimension", "event_time");
    Response response = this
        .exec("get", "/metastore/cubes/rrcube/latestdate", servLens, null, query, MediaType.APPLICATION_XML_TYPE, null,
            query);
    DateTime dt = (DateTime) Util.getObject(response.readEntity(String.class), DateTime.class);
  }

  public void getLatestDate() throws Exception {
    getLatestDate(sessionHandleString);
  }

  public StringList getAllFactsOfCube(String cubeName, String sessionHandleString)
    throws InstantiationException, IllegalAccessException, JAXBException, LensException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = this.exec("get", MetastoreURL.METASTORE_CUBES_URL + "/" + cubeName + "/facts", servLens, null,
        query, MediaType.APPLICATION_XML_TYPE);
    AssertUtil.assertSucceededResponse(response);
    StringList factList = response.readEntity(new GenericType<StringList>(){});
    return factList;
  }

}

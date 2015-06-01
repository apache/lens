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


import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import javax.xml.bind.JAXBException;

import org.apache.lens.api.StringList;
import org.apache.lens.regression.core.constants.MetastoreURL;
import org.apache.lens.regression.core.type.MapBuilder;
import org.apache.lens.regression.util.AssertUtil;
import org.apache.lens.server.api.error.LensException;

import org.apache.log4j.Logger;




public class MetastoreHelper extends ServiceManagerHelper {

  private static Logger logger = Logger.getLogger(MetastoreHelper.class);

  private WebTarget servLens = ServiceManagerHelper.getServerLens();
  private String sessionHandleString = ServiceManagerHelper.getSessionHandle();

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
    logger.info(responseString.trim());
    if (!responseString.trim().equals(currentDBName)) {
      throw new LensException("Could not set database");
    }
    logger.info("Set Current database to " + currentDBName);
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
    AssertUtil.assertSucceeded(response);
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
    AssertUtil.assertSucceeded(response);
  }

  public void dropDatabase(String dbName) throws JAXBException, LensException {
    dropDatabase(dbName, sessionHandleString);
  }

}

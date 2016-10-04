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

package org.apache.lens.regression.client;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBException;

import org.apache.lens.api.query.PersistentQueryResult;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.save.*;
import org.apache.lens.regression.core.helpers.*;
import org.apache.lens.regression.core.testHelper.BaseTestClass;
import org.apache.lens.regression.core.type.MapBuilder;
import org.apache.lens.regression.util.AssertUtil;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.util.LensUtil;

import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.*;

import com.google.common.collect.ImmutableSet;

public class ITSavedQueryTests extends BaseTestClass {

  WebTarget servLens;
  private String sessionHandleString;

  private static Logger logger = Logger.getLogger(ITSavedQueryTests.class);

  @BeforeClass(alwaysRun = true)
  public void initialize() throws Exception {
    servLens = ServiceManagerHelper.init();
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp(Method method) throws Exception {
    logger.info("Test Name: " + method.getName());
    logger.info("Creating a new Session");
    sessionHandleString = sHelper.openSession(lens.getCurrentDB());
  }

  @AfterMethod(alwaysRun = true)
  public void closeSession() throws Exception {
    deleteAllSavedQueries();
    logger.info("Closing Session");
    sHelper.closeSession();
  }

  protected ResourceModifiedResponse getSampleSavedQuery(String name) throws IOException, JAXBException {

    List<Parameter> paramList = new ArrayList<>();
    String query = "select id, name from sample_dim where name != :param";
    paramList.add(savedQueryResourceHelper.getParameter("param", ParameterDataType.STRING, "first",
        ParameterCollectionType.SINGLE));
    ResourceModifiedResponse savedQuery = savedQueryResourceHelper.createSavedQuery(query, name, paramList);
    return savedQuery;
  }


  @Test
  public void listSavedQueries() throws Exception {

    ListResponse savedQueryList = savedQueryResourceHelper.listSavedQueries("0", "10", sessionHandleString);
    long initialSize = savedQueryList.getTotalCount();

    ResourceModifiedResponse rmr1 = getSampleSavedQuery("list-query1");
    ResourceModifiedResponse rmr2 = getSampleSavedQuery("list-query2");

    savedQueryList = savedQueryResourceHelper.listSavedQueries("0", "10", sessionHandleString);
    Assert.assertEquals(savedQueryList.getResoures().size(), initialSize + 2);

    savedQueryList = savedQueryResourceHelper.listSavedQueries(Long.toString(initialSize+1), "10", sessionHandleString);
    Assert.assertEquals(savedQueryList.getResoures().size(), 1);
  }

  @Test
  public void createNRunSavedQuery() throws Exception {

    String session = sHelper.openSession("user1", "pwd1", lens.getCurrentDB());
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "false");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");

    //creating saved query
    List<Parameter> paramList = new ArrayList<>();
    String query = "select id, name from sample_dim where name != :param";
    paramList.add(savedQueryResourceHelper.getParameter("param", ParameterDataType.STRING, "first",
        ParameterCollectionType.SINGLE));
    ResourceModifiedResponse rmr = savedQueryResourceHelper.createSavedQuery(query, "query-1", paramList, session);
    Assert.assertNotNull(rmr.getId());

    //running above created saved query
    HashMap<String, String> paramMap = LensUtil.getHashMap("param", "second");
    QueryHandle handle = savedQueryResourceHelper.runSavedQuery(rmr.getId(), session, paramMap).getData();
    qHelper.waitForCompletion(handle);
    PersistentQueryResult result = (PersistentQueryResult) qHelper.getResultSet(handle);
    Assert.assertEquals(result.getNumRows().intValue(), 7);
    //TODO : Do result validation. Check if it doesn't contain row having "second" field

    //running above created saved query with different parameter
    paramMap = LensUtil.getHashMap("param", "third");
    handle = savedQueryResourceHelper.runSavedQuery(rmr.getId(), session, paramMap).getData();
    qHelper.waitForCompletion(handle);
    result = (PersistentQueryResult) qHelper.getResultSet(handle);
    Assert.assertEquals(result.getNumRows().intValue(), 7);

  }

  //TODO Enable when LENS-1238 is fixed
  @Test(enabled = false)
  public void createNRunSavedQueryNumberParam() throws Exception {

    List<Parameter> paramList = new ArrayList<>();
    String query = "cube select id, name from sample_db_dim where id = :value";
    paramList.add(savedQueryResourceHelper.getParameter("value", ParameterDataType.NUMBER, "1",
        ParameterCollectionType.SINGLE));
    ResourceModifiedResponse rmr = savedQueryResourceHelper.createSavedQuery(query, "query1", paramList);

    //running above created saved query
    HashMap<String, String> paramMap = LensUtil.getHashMap("value", "2");
    QueryHandle handle = savedQueryResourceHelper.runSavedQuery(rmr.getId(), paramMap).getData();
    qHelper.waitForCompletion(handle);
    PersistentQueryResult result = (PersistentQueryResult) qHelper.getResultSet(handle);
    Assert.assertEquals(result.getNumRows().intValue(), 7);
  }


  @Test
  public void createNRunSavedQueryMultipleParams() throws Exception {

    List<Parameter> paramList = new ArrayList<>();
    String savedQuery = "select product_id from sales where time_range_in(delivery_time, :start_time, :end_time)";
    paramList.add(savedQueryResourceHelper.getParameter("start_time", ParameterDataType.STRING, "2015-01-12",
        ParameterCollectionType.SINGLE));
    paramList.add(savedQueryResourceHelper.getParameter("end_time", ParameterDataType.STRING, "2015-01-13",
        ParameterCollectionType.SINGLE));
    ResourceModifiedResponse rmr = savedQueryResourceHelper.createSavedQuery(savedQuery, "query1", paramList);

    HashMap<String, String> paramMap = LensUtil.getHashMap("start_time", "2015-04-12", "end_time", "2015-04-13");
    QueryHandle handle = savedQueryResourceHelper.runSavedQuery(rmr.getId(), paramMap).getData();
    qHelper.waitForCompletion(handle);
    PersistentQueryResult result = (PersistentQueryResult) qHelper.getResultSet(handle);
    Assert.assertEquals(result.getNumRows().intValue(), 2);
  }

  //TODO Enable when LENS-1237 is fixed
  @Test(enabled = false)
  public void createNRunSavedQueryMultipleCollectionType() throws Exception {

    List<Parameter> paramList = new ArrayList<>();
    String savedQuery = "cube select id,name from sample_dim where name in :values";
    paramList.add(savedQueryResourceHelper.getParameter("values", ParameterDataType.STRING, "first,second",
        ParameterCollectionType.MULTIPLE));
    ResourceModifiedResponse rmr = savedQueryResourceHelper.createSavedQuery(savedQuery, "query1", paramList);

    HashMap<String, String> paramMap = LensUtil.getHashMap("values", "first,second,third");
    QueryHandle handle = savedQueryResourceHelper.runSavedQuery(rmr.getId(), paramMap).getData();
    qHelper.waitForCompletion(handle);
    PersistentQueryResult result = (PersistentQueryResult) qHelper.getResultSet(handle);
    Assert.assertEquals(result.getNumRows().intValue(), 5);
  }

  @Test
  public void deleteSavedQuery() throws Exception {

    ResourceModifiedResponse savedQuery = getSampleSavedQuery("query-delete-test");
    Long queryId = savedQuery.getId();

    //deleting saved query
    savedQueryResourceHelper.deleteSavedQuery(queryId, sessionHandleString);

    //checking if the above deleted saved query is being deleted
    MapBuilder map = new MapBuilder("sessionid", sessionHandleString, "id", Long.toString(queryId));
    Response res = lens.exec("get", SavedQueryResourceHelper.SAVED_QUERY_BASE_URL + "/" + queryId , servLens,
        null, map);
    AssertUtil.assertNotFound(res);
  }


  //getting saved query using different session (not the 1 used for creating saved query)
  @Test
  public void getSavedQueryUsingDiffSessionId() throws Exception {

    ResourceModifiedResponse rmr = getSampleSavedQuery("query-1");
    SavedQuery savedQuery = savedQueryResourceHelper.getSavedQuery(rmr.getId());

    String newSession = sHelper.openSession("user1", "pwd1", lens.getCurrentDB());
    SavedQuery savedQueryOutput = savedQueryResourceHelper.getSavedQuery(rmr.getId(), newSession);

    Assert.assertEquals(savedQuery, savedQueryOutput);
  }

  @Test
  public void updateSavedQuery() throws Exception {

    Long queryId = getSampleSavedQuery("query-update-test").getId();
    //Modify query name
    SavedQuery savedQuery = savedQueryResourceHelper.getSavedQuery(queryId);
    savedQuery.setName("query-rename");

    savedQueryResourceHelper.updateSavedQuery(queryId, sessionHandleString, savedQuery);
    SavedQuery savedQueryModified = savedQueryResourceHelper.getSavedQuery(queryId);
    Assert.assertTrue(savedQueryModified.getName().equals("query-rename"));
  }

  @Test(enabled = false)  //TODO : Enable this once LENS-1341 is fixed
  public void getSavedQueryParameters() throws Exception {
    String query = "cube select id,name from sample_dim where id = :value";
    ParameterParserResponse res = savedQueryResourceHelper.getSavedQueryParameter(query, sessionHandleString);
    ImmutableSet<Parameter> paramList = res.getParameters();
    Assert.assertEquals(paramList.size(), 1);
    Assert.assertEquals(paramList.iterator().next().getName(), "value");
  }

  @Test
  public void deleteAllSavedQueries() throws Exception {
    savedQueryResourceHelper.deleteAllSavedQueries();
  }

}

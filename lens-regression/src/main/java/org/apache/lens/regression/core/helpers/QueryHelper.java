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

import java.util.List;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import javax.xml.bind.JAXBException;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.query.*;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.regression.core.constants.QueryURL;
import org.apache.lens.regression.core.type.FormBuilder;
import org.apache.lens.regression.core.type.MapBuilder;
import org.apache.lens.regression.core.type.PrepareQueryHandles;
import org.apache.lens.regression.core.type.QueryHandles;
import org.apache.lens.regression.util.AssertUtil;
import org.apache.lens.regression.util.Util;
import org.apache.lens.server.api.error.LensException;

import org.apache.log4j.Logger;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;



public class QueryHelper extends ServiceManagerHelper {

  private static Logger logger = Logger.getLogger(QueryHelper.class);
  private WebTarget servLens = ServiceManagerHelper.getServerLens();
  private String sessionHandleString = ServiceManagerHelper.getSessionHandle();

  public QueryHelper() {
  }

  public QueryHelper(String envFileName) {
    super(envFileName);
  }

  /**
   * Execute with conf
   *
   * @param queryString
   * @param queryName
   * @param sessionHandleString
   * @param conf
   * @return the query Handle
   */
  public QueryHandle executeQuery(String queryString, String queryName, String sessionHandleString, String conf) throws
      InstantiationException, IllegalAccessException, JAXBException, LensException {
    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", sessionHandleString);
    formData.add("query", queryString);
    formData.add("operation", "EXECUTE");
    formData.add("conf", conf);
    if (queryName != null) {
      formData.add("queryName", queryName);
    }
    Response response = this.exec("post", QueryURL.QUERY_URL, servLens, null, null, MediaType.MULTIPART_FORM_DATA_TYPE,
        MediaType.APPLICATION_XML, formData.getForm());
    AssertUtil.assertSucceededResponse(response);
    String queryHandleString = response.readEntity(String.class);
    logger.info(queryHandleString);
    LensAPIResult successResponse = (LensAPIResult) Util.getObject(queryHandleString, LensAPIResult.class);
    QueryHandle queryHandle = (QueryHandle) successResponse.getData();
    if (queryHandle == null) {
      throw new LensException("Query Execute Failed");
    }
    logger.info("Query Handle : " + queryHandle);
    return queryHandle;
  }

  public QueryHandle executeQuery(String queryString, String queryName, String sessionHandleString) throws
      InstantiationException, IllegalAccessException, JAXBException, LensException {
    return executeQuery(queryString, queryName, sessionHandleString,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><conf />");
  }

  public QueryHandle executeQuery(String queryString, String queryName) throws
      InstantiationException, IllegalAccessException, JAXBException, LensException {
    return executeQuery(queryString, queryName, sessionHandleString);
  }

  public QueryHandle executeQuery(String queryString) throws
      InstantiationException, IllegalAccessException, JAXBException, LensException {
    return executeQuery(queryString, null);
  }

  /**
   * Execute with timeout
   *
   * @param queryString
   * @param timeout
   * @param queryName
   * @param sessionHandleString
   * @param conf
   * @return the queryHandleWithResultSet
   */

  public QueryHandleWithResultSet executeQueryTimeout(String queryString, String timeout, String queryName,
      String sessionHandleString, String conf) throws InstantiationException, IllegalAccessException, JAXBException,
      LensException {
    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", sessionHandleString);
    formData.add("query", queryString);
    formData.add("operation", "EXECUTE_WITH_TIMEOUT");
    formData.add("conf", conf);
    if (timeout != null) {
      formData.add("timeoutmillis", timeout);
    }
    if (queryName != null) {
      formData.add("queryName", queryName);
    }
    Response response = this.exec("post", QueryURL.QUERY_URL, servLens, null, null, MediaType.MULTIPART_FORM_DATA_TYPE,
        MediaType.APPLICATION_XML, formData.getForm());
    AssertUtil.assertSucceededResponse(response);
    String queryHandleString = response.readEntity(String.class);
    logger.info(queryHandleString);
    LensAPIResult successResponse = (LensAPIResult) Util.getObject(queryHandleString, LensAPIResult.class);
    QueryHandleWithResultSet queryHandleWithResultSet = (QueryHandleWithResultSet) successResponse.getData();
    if (queryHandleWithResultSet==null) {
      throw new LensException("Query Execute Failed");
    }
    logger.info("Query Handle with ResultSet : " + queryHandleWithResultSet);
    return queryHandleWithResultSet;
  }

  public QueryHandleWithResultSet executeQueryTimeout(String queryString, String timeout, String queryName,
      String sessionHandleString) throws InstantiationException, IllegalAccessException, JAXBException, LensException {
    return executeQueryTimeout(queryString, timeout, queryName, sessionHandleString,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><conf />");
  }

  public QueryHandleWithResultSet executeQueryTimeout(String queryString, String timeout, String queryName) throws
      InstantiationException, IllegalAccessException, JAXBException, LensException {
    return executeQueryTimeout(queryString, timeout, queryName, sessionHandleString);
  }

  public QueryHandleWithResultSet executeQueryTimeout(String queryString, String timeout) throws
      InstantiationException, IllegalAccessException, JAXBException, LensException {
    return executeQueryTimeout(queryString, timeout, null);
  }

  public QueryHandleWithResultSet executeQueryTimeout(String queryString) throws
      InstantiationException, IllegalAccessException, JAXBException, LensException {
    return executeQueryTimeout(queryString, null);
  }

  /**
   * Execute the query
   *
   * @param queryString
   * @param queryName
   * @param user
   * @param sessionHandleString
   * @param conf
   * @return the query Handle
   */

  public QueryHandle executeQuery(String queryString, String queryName, String user, String sessionHandleString,
      LensConf conf) throws JAXBException, InstantiationException, IllegalAccessException, LensException {

    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", sessionHandleString);
    formData.add("query", queryString);
    formData.add("operation", "EXECUTE");
    if (queryName != null) {
      formData.add("queryName", queryName);
    }
    if (user != null) {
      formData.add("user", user);
    }
    formData.getForm().bodyPart(
        new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
            MediaType.APPLICATION_XML_TYPE));
    Response response = this.exec("post", "/queryapi/queries", servLens, null, null, MediaType.MULTIPART_FORM_DATA_TYPE,
        MediaType.APPLICATION_XML, formData.getForm());
    AssertUtil.assertSucceededResponse(response);
    String queryHandleString = response.readEntity(String.class);
    logger.info(queryHandleString);
    LensAPIResult successResponse = (LensAPIResult) Util.getObject(queryHandleString, LensAPIResult.class);
    QueryHandle queryHandle = (QueryHandle) successResponse.getData();
    return queryHandle;
  }

  /**
   * Explain the query
   *
   * @param queryString
   * @param sessionHandleString
   * @param conf
   * @return the query Plan
   */

  public QueryPlan explainQuery(String queryString, String sessionHandleString, String conf) throws
      JAXBException, InstantiationException, IllegalAccessException, LensException {
    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", sessionHandleString);
    formData.add("query", queryString);
    formData.add("operation", "EXPLAIN");
    formData.getForm().bodyPart(
        new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
            MediaType.APPLICATION_XML_TYPE));
    Response response = this.exec("post", "/queryapi/queries", servLens, null, null, MediaType.MULTIPART_FORM_DATA_TYPE,
        MediaType.APPLICATION_XML, formData.getForm());
    AssertUtil.assertSucceededResponse(response);
    String queryPlanString = response.readEntity(String.class);
    logger.info(queryPlanString);
    LensAPIResult successResponse = (LensAPIResult) Util.getObject(queryPlanString, LensAPIResult.class);
    QueryPlan queryPlan = (QueryPlan) successResponse.getData();
    return queryPlan;
  }

  public QueryPlan explainQuery(String queryString, String sessionHandleString) throws
      JAXBException, InstantiationException, IllegalAccessException, LensException {
    return explainQuery(queryString, sessionHandleString,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><conf />");
  }

  public QueryPlan explainQuery(String queryString) throws
      JAXBException, InstantiationException, IllegalAccessException, LensException {
    return explainQuery(queryString, sessionHandleString);
  }

  /**
   * Estimate the query
   *
   * @param queryString
   * @param sessionHandleString
   * @param conf
   * @return the Estimate result
   */

  public QueryCost estimateQuery(String queryString, String sessionHandleString, String conf) throws
      InstantiationException, IllegalAccessException, JAXBException, LensException {
    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", sessionHandleString);
    formData.add("query", queryString);
    formData.add("operation", "ESTIMATE");
    formData.add("conf", conf);
    Response response = this.exec("post", QueryURL.QUERY_URL, servLens, null, null, MediaType.MULTIPART_FORM_DATA_TYPE,
        MediaType.APPLICATION_XML, formData.getForm());
    AssertUtil.assertSucceededResponse(response);
    String queryCostString = response.readEntity(String.class);
    logger.info(queryCostString);
    LensAPIResult successResponse = (LensAPIResult) Util.getObject(queryCostString, LensAPIResult.class);
    QueryCost queryCost = (QueryCost) successResponse.getData();
    if (queryCost == null) {
      throw new LensException("Estimate Failed");
    }
    return queryCost;
  }

  public QueryCost estimateQuery(String queryString, String sessionHandleString) throws
      InstantiationException, IllegalAccessException, JAXBException, LensException {
    return estimateQuery(queryString, sessionHandleString,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><conf />");
  }

  public QueryCost estimateQuery(String queryString) throws
      InstantiationException, IllegalAccessException, JAXBException, LensException {
    return estimateQuery(queryString, sessionHandleString);
  }

  /**
   * Prepare and Explain the query
   *
   * @param queryString
   * @param sessionHandleString
   * @param conf
   * @return the query Plan
   */

  public QueryPlan explainAndPrepareQuery(String queryString, String sessionHandleString, String conf) throws
      InstantiationException, IllegalAccessException, JAXBException, LensException {

    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", sessionHandleString);
    formData.add("query", queryString);
    formData.add("operation", "EXPLAIN_AND_PREPARE");
    formData.add("conf", conf);
    Response response = this
        .exec("post", "/queryapi/preparedqueries", servLens, null, null, MediaType.MULTIPART_FORM_DATA_TYPE,
            MediaType.APPLICATION_XML, formData.getForm());
    String queryPlanString = response.readEntity(String.class);
    logger.info(queryPlanString);
    QueryPlan queryPlan = (QueryPlan) Util.getObject(queryPlanString, QueryPlan.class);
    return queryPlan;
  }

  public QueryPlan explainAndPrepareQuery(String queryString, String sessionHandleString) throws
      JAXBException, InstantiationException, IllegalAccessException, LensException {
    return explainAndPrepareQuery(queryString, sessionHandleString,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><conf />");
  }

  public QueryPlan explainAndPrepareQuery(String queryString) throws
      JAXBException, InstantiationException, IllegalAccessException, LensException {
    return explainAndPrepareQuery(queryString, sessionHandleString);
  }

  /**
   * Get the Result set
   *
   * @param queryHandle
   * @param fromIndex
   * @param fetchSize
   * @param sessionHandleString
   * @return the query Result
   */
  public QueryResult getResultSet(QueryHandle queryHandle, String fromIndex, String fetchSize,
      String sessionHandleString) throws JAXBException, InstantiationException, IllegalAccessException, LensException {

    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    query.put("fromindex", fromIndex);
    query.put("fetchsize", fetchSize);

    Response response = this
        .exec("get", QueryURL.QUERY_URL + "/" + queryHandle.toString() + "/resultset", servLens, null, query);
    AssertUtil.assertSucceededResponse(response);
    logger.info(response);
    String queryResultString = response.readEntity(String.class);
    logger.info(queryResultString);
    QueryResult queryResult = (QueryResult) Util.getObject(queryResultString, QueryResult.class);
    return queryResult;
  }

  public QueryResult getResultSet(QueryHandle queryHandle, String fromIndex, String fetchSize) throws
      JAXBException, InstantiationException, IllegalAccessException, LensException {
    return getResultSet(queryHandle, fromIndex, fetchSize, sessionHandleString);
  }

  public QueryResult getResultSet(QueryHandle queryHandle) throws
      JAXBException, InstantiationException, IllegalAccessException, LensException {
    return getResultSet(queryHandle, "0", "100", sessionHandleString);
  }

  /**
   * Get the HTTP result set
   *
   * @param queryHandle
   * @return the query Result
   */

  public QueryResult getHttpResultSet(QueryHandle queryHandle) throws
      JAXBException, InstantiationException, IllegalAccessException, LensException {
    Response response = this
        .exec("get", QueryURL.QUERY_URL + "/" + queryHandle.toString() + "/httpresultset", servLens, null, null);
    AssertUtil.assertSucceededResponse(response);
    logger.info(response);
    String queryResultString = response.readEntity(String.class);
    logger.info(queryResultString);
    QueryResult queryResult = (QueryResult) Util.getObject(queryResultString, QueryResult.class);
    return queryResult;
  }

  /**
   * Execute prepared Query
   *
   * @param queryHandle
   * @param sessionHandleString
   * @param conf
   * @return the query Handle
   */

  public QueryHandle executePreparedQuery(QueryPrepareHandle queryHandle, String sessionHandleString,
      String conf) throws InstantiationException, IllegalAccessException, JAXBException, LensException {
    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", sessionHandleString);
    formData.add("prepareHandle", queryHandle.toString());
    formData.add("operation", "EXECUTE");
    formData.add("conf", conf);
    Response response = this.exec("post", "/queryapi/preparedqueries/" + queryHandle.toString(), servLens, null, null,
        MediaType.MULTIPART_FORM_DATA_TYPE, MediaType.APPLICATION_XML, formData.getForm());
    String queryHandleString = response.readEntity(String.class);
    AssertUtil.assertSucceededResponse(response);
    logger.info(queryHandleString);
    QueryHandle handle = (QueryHandle) Util.getObject(queryHandleString, QueryHandle.class);
    return handle;
  }

  public QueryHandle executePreparedQuery(QueryPrepareHandle queryHandle, String sessionHandleString) throws
      InstantiationException, IllegalAccessException, JAXBException, LensException {
    return executePreparedQuery(queryHandle, sessionHandleString,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><conf />");
  }

  public QueryHandle executePreparedQuery(QueryPrepareHandle queryHandle) throws
      InstantiationException, IllegalAccessException, JAXBException, LensException {
    return executePreparedQuery(queryHandle, sessionHandleString);
  }

  /**
   * Execute prepared Query with timeout
   *
   * @param queryHandle
   * @param timeout
   * @param sessionHandleString
   * @param conf
   * @return the query Handle with result set
   */

  public QueryHandleWithResultSet executePreparedQueryTimeout(QueryPrepareHandle queryHandle, String timeout,
      String sessionHandleString, String conf) throws InstantiationException, IllegalAccessException,
      JAXBException, LensException {
    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", sessionHandleString);
    formData.add("prepareHandle", queryHandle.toString());
    formData.add("operation", "EXECUTE_WITH_TIMEOUT");
    formData.add("conf", conf);
    if (timeout != null) {
      formData.add("timeoutmillis", timeout);
    }
    Response response = this.exec("post", "/queryapi/preparedqueries/" + queryHandle.toString(), servLens, null, null,
        MediaType.MULTIPART_FORM_DATA_TYPE, MediaType.APPLICATION_XML, formData.getForm());
    String queryHandleString = response.readEntity(String.class);
    AssertUtil.assertSucceededResponse(response);
    logger.info(queryHandleString);
    QueryHandleWithResultSet handle = (QueryHandleWithResultSet) Util
        .getObject(queryHandleString, QueryHandleWithResultSet.class);
    return handle;
  }

  public QueryHandleWithResultSet executePreparedQueryTimeout(QueryPrepareHandle queryHandle, String timeout,
      String sessionHandleString) throws InstantiationException, IllegalAccessException, JAXBException, LensException {
    return executePreparedQueryTimeout(queryHandle, timeout, sessionHandleString,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><conf />");
  }

  public QueryHandleWithResultSet executePreparedQueryTimeout(QueryPrepareHandle queryHandle, String timeout) throws
      InstantiationException, IllegalAccessException, JAXBException, LensException {
    return executePreparedQueryTimeout(queryHandle, timeout, sessionHandleString);
  }

  public QueryHandleWithResultSet executePreparedQueryTimeout(QueryPrepareHandle queryHandle) throws
      InstantiationException, IllegalAccessException, JAXBException, LensException {
    return executePreparedQueryTimeout(queryHandle, null);
  }

  /**
   * Submit prepared Query
   *
   * @param queryString
   * @param sessionHandleString
   * @param conf
   * @return the query Prepare Handle
   */

  public QueryPrepareHandle submitPreparedQuery(String queryString, String queryName, String sessionHandleString,
      String conf) throws JAXBException, InstantiationException, IllegalAccessException, LensException {
    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", sessionHandleString);
    formData.add("query", queryString);
    formData.add("operation", "PREPARE");
    formData.add("conf", conf);
    if (queryName != null) {
      formData.add("queryName", queryName);
    }
    Response response = this
        .exec("post", "/queryapi/preparedqueries", servLens, null, null, MediaType.MULTIPART_FORM_DATA_TYPE,
            MediaType.APPLICATION_XML, formData.getForm());
    String queryHandleString = response.readEntity(String.class);
    logger.info(queryHandleString);
    AssertUtil.assertSucceededResponse(response);
    QueryPrepareHandle queryHandle = (QueryPrepareHandle) Util.getObject(queryHandleString, QueryPrepareHandle.class);
    return queryHandle;
  }

  public QueryPrepareHandle submitPreparedQuery(String queryString, String queryName, String sessionHandleString) throws
      JAXBException, InstantiationException, IllegalAccessException, LensException {
    return submitPreparedQuery(queryString, queryName, sessionHandleString,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><conf />");
  }

  public QueryPrepareHandle submitPreparedQuery(String queryString, String queryName) throws
      JAXBException, InstantiationException, IllegalAccessException, LensException {
    return submitPreparedQuery(queryString, queryName, sessionHandleString);
  }

  public QueryPrepareHandle submitPreparedQuery(String queryString) throws
      JAXBException, InstantiationException, IllegalAccessException, LensException {
    return submitPreparedQuery(queryString, null);
  }

  /**
   * Destroy prepared Query
   *
   * @param queryPreparedHandle
   * @param sessionHandleString
   */

  public void destoryPreparedQueryByHandle(QueryPrepareHandle queryPreparedHandle, String sessionHandleString) throws
      JAXBException, LensException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = this
        .exec("delete", QueryURL.PREPAREDQUERY_URL + "/" + queryPreparedHandle.toString(), servLens, null, query);
    logger.info("Response : " + response);
    AssertUtil.assertSucceededResponse(response);
  }

  public void destoryPreparedQueryByHandle(QueryPrepareHandle queryPreparedHandle) throws JAXBException, LensException {
    destoryPreparedQueryByHandle(queryPreparedHandle, sessionHandleString);
  }

  /**
   * Get Prepared QueryHandle List
   *
   * @param queryName
   * @param user
   * @param sessionHandleString
   * @param fromDate
   * @param toDate
   * @return the query Handle
   */

  public List<QueryPrepareHandle> getPreparedQueryHandleList(String queryName, String user, String sessionHandleString,
      String fromDate, String toDate) throws InstantiationException, IllegalAccessException {
    MapBuilder queryList = new MapBuilder("sessionid", sessionHandleString);
    if (queryName != null) {
      queryList.put("queryName", queryName);
    }
    if (user != null) {
      queryList.put("user", user);
    }
    if (fromDate != null) {
      queryList.put("fromDate", fromDate);
    }
    if (toDate != null) {
      queryList.put("toDate", toDate);
    }
    Response response = this.sendQuery("get", QueryURL.PREPAREDQUERY_URL, queryList);
    logger.info("Response : " + response);
    String responseString = response.readEntity(String.class);
    logger.info(responseString);
    PrepareQueryHandles result = (PrepareQueryHandles) Util.getObject(responseString, PrepareQueryHandles.class);
    List<QueryPrepareHandle> list = result.getQueryHandles();
    return list;
  }

  public List<QueryPrepareHandle> getPreparedQueryHandleList(String queryName, String user,
      String sessionHandleString) throws InstantiationException, IllegalAccessException {
    return getPreparedQueryHandleList(queryName, user, sessionHandleString, null, null);
  }

  public List<QueryPrepareHandle> getPreparedQueryHandleList(String queryName, String user) throws
      InstantiationException, IllegalAccessException {
    return getPreparedQueryHandleList(queryName, user, sessionHandleString);
  }

  public List<QueryPrepareHandle> getPreparedQueryHandleList(String queryName) throws
      InstantiationException, IllegalAccessException {
    return getPreparedQueryHandleList(queryName, null);
  }

  public List<QueryPrepareHandle> getPreparedQueryHandleList() throws InstantiationException, IllegalAccessException {
    return getPreparedQueryHandleList(null);
  }

  /**
   * Destroy prepared Query
   *
   * @param queryName
   * @param sessionHandleString
   * @param fromDate
   * @param toDate
   */

  public void destroyPreparedQuery(String queryName, String user, String sessionHandleString, String fromDate,
      String toDate) throws JAXBException, LensException {

    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    if (queryName != null) {
      query.put("queryName", queryName);
    }
    if (user != null) {
      query.put("user", user);
    }
    if (fromDate != null) {
      query.put("fromDate", fromDate);
    }
    if (toDate != null) {
      query.put("toDate", toDate);
    }

    Response response = this.exec("delete", QueryURL.PREPAREDQUERY_URL, servLens, null, query);
    logger.info("Response : " + response);
    AssertUtil.assertSucceededResponse(response);
  }

  public void destroyPreparedQuery(String queryName, String user, String sessionHandleString) throws
      JAXBException, LensException {
    destroyPreparedQuery(queryName, user, sessionHandleString, null, null);
  }

  public void destroyPreparedQuery(String queryName, String user) throws JAXBException, LensException {
    destroyPreparedQuery(queryName, user, sessionHandleString);
  }

  public void destroyPreparedQuery(String queryName) throws JAXBException, LensException {
    destroyPreparedQuery(queryName, null);
  }

  public void destroyPreparedQuery() throws JAXBException, LensException {
    destroyPreparedQuery(null);
  }

  /**
   * Get prepared Query
   *
   * @param queryPrepareHandle
   * @param sessionHandleString
   * @return the client response
   */

  public Response getPreparedQuery(QueryPrepareHandle queryPrepareHandle, String sessionHandleString) {

    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = this
        .exec("get", QueryURL.PREPAREDQUERY_URL + "/" + queryPrepareHandle.toString(), servLens, null, query);
    return response;
  }

  public Response getPreparedQuery(QueryPrepareHandle queryPrepareHandle) {
    return getPreparedQuery(queryPrepareHandle, sessionHandleString);
  }

  /**
   * List Query Handle
   *
   * @param queryName
   * @param state
   * @param user
   * @param sessionHandleString
   * @param fromDate
   * @param toDate
   * @return the query Handle list
   */
  public List<QueryHandle> getQueryHandleList(String queryName, String state, String user, String sessionHandleString,
      String fromDate, String toDate) throws InstantiationException, IllegalAccessException {
    MapBuilder queryList = new MapBuilder("sessionid", sessionHandleString);
    if (queryName != null) {
      queryList.put("queryName", queryName);
    }
    if (state != null) {
      queryList.put("state", state);
    }
    if (user != null) {
      queryList.put("user", user);
    }
    if (fromDate != null) {
      queryList.put("fromDate", fromDate);
    }
    if (toDate != null) {
      queryList.put("toDate", toDate);
    }
    Response response = this.sendQuery("get", QueryURL.QUERY_URL, queryList);
    logger.info("Response : " + response);
    String responseString = response.readEntity(String.class);
    QueryHandles result = (QueryHandles) Util.getObject(responseString, QueryHandles.class);
    List<QueryHandle> list = result.getQueryHandles();
    return list;
  }

  public List<QueryHandle> getQueryHandleList(String queryName, String state, String user,
      String sessionHandleString) throws InstantiationException, IllegalAccessException {
    return getQueryHandleList(queryName, state, user, sessionHandleString, null, null);
  }

  public List<QueryHandle> getQueryHandleList(String queryName, String state, String user) throws
      InstantiationException, IllegalAccessException {
    return getQueryHandleList(queryName, state, user, sessionHandleString);
  }

  public List<QueryHandle> getQueryHandleList(String queryName, String state) throws
      InstantiationException, IllegalAccessException {
    return getQueryHandleList(queryName, state, null);
  }

  public List<QueryHandle> getQueryHandleList(String queryName) throws InstantiationException, IllegalAccessException {
    return getQueryHandleList(queryName, null);
  }

  public List<QueryHandle> getQueryHandleList() throws InstantiationException, IllegalAccessException {
    return getQueryHandleList(null);
  }

  /**
   * Wait for Completion
   *
   * @param sessionHandleString
   * @param queryHandle
   * @return the lens query
   */

  public LensQuery waitForCompletion(String sessionHandleString, QueryHandle queryHandle) throws
      JAXBException, InterruptedException, InstantiationException, IllegalAccessException, LensException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = this.exec("get", QueryURL.QUERY_URL + "/" + queryHandle.toString(), servLens, null, query);
    AssertUtil.assertSucceededResponse(response);
    String responseString = response.readEntity(String.class);
    LensQuery lensQuery = (LensQuery) Util.getObject(responseString, LensQuery.class);
    while (!lensQuery.getStatus().finished()) {
      Thread.sleep(1000);
      logger.info("Waiting...");
      response = this.exec("get", QueryURL.QUERY_URL + "/" + queryHandle.toString(), servLens, null, query);
      lensQuery = (LensQuery) Util.getObject(response.readEntity(String.class), LensQuery.class);
    }
    logger.info(lensQuery.getStatus().getStatusMessage());
    return lensQuery;
  }

  public LensQuery waitForCompletion(QueryHandle queryHandle) throws
      JAXBException, InterruptedException, InstantiationException, IllegalAccessException, LensException {
    return waitForCompletion(sessionHandleString, queryHandle);
  }

  /**
   * Wait for Query to run
   *
   * @param queryHandle
   * @param sessionHandleString
   * @return the query status
   */

  public QueryStatus waitForQueryToRun(QueryHandle queryHandle, String sessionHandleString) throws
      JAXBException, InterruptedException, InstantiationException, IllegalAccessException, LensException {
    QueryStatus queryStatus = getQueryStatus(sessionHandleString, queryHandle);
    while (queryStatus.getStatus() == QueryStatus.Status.QUEUED) {
      logger.info("Waiting for Query to be in Running Phase");
      Thread.sleep(1000);
      queryStatus = getQueryStatus(sessionHandleString, queryHandle);
    }
    return queryStatus;
  }

  public QueryStatus waitForQueryToRun(QueryHandle queryHandle) throws
      JAXBException, InterruptedException, InstantiationException, IllegalAccessException, LensException {
    return waitForQueryToRun(queryHandle, sessionHandleString);
  }

  /**
   * Get Query Status
   *
   * @param sessionHandleString
   * @param queryHandle
   * @return the query Status
   */

  public QueryStatus getQueryStatus(String sessionHandleString, QueryHandle queryHandle) throws
      JAXBException, InstantiationException, IllegalAccessException, LensException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = this.exec("get", QueryURL.QUERY_URL + "/" + queryHandle.toString(), servLens, null, query);
    logger.info("Response : " + response);
    AssertUtil.assertSucceededResponse(response);
    LensQuery lensQuery = (LensQuery) Util.getObject(response.readEntity(String.class), LensQuery.class);
    QueryStatus qStatus = lensQuery.getStatus();
    logger.info("Query Status : " + qStatus);
    return qStatus;
  }

  public QueryStatus getQueryStatus(QueryHandle queryHandle) throws
      JAXBException, InstantiationException, IllegalAccessException, LensException {
    return getQueryStatus(sessionHandleString, queryHandle);
  }

  /**
   * Kill Query by QueryHandle
   *
   * @param sessionHandleString
   * @param queryHandle
   */

  public void killQueryByQueryHandle(String sessionHandleString, QueryHandle queryHandle) throws
      JAXBException, LensException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = this.exec("delete", QueryURL.QUERY_URL + "/" + queryHandle.toString(), servLens, null, query);
    logger.info("Response : " + response);
    AssertUtil.assertSucceededResponse(response);
  }

  public void killQueryByQueryHandle(QueryHandle queryHandle) throws JAXBException, LensException {
    killQueryByQueryHandle(sessionHandleString, queryHandle);
  }

  /**
   * Kill Query
   *
   * @param queryName
   * @param state
   * @param user
   * @param sessionHandleString
   * @param fromDate
   * @param toDate
   */

  public void killQuery(String queryName, String state, String user, String sessionHandleString, String fromDate,
      String toDate) throws JAXBException, LensException {

    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    if (queryName != null) {
      query.put("queryName", queryName);
    }

    if (state != null) {
      query.put("state", state);
    }

    if (user != null) {
      query.put("user", user);
    }

    if (fromDate != null) {
      query.put("fromDate", fromDate);
    }

    if (toDate != null) {
      query.put("toDate", toDate);
    }

    Response response = this.exec("delete", QueryURL.QUERY_URL, servLens, null, query);
    logger.info("Response : " + response);
    AssertUtil.assertSucceededResponse(response);
  }

  public void killQuery(String queryName, String state, String user, String sessionHandleString) throws
      JAXBException, LensException {
    killQuery(queryName, state, user, sessionHandleString, null, null);
  }

  public void killQuery(String queryName, String state, String user) throws JAXBException, LensException {
    killQuery(queryName, state, user, sessionHandleString);
  }

  public void killQuery(String queryName, String state) throws JAXBException, LensException {
    killQuery(queryName, state, null);
  }

  public void killQuery(String queryName) throws JAXBException, LensException {
    killQuery(queryName, null);
  }

  public void killQuery() throws JAXBException, LensException {
    killQuery(null);
  }

}

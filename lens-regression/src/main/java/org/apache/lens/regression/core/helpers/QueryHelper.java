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

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBException;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.query.*;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.api.result.QueryCostTO;
import org.apache.lens.regression.core.constants.QueryURL;
import org.apache.lens.regression.core.type.FormBuilder;
import org.apache.lens.regression.core.type.MapBuilder;
import org.apache.lens.regression.util.AssertUtil;
import org.apache.lens.server.api.error.LensException;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryHelper extends ServiceManagerHelper {


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
  public LensAPIResult executeQuery(String queryString, String queryName, String sessionHandleString,
      String conf) throws InstantiationException, IllegalAccessException, JAXBException, LensException {
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
    LensAPIResult result = response.readEntity(new GenericType<LensAPIResult>(){});
    return result;
  }

  public LensAPIResult executeQuery(String queryString, String queryName, String sessionHandleString) throws
      InstantiationException, IllegalAccessException, JAXBException, LensException {
    return executeQuery(queryString, queryName, sessionHandleString,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><conf />");
  }

  public LensAPIResult executeQuery(String queryString, String queryName) throws
      InstantiationException, IllegalAccessException, JAXBException, LensException {
    return executeQuery(queryString, queryName, sessionHandleString);
  }

  public LensAPIResult executeQuery(String queryString) throws
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

  public LensAPIResult executeQueryTimeout(String queryString, String timeout, String queryName,
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
    LensAPIResult result = response.readEntity(new GenericType<LensAPIResult>(){});
    return result;
  }

  public LensAPIResult executeQueryTimeout(String queryString, String timeout, String queryName,
      String sessionHandleString) throws InstantiationException, IllegalAccessException, JAXBException, LensException {
    return executeQueryTimeout(queryString, timeout, queryName, sessionHandleString,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><conf />");
  }

  public LensAPIResult executeQueryTimeout(String queryString, String timeout, String queryName) throws
      InstantiationException, IllegalAccessException, JAXBException, LensException {
    return executeQueryTimeout(queryString, timeout, queryName, sessionHandleString);
  }

  public LensAPIResult executeQueryTimeout(String queryString, String timeout) throws
      InstantiationException, IllegalAccessException, JAXBException, LensException {
    return executeQueryTimeout(queryString, timeout, null);
  }

  public LensAPIResult executeQueryTimeout(String queryString) throws
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

  public LensAPIResult executeQuery(String queryString, String queryName, String user, String sessionHandleString,
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
    LensAPIResult result = response.readEntity(new GenericType<LensAPIResult>(){});
    log.info("QueryHandle String:{}", result);
    return result;
  }

  /**
   * Explain the query
   *
   * @param queryString
   * @param sessionHandleString
   * @param conf
   * @return the query Plan
   */

  public LensAPIResult<QueryPlan> explainQuery(String queryString, String sessionHandleString, String conf) throws
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
    LensAPIResult<QueryPlan> result = response.readEntity(new GenericType<LensAPIResult<QueryPlan>>(){});
    log.info("QueryPlan String:{}", result);
    return result;
  }

  public LensAPIResult<QueryPlan> explainQuery(String queryString, String sessionHandleString) throws
      JAXBException, InstantiationException, IllegalAccessException, LensException {
    return explainQuery(queryString, sessionHandleString,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><conf />");
  }

  public LensAPIResult<QueryPlan> explainQuery(String queryString) throws
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

  public LensAPIResult<QueryCostTO> estimateQuery(String queryString, String sessionHandleString, String conf) throws
      InstantiationException, IllegalAccessException, JAXBException, LensException {
    FormBuilder formData = new FormBuilder();
    formData.add("sessionid", sessionHandleString);
    formData.add("query", queryString);
    formData.add("operation", "ESTIMATE");
    formData.add("conf", conf);
    Response response = this.exec("post", QueryURL.QUERY_URL, servLens, null, null, MediaType.MULTIPART_FORM_DATA_TYPE,
        MediaType.APPLICATION_XML, formData.getForm());
    LensAPIResult<QueryCostTO> result = response.readEntity(new GenericType<LensAPIResult<QueryCostTO>>(){});
    log.info("QueryCost String:{}", result);
    return result;
  }

  public LensAPIResult<QueryCostTO> estimateQuery(String queryString, String sessionHandleString) throws
      InstantiationException, IllegalAccessException, JAXBException, LensException {
    return estimateQuery(queryString, sessionHandleString,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><conf />");
  }

  public LensAPIResult<QueryCostTO> estimateQuery(String queryString) throws
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
    Response response = this.exec("post", "/queryapi/preparedqueries", servLens, null, null,
        MediaType.MULTIPART_FORM_DATA_TYPE, MediaType.APPLICATION_XML, formData.getForm());
    LensAPIResult<QueryPlan> result = response.readEntity(new GenericType<LensAPIResult<QueryPlan>>() {
    });
    log.info("QueryPlan String:{}", result);
    return result.getData();
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
    QueryResult result = response.readEntity(new GenericType<QueryResult>(){});
    log.info("QueryResult String:{}", result);
    return result;
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
    QueryResult result = response.readEntity(new GenericType<QueryResult>(){});
    return result;
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
    AssertUtil.assertSucceededResponse(response);
    QueryHandle result = response.readEntity(new GenericType<QueryHandle>(){});
    log.info("QueryHandle String:{}", result);
    return result;
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
    QueryHandleWithResultSet result = response.readEntity(new GenericType<QueryHandleWithResultSet>(){});
    return result;
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
    AssertUtil.assertSucceededResponse(response);
    LensAPIResult<QueryPrepareHandle> result = response.readEntity(
        new GenericType<LensAPIResult<QueryPrepareHandle>>(){});
    return result.getData();
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
    log.info("Response : {}" + response);
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
    log.info("Response : {}" + response);
    List<QueryPrepareHandle> list = response.readEntity(new GenericType<List<QueryPrepareHandle>>(){});
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
    log.info("Response : {}", response);
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
      String fromDate, String toDate, String driver) throws InstantiationException, IllegalAccessException {
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
    if (driver != null) {
      queryList.put("driver", driver);
    }
    Response response = this.sendQuery("get", QueryURL.QUERY_URL, queryList);
    log.info("Response : {}", response);
    List<QueryHandle> list = response.readEntity(new GenericType<List<QueryHandle>>(){});
    return list;
  }

  public List<QueryHandle> getQueryHandleList(String queryName, String state, String user,
      String sessionHandleString, String fromDate, String toDate) throws InstantiationException,
          IllegalAccessException {
    return getQueryHandleList(queryName, state, user, sessionHandleString, fromDate, toDate, null);
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
    LensQuery lensQuery = response.readEntity(new GenericType<LensQuery>(){});

    while (!lensQuery.getStatus().finished()) {
      log.info("Waiting...");
      Thread.sleep(1000);
      response = this.exec("get", QueryURL.QUERY_URL + "/" + queryHandle.toString(), servLens, null, query);
      lensQuery = response.readEntity(new GenericType<LensQuery>(){});
    }
    log.info("QueryStatus message:{}", lensQuery.getStatus().getStatusMessage());
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
      log.info("Waiting for Query to be in Running Phase");
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
    log.info("Response : {}", response);
    AssertUtil.assertSucceededResponse(response);
    LensQuery lensQuery = response.readEntity(new GenericType<LensQuery>(){});
    QueryStatus qStatus = lensQuery.getStatus();
    log.info("Query Status for {} : {}", lensQuery.getQueryHandleString(), qStatus);
    return qStatus;
  }

  public LensQuery getLensQuery(String sessionHandleString, QueryHandle queryHandle) throws
      JAXBException, InstantiationException, IllegalAccessException, LensException {
    MapBuilder query = new MapBuilder("sessionid", sessionHandleString);
    Response response = this.exec("get", QueryURL.QUERY_URL + "/" + queryHandle.toString(), servLens, null, query);
    log.info("Response : {}", response);
    AssertUtil.assertSucceededResponse(response);
    LensQuery lensQuery = response.readEntity(new GenericType<LensQuery>(){});
    return lensQuery;
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
    log.info("Response : {}", response);
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
    log.info("Response : {}", response);
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

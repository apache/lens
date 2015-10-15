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

package org.apache.lens.server.common;

import static org.apache.lens.server.common.FormDataMultiPartFactory.createFormDataMultiPartForFact;
import static org.apache.lens.server.common.FormDataMultiPartFactory.createFormDataMultiPartForSession;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.metastore.ObjectFactory;
import org.apache.lens.api.metastore.XCube;
import org.apache.lens.api.metastore.XFactTable;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryResult;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.api.result.LensAPIResult;

import org.glassfish.jersey.media.multipart.FormDataMultiPart;

import com.google.common.base.Optional;

public class RestAPITestUtil {

  private static ObjectFactory cubeObjectFactory = new ObjectFactory();

  protected RestAPITestUtil() {
    throw new UnsupportedOperationException();
  }

  public static LensSessionHandle openFooBarSession(final WebTarget target) {
    return openSession(target, "foo", "bar");
  }

  public static LensSessionHandle openSession(final WebTarget target, final String userName, final String passwd) {
    return openSession(target, userName, passwd, new LensConf());
  }

  public static LensSessionHandle openSession(final WebTarget target, final String userName, final String passwd,
    final LensConf conf) {

    final FormDataMultiPart mp = createFormDataMultiPartForSession(Optional.<LensSessionHandle>absent(),
      Optional.of(userName), Optional.of(passwd), Optional.of(conf));

    return target.path("session").request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      LensSessionHandle.class);
  }

  public static Response estimate(final WebTarget target, final Optional<LensSessionHandle> sessionId,
    final Optional<String> query) {

    return postQuery(target, sessionId, query, Optional.of("estimate"), Optional.<LensConf>absent());
  }

  public static Response execute(final WebTarget target, final Optional<LensSessionHandle> sessionId,
    final Optional<String> query) {
    return execute(target, sessionId, query, Optional.<LensConf>absent());
  }

  public static Response execute(final WebTarget target, final Optional<LensSessionHandle> sessionId,
    final Optional<String> query, final Optional<LensConf> lensConf) {
    return postQuery(target, sessionId, query, Optional.of("execute"), lensConf);
  }

  public static <T> T executeAndGetHandle(final WebTarget target, final Optional<LensSessionHandle> sessionId,
    final Optional<String> query, final Optional<LensConf> lensConf) {
    Response resp = postQuery(target, sessionId, query, Optional.of("execute"), lensConf);
    assertEquals(resp.getStatus(), Response.Status.OK.getStatusCode());
    T handle = resp.readEntity(new GenericType<LensAPIResult<T>>() {}).getData();
    assertNotNull(handle);
    return handle;
  }

  public static Response postQuery(final WebTarget target, final Optional<LensSessionHandle> sessionId,
    final Optional<String> query, final Optional<String> operation) {
    return postQuery(target, sessionId, query, operation, Optional.<LensConf>absent());
  }

  public static Response postQuery(final WebTarget target, final Optional<LensSessionHandle> sessionId,
    final Optional<String> query, final Optional<String> operation, Optional<LensConf> lensConfOptional) {

    FormDataMultiPart mp = FormDataMultiPartFactory
      .createFormDataMultiPartForQuery(sessionId, query, operation, lensConfOptional.or(new LensConf()));

    return target.path("queryapi/queries").request(MediaType.APPLICATION_XML).post(
      Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));
  }

  public static LensQuery executeAndWaitForQueryToFinish(WebTarget target, LensSessionHandle lensSessionId,
    String query, Optional<LensConf> conf, Optional<QueryStatus.Status> statusOptional) throws InterruptedException {
    QueryHandle handle = executeAndGetHandle(target, Optional.of(lensSessionId), Optional.of(query), conf);
    if (statusOptional.isPresent()) {
      return waitForQueryToFinish(target, lensSessionId, handle, statusOptional.get());
    } else {
      return waitForQueryToFinish(target, lensSessionId, handle);
    }
  }

  public static void closeSessionFailFast(final WebTarget target, final LensSessionHandle sessionId) {
    APIResult result = closeSession(target, sessionId);
    checkResponse(result);
  }

  public static APIResult closeSession(final WebTarget target, final LensSessionHandle sessionId) {
    return target.path("session").queryParam("sessionid", sessionId).request().delete(APIResult.class);
  }

  public static String getCurrentDatabase(final WebTarget target, final LensSessionHandle sessionId) {
    WebTarget dbTarget = target.path("metastore").path("databases/current");
    Invocation.Builder builder = dbTarget.queryParam("sessionid", sessionId).request(MediaType.APPLICATION_XML);
    String response = builder.get(String.class);
    return response;
  }

  public static APIResult createCube(final WebTarget target, final LensSessionHandle sessionId, final XCube cube) {

    return target.path("metastore").path("cubes").queryParam("sessionid", sessionId).request(MediaType.APPLICATION_XML)
      .post(Entity.xml(cubeObjectFactory.createXCube(cube)), APIResult.class);
  }

  public static void createCubeFailFast(final WebTarget target, final LensSessionHandle sessionId, final XCube cube) {
    APIResult result = createCube(target, sessionId, cube);
    checkResponse(result);
  }

  public static APIResult createFact(final WebTarget target, final LensSessionHandle sessionId,
    final XFactTable factTable) {

    FormDataMultiPart mp = createFormDataMultiPartForFact(sessionId, factTable);
    return target.path("metastore").path("facts").queryParam("sessionid", sessionId).request(MediaType.APPLICATION_XML)
      .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
  }

  public static void createFactFailFast(final WebTarget target, final LensSessionHandle sessionId,
    final XFactTable factTable) {

    APIResult result = createFact(target, sessionId, factTable);
    checkResponse(result);
  }

  public static APIResult setCurrentDatabase(final WebTarget target, final LensSessionHandle sessionId,
    final String dbName) {

    WebTarget dbTarget = target.path("metastore").path("databases/current");
    return dbTarget.queryParam("sessionid", sessionId).request(MediaType.APPLICATION_XML)
      .put(Entity.xml(dbName),
        APIResult.class);
  }

  public static void setCurrentDatabaseFailFast(final WebTarget target, final LensSessionHandle sessionId,
    final String dbName) {

    APIResult result = setCurrentDatabase(target, sessionId, dbName);
    checkResponse(result);
  }

  public static APIResult createDatabase(final WebTarget target, final LensSessionHandle sessionId,
    final String dbName) {

    WebTarget dbTarget = target.path("metastore").path("databases");
    return dbTarget.queryParam("sessionid", sessionId).request(MediaType.APPLICATION_XML)
      .post(Entity.xml(dbName), APIResult.class);
  }

  public static void createDatabaseFailFast(final WebTarget target, final LensSessionHandle sessionId,
    final String dbName) {

    APIResult result = createDatabase(target, sessionId, dbName);
    checkResponse(result);
  }

  public static void createAndSetCurrentDbFailFast(final WebTarget target, final LensSessionHandle sessionId,
    final String dbName) {

    createDatabaseFailFast(target, sessionId, dbName);
    setCurrentDatabaseFailFast(target, sessionId, dbName);
  }

  public static APIResult dropDatabaseFailFast(final WebTarget target, final LensSessionHandle sessionId,
    String dbName) {

    WebTarget dbTarget = target.path("metastore").path("databases").path(dbName);
    return dbTarget.queryParam("cascade", "true")
      .queryParam("sessionid", sessionId).request(MediaType.APPLICATION_XML).delete(APIResult.class);
  }

  private static void checkResponse(final APIResult result) {
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      throw new RuntimeException("Setup failed");
    }
  }

  public static LensQuery waitForQueryToFinish(final WebTarget target, final LensSessionHandle lensSessionHandle,
    final QueryHandle handle) throws InterruptedException {
    LensQuery ctx = getLensQuery(target, lensSessionHandle, handle);
    while (!ctx.getStatus().finished()) {
      ctx = getLensQuery(target, lensSessionHandle, handle);
      Thread.sleep(1000);
    }
    return ctx;
  }

  public static LensQuery waitForQueryToFinish(final WebTarget target, final LensSessionHandle lensSessionHandle,
    final QueryHandle handle, QueryStatus.Status status) throws InterruptedException {
    LensQuery lensQuery = waitForQueryToFinish(target, lensSessionHandle, handle);
    assertEquals(lensQuery.getStatus().getStatus(), status);
    return lensQuery;
  }

  public static LensQuery getLensQuery(final WebTarget target, final LensSessionHandle lensSessionHandle,
    final QueryHandle handle) {
    return target.path("queryapi/queries").path(handle.toString()).queryParam("sessionid", lensSessionHandle).request()
      .get(LensQuery.class);
  }

  public static QueryResult getLensQueryResult(final WebTarget target, final LensSessionHandle lensSessionHandle,
    final QueryHandle handle) {
    return target.path("queryapi/queries").path(handle.toString()).path("resultset")
      .queryParam("sessionid", lensSessionHandle).request().get(QueryResult.class);
  }

  public static Response getLensQueryHttpResult(final WebTarget target, final LensSessionHandle lensSessionHandle,
    final QueryHandle handle) {
    return target.path("queryapi/queries").path(handle.toString()).path("httpresultset")
      .queryParam("sessionid", lensSessionHandle).request().get(Response.class);
  }
}

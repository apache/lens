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

import static org.apache.lens.server.common.FormDataMultiPartFactory.createFormDataMultiPartForSession;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBElement;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.metastore.*;
import org.apache.lens.api.query.*;
import org.apache.lens.api.result.LensAPIResult;

import org.glassfish.jersey.media.multipart.FormDataMultiPart;

import com.google.common.base.Optional;

public class RestAPITestUtil {

  private static ObjectFactory cubeObjectFactory = new ObjectFactory();

  protected RestAPITestUtil() {
    throw new UnsupportedOperationException();
  }

  public static LensSessionHandle openFooBarSession(final WebTarget target, MediaType mt) {
    return openSession(target, "foo", "bar", mt);
  }

  public static LensSessionHandle openSession(final WebTarget target, final String userName, final String passwd,
    MediaType mt) {
    return openSession(target, userName, passwd, new LensConf(), mt);
  }

  public static LensSessionHandle openSession(final WebTarget target, final String userName, final String passwd,
    final LensConf conf, MediaType mt) {

    final FormDataMultiPart mp = createFormDataMultiPartForSession(Optional.of(userName), Optional.of(passwd),
      Optional.of(conf), mt);

    return target.path("session").request(mt).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      LensSessionHandle.class);
  }

  public static Response estimate(final WebTarget target, final Optional<LensSessionHandle> sessionId,
    final Optional<String> query, MediaType mt) {
    return postQuery(target, sessionId, query, Optional.of("estimate"), Optional.<LensConf>absent(), mt);
  }
  public static Response explain(final WebTarget target, final Optional<LensSessionHandle> sessionId,
                                  final Optional<String> query, MediaType mt) {
    return postQuery(target, sessionId, query, Optional.of("explain"), Optional.<LensConf>absent(), mt);
  }
  public static Response execute(final WebTarget target, final Optional<LensSessionHandle> sessionId,
    final Optional<String> query, MediaType mt) {
    return execute(target, sessionId, query, Optional.<LensConf>absent(), mt);
  }

  public static Response execute(final WebTarget target, final Optional<LensSessionHandle> sessionId,
    final Optional<String> query, final Optional<LensConf> lensConf, MediaType mt) {
    return postQuery(target, sessionId, query, Optional.of("execute"), lensConf, mt);
  }

  public static <T> T executeAndGetHandle(final WebTarget target, final Optional<LensSessionHandle> sessionId,
    final Optional<String> query, final Optional<LensConf> lensConf, MediaType mt) {
    Response resp = postQuery(target, sessionId, query, Optional.of("execute"), lensConf, mt);
    assertEquals(resp.getStatus(), Response.Status.OK.getStatusCode());
    T handle = resp.readEntity(new GenericType<LensAPIResult<T>>() {}).getData();
    assertNotNull(handle);
    return handle;
  }

  public static Response postQuery(final WebTarget target, final Optional<LensSessionHandle> sessionId,
                                   final Optional<String> query, final Optional<String> operation, MediaType mt) {
    return postQuery(target, sessionId, query, operation, Optional.<LensConf>absent(), mt);
  }

  public static Response postQuery(final WebTarget target, final Optional<LensSessionHandle> sessionId,
    final Optional<String> query, final Optional<String> operation, Optional<LensConf> lensConfOptional, MediaType mt) {

    FormDataMultiPart mp = FormDataMultiPartFactory
      .createFormDataMultiPartForQuery(sessionId, query, operation, lensConfOptional.or(new LensConf()), mt);

    return target.path("queryapi/queries").request(mt).post(
      Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));
  }

  public static LensQuery executeAndWaitForQueryToFinish(WebTarget target, LensSessionHandle lensSessionId,
    String query, Optional<LensConf> conf, Optional<QueryStatus.Status> statusOptional, MediaType mt)
    throws InterruptedException {
    QueryHandle handle = executeAndGetHandle(target, Optional.of(lensSessionId), Optional.of(query), conf, mt);
    if (statusOptional.isPresent()) {
      return waitForQueryToFinish(target, lensSessionId, handle, statusOptional.get(), mt);
    } else {
      return waitForQueryToFinish(target, lensSessionId, handle, mt);
    }
  }

  public static void closeSessionFailFast(final WebTarget target, final LensSessionHandle sessionId, MediaType mt) {
    APIResult result = closeSession(target, sessionId, mt);
    checkResponse(result);
  }

  public static APIResult closeSession(final WebTarget target, final LensSessionHandle sessionId, MediaType mt) {
    return target.path("session").queryParam("sessionid", sessionId).request(mt).delete(APIResult.class);
  }

  public static String getCurrentDatabase(final WebTarget target, final LensSessionHandle sessionId, MediaType mt) {
    WebTarget dbTarget = target.path("metastore").path("databases/current");
    Invocation.Builder builder = dbTarget.queryParam("sessionid", sessionId).request(mt);
    String response = builder.get(String.class);
    return response;
  }

  public static APIResult createCube(final WebTarget target, final LensSessionHandle sessionId, final XCube cube,
    MediaType mt) {
    return target.path("metastore").path("cubes").queryParam("sessionid", sessionId).request(mt)
      .post(Entity.entity(
        new GenericEntity<JAXBElement<XCube>>(cubeObjectFactory.createXCube(cube)){}, mt), APIResult.class);
  }

  public static void createCubeFailFast(final WebTarget target, final LensSessionHandle sessionId, final XCube cube,
    MediaType mt) {
    APIResult result = createCube(target, sessionId, cube, mt);
    checkResponse(result);
  }

  public static void createFactFailFast(final WebTarget target, final LensSessionHandle sessionId,
    final XFactTable factTable, MediaType mt) {

    APIResult result = target.path("metastore").path("facts").queryParam("sessionid", sessionId)
      .request(mt).post(Entity.entity(
          new GenericEntity<JAXBElement<XFact>>(cubeObjectFactory.createXFact(factTable)) {
          }, mt),
        APIResult.class);
    checkResponse(result);
  }

  public static void createStorageFailFast(final WebTarget target, final LensSessionHandle sessionId,
    final XStorage storage, MediaType mt) {
    APIResult result = target.path("metastore").path("storages").queryParam("sessionid", sessionId)
      .request(mt).post(Entity.entity(
        new GenericEntity<JAXBElement<XStorage>>(cubeObjectFactory.createXStorage(storage)) {
        }, mt),
        APIResult.class);
    checkResponse(result);
  }

  public static APIResult setCurrentDatabase(final WebTarget target, final LensSessionHandle sessionId,
    final String dbName, MediaType mt) {

    WebTarget dbTarget = target.path("metastore").path("databases/current");
    return dbTarget.queryParam("sessionid", sessionId).request(mt)
      .put(Entity.xml(dbName),
        APIResult.class);
  }

  public static void setCurrentDatabaseFailFast(final WebTarget target, final LensSessionHandle sessionId,
    final String dbName, MediaType mt) {

    APIResult result = setCurrentDatabase(target, sessionId, dbName, mt);
    checkResponse(result);
  }

  public static APIResult createDatabase(final WebTarget target, final LensSessionHandle sessionId,
    final String dbName, MediaType mt) {

    WebTarget dbTarget = target.path("metastore").path("databases");
    return dbTarget.queryParam("sessionid", sessionId).request(mt)
      .post(Entity.xml(dbName), APIResult.class);
  }

  public static void createDatabaseFailFast(final WebTarget target, final LensSessionHandle sessionId,
    final String dbName, MediaType mt) {

    APIResult result = createDatabase(target, sessionId, dbName, mt);
    checkResponse(result);
  }

  public static void createAndSetCurrentDbFailFast(final WebTarget target, final LensSessionHandle sessionId,
    final String dbName, MediaType mt) {

    createDatabaseFailFast(target, sessionId, dbName, mt);
    setCurrentDatabaseFailFast(target, sessionId, dbName, mt);
  }

  public static APIResult dropDatabaseFailFast(final WebTarget target, final LensSessionHandle sessionId,
    String dbName, MediaType mt) {

    WebTarget dbTarget = target.path("metastore").path("databases").path(dbName);
    return dbTarget.queryParam("cascade", "true")
      .queryParam("sessionid", sessionId).request(mt).delete(APIResult.class);
  }

  private static void checkResponse(final APIResult result) {
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      throw new RuntimeException("Setup failed");
    }
  }

  public static LensQuery waitForQueryToFinish(final WebTarget target, final LensSessionHandle lensSessionHandle,
    final QueryHandle handle, MediaType mt) throws InterruptedException {
    LensQuery ctx = getLensQuery(target, lensSessionHandle, handle, mt);
    while (!ctx.getStatus().finished()) {
      ctx = getLensQuery(target, lensSessionHandle, handle, mt);
      Thread.sleep(1000);
    }
    return ctx;
  }

  public static LensQuery waitForQueryToFinish(final WebTarget target, final LensSessionHandle lensSessionHandle,
    final QueryHandle handle, QueryStatus.Status status, MediaType mt) throws InterruptedException {
    LensQuery lensQuery = waitForQueryToFinish(target, lensSessionHandle, handle, mt);
    assertEquals(lensQuery.getStatus().getStatus(), status, String.valueOf(lensQuery));
    return lensQuery;
  }

  public static LensQuery getLensQuery(final WebTarget target, final LensSessionHandle lensSessionHandle,
    final QueryHandle handle, MediaType mt) {
    return target.path("queryapi/queries").path(handle.toString()).queryParam("sessionid", lensSessionHandle)
      .request(mt).get(LensQuery.class);
  }

  public static String getLensQueryResultAsString(final WebTarget target, final LensSessionHandle lensSessionHandle,
    final QueryHandle handle, MediaType mt) {
    return target.path("queryapi/queries").path(handle.toString()).path("resultset")
      .queryParam("sessionid", lensSessionHandle).request(mt).get(String.class);
  }

  public static PersistentQueryResult getLensQueryResult(final WebTarget target,
    final LensSessionHandle lensSessionHandle, final QueryHandle handle, MediaType mt) throws InterruptedException {
    return getLensQueryResult(target, lensSessionHandle, handle, PersistentQueryResult.class, mt);
  }
  public static <T> T getLensQueryResult(final WebTarget target, final LensSessionHandle lensSessionHandle,
    final QueryHandle handle, Class<T> clazz, MediaType mt) throws InterruptedException {
    waitForQueryToFinish(target, lensSessionHandle, handle, QueryStatus.Status.SUCCESSFUL, mt);
    return target.path("queryapi/queries").path(handle.toString()).path("resultset")
      .queryParam("sessionid", lensSessionHandle).request(mt).get(clazz);
  }

  public static Response getLensQueryHttpResult(final WebTarget target, final LensSessionHandle lensSessionHandle,
    final QueryHandle handle) {
    return target.path("queryapi/queries").path(handle.toString()).path("httpresultset")
      .queryParam("sessionid", lensSessionHandle).request().get(Response.class);
  }
}

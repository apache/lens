/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.query.save;

import static org.apache.lens.api.query.save.ParameterCollectionType.SINGLE;
import static org.apache.lens.api.query.save.ParameterDataType.STRING;

import static org.testng.Assert.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.*;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.jaxb.LensJAXBContextResolver;
import org.apache.lens.api.query.save.*;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.api.query.save.SavedQueryService;
import org.apache.lens.server.error.LensExceptionMapper;
import org.apache.lens.server.query.QueryExecutionServiceImpl;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.beust.jcommander.internal.Maps;
import com.beust.jcommander.internal.Sets;
import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Test(groups = "unit-test")
public class TestSavedQueryService extends LensJerseyTest {
  SavedQueryServiceImpl savedQueryService;
  QueryExecutionServiceImpl queryService;
  MetricsService metricsSvc;
  LensSessionHandle lensSessionId;

  private static final String QUERY_STRING = "select * from table where "
    + "col = :param1 ";

  private static final SavedQuery QUERY = new SavedQuery(
    1,
    "query_name",
    "description",
    QUERY_STRING
    ,
    Lists.newArrayList(
      new Parameter(
        "param1", "Param1", new String[]{"val"}, STRING, SINGLE
      )
    )
  );

  public static class SavedQueryTestApp extends SavedQueryApp {

    @Override
    public Set<Class<?>> getClasses() {
      final Set<Class<?>> classes = super.getClasses();
      classes.add(LensExceptionMapper.class);
      classes.add(LensJAXBContextResolver.class);
      return classes;
    }
  }

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
    savedQueryService = LensServices.get().getService(SavedQueryService.NAME);
    queryService = LensServices.get().getService(QueryExecutionService.NAME);
    metricsSvc = LensServices.get().getService(MetricsService.NAME);
    Map<String, String> sessionconf = Maps.newHashMap();
    sessionconf.put("test.session.key", "svalue");
    lensSessionId = queryService.openSession("foo", "bar", sessionconf); // @localhost should be removed
  }

  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
    queryService.closeSession(lensSessionId);
    super.tearDown();
  }

  @Override
  protected Application configure() {
    return new SavedQueryTestApp();
  }

  @Test
  public void testResource() throws InterruptedException {
    assertEquals(
      savedQueriesRoot().path("health").request().get().getStatus(),
      200,
      "Saved query resource is not up"
    );
  }

  private WebTarget savedQueriesRoot() {
    return target()
      .path("queryapi")
      .path("savedqueries");
  }

  private ResourceModifiedResponse updateQuery(long id) {
    Response savedquery = savedQueriesRoot()
      .path(String.valueOf(id))
      .request(MediaType.APPLICATION_JSON_TYPE)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .put(Entity.json(QUERY));
    savedquery.getStringHeaders().putSingle(HttpHeaders.CONTENT_TYPE, "application/json");
    return savedquery.readEntity(ResourceModifiedResponse.class);
  }

  private ResourceModifiedResponse deleteQuery(long id) {
    Response savedquery = savedQueriesRoot()
      .path(String.valueOf(id))
      .request(MediaType.APPLICATION_JSON_TYPE)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .delete();
    savedquery.getStringHeaders().putSingle(HttpHeaders.CONTENT_TYPE, "application/json");
    return savedquery.readEntity(ResourceModifiedResponse.class);
  }

  private SavedQuery get(long id) {
    Response savedquery = savedQueriesRoot()
      .path(String.valueOf(id))
      .request(MediaType.APPLICATION_JSON_TYPE)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .get();
    savedquery.getStringHeaders().putSingle(HttpHeaders.CONTENT_TYPE, "application/json");
    return savedquery.readEntity(SavedQuery.class);
  }

  private ParameterParserResponse extractParameters() {
    Response parameters = savedQueriesRoot()
      .path("parameters")
      .queryParam("query", QUERY_STRING)
      .request(MediaType.APPLICATION_JSON_TYPE)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .get();
    parameters.getStringHeaders().putSingle(HttpHeaders.CONTENT_TYPE, "application/json");
    return parameters.readEntity(ParameterParserResponse.class);
  }

  private ResourceModifiedResponse saveQuery() {
    Response savedquery = savedQueriesRoot()
      .request(MediaType.APPLICATION_JSON_TYPE)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(QUERY));
    savedquery.getStringHeaders().putSingle(HttpHeaders.CONTENT_TYPE, "application/json");
    return savedquery.readEntity(ResourceModifiedResponse.class);
  }

  private ListResponse list(long offset, long count) {
    Response savedquery = savedQueriesRoot()
      .queryParam("start", offset)
      .queryParam("count", count)
      .request(MediaType.APPLICATION_JSON_TYPE)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .get();
    savedquery.getStringHeaders().putSingle(HttpHeaders.CONTENT_TYPE, "application/json");
    return savedquery.readEntity(ListResponse.class);
  }

  @Test
  public void testSaveQuery() {
    assertEquals(saveQuery().getStatus(), ResourceModifiedResponse.Action.CREATED);
  }

  @Test
  public void testUpdateQuery() {
    ResourceModifiedResponse saved = saveQuery();
    ResourceModifiedResponse updated = updateQuery(saved.getId());
    assertEquals(updated.getStatus(), ResourceModifiedResponse.Action.UPDATED);
    assertEquals(updated.getId(), saved.getId());
  }

  @Test
  public void testUpdateQueryNonExistentResource() {
    try {
      updateQuery(99999);
      fail("Did not fail when querying for a non existent resource");
    } catch (Throwable e) {
      assertTrue(true);
    }
  }

  @Test
  public void testGetQuery() {
    final long id = saveQuery().getId();
    SavedQuery savedQuery = get(id);
    assertEquals(savedQuery.getId(), id);
  }

  @Test
  public void testGetQueryNonExistentResource() {
    try {
      get(99999);
      fail("Did not fail when querying for a non existent resource");
    } catch (Throwable e) {
      assertTrue(true);
    }
  }

  @Test
  public void testListQuery() {
    final Set<Long> ids = Sets.newHashSet();
    ids.add(saveQuery().getId());
    ids.add(saveQuery().getId());
    ids.add(saveQuery().getId());
    ids.add(saveQuery().getId());
    final ListResponse list = list(0, 4);
    final List<SavedQuery> queries = list.getResoures();
    assertEquals(ids.size(), queries.size());
  }

  @Test
  public void testDeleteQuery() {
    long id = saveQuery().getId();
    deleteQuery(id);
    try {
      get(id);
      fail("Resource not deleted");
    } catch (Throwable e) {
      assertTrue(true);
    }

  }

  @Test
  public void testDeleteQueryNonExistentResource() {
    long id = saveQuery().getId();
    deleteQuery(id);
    try {
      deleteQuery(id);
      fail("Succeeded in deleting a non existent resource");
    } catch (Throwable e) {
      assertTrue(true);
    }
  }

}

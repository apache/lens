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
package org.apache.lens.server;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.List;

import javax.ws.rs.NotAllowedException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.server.LensServices.SERVICE_MODE;
import org.apache.lens.server.common.RestAPITestUtil;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * The Class TestServerMode.
 */
@Test(alwaysRun = true, groups = "filter-test", dependsOnGroups = "restart-test")
public class TestServerMode extends LensAllApplicationJerseyTest {

  private LensSessionHandle lensSessionHandle;

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#setUp()
   */
  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
    LensServerTestUtil.createTable("test_table", target(), RestAPITestUtil.openFooBarSession(target(), defaultMT),
      defaultMT);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#tearDown()
   */
  @AfterTest
  public void tearDown() throws Exception {
    RestAPITestUtil.closeSession(target(), lensSessionHandle, defaultMT);
    super.tearDown();
  }

  /**
   * Test read only mode.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testReadOnlyMode() throws InterruptedException {
    testMode(SERVICE_MODE.READ_ONLY);
  }

  /**
   * Test metastore no drop mode.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testMetastoreNoDropMode() throws InterruptedException {
    testMode(SERVICE_MODE.METASTORE_NODROP);
  }

  /**
   * Test metastore read only mode.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testMetastoreReadOnlyMode() throws InterruptedException {
    testMode(SERVICE_MODE.METASTORE_READONLY);
  }

  /**
   * Test open mode.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testOpenMode() throws InterruptedException {
    testMode(SERVICE_MODE.OPEN);
  }

  /**
   * Test mode.
   *
   * @param mode the mode
   * @throws InterruptedException the interrupted exception
   */
  private void testMode(SERVICE_MODE mode) throws InterruptedException {
    LensServices.get().setServiceMode(mode);
    // open a session
    // should always pass
    final WebTarget target = target().path("session");
    final FormDataMultiPart mp = new FormDataMultiPart();

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(), "foo"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(), "bar"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
      new LensConf(), MediaType.APPLICATION_XML_TYPE));

    final LensSessionHandle lensSessionId = target.request().post(
      Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), LensSessionHandle.class);
    Assert.assertNotNull(lensSessionId);

    // creata a database
    WebTarget dbTarget = target().path("metastore").path("databases");

    try {
      APIResult result = dbTarget.queryParam("sessionid", lensSessionId).request(MediaType.APPLICATION_XML_TYPE)
        .post(Entity.xml("newdb"), APIResult.class);
      assertNotNull(result);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    } catch (NotAllowedException nae) {
      if (mode.equals(SERVICE_MODE.READ_ONLY) || mode.equals(SERVICE_MODE.METASTORE_READONLY)) {
        // expected
        System.out.println("Create databse not allowed in mode:" + mode);
      } else {
        Assert.fail("Creating database should pass");
      }
    }

    // drop the database
    try {
      APIResult drop = dbTarget.path("newdb").queryParam("sessionid", lensSessionId).request().delete(APIResult.class);
      assertNotNull(drop);
      assertEquals(drop.getStatus(), APIResult.Status.SUCCEEDED);
    } catch (NotAllowedException nae) {
      if (mode.equals(SERVICE_MODE.READ_ONLY) || mode.equals(SERVICE_MODE.METASTORE_READONLY)
        || mode.equals(SERVICE_MODE.METASTORE_NODROP)) {
        // expected
        System.out.println("Drop databse not allowed in mode:" + mode);
      } else {
        Assert.fail("Dropping database should pass");
      }
    }

    // Try launching query
    final WebTarget queryTarget = target().path("queryapi/queries");

    final FormDataMultiPart query = new FormDataMultiPart();

    query.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      MediaType.APPLICATION_XML_TYPE));
    query.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), "select id from test_table"));
    query.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    query.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(),
      new LensConf(), MediaType.APPLICATION_XML_TYPE));

    QueryHandle qhandle = null;
    try {
      qhandle = queryTarget.request().post(Entity.entity(query, MediaType.MULTIPART_FORM_DATA_TYPE),
          new GenericType<LensAPIResult<QueryHandle>>() {}).getData();
    } catch (NotAllowedException nae) {
      if (mode.equals(SERVICE_MODE.READ_ONLY)) {
        // expected
        System.out.println("Launching query not allowed in mode:" + mode);
      } else {
        Assert.fail("Posting query should pass");
      }
    }

    // Get all queries; should always pass
    List<QueryHandle> allQueriesXML = queryTarget.queryParam("sessionid", lensSessionId)
      .request(MediaType.APPLICATION_XML).get(new GenericType<List<QueryHandle>>() {
      });
    Assert.assertTrue(allQueriesXML.size() >= 1);

    if (!mode.equals(SERVICE_MODE.READ_ONLY)) {
      assertNotNull(qhandle);
      // wait for query completion if mode is not read only
      LensQuery ctx = queryTarget.path(qhandle.toString()).queryParam("sessionid", lensSessionId).request()
        .get(LensQuery.class);
      // Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.QUEUED);

      // wait till the query finishes
      QueryStatus stat = ctx.getStatus();
      while (!stat.finished()) {
        ctx = queryTarget.path(qhandle.toString()).queryParam("sessionid", lensSessionId).request()
          .get(LensQuery.class);
        stat = ctx.getStatus();
        Thread.sleep(1000);
      }
    }

    // close the session
    APIResult sessionclose = target.queryParam("sessionid", lensSessionId).request().delete(APIResult.class);
    Assert.assertEquals(sessionclose.getStatus(), APIResult.Status.SUCCEEDED);
    LensServices.get().setServiceMode(SERVICE_MODE.OPEN);
  }
}

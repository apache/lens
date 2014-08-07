package com.inmobi.grill.server;

/*
 * #%L
 * Grill Server
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.List;

import javax.ws.rs.NotAllowedException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.query.GrillQuery;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.server.GrillServices.SERVICE_MODE;

@Test(alwaysRun=true, groups="filter-test",dependsOnGroups="restart-test")
public class TestServerMode extends GrillAllApplicationJerseyTest {

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }

  @AfterTest
  public void tearDown() throws Exception {
    GrillServices.get().setServiceMode(SERVICE_MODE.OPEN);
    super.tearDown();
  }

  @Override
  protected int getTestPort() {
    return 8090;
  }

  @Test
  public void testReadOnlyMode() throws InterruptedException {
    testMode(SERVICE_MODE.READ_ONLY);
  }

  @Test
  public void testMetastoreNoDropMode() throws InterruptedException {
    testMode(SERVICE_MODE.METASTORE_NODROP);
  }

  @Test
  public void testMetastoreReadOnlyMode() throws InterruptedException {
    testMode(SERVICE_MODE.METASTORE_READONLY);
  }

  @Test
  public void testOpenMode() throws InterruptedException {
    testMode(SERVICE_MODE.OPEN);
  }

  private void testMode(SERVICE_MODE mode) throws InterruptedException {
    GrillServices.get().setServiceMode(mode);
    // open a session
    // should always pass
    final WebTarget target = target().path("session");
    final FormDataMultiPart mp = new FormDataMultiPart();

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(),
        "user1"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(),
        "psword"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
        new GrillConf(),
        MediaType.APPLICATION_XML_TYPE));

    final GrillSessionHandle grillSessionId = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), GrillSessionHandle.class);
    Assert.assertNotNull(grillSessionId);

    // creata a database
    WebTarget dbTarget = target().path("metastore").path("databases");

    try {
      APIResult result = dbTarget.queryParam("sessionid",
          grillSessionId).request(MediaType.APPLICATION_XML_TYPE).post(Entity.xml("newdb"), APIResult.class);
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
      APIResult drop = dbTarget.path("newdb").queryParam("sessionid", grillSessionId).request().delete(APIResult.class);
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

    query.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    query.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        "select name from table"));
    query.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(),
        "execute"));
    query.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        new GrillConf(),
        MediaType.APPLICATION_XML_TYPE));

    QueryHandle qhandle = null;
    try {
      qhandle = queryTarget.request().post(
          Entity.entity(query, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);
    } catch (NotAllowedException nae) {
      if (mode.equals(SERVICE_MODE.READ_ONLY)) {
        // expected
        System.out.println("Launching query not allowed in mode:" + mode);
      } else {
        Assert.fail("Posting query should pass");
      }
    }

    // Get all queries; should always pass
    List<QueryHandle> allQueriesXML = queryTarget.queryParam("sessionid", grillSessionId).request(MediaType.APPLICATION_XML)
        .get(new GenericType<List<QueryHandle>>() {
        });
    Assert.assertTrue(allQueriesXML.size() >= 1);

    if (!mode.equals(SERVICE_MODE.READ_ONLY)) {
      assertNotNull(qhandle);
      // wait for query completion if mode is not read only
      GrillQuery ctx = queryTarget.path(qhandle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
      // Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.QUEUED);

      // wait till the query finishes
      QueryStatus stat = ctx.getStatus();
      while (!stat.isFinished()) {
        ctx = queryTarget.path(qhandle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
        stat = ctx.getStatus();
        Thread.sleep(1000);
      }
    }
    
    // close the session
    APIResult sessionclose = target.queryParam("sessionid", grillSessionId).request().delete(APIResult.class);
    Assert.assertEquals(sessionclose.getStatus(), APIResult.Status.SUCCEEDED);
  }
}

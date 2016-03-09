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
package org.apache.lens.server.session;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.APIResult.Status;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.StringList;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.lens.server.api.session.SessionService;
import org.apache.lens.server.common.LenServerTestException;
import org.apache.lens.server.common.LensServerTestFileUtils;
import org.apache.lens.server.common.RestAPITestUtil;
import org.apache.lens.server.common.TestResourceFile;

import org.apache.commons.io.FileUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.Hive;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.test.TestProperties;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * The Class TestSessionResource.
 */
@Test(groups = "unit-test")
public class TestSessionResource extends LensJerseyTest {


  /** The metrics svc. */
  MetricsService metricsSvc;

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#setUp()
   */
  @BeforeTest
  public void setUp() throws Exception {
    metricsSvc = LensServices.get().getService(MetricsService.NAME);
    LensServices.get().getLogSegregationContext().setLogSegregationId("logid");
    super.setUp();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#tearDown()
   */
  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#configure()
   */
  @Override
  protected Application configure() {
    enable(TestProperties.LOG_TRAFFIC);
    enable(TestProperties.DUMP_ENTITY);
    return new SessionApp();
  }

  @Test
  public void testDefaultResponseType() {
    final WebTarget target = target().path("session");
    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(), "foo"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(), "bar"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
      new LensConf(), defaultMT));

    final String handle = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      String.class);
    Assert.assertNotNull(handle);
    Assert.assertTrue(handle.contains("xml"), "Handle is " + handle);
    Assert.assertTrue(handle.contains("publicId"), "Handle is " + handle);
    Assert.assertTrue(handle.contains("secretId"), "Handle is " + handle);

    String result = target.queryParam("sessionid", handle).request().delete(String.class);
    Assert.assertNotNull(result);
    Assert.assertTrue(result.contains("xml"), "Result is " + result);
    Assert.assertTrue(result.contains("succeeded"), "Result is " + result);
  }

  /**
   * Test session.
   */
  @Test(dataProvider = "mediaTypeData")
  public void testSession(MediaType mt) {
    final WebTarget target = target().path("session");
    final FormDataMultiPart mp = new FormDataMultiPart();

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(), "foo"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(), "bar"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
      new LensConf(), mt));

    final LensSessionHandle handle = target.request(mt).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      LensSessionHandle.class);
    Assert.assertNotNull(handle);

    // get all session params
    final WebTarget paramtarget = target().path("session/params");
    StringList sessionParams = paramtarget.queryParam("sessionid", handle).request(mt).get(StringList.class);
    System.out.println("Session params:" + sessionParams.getElements());
    Assert.assertTrue(sessionParams.getElements().size() > 1);
    Assert.assertTrue(sessionParams.getElements().contains("lens.session.cluster.user=testlensuser"));
    Assert.assertTrue(sessionParams.getElements().contains("lens.session.loggedin.user=foo"));

    // set hive variable
    FormDataMultiPart setpart = new FormDataMultiPart();
    setpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), handle,
      mt));
    setpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("key").build(), "hivevar:myvar"));
    setpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("value").build(), "10"));
    APIResult result = paramtarget.request(mt).put(Entity.entity(setpart, MediaType.MULTIPART_FORM_DATA_TYPE),
      APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    // get myvar session params
    sessionParams = paramtarget.queryParam("sessionid", handle).queryParam("key", "hivevar:myvar").request(mt)
      .get(StringList.class);
    System.out.println("Session params:" + sessionParams.getElements());
    Assert.assertEquals(sessionParams.getElements().size(), 1);
    Assert.assertTrue(sessionParams.getElements().contains("hivevar:myvar=10"));

    // set hive conf
    setpart = new FormDataMultiPart();
    setpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), handle,
      mt));
    setpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("key").build(), "hiveconf:my.conf"));
    setpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("value").build(), "myvalue"));
    result = paramtarget.request(mt).put(Entity.entity(setpart, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    // get the my.conf session param
    sessionParams = paramtarget.queryParam("sessionid", handle).queryParam("key", "my.conf").request(mt)
      .get(StringList.class);
    System.out.println("Session params:" + sessionParams.getElements());
    Assert.assertEquals(sessionParams.getElements().size(), 1);
    Assert.assertTrue(sessionParams.getElements().contains("my.conf=myvalue"));
    // get server params on session
    try {
      paramtarget.queryParam("sessionid", handle).queryParam("key", "lens.server.persist.location").request(mt)
        .get(StringList.class);
      Assert.fail("Expected 404");
    } catch (Exception ne) {
      Assert.assertTrue(ne instanceof NotFoundException);
    }
    // get all params verbose
    sessionParams = paramtarget.queryParam("sessionid", handle).queryParam("verbose", true).request(mt)
      .get(StringList.class);
    System.out.println("Session params:" + sessionParams.getElements());
    Assert.assertTrue(sessionParams.getElements().size() > 1);

    // Create another session
    final LensSessionHandle handle2 = target.request(mt).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      LensSessionHandle.class);
    Assert.assertNotNull(handle2);

    // get myvar session params on handle2
    try {
      paramtarget.queryParam("sessionid", handle2).queryParam("key", "hivevar:myvar").request(mt)
        .get(StringList.class);
      Assert.fail("Expected 404");
    } catch (Exception ne) {
      Assert.assertTrue(ne instanceof NotFoundException);
    }
    // get the my.conf session param on handle2
    try {
      sessionParams = paramtarget.queryParam("sessionid", handle2).queryParam("key", "my.conf").request(mt)
        .get(StringList.class);
      System.out.println("sessionParams:" + sessionParams.getElements());
      Assert.fail("Expected 404");
    } catch (Exception ne) {
      Assert.assertTrue(ne instanceof NotFoundException);
    }

    // close session
    result = target.queryParam("sessionid", handle).request(mt).delete(APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    // now getting session params should return session is expired
    try {
      sessionParams = paramtarget.queryParam("sessionid", handle).queryParam("key", "hivevar:myvar").request(mt)
            .get(StringList.class);
      Assert.fail("Expected 410");
    } catch(ClientErrorException ce) {
      Assert.assertEquals(ce.getResponse().getStatus(), 410);
    }

    result = target.queryParam("sessionid", handle2).request(mt).delete(APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  /**
   * Test resource.
   */
  @Test(dataProvider = "mediaTypeData")
  public void testResource(MediaType mt) {
    final WebTarget target = target().path("session");
    final FormDataMultiPart mp = new FormDataMultiPart();

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(), "foo"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(), "bar"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
      new LensConf(), mt));

    final LensSessionHandle handle = target.request(mt).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      LensSessionHandle.class);
    Assert.assertNotNull(handle);

    // add a resource
    final String lensSiteFilePath = TestSessionResource.class.getClassLoader().getResource("lens-site.xml").getPath();
    final WebTarget resourcetarget = target().path("session/resources");
    final FormDataMultiPart mp1 = new FormDataMultiPart();
    mp1.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), handle,
      mt));
    mp1.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("type").build(), "file"));
    mp1.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("path").build(),
        lensSiteFilePath));
    APIResult result = resourcetarget.path("add").request(mt)
      .put(Entity.entity(mp1, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    Assert.assertEquals(result.getStatus(), Status.SUCCEEDED);

    // list all resources
    StringList listResources = resourcetarget.path("list").queryParam("sessionid", handle).request(mt)
      .get(StringList.class);
    Assert.assertEquals(listResources.getElements().size(), 1);

    // delete the resource
    final FormDataMultiPart mp2 = new FormDataMultiPart();
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), handle,
      mt));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("type").build(), "file"));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("path").build(),
        lensSiteFilePath));
    result = resourcetarget.path("delete").request(mt)
      .put(Entity.entity(mp2, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    // list all resources
    StringList listResourcesAfterDeletion = resourcetarget.path("list").queryParam("sessionid", handle)
      .request(mt).get(StringList.class);
    Assert.assertTrue(listResourcesAfterDeletion.getElements() == null
      || listResourcesAfterDeletion.getElements().isEmpty());

    // close session
    result = target.queryParam("sessionid", handle).request(mt).delete(APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  /**
   * Test aux jars.
   *
   * @throws org.apache.lens.server.api.error.LensException the lens exception
   */
  @Test(dataProvider = "mediaTypeData")
  public void testAuxJars(MediaType mt) throws LensException, IOException, LenServerTestException {
    final WebTarget target = target().path("session");
    final FormDataMultiPart mp = new FormDataMultiPart();
    final LensConf sessionconf = new LensConf();

    String jarFileName = TestResourceFile.TEST_AUX_JAR.getValue();
    File jarFile = new File(jarFileName);
    FileUtils.touch(jarFile);

    try {
      sessionconf.addProperty(LensConfConstants.AUX_JARS, jarFileName);
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(), "foo"));
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(), "bar"));
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
        sessionconf, mt));

      final LensSessionHandle handle = target.request(mt)
        .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), LensSessionHandle.class);
      Assert.assertNotNull(handle);

      // verify aux jars are loaded
      HiveSessionService service = LensServices.get().getService(SessionService.NAME);
      ClassLoader loader = service.getSession(handle).getSessionState().getConf().getClassLoader();
      boolean found = false;
      for (URL path : ((URLClassLoader) loader).getURLs()) {
        if (path.toString().contains(jarFileName)) {
          found = true;
        }
      }
      Assert.assertTrue(found);

      final WebTarget resourcetarget = target().path("session/resources");
      // list all resources
      StringList listResources = resourcetarget.path("list").queryParam("sessionid", handle).request(mt)
        .get(StringList.class);
      Assert.assertEquals(listResources.getElements().size(), 1);
      Assert.assertTrue(listResources.getElements().get(0).contains(jarFileName));

      // close session
      APIResult result = target.queryParam("sessionid", handle).request(mt).delete(APIResult.class);
      Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    } finally {
      LensServerTestFileUtils.deleteFile(jarFile);
    }
  }

  /**
   * Test wrong auth.
   */
  @Test(dataProvider = "mediaTypeData")
  public void testWrongAuth(MediaType mt) {
    final WebTarget target = target().path("session");
    final FormDataMultiPart mp = new FormDataMultiPart();

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(), "a"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(), "b"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
      new LensConf(), mt));

    final Response handle = target.request(mt).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));
    Assert.assertEquals(handle.getStatus(), 401);
  }

  @Test(dataProvider = "mediaTypeData")
  public void testServerMustRestartOnManualDeletionOfAddedResources(MediaType mt)
    throws IOException, LensException, LenServerTestException {

    /* Begin: Setup */

    /* Add a resource jar to current working directory */
    File jarFile = new File(TestResourceFile.TEST_RESTART_ON_RESOURCE_MOVE_JAR.getValue());
    FileUtils.touch(jarFile);

    /* Add the created resource jar to lens server */
    LensSessionHandle sessionHandle = openSession("foo", "bar", new LensConf(), mt);
    addResource(sessionHandle, "jar", jarFile.getPath(), mt);

    /* Delete resource jar from current working directory */
    LensServerTestFileUtils.deleteFile(jarFile);

    /* End: Setup */

    /* Verification Steps: server should restart without exceptions */
    restartLensServer();
    HiveSessionService service = LensServices.get().getService(SessionService.NAME);
    service.closeSession(sessionHandle);
  }

  private LensSessionHandle openSession(final String userName, final String passwd, final LensConf conf, MediaType mt) {

    final WebTarget target = target().path("session");
    final FormDataMultiPart mp = new FormDataMultiPart();

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(), userName));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(), passwd));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
        conf, mt));

    return target.request(mt).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      LensSessionHandle.class);

  }

  private void addResource(final LensSessionHandle lensSessionHandle, final String resourceType,
    final String resourcePath, MediaType mt) {
    final WebTarget target = target().path("session/resources");
    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionHandle,
      mt));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("type").build(), resourceType));
    mp.bodyPart(
      new FormDataBodyPart(FormDataContentDisposition.name("path").build(), resourcePath));
    APIResult result = target.path("add").request(mt)
      .put(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);

    if (!result.getStatus().equals(Status.SUCCEEDED)) {
      throw new RuntimeException("Could not add resource:" + result);
    }
  }

  @Test(dataProvider = "mediaTypeData")
  public void testOpenSessionWithDatabase(MediaType mt) throws Exception {
    // TEST1 - Check if call with database parameter sets current database
    // Create the test DB
    Hive hive = Hive.get(new HiveConf());
    final String testDbName = TestSessionResource.class.getSimpleName();
    Database testOpenDb = new Database();
    testOpenDb.setName(testDbName);
    hive.createDatabase(testOpenDb, true);

    final WebTarget target = target().path("session");
    final FormDataMultiPart mp = new FormDataMultiPart();

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(), "foo"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(), "bar"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("database").build(), testDbName));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
      new LensConf(), mt));

    final LensSessionHandle handle = target.request(mt).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      LensSessionHandle.class);
    Assert.assertNotNull(handle);

    // Check if DB set in session service.
    HiveSessionService service = LensServices.get().getService(SessionService.NAME);
    LensSessionImpl session = service.getSession(handle);
    Assert.assertEquals(session.getCurrentDatabase(), testDbName, "Expected current DB to be set to " + testDbName);
    APIResult result = target.queryParam("sessionid", handle).request().delete(APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    // TEST 2 - Try set database with invalid db name
    final String invalidDB = testDbName + "_invalid_db";
    final FormDataMultiPart form2 = new FormDataMultiPart();

    form2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(), "foo"));
    form2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(), "bar"));
    form2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("database").build(), invalidDB));
    form2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
      new LensConf(), mt));
    try {
      final LensSessionHandle handle2 = target.request(mt).post(Entity.entity(form2,
          MediaType.MULTIPART_FORM_DATA_TYPE), LensSessionHandle.class);
      Assert.fail("Expected above call to fail with not found exception");
    } catch (NotFoundException nfe) {
      // PASS
    }
  }

  /**
   * Test acquire and release behaviour for closed sessions
   */
  @Test(dataProvider = "mediaTypeData")
  public void testAcquireReleaseClosedSession(MediaType mt) throws Exception {
    HiveSessionService sessionService = LensServices.get().getService(SessionService.NAME);

    LensSessionHandle sessionHandle = sessionService.openSession("foo@localhost", "bar", new HashMap<String, String>());
    Assert.assertNotNull(sessionHandle, "Expected session to be opened");

    sessionService.closeSession(sessionHandle);
    final String sessionIdentifier = sessionHandle.getPublicId().toString();

    try {
      sessionService.acquire(sessionIdentifier);
      // Above statement should throw notfound exception
      Assert.fail("Should not reach here since above statement should have thrown NotFoundException");
    } catch (NotFoundException nfe) {
      // Pass
    } finally {
      // Should not do anything
      sessionService.release(sessionIdentifier);
    }

    // Get session for null handle should throw bad request
    try {
      sessionService.getSession(sessionHandle);
      Assert.fail("Above statement should have thrown erorr");
    } catch (ClientErrorException cee) {
      // PASS
    }
  }

  private FormDataMultiPart getMultiFormData(String username, String password, MediaType mt) {
    final FormDataMultiPart mp = new FormDataMultiPart();

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(), username));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(), password));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
        new LensConf(), mt));
    return mp;
  }

  @Test
  public void testMaxSessionsPerUser() throws Exception {
    HiveSessionService sessionService = LensServices.get().getService(SessionService.NAME);
    HiveConf conf = LensServerConf.getHiveConf();
    Integer maxSessionsLimitPerUser = conf.getInt(LensConfConstants.MAX_SESSIONS_PER_USER,
      LensConfConstants.DEFAULT_MAX_SESSIONS_PER_USER);
    List<LensSessionHandle> sessions = new ArrayList<>();
    try {
      for (int i = 0; i < maxSessionsLimitPerUser; i++) {
        LensSessionHandle sessionHandle = sessionService.openSession("test@localhost", "test",
          new HashMap<String, String>());
        sessions.add(sessionHandle);
        Assert.assertNotNull(sessionHandle);
      }
      try {
        sessionService.openSession("test@localhost", "test", new HashMap<String, String>());
        Assert.fail("Session should not be created as session limit is already reached");
      } catch (LensException le) {
        // Exception expected as max session limit is reached for user
        Assert.assertEquals(le.getErrorCode(), 2004);
      }
      // User should be able to open a new session by closing the one of the existing opened sessions
      sessionService.closeSession(sessions.remove(0));
      LensSessionHandle sessionHandle = sessionService.openSession("test@localhost", "test",
        new HashMap<String, String>());
      sessions.add(sessionHandle);
      Assert.assertNotNull(sessionHandle);
    } finally {
      for (LensSessionHandle sessionHandle : sessions) {
        sessionService.closeSession(sessionHandle);
      }
    }
  }

  @Test(dataProvider = "mediaTypeData")
  public void testSessionLimit(MediaType mt) {
    HiveConf conf = LensServerConf.getHiveConf();
    Integer maxSessionsLimitPerUser = conf.getInt(LensConfConstants.MAX_SESSIONS_PER_USER,
        LensConfConstants.DEFAULT_MAX_SESSIONS_PER_USER);

    List<LensSessionHandle> sessionHandleList = new ArrayList<>();
    try {
      for (int i = 0; i < maxSessionsLimitPerUser; i++) {
        final LensSessionHandle handle = RestAPITestUtil.openSession(target(), "test", "test", mt);
        Assert.assertNotNull(handle);
        sessionHandleList.add(handle);
      }
      try {
        RestAPITestUtil.openSession(target(), "test", "test", mt);

        Assert.fail("Should not open a new session for user: 'test' as user has already "
            + maxSessionsLimitPerUser + "active sessions");
      } catch (ClientErrorException e) {
        Assert.assertEquals(e.getResponse().getStatus(), 429);
      }
      // User should be able to open a new session by closing the one of the existing opened sessions
      RestAPITestUtil.closeSession(target(), sessionHandleList.remove(0), mt);

      LensSessionHandle lensSessionHandle = RestAPITestUtil.openSession(target(), "test", "test", mt);
      Assert.assertNotNull(lensSessionHandle);
      sessionHandleList.add(lensSessionHandle);
    } finally {
      for (LensSessionHandle sessionHandle : sessionHandleList) {
        RestAPITestUtil.closeSession(target(), sessionHandle, mt);
      }
    }
  }

  @Test(dataProvider = "mediaTypeData")
  public void testSessionEvents(MediaType mt) {
    final WebTarget target = target().path("session");
    FormDataMultiPart mp = getMultiFormData("foo", "bar", mt);

    LensSessionHandle lensSessionHandle = target.request(mt).post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), LensSessionHandle.class);
    Assert.assertTrue(lensSessionHandle != null);
    Assert.assertTrue(metricsSvc.getTotalOpenedSessions() >= 1);
    Assert.assertTrue(metricsSvc.getActiveSessions() >= 1);

    LensSessionHandle lensSessionHandle1 = target.request(mt).post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), LensSessionHandle.class);
    Assert.assertTrue(lensSessionHandle1 != null);
    Assert.assertTrue(metricsSvc.getTotalOpenedSessions() >= 2);
    Assert.assertTrue(metricsSvc.getActiveSessions() >= 2);

    APIResult result = target.queryParam("sessionid", lensSessionHandle).request(mt).delete(APIResult.class);
    Assert.assertTrue(metricsSvc.getTotalOpenedSessions() >= 1);
    Assert.assertTrue(metricsSvc.getTotalClosedSessions() >= 1);
    Assert.assertTrue(metricsSvc.getActiveSessions() >= 1);

    result = target.queryParam("sessionid", lensSessionHandle1).request(mt).delete(APIResult.class);
    Assert.assertTrue(metricsSvc.getTotalClosedSessions() >= 2);
  }
}

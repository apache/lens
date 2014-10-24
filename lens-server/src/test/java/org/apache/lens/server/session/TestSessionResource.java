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

import java.net.URL;
import java.net.URLClassLoader;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensException;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.StringList;
import org.apache.lens.api.APIResult.Status;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.session.HiveSessionService;
import org.apache.lens.server.session.SessionApp;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * The Class TestSessionResource.
 */
@Test(groups = "unit-test")
public class TestSessionResource extends LensJerseyTest {

  /*
   * (non-Javadoc)
   * 
   * @see org.glassfish.jersey.test.JerseyTest#setUp()
   */
  @BeforeTest
  public void setUp() throws Exception {
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

  @Override
  protected int getTestPort() {
    return 9000;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.glassfish.jersey.test.JerseyTest#configure()
   */
  @Override
  protected Application configure() {
    return new SessionApp();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.glassfish.jersey.test.JerseyTest#configureClient(org.glassfish.jersey.client.ClientConfig)
   */
  @Override
  protected void configureClient(ClientConfig config) {
    config.register(MultiPartFeature.class);
  }

  /**
   * Test session.
   */
  @Test
  public void testSession() {
    final WebTarget target = target().path("session");
    final FormDataMultiPart mp = new FormDataMultiPart();

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(), "foo"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(), "bar"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
        new LensConf(), MediaType.APPLICATION_XML_TYPE));

    final LensSessionHandle handle = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
        LensSessionHandle.class);
    Assert.assertNotNull(handle);

    // get all session params
    final WebTarget paramtarget = target().path("session/params");
    StringList sessionParams = paramtarget.queryParam("sessionid", handle).request().get(StringList.class);
    System.out.println("Session params:" + sessionParams.getElements());
    Assert.assertTrue(sessionParams.getElements().size() > 1);
    Assert.assertTrue(sessionParams.getElements().contains("lens.session.cluster.user=testlensuser"));
    Assert.assertTrue(sessionParams.getElements().contains("lens.session.loggedin.user=foo"));

    // set hive variable
    FormDataMultiPart setpart = new FormDataMultiPart();
    setpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), handle,
        MediaType.APPLICATION_XML_TYPE));
    setpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("key").build(), "hivevar:myvar"));
    setpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("value").build(), "10"));
    APIResult result = paramtarget.request().put(Entity.entity(setpart, MediaType.MULTIPART_FORM_DATA_TYPE),
        APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    // get myvar session params
    sessionParams = paramtarget.queryParam("sessionid", handle).queryParam("key", "hivevar:myvar").request()
        .get(StringList.class);
    System.out.println("Session params:" + sessionParams.getElements());
    Assert.assertEquals(sessionParams.getElements().size(), 1);
    Assert.assertTrue(sessionParams.getElements().contains("hivevar:myvar=10"));

    // set hive conf
    setpart = new FormDataMultiPart();
    setpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), handle,
        MediaType.APPLICATION_XML_TYPE));
    setpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("key").build(), "hiveconf:my.conf"));
    setpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("value").build(), "myvalue"));
    result = paramtarget.request().put(Entity.entity(setpart, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    // get the my.conf session param
    sessionParams = paramtarget.queryParam("sessionid", handle).queryParam("key", "my.conf").request()
        .get(StringList.class);
    System.out.println("Session params:" + sessionParams.getElements());
    Assert.assertEquals(sessionParams.getElements().size(), 1);
    Assert.assertTrue(sessionParams.getElements().contains("my.conf=myvalue"));

    // get all params verbose
    sessionParams = paramtarget.queryParam("sessionid", handle).queryParam("verbose", true).request()
        .get(StringList.class);
    System.out.println("Session params:" + sessionParams.getElements());
    Assert.assertTrue(sessionParams.getElements().size() > 1);

    // Create another session
    final LensSessionHandle handle2 = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
        LensSessionHandle.class);
    Assert.assertNotNull(handle);

    // get myvar session params on handle2
    try {
      sessionParams = paramtarget.queryParam("sessionid", handle2).queryParam("key", "hivevar:myvar").request()
          .get(StringList.class);
      Assert.fail("Expected 404");
    } catch (NotFoundException ne) {
    }
    // get the my.conf session param on handle2
    try {
      sessionParams = paramtarget.queryParam("sessionid", handle2).queryParam("key", "my.conf").request()
          .get(StringList.class);
      System.out.println("sessionParams:" + sessionParams.getElements());
      Assert.fail("Expected 404");
    } catch (NotFoundException ne) {
    }

    // close session
    result = target.queryParam("sessionid", handle).request().delete(APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    result = target.queryParam("sessionid", handle2).request().delete(APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  /**
   * Test resource.
   */
  @Test
  public void testResource() {
    final WebTarget target = target().path("session");
    final FormDataMultiPart mp = new FormDataMultiPart();

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(), "foo"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(), "bar"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
        new LensConf(), MediaType.APPLICATION_XML_TYPE));

    final LensSessionHandle handle = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
        LensSessionHandle.class);
    Assert.assertNotNull(handle);

    // add a resource
    final WebTarget resourcetarget = target().path("session/resources");
    final FormDataMultiPart mp1 = new FormDataMultiPart();
    mp1.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), handle,
        MediaType.APPLICATION_XML_TYPE));
    mp1.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("type").build(), "file"));
    mp1.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("path").build(),
        "target/test-classes/lens-site.xml"));
    APIResult result = resourcetarget.path("add").request()
        .put(Entity.entity(mp1, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    Assert.assertEquals(result.getStatus(), Status.SUCCEEDED);

    // delete the resource
    final FormDataMultiPart mp2 = new FormDataMultiPart();
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), handle,
        MediaType.APPLICATION_XML_TYPE));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("type").build(), "file"));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("path").build(),
        "target/test-classes/lens-site.xml"));
    result = resourcetarget.path("delete").request()
        .put(Entity.entity(mp2, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    // close session
    result = target.queryParam("sessionid", handle).request().delete(APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  /**
   * Test aux jars.
   *
   * @throws LensException
   *           the lens exception
   */
  @Test
  public void testAuxJars() throws LensException {
    final WebTarget target = target().path("session");
    final FormDataMultiPart mp = new FormDataMultiPart();
    final LensConf sessionconf = new LensConf();

    sessionconf.addProperty(LensConfConstants.AUX_JARS, "test-util/test-aux.jar");
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(), "foo"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(), "bar"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
        sessionconf, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
        new LensConf(), MediaType.APPLICATION_XML_TYPE));

    final LensSessionHandle handle = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
        LensSessionHandle.class);
    Assert.assertNotNull(handle);

    // verify aux jars are loaded
    HiveSessionService service = (HiveSessionService) LensServices.get().getService(HiveSessionService.NAME);
    ClassLoader loader = service.getSession(handle).getSessionState().getConf().getClassLoader();
    boolean found = false;
    for (URL path : ((URLClassLoader) loader).getURLs()) {
      if (path.toString().contains("test-aux.jar")) {
        found = true;
      }
    }
    Assert.assertTrue(found);

    // close session
    APIResult result = target.queryParam("sessionid", handle).request().delete(APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  /**
   * Test wrong auth.
   */
  @Test
  public void testWrongAuth() {
    final WebTarget target = target().path("session");
    final FormDataMultiPart mp = new FormDataMultiPart();

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(), "a"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(), "b"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
        new LensConf(), MediaType.APPLICATION_XML_TYPE));

    final Response handle = target.request().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));
    Assert.assertEquals(handle.getStatus(), 401);
  }
}

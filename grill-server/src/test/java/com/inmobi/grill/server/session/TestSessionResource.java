package com.inmobi.grill.server.session;

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

import java.net.URL;
import java.net.URLClassLoader;

import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.APIResult.Status;
import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.StringList;
import com.inmobi.grill.server.GrillJerseyTest;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.GrillConfConstants;

import org.apache.hadoop.hive.conf.HiveConf;
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

@Test(groups="unit-test")
public class TestSessionResource extends GrillJerseyTest {

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }

  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Override
  protected int getTestPort() {
    return 9000;
  }

  @Override
  protected Application configure() {
    return new SessionApp();
  }

  @Override
  protected void configureClient(ClientConfig config) {
    config.register(MultiPartFeature.class);
  }

  @Test
  public void testSession() {
    final WebTarget target = target().path("session");
    final FormDataMultiPart mp = new FormDataMultiPart();

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(),
        "foo"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(),
        "bar"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
        new GrillConf(),
        MediaType.APPLICATION_XML_TYPE));

    final GrillSessionHandle handle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), GrillSessionHandle.class);
    Assert.assertNotNull(handle);

    // get all session params
    final WebTarget paramtarget = target().path("session/params");
    StringList sessionParams = paramtarget.queryParam("sessionid", handle).request().get(
        StringList.class);
    System.out.println("Session params:" + sessionParams.getElements());
    Assert.assertTrue(sessionParams.getElements().size() > 1);
    Assert.assertTrue(sessionParams.getElements().contains("grill.session.cluster.user=testgrilluser"));
    Assert.assertTrue(sessionParams.getElements().contains("grill.session.loggedin.user=foo"));

    // set hive variable
    FormDataMultiPart setpart = new FormDataMultiPart();
    setpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        handle, MediaType.APPLICATION_XML_TYPE));
    setpart.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("key").build(), "hivevar:myvar"));
    setpart.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("value").build(), "10"));
    APIResult result = paramtarget.request().put(
        Entity.entity(setpart, MediaType.MULTIPART_FORM_DATA_TYPE),
        APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    // get myvar session params
    sessionParams = paramtarget.queryParam("sessionid", handle)
        .queryParam("key", "hivevar:myvar").request().get(
            StringList.class);
    System.out.println("Session params:" + sessionParams.getElements());
    Assert.assertEquals(sessionParams.getElements().size(), 1);
    Assert.assertTrue(sessionParams.getElements().contains("hivevar:myvar=10"));

    // set hive conf
    setpart = new FormDataMultiPart();
    setpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        handle, MediaType.APPLICATION_XML_TYPE));
    setpart.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("key").build(), "hiveconf:my.conf"));
    setpart.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("value").build(), "myvalue"));
    result = paramtarget.request().put(
        Entity.entity(setpart, MediaType.MULTIPART_FORM_DATA_TYPE),
        APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    // get the my.conf session param
    sessionParams = paramtarget.queryParam("sessionid", handle)
        .queryParam("key", "my.conf").request().get(
            StringList.class);
    System.out.println("Session params:" + sessionParams.getElements());
    Assert.assertEquals(sessionParams.getElements().size(), 1);
    Assert.assertTrue(sessionParams.getElements().contains("my.conf=myvalue"));

    // get all params verbose
    sessionParams = paramtarget.queryParam("sessionid", handle)
        .queryParam("verbose", true).request().get(
            StringList.class);
    System.out.println("Session params:" + sessionParams.getElements());
    Assert.assertTrue(sessionParams.getElements().size() > 1);

    // Create another session 
    final GrillSessionHandle handle2 = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), GrillSessionHandle.class);
    Assert.assertNotNull(handle);

    // get myvar session params on handle2
    try {
      sessionParams = paramtarget.queryParam("sessionid", handle2)
          .queryParam("key", "hivevar:myvar").request().get(
              StringList.class);
      Assert.fail("Expected 404");
    } catch (NotFoundException ne) {
    }
    // get the my.conf session param on handle2
    try {
      sessionParams = paramtarget.queryParam("sessionid", handle2)
          .queryParam("key", "my.conf").request().get(
              StringList.class);
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

  @Test
  public void testResource() {
    final WebTarget target = target().path("session");
    final FormDataMultiPart mp = new FormDataMultiPart();

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(),
        "foo"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(),
        "bar"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
        new GrillConf(),
        MediaType.APPLICATION_XML_TYPE));

    final GrillSessionHandle handle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), GrillSessionHandle.class);
    Assert.assertNotNull(handle);

    // add a resource
    final WebTarget resourcetarget = target().path("session/resources");
    final FormDataMultiPart mp1 = new FormDataMultiPart();
    mp1.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        handle, MediaType.APPLICATION_XML_TYPE));
    mp1.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("type").build(),
        "file"));
    mp1.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("path").build(),
        "target/test-classes/grill-site.xml"));
    APIResult result = resourcetarget.path("add").request().put(
        Entity.entity(mp1, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    Assert.assertEquals(result.getStatus(), Status.SUCCEEDED);


    // delete the resource
    final FormDataMultiPart mp2 = new FormDataMultiPart();
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        handle, MediaType.APPLICATION_XML_TYPE));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("type").build(),
        "file"));
    mp2.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("path").build(),
        "target/test-classes/grill-site.xml"));
    result = resourcetarget.path("delete").request().put(
        Entity.entity(mp2, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    // close session
    result = target.queryParam("sessionid", handle).request().delete(APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  @Test
  public void testAuxJars() throws GrillException {
    final WebTarget target = target().path("session");
    final FormDataMultiPart mp = new FormDataMultiPart();
    final GrillConf sessionconf = new GrillConf();

    sessionconf.addProperty(GrillConfConstants.AUX_JARS, "test-util/test-aux.jar");
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(),
        "foo"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(),
        "bar"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
        sessionconf,
        MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
        new GrillConf(),
        MediaType.APPLICATION_XML_TYPE));

    final GrillSessionHandle handle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), GrillSessionHandle.class);
    Assert.assertNotNull(handle);

    // verify aux jars are loaded
    HiveSessionService service = (HiveSessionService)GrillServices.get().getService(HiveSessionService.NAME);
    ClassLoader loader = service.getSession(handle).getSessionState().getConf().getClassLoader();
    boolean found = false;
    for (URL path : ((URLClassLoader)loader).getURLs()) {
      if (path.toString().contains("test-aux.jar")) {
        found = true;
      }
    }
    Assert.assertTrue(found);

    // close session
    APIResult result = target.queryParam("sessionid", handle).request().delete(APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  @Test
  public void testWrongAuth() {
    final WebTarget target = target().path("session");
    final FormDataMultiPart mp = new FormDataMultiPart();

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(),
      "a"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(),
      "b"));
    mp.bodyPart(new FormDataBodyPart(
      FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
      new GrillConf(),
      MediaType.APPLICATION_XML_TYPE));

    final Response handle = target.request().post(
      Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));
    Assert.assertEquals(handle.getStatus(), 401);
  }
}

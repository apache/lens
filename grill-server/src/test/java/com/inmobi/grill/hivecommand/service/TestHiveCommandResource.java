package com.inmobi.grill.hivecommand.service;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.grill.common.APIResult;
import com.inmobi.grill.common.GrillConf;
import com.inmobi.grill.common.GrillSessionHandle;
import com.inmobi.grill.common.StringList;
import com.inmobi.grill.common.APIResult.Status;
import com.inmobi.grill.service.GrillJerseyTest;

public class TestHiveCommandResource extends GrillJerseyTest {

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
    return new CommandApp();
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
        "user1"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(),
        "psword"));
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

    // set a system property
    FormDataMultiPart setpart = new FormDataMultiPart();
    setpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        handle, MediaType.APPLICATION_XML_TYPE));
    setpart.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("key").build(),"system:my.property"));
    setpart.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("value").build(),"myvalue"));
    APIResult result = paramtarget.request().put(
        Entity.entity(setpart, MediaType.MULTIPART_FORM_DATA_TYPE),
        APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    Assert.assertEquals(System.getProperty("my.property"), "myvalue");

    // set hive variable
    setpart = new FormDataMultiPart();
    setpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        handle, MediaType.APPLICATION_XML_TYPE));
    setpart.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("key").build(),"hivevar:myvar"));
    setpart.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("value").build(),"10"));
    result = paramtarget.request().put(
        Entity.entity(setpart, MediaType.MULTIPART_FORM_DATA_TYPE),
        APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    // get myvar session params
    sessionParams = paramtarget.queryParam("sessionid", handle)
        .queryParam("key", "myvar").request().get(
        StringList.class);
    System.out.println("Session params:" + sessionParams.getElements());
    Assert.assertEquals(sessionParams.getElements().size(), 1);
    Assert.assertTrue(sessionParams.getElements().contains("myvar=10"));

    // set hive conf
    setpart = new FormDataMultiPart();
    setpart.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        handle, MediaType.APPLICATION_XML_TYPE));
    setpart.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("key").build(),"my.property"));
    setpart.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("value").build(),"myvalue"));
    result = paramtarget.request().put(
        Entity.entity(setpart, MediaType.MULTIPART_FORM_DATA_TYPE),
        APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    // get the my.property session param
    sessionParams = paramtarget.queryParam("sessionid", handle)
        .queryParam("key", "my.property").request().get(
        StringList.class);
    System.out.println("Session params:" + sessionParams.getElements());
    Assert.assertEquals(sessionParams.getElements().size(), 1);
    Assert.assertTrue(sessionParams.getElements().contains("my.property=myvalue"));

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
    sessionParams = paramtarget.queryParam("sessionid", handle2)
        .queryParam("key", "myvar").request().get(
        StringList.class);
    System.out.println("Session params:" + sessionParams.getElements());
    Assert.assertEquals(sessionParams.getElements().size(), 1);
    Assert.assertTrue(sessionParams.getElements().contains("myvar is undefined"));

    // get the my.property session param on handle2
    sessionParams = paramtarget.queryParam("sessionid", handle2)
        .queryParam("key", "my.property").request().get(
        StringList.class);
    System.out.println("Session params:" + sessionParams.getElements());
    Assert.assertEquals(sessionParams.getElements().size(), 1);
    Assert.assertTrue(sessionParams.getElements().contains("my.property is undefined"));

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
        "user1"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(),
        "psword"));
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
        "test-classes/grill-site.xml"));
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
        "test-classes/grill-site.xml"));
    result = resourcetarget.path("delete").request().put(
        Entity.entity(mp2, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    // close session
    result = target.queryParam("sessionid", handle).request().delete(APIResult.class);
    Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

}

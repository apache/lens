package com.inmobi.grill.query.service;

import java.net.URI;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import com.inmobi.grill.client.api.QueryConf;
import com.inmobi.grill.service.GrillJerseyTest;
import com.inmobi.grill.api.QueryHandle;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.test.JerseyTest;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestQueryService extends GrillJerseyTest {

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }

  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Override
  protected Application configure() {
      return new QueryApp();
  }

  @Override
  protected void configureClient(ClientConfig config) {
      config.register(MultiPartFeature.class);
  }

  @Test
  public void testQuery() {
    final WebTarget target = target().path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        "select name from table"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(),
        "execute"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
            new QueryConf(),
            MediaType.APPLICATION_XML_TYPE));

    final QueryHandle s = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);
    System.out.println("QueryHandle:" + s.getHandleId());
  }

  @Override
  protected int getTestPort() {
    return 8083;
  }
}

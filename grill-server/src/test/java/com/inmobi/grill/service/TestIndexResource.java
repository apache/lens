package com.inmobi.grill.service;

import java.net.URI;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.UriBuilder;

import org.glassfish.jersey.test.JerseyTest;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestIndexResource extends GrillJerseyTest {

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
    return new IndexApp();
  }

  protected int getTestPort() {
    return 8081;
  }

  @Test
  public void testClientStringResponse() {
    WebTarget target = target().path("index");
    System.out.println("target path:" + target.getUri());
    String s = target().path("index").request().get(String.class);
    Assert.assertEquals("Hello World! from grill", s);
  }
}

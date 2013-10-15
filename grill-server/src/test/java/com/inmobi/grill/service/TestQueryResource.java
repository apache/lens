package com.inmobi.grill.service;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestQueryResource {
  private EmbeddedServer server;
  private WebTarget target;

  @BeforeTest
  public void setUp() throws Exception {
    server = new EmbeddedServer();
    server.start();

    Client c = ClientBuilder.newClient();
    target = c.target("http://localhost:8080/");
  }

  @AfterTest
  public void tearDown() throws Exception {
    server.stop();
  }

  /**
   * Test to see that the message "Got it!" is sent in the response.
   */
  //@Test
  public void testGetIt() {
    System.out.println(target.path("queryapi").getUri());
    System.out.println(target.path("queryapi").request().get());
    String responseMsg = target.path("queryapi").request().get(String.class);
    Assert.assertEquals("Hello World!", responseMsg);
  }
}

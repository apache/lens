package com.inmobi.grill.metastore.service;

import java.net.URI;

import javax.ws.rs.client.*;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.MediaType;

import com.inmobi.grill.client.api.APIResult;
import com.inmobi.grill.metastore.model.Database;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.test.JerseyTest;

import static org.testng.Assert.*;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestMetastoreService extends JerseyTest {
  public static final Logger LOG = LogManager.getLogger(TestMetastoreService.class);

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
    BasicConfigurator.configure();
  }

  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Override
  protected URI getBaseUri() {
    return UriBuilder.fromUri(super.getBaseUri()).path("grill-server").build();
  }

  @Override
  protected Application configure() {
    return new MetastoreApp();
  }

  @Override
  protected void configureClient(ClientConfig config) {
    config.register(MultiPartFeature.class);
  }

  @Test
  public void testGetDatabase() throws Exception {
    WebTarget dbTarget = target().path("metastore").path("database");
    Invocation.Builder builder = dbTarget.request(MediaType.APPLICATION_XML);
    Database response = builder.get(Database.class);
    assertEquals(response.getName(), "default");

    // Test JSON
    Database jsonResp = dbTarget.request(MediaType.APPLICATION_JSON).get(Database.class);
    assertEquals(jsonResp.getName(), "default");
  }

  @Test
  public void testSetDatabase() throws Exception {
    WebTarget dbTarget = target().path("metastore").path("database");
    Database db = new Database();
    db.setName("test_db");
    APIResult result = dbTarget.request(MediaType.APPLICATION_XML).put(Entity.xml(db), APIResult.class);
    assertNotNull(result);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    Database current = dbTarget.request(MediaType.APPLICATION_XML).get(Database.class);
    assertEquals(current.getName(), db.getName());
  }

  @Test
  public void testCreateDatabase() throws Exception {
    final String newDb = "new_db";
    WebTarget dbTarget = target().path("metastore").path("database").path(newDb);

    Database db = new Database();
    db.setName(newDb);
    db.setIgnoreIfExisting(true);

    APIResult result = dbTarget.request(MediaType.APPLICATION_XML).put(Entity.xml(db), APIResult.class);
    assertNotNull(result);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    // Create again
    db.setIgnoreIfExisting(false);
    result = dbTarget.request(MediaType.APPLICATION_XML).put(Entity.xml(db), APIResult.class);
    assertEquals(result.getStatus(), APIResult.Status.FAILED);
    LOG.info(">> Result message " + result.getMessage());
  }

  @Test
  public void testDropDatabase() throws Exception {
    final String dbName = "del_db";
    final WebTarget dbTarget = target().path("metastore").path("database").path(dbName);
    final Database db = new Database();
    db.setName(dbName);
    db.setIgnoreIfExisting(true);

    // First create the database
    APIResult create = dbTarget.request(MediaType.APPLICATION_XML).put(Entity.xml(db), APIResult.class);
    assertEquals(create.getStatus(), APIResult.Status.SUCCEEDED);

    // Now drop it
    APIResult drop = dbTarget
      .queryParam("cascade", "true")
      .request(MediaType.APPLICATION_XML).delete(APIResult.class);
    assertEquals(drop.getStatus(), APIResult.Status.SUCCEEDED);
  }
}

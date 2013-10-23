package com.inmobi.grill.metastore.service;

import java.net.URI;
import java.util.*;

import javax.ws.rs.client.*;
import javax.ws.rs.core.*;
import javax.xml.bind.JAXBElement;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import com.inmobi.grill.client.api.APIResult;
import com.inmobi.grill.metastore.model.*;
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
  private ObjectFactory cubeObjectFactory;

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
    BasicConfigurator.configure();
    cubeObjectFactory = new ObjectFactory();
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

    // Drop
    dbTarget.request().delete();
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

  @Test
  public void testGetAllDatabases() throws Exception {
    final String[] dbsToCreate = {"db_1", "db_2", "db_3"};
    final WebTarget dbTarget = target().path("metastore").path("database");

    for (String name : dbsToCreate) {
      Database db = new Database();
      db.setName(name);
      db.setIgnoreIfExisting(true);
      dbTarget.path(name).request(MediaType.APPLICATION_XML).put(Entity.xml(db));
    }


    List<Database> allDbs = target().path("metastore").path("databases")
      .request(MediaType.APPLICATION_JSON)
      .get(new GenericType<List<Database>>() {
      });
    assertEquals(allDbs.size(), 4);

    List<String> actualNames = new ArrayList<String>();
    for (Database db : allDbs) {
      actualNames.add(db.getName());
    }
    List<String> expected = new ArrayList<String>(Arrays.asList(dbsToCreate));
    // Default is always there
    expected.add("default");

    assertEquals(actualNames, expected);

    for (String name : dbsToCreate) {
      dbTarget.path(name).queryParam("cascade", "true").request().delete();
    }

  }

  @Test
  public void testCreateCube() throws Exception {
    GregorianCalendar c = new GregorianCalendar();
    c.setTime(new Date());
    final XMLGregorianCalendar startDate = DatatypeFactory.newInstance().newXMLGregorianCalendar(c);
    c.add(GregorianCalendar.DAY_OF_MONTH, 7);
    final XMLGregorianCalendar endDate = DatatypeFactory.newInstance().newXMLGregorianCalendar(c);


    XCube cube = cubeObjectFactory.createXCube();
    cube.setName("testCube");

    XDimensions xdims = cubeObjectFactory.createXDimensions();

    XDimension xd1 = cubeObjectFactory.createXDimension();
    xd1.setName("dim1");
    xd1.setType("string");
    xd1.setStarttime(startDate);
    xd1.setEndtime(endDate);
    xd1.setCost(10.0);

    XDimension xd2 = cubeObjectFactory.createXDimension();
    xd2.setName("dim2");
    xd2.setType("integer");
    xd2.setStarttime(startDate);
    xd2.setEndtime(endDate);
    xd2.setCost(5.0);

    xdims.getDimension().add(xd1);
    xdims.getDimension().add(xd2);
    cube.setDimensions(xdims);


    XMeasures measures = cubeObjectFactory.createXMeasures();

    XMeasure xm1 = new XMeasure();
    xm1.setName("msr1");
    xm1.setType("double");
    xm1.setCost(10.0);
    xm1.setStarttime(startDate);
    xm1.setEndtime(endDate);
    xm1.setDefaultaggr("sum");

    XMeasure xm2 = new XMeasure();
    xm2.setName("msr2");
    xm2.setType("integer");
    xm2.setCost(10.0);
    xm2.setStarttime(startDate);
    xm2.setEndtime(endDate);
    xm2.setDefaultaggr("max");

    measures.getMeasure().add(xm1);
    measures.getMeasure().add(xm2);
    cube.setMeasures(measures);

    XProperties properties = cubeObjectFactory.createXProperties();
    XProperty xp1 = cubeObjectFactory.createXProperty();
    xp1.setName("foo");
    xp1.setValue("bar");
    properties.getProperty().add(xp1);

    cube.setProperties(properties);

    final WebTarget target = target().path("metastore").path("cubes");
    APIResult result = target.request(MediaType.APPLICATION_XML).post(Entity.xml(cubeObjectFactory.createXCube(cube)), APIResult.class);
    assertNotNull(result);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

}

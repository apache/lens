package com.inmobi.grill.metastore.service;

import java.net.URI;
import java.util.*;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.*;
import javax.ws.rs.core.*;
import javax.xml.bind.JAXBElement;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import com.inmobi.grill.client.api.APIResult;
import com.inmobi.grill.metastore.model.*;
import com.inmobi.grill.service.GrillJerseyTest;

import org.apache.hadoop.hive.ql.cube.metadata.Cube;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import static org.testng.Assert.*;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestMetastoreService extends GrillJerseyTest {
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

  protected int getTestPort() {
    return 8082;
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

  private void createDatabase(String dbName) throws Exception {
    WebTarget dbTarget = target().path("metastore").path("database").path(dbName);

    Database db = new Database();
    db.setName(dbName);
    db.setIgnoreIfExisting(true);

    APIResult result = dbTarget.request(MediaType.APPLICATION_XML).put(Entity.xml(db), APIResult.class);
    assertNotNull(result);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  private void dropDatabase(String dbName) throws Exception {
    WebTarget dbTarget = target().path("metastore").path("database").path(dbName);

    APIResult result = dbTarget.queryParam("cascade", "true")
      .request(MediaType.APPLICATION_XML).delete(APIResult.class);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  private void setCurrentDatabase(String dbName) throws Exception {
    WebTarget dbTarget = target().path("metastore").path("database");
    Database db = new Database();
    db.setName(dbName);
    APIResult result = dbTarget.request(MediaType.APPLICATION_XML).put(Entity.xml(db), APIResult.class);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  private String getCurrentDatabase() throws Exception {
    WebTarget dbTarget = target().path("metastore").path("database");
    Invocation.Builder builder = dbTarget.request(MediaType.APPLICATION_XML);
    Database response = builder.get(Database.class);
    return response.getName();
  }

  private XCube createTestCube(String cubeName) throws Exception {
    GregorianCalendar c = new GregorianCalendar();
    c.setTime(new Date());
    final XMLGregorianCalendar startDate = DatatypeFactory.newInstance().newXMLGregorianCalendar(c);
    c.add(GregorianCalendar.DAY_OF_MONTH, 7);
    final XMLGregorianCalendar endDate = DatatypeFactory.newInstance().newXMLGregorianCalendar(c);


    XCube cube = cubeObjectFactory.createXCube();
    cube.setName(cubeName);
    cube.setWeight(100.0);
    XDimensions xdims = cubeObjectFactory.createXDimensions();

    XDimension xd1 = cubeObjectFactory.createXDimension();
    xd1.setName("dim1");
    xd1.setType("string");
    xd1.setStarttime(startDate);
    // Don't set endtime on this dim to validate null handling on server side
    xd1.setCost(10.0);

    XDimension xd2 = cubeObjectFactory.createXDimension();
    xd2.setName("dim2");
    xd2.setType("int");
    // Don't set start time on this dim to validate null handling on server side
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
    // Don't set start time and end time to validate null handling on server side.
    //xm1.setStarttime(startDate);
    //xm1.setEndtime(endDate);
    xm1.setDefaultaggr("sum");

    XMeasure xm2 = new XMeasure();
    xm2.setName("msr2");
    xm2.setType("int");
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
    return cube;
  }

  @Test
  public void testCreateCube() throws Exception {
    final String DB = "test_create_cube";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);
    try {
      final XCube cube = createTestCube("testCube1");
      final WebTarget target = target().path("metastore").path("cubes");
      APIResult result = target.request(MediaType.APPLICATION_XML).post(Entity.xml(cubeObjectFactory.createXCube(cube)), APIResult.class);
      assertNotNull(result);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    }
    finally {
      dropDatabase(DB);
      setCurrentDatabase(prevDb);
    }
  }

  @Test
  public void testGetCube() throws Exception {
    final String DB = "test_get_cube";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);

    try {
      final XCube cube = createTestCube("testGetCube");
      // Create this cube first
      WebTarget target = target().path("metastore").path("cubes");
      JAXBElement<XCube> element = cubeObjectFactory.createXCube(cube);
      APIResult result =
        target.request(MediaType.APPLICATION_XML).post(Entity.xml(element), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Now get
      target = target().path("metastore").path("cubes").path("testGetCube");
      JAXBElement<XCube> actualElement =
        target.request(MediaType.APPLICATION_XML).get(new GenericType<JAXBElement<XCube>>() {});
      XCube actual = actualElement.getValue();
      assertNotNull(actual);

      assertTrue(cube.getName().equalsIgnoreCase(actual.getName()));
      assertNotNull(actual.getMeasures());
      assertEquals(actual.getMeasures().getMeasure().size(), cube.getMeasures().getMeasure().size());
      assertEquals(actual.getDimensions().getDimension().size(), cube.getDimensions().getDimension().size());
      assertEquals(actual.getWeight(), 100.0d);

    } finally {
      dropDatabase(DB);
      setCurrentDatabase(prevDb);
    }
  }

  @Test
  public void testDropCube() throws Exception {
    final String DB = "test_drop_cube";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);

    try {
      final XCube cube = createTestCube("test_drop_cube");
      // Create this cube first
      WebTarget target = target().path("metastore").path("cubes");
      JAXBElement<XCube> element = cubeObjectFactory.createXCube(cube);
      APIResult result =
        target.request(MediaType.APPLICATION_XML).post(Entity.xml(element), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      target = target().path("metastore").path("cubes").path("test_drop_cube").queryParam("cascade", "true");
      result = target.request(MediaType.APPLICATION_XML).delete(APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Now get should give 404
      try {
        target = target().path("metastore").path("cubes").path("test_drop_cube");
        JAXBElement<XCube> got =
          target.request(MediaType.APPLICATION_XML).get(new GenericType<JAXBElement<XCube>>() {});
        fail("Should have thrown 404");
      } catch (NotFoundException ex) {
        ex.printStackTrace();
      }
    } finally {
      dropDatabase(DB);
      setCurrentDatabase(prevDb);
    }
  }


  @Test
  public void testUpdateCube() throws Exception {
    final String cubeName = "test_update";
    final String DB = "test_update_cube";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);

    try {
      final XCube cube = createTestCube(cubeName);
      // Create this cube first
      WebTarget target = target().path("metastore").path("cubes");
      JAXBElement<XCube> element = cubeObjectFactory.createXCube(cube);
      APIResult result =
        target.request(MediaType.APPLICATION_XML).post(Entity.xml(element), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Update something
      cube.setWeight(200.0);
      // Add a measure and dimension
      XMeasure xm2 = new XMeasure();
      xm2.setName("msr3");
      xm2.setType("double");
      xm2.setCost(20.0);
      xm2.setDefaultaggr("sum");
      cube.getMeasures().getMeasure().add(xm2);

      XDimension xd2 = cubeObjectFactory.createXDimension();
      xd2.setName("dim3");
      xd2.setType("string");
      xd2.setCost(55.0);
      cube.getDimensions().getDimension().add(xd2);

      XProperty xp = new XProperty();
      xp.setName("foo2");
      xp.setValue("bar2");
      cube.getProperties().getProperty().add(xp);


      element = cubeObjectFactory.createXCube(cube);
      result =
        target.request(MediaType.APPLICATION_XML).put(Entity.xml(element), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      JAXBElement<XCube> got =
        target.path(cubeName)
          .request(MediaType.APPLICATION_XML).get(new GenericType<JAXBElement<XCube>>() {});
      XCube actual = got.getValue();
      assertEquals(actual.getWeight(), 200.0);
      assertEquals(actual.getDimensions().getDimension().size(), 3);
      assertEquals(actual.getMeasures().getMeasure().size(), 3);

      Cube hcube = JAXBUtils.hiveCubeFromXCube(actual);
      assertTrue(hcube.getMeasureByName("msr3").getAggregate().equals("sum"));
      assertEquals(hcube.getMeasureByName("msr3").getCost(), 20.0);
      assertNotNull(hcube.getDimensionByName("dim3"));
      assertEquals(hcube.getProperties().get("foo2"), "bar2");

    } finally {
      dropDatabase(DB);
      setCurrentDatabase(prevDb);
    }
  }

  @Test
  public void testCreateDimensionTable() throws Exception {
    final String table = "test_create_dim";
    final String DB = "test_dim_db";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);

    try {
      DimensionTable dt = cubeObjectFactory.createDimensionTable();
      dt.setName(table);
      dt.setWeight(15.0);

      Columns cols = cubeObjectFactory.createColumns();

      Column c1 = cubeObjectFactory.createColumn();
      c1.setName("col1");
      c1.setType("string");
      c1.setComment("Fisrt column");
      cols.getColumns().add(c1);
      Column c2 = cubeObjectFactory.createColumn();
      c2.setName("col2");
      c2.setType("int");
      c2.setComment("Second column");
      cols.getColumns().add(c2);
      dt.setColumns(cols);

      XProperty p1 = cubeObjectFactory.createXProperty();
      p1.setName("foodim");
      p1.setValue("bardim");
      XProperties properties = cubeObjectFactory.createXProperties();
      properties.getProperty().add(p1);
      dt.setProperties(properties);

      DimensionReferences refs = cubeObjectFactory.createDimensionReferences();
      DimensionReference drf = cubeObjectFactory.createDimensionReference();
      drf.setDimensionColumn("col1");
      XTablereference tref1 = cubeObjectFactory.createXTablereference();
      tref1.setDestcolumn("dim2id");
      tref1.setDesttable("dim2");
      XTablereference tref2 = cubeObjectFactory.createXTablereference();
      tref2.setDestcolumn("dim3id");
      tref2.setDesttable("dim3");
      drf.getTableReference().add(tref1);
      drf.getTableReference().add(tref2);
      refs.getReference().add(drf);
      dt.setDimensionsReferences(refs);

      UpdatePeriods periods = cubeObjectFactory.createUpdatePeriods();

      UpdatePeriodElement ue1 = cubeObjectFactory.createUpdatePeriodElement();
      ue1.setUpdatePeriod("HOURLY");

      XStorage xs1 = cubeObjectFactory.createXStorage();
      xs1.setName(table + "_hourly");
      xs1.setCollectionDelimiter(",");
      xs1.setEscapeChar("\\");
      xs1.setFieldDelimiter("");
      xs1.setFieldDelimiter("\t");
      xs1.setInputFormat("SequenceFileInputFormat");
      xs1.setOutputFormat("SequenceFileOutputFormat");
      xs1.setIsCompressed(false);
      xs1.setLineDelimiter("\n");
      xs1.setMapKeyDelimiter("\r");
      xs1.setSerdeClassName("com.inmobi.grill.TestSerde");
      xs1.setPartLocation("/tmp/part");
      xs1.setTableLocation("/tmp/" + table);
      xs1.setTableType("EXTERNAL");

      ue1.setStorageAttr(xs1);
      periods.getUpdatePeriodElement().add(ue1);
      dt.setUpdatePeriods(periods);

      try {
        APIResult result = target()
          .path("metastore")
          .path("dimensions")
          .request(MediaType.APPLICATION_XML)
          .post(Entity.xml(cubeObjectFactory.createDimensionTable(dt)), APIResult.class);
        assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
      } catch (Exception exc) {
        LOG.error(exc);
        throw exc;
      }
    } finally {
      setCurrentDatabase(prevDb);
      dropDatabase(DB);
    }


  }

}

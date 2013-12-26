package com.inmobi.grill.metastore.service;

import java.text.SimpleDateFormat;
import java.util.*;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.*;
import javax.ws.rs.core.*;
import javax.xml.bind.JAXBElement;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import com.inmobi.grill.client.api.APIResult;
import com.inmobi.grill.client.api.APIResult.Status;
import com.inmobi.grill.metastore.model.*;
import com.inmobi.grill.service.GrillJerseyTest;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.cube.metadata.Cube;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import static org.testng.Assert.*;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestMetastoreService extends GrillJerseyTest {
  public static final Logger LOG = LogManager.getLogger(TestMetastoreService.class);
  private ObjectFactory cubeObjectFactory;
  protected String mediaType = MediaType.APPLICATION_XML;

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
  public void testSetDatabase() throws Exception {
    WebTarget dbTarget = target().path("metastore").path("database");
    Database db = new Database();
    db.setName("test_db");
    APIResult result = dbTarget.request(mediaType).put(Entity.xml(db), APIResult.class);
    assertNotNull(result);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    Database current = dbTarget.request(mediaType).get(Database.class);
    assertEquals(current.getName(), db.getName());
  }

  @Test
  public void testCreateDatabase() throws Exception {
    final String newDb = "new_db";
    WebTarget dbTarget = target().path("metastore").path("database").path(newDb);

    Database db = new Database();
    db.setName(newDb);
    db.setIgnoreIfExisting(true);

    APIResult result = dbTarget.request(mediaType).put(Entity.xml(db), APIResult.class);
    assertNotNull(result);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    // Create again
    db.setIgnoreIfExisting(false);
    result = dbTarget.request(mediaType).put(Entity.xml(db), APIResult.class);
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
    APIResult create = dbTarget.request(mediaType).put(Entity.xml(db), APIResult.class);
    assertEquals(create.getStatus(), APIResult.Status.SUCCEEDED);

    // Now drop it
    APIResult drop = dbTarget
      .queryParam("cascade", "true")
      .request(mediaType).delete(APIResult.class);
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
      dbTarget.path(name).request(mediaType).put(Entity.xml(db));
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

    APIResult result = dbTarget.request(mediaType).put(Entity.xml(db), APIResult.class);
    assertNotNull(result);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  private void dropDatabase(String dbName) throws Exception {
    WebTarget dbTarget = target().path("metastore").path("database").path(dbName);

    APIResult result = dbTarget.queryParam("cascade", "true")
      .request(mediaType).delete(APIResult.class);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  private void setCurrentDatabase(String dbName) throws Exception {
    WebTarget dbTarget = target().path("metastore").path("database");
    Database db = new Database();
    db.setName(dbName);
    APIResult result = dbTarget.request(mediaType).put(Entity.xml(db), APIResult.class);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  private String getCurrentDatabase() throws Exception {
    WebTarget dbTarget = target().path("metastore").path("database");
    Invocation.Builder builder = dbTarget.request(mediaType);
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
      APIResult result = target.request(mediaType).post(Entity.xml(cubeObjectFactory.createXCube(cube)), APIResult.class);
      assertNotNull(result);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      StringList cubes = target().path("metastore/cubes").request(mediaType).get(StringList.class);
      boolean foundcube = false;
      for (String c : cubes.getElements()) {
        if (c.equalsIgnoreCase("testCube1")) {
          foundcube = true;
          break;
        }
      }

      assertTrue(foundcube);

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
        target.request(mediaType).post(Entity.xml(element), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Now get
      target = target().path("metastore").path("cubes").path("testGetCube");
      JAXBElement<XCube> actualElement =
        target.request(mediaType).get(new GenericType<JAXBElement<XCube>>() {});
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
        target.request(mediaType).post(Entity.xml(element), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      target = target().path("metastore").path("cubes").path("test_drop_cube").queryParam("cascade", "true");
      result = target.request(mediaType).delete(APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Now get should give 404
      try {
        target = target().path("metastore").path("cubes").path("test_drop_cube");
        JAXBElement<XCube> got =
          target.request(mediaType).get(new GenericType<JAXBElement<XCube>>() {});
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
        target.request(mediaType).post(Entity.xml(element), APIResult.class);
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
      result = target.path(cubeName)
        .request(mediaType).put(Entity.xml(element), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      JAXBElement<XCube> got =
        target.path(cubeName)
          .request(mediaType).get(new GenericType<JAXBElement<XCube>>() {});
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
  
  private DimensionTable createDimTable(String table) {
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

    XStorage xs1 = createXStorage(table + "_hourly");

    ue1.setStorageAttr(xs1);
    periods.getUpdatePeriodElement().add(ue1);
    dt.setUpdatePeriods(periods);
    return dt;
  }
  
  private XStorage createXStorage(String name) {
    XStorage xs1 = cubeObjectFactory.createXStorage();
    xs1.setName(name);
    xs1.setCollectionDelimiter(",");
    xs1.setEscapeChar("\\");
    xs1.setFieldDelimiter("");
    xs1.setFieldDelimiter("\t");
    //xs1.setInputFormat("SequenceFileInputFormat");
    //xs1.setOutputFormat("SequenceFileOutputFormat");
    xs1.setIsCompressed(false);
    xs1.setLineDelimiter("\n");
    xs1.setMapKeyDelimiter("\r");
    xs1.setSerdeClassName("com.inmobi.grill.TestSerde");
    xs1.setPartLocation("/tmp/part");
    xs1.setTableLocation("/tmp/" + name);
    xs1.setTableType("EXTERNAL");
    return xs1;
  }

  @Test
  public void testCreateAndDropDimensionTable() throws Exception {
    final String table = "test_create_dim";
    final String DB = "test_dim_db";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);

    try {
      DimensionTable dt = createDimTable(table);
      try {
        APIResult result = target()
          .path("metastore")
          .path("dimensions")
          .request(mediaType)
          .post(Entity.xml(cubeObjectFactory.createDimensionTable(dt)), APIResult.class);
        assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
      } catch (Exception exc) {
        LOG.error(exc);
        throw exc;
      }

      // Drop the table now
      APIResult result =
        target().path("metastore/dimensions").path(table)
          .queryParam("cascade", "true")
          .request(mediaType).delete(APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Drop again, should get 404 now
      try {
        result = target().path("metastore/dimensions").path(table)
          .queryParam("cascade", "true")
          .request(mediaType).delete(APIResult.class);
        fail("Should have got 404");
      } catch (NotFoundException e404) {
        LOG.info("correct");
      }

    } finally {
      setCurrentDatabase(prevDb);
      dropDatabase(DB);
    }
  }
  
  @Test 
  public void testGetAndUpdateDimensionTable() throws Exception {
  	final String table = "test_get_dim";
    final String DB = "test_get_dim_db";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);
    try {
    	DimensionTable dt1 = createDimTable(table);
    	try {
        APIResult result = target()
          .path("metastore")
          .path("dimensions")
          .request(mediaType)
          .post(Entity.xml(cubeObjectFactory.createDimensionTable(dt1)), APIResult.class);
        assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
      } catch (Exception exc) {
        LOG.error(exc);
        throw exc;
      }
    	
    	JAXBElement<DimensionTable> dtElement = target().path("metastore/dimensions").path(table)
    			.request(mediaType)
    			.get(new GenericType<JAXBElement<DimensionTable>>() {});
    	DimensionTable dt2 = dtElement.getValue();
    	assertTrue (dt1 != dt2);
    	assertEquals(dt2.getName(), table);
    	assertEquals(dt2.getDimensionsReferences().getReference().size(), 
    			dt1.getDimensionsReferences().getReference().size());
    	assertEquals(dt2.getWeight(), dt1.getWeight());
    	Map<String, String> props = JAXBUtils.mapFromXProperties(dt2.getProperties());
    	assertTrue(props.containsKey("foodim"));
    	assertEquals(props.get("foodim"), "bardim");
    	
    	
    	// Update a property
    	props.put("foodim", "bardim1");
    	dt2.setProperties(JAXBUtils.xPropertiesFromMap(props));
    	dt2.setWeight(200.0);
    	// Add a column
    	Column c = cubeObjectFactory.createColumn();
    	c.setName("col3");
    	c.setType("string");
    	c.setComment("Added column");
    	dt2.getColumns().getColumns().add(c);
    	
    	// Update the table
    	APIResult result = target().path("metastore/dimensions")
    			.path(table)
    			.request(mediaType)
    			.put(Entity.xml(cubeObjectFactory.createDimensionTable(dt2)), APIResult.class);
    	assertEquals(result.getStatus(), Status.SUCCEEDED);
    	
    	// Get the updated table
    	JAXBElement<DimensionTable> dtElement2 = target().path("metastore/dimensions").path(table)
    			.request(mediaType)
    			.get(new GenericType<JAXBElement<DimensionTable>>() {});
    	DimensionTable dt3 = dtElement2.getValue();
    	assertEquals(dt3.getWeight(), 200.0);
    	
    	Columns cols = dt3.getColumns();
    	List<Column> colList = cols.getColumns();
    	boolean foundCol = false;
    	for (Column col : colList) {
    		if (col.getName().equals("col3") && col.getType().equals("string") && 
    				"Added column".equalsIgnoreCase(col.getComment())) {
    			foundCol = true;
    			break;
    		}
    	}
    	assertTrue(foundCol);
    	Map<String, String> updProps = JAXBUtils.mapFromXProperties(dt3.getProperties());
    	assertEquals(updProps.get("foodim"), "bardim1");
    	
    	// Drop table
    	result =
          target().path("metastore/dimensions").path(table)
            .queryParam("cascade", "true")
            .request(mediaType).delete(APIResult.class);
        assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    } finally {
    	setCurrentDatabase(prevDb);
    	dropDatabase(DB);
    }
  }
  
  @Test
  public void testGetDimensionStorages() throws Exception {
  	final String table = "test_get_storage";
    final String DB = "test_get_dim_storage_db";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);
    try {
    	DimensionTable dt1 = createDimTable(table);
    	try {
        APIResult result = target()
          .path("metastore")
          .path("dimensions")
          .request(mediaType)
          .post(Entity.xml(cubeObjectFactory.createDimensionTable(dt1)), APIResult.class);
        assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
      } catch (Exception exc) {
        LOG.error(exc);
        throw exc;
      }
    	List storages = target().path("metastore").path("dimensions")
    			.path(table).path("storages")
          .request(mediaType)
          .get(new GenericType<List<JAXBElement<XStorage>>>() {
          });
    	assertEquals(storages.size(), 1);
      Object storage = storages.get(0);
      if ( storage instanceof XStorage) {
        XStorage xs = (XStorage) storages.get(0);
        assertEquals(xs.getName(), table + "_hourly");
      } else {
        JAXBElement<XStorage> jxEl = (JAXBElement<XStorage>) storages.get(0);
        assertEquals(jxEl.getValue().getName(), table + "_hourly");
      }


    } finally {
    	setCurrentDatabase(prevDb);
    	dropDatabase(DB);
    }
  }
  
  @Test
  public void testAddDimensionStorages() throws Exception {
  	final String table = "test_add_storage";
    final String DB = "test_add_dim_storage_db";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);
    try {
    	DimensionTable dt1 = createDimTable(table);
    	
    	try {
        APIResult result = target()
          .path("metastore")
          .path("dimensions")
          .request(mediaType)
          .post(Entity.xml(cubeObjectFactory.createDimensionTable(dt1)), APIResult.class);
        assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
      } catch (Exception exc) {
        LOG.error(exc);
        throw exc;
      }
    	
    	// Add update period
    	UpdatePeriodElement uel = cubeObjectFactory.createUpdatePeriodElement();
    	uel.setUpdatePeriod("DAILY");
    	uel.setStorageAttr(createXStorage("S2"));
    	
    	// Check storage is returned in get storage call
    	APIResult result = target().path("metastore/dimensions").path(table).path("/storages")
    			.request(mediaType)
    			.post(Entity.xml(cubeObjectFactory.createUpdatePeriodElement(uel)), APIResult.class);
    	assertEquals(result.getStatus(), Status.SUCCEEDED);
    	
    	List storages = target().path("metastore").path("dimensions")
    			.path(table).path("storages")
          .request(mediaType)
          .get(new GenericType<List<JAXBElement<XStorage>>>() {
          });
    	assertEquals(storages.size(), 2);
    	
    	boolean foundStorage = false;
    	for (int i = 0; i < storages.size(); i++) {
        Object str = storages.get(0);
        if (str instanceof  XStorage) {
          XStorage xs = (XStorage) storages.get(i);
          if (xs.getName().equalsIgnoreCase("S2")) {
            foundStorage = true;
            break;
          }
        } else {
          JAXBElement<XStorage> xs = (JAXBElement<XStorage>) storages.get(i);
          if (xs.getValue().getName().equalsIgnoreCase("S2")) {
            foundStorage = true;
            break;
          }
        }
    	}
    	assertTrue(foundStorage);
    	
    	// Check get table also contains the storage
    	JAXBElement<DimensionTable> dt = target().path("metastore/dimensions").path(table)
    			.request(mediaType)
    			.get(new GenericType<JAXBElement<DimensionTable>>() {});
    	DimensionTable dimTable = dt.getValue();
    	CubeDimensionTable cdim = JAXBUtils.cubeDimTableFromDimTable(dimTable);
    	assertTrue(cdim.getStorages().contains("S2"));
    	assertEquals(cdim.getSnapshotDumpPeriods().get("S2"), UpdatePeriod.DAILY);
    } finally {
    	setCurrentDatabase(prevDb);
    	dropDatabase(DB);
    }
  }
    
  @Test
  public void testAddDropAllDimStorages() throws Exception {
    final String table = "testAddDropAllDimStorages";
    final String DB = "testAddDropAllDimStorages_db";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);
    try {
      DimensionTable dt1 = createDimTable(table);
      // Add update period
      UpdatePeriodElement uel = cubeObjectFactory.createUpdatePeriodElement();
      uel.setUpdatePeriod("DAILY");
      uel.setStorageAttr(createXStorage("S2"));
      dt1.getUpdatePeriods().getUpdatePeriodElement().add(uel);
      try {
        APIResult result = target()
          .path("metastore")
          .path("dimensions")
          .request(mediaType)
          .post(Entity.xml(cubeObjectFactory.createDimensionTable(dt1)), APIResult.class);
        assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
      } catch (Exception exc) {
        LOG.error(exc);
        throw exc;
      }

      APIResult result = target().path("metastore/dimensions/").path(table).path("storages")
          .request(mediaType)
          .delete(APIResult.class);
      assertEquals(result.getStatus(), Status.SUCCEEDED);

      JAXBElement<DimensionTable> dt = target().path("metastore/dimensions").path(table)
          .request(mediaType)
          .get(new GenericType<JAXBElement<DimensionTable>>() {});
      DimensionTable dimTable = dt.getValue();
      CubeDimensionTable cdim = JAXBUtils.cubeDimTableFromDimTable(dimTable);

      assertTrue(cdim.getSnapshotDumpPeriods() == null || cdim.getSnapshotDumpPeriods().isEmpty());
    } finally {
      setCurrentDatabase(prevDb);
      dropDatabase(DB);
    }
  }
    
  @Test
  public void testDropStorageFromDim() throws Exception {
    final String table = "testDropStorageFromDim";
    final String DB = "testDropStorageFromDim_DB";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);
    try {
      DimensionTable dt1 = createDimTable(table);
      // Add update period
      UpdatePeriodElement uel = cubeObjectFactory.createUpdatePeriodElement();
      uel.setUpdatePeriod("DAILY");
      uel.setStorageAttr(createXStorage("S2"));
      dt1.getUpdatePeriods().getUpdatePeriodElement().add(uel);
      try {
        APIResult result = target()
          .path("metastore")
          .path("dimensions")
          .request(mediaType)
          .post(Entity.xml(cubeObjectFactory.createDimensionTable(dt1)), APIResult.class);
        assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
      } catch (Exception exc) {
        LOG.error(exc);
        throw exc;
      }

      // Test that storage has been created
      XStorage s2 = target().path("/metastore/dimensions/").path(table).path("storages").path("S2")
      .request(mediaType).get(XStorage.class);
      assertNotNull(s2);
      // Get storage API sets only the name of the storage object.
      assertEquals(s2.getName(), "S2");

      APIResult result = target().path("metastore/dimensions/").path(table).path("storages").path("S2")
          .request(mediaType)
          .delete(APIResult.class);
      assertEquals(result.getStatus(), Status.SUCCEEDED);

      JAXBElement<DimensionTable> dt = target().path("metastore/dimensions").path(table)
          .request(mediaType)
          .get(new GenericType<JAXBElement<DimensionTable>>() {});
      DimensionTable dimTable = dt.getValue();

      CubeDimensionTable cdim = JAXBUtils.cubeDimTableFromDimTable(dimTable);
      assertFalse(cdim.getStorages().contains("S2"));
      assertTrue(cdim.getStorages().contains(table+"_hourly"));
      assertEquals(cdim.getSnapshotDumpPeriods().get(table + "_hourly"), UpdatePeriod.HOURLY);

    } finally {
      setCurrentDatabase(prevDb);
      dropDatabase(DB);
    }
  }

  private FactTable createFactTable(String factName, String[] storages, String[] updatePeriods) {
    FactTable f = cubeObjectFactory.createFactTable();
    f.setName(factName);
    f.setWeight(10.0);
    f.setCubeName("testCube");

    Columns cols = cubeObjectFactory.createColumns();
    Column c1 = cubeObjectFactory.createColumn();
    c1.setName("c1");
    c1.setType("string");
    c1.setComment("col1");
    cols.getColumns().add(c1);

    Column c2 = cubeObjectFactory.createColumn();
    c2.setName("c2");
    c2.setType("string");
    c2.setComment("col1");
    cols.getColumns().add(c2);

    f.setColumns(cols);

    Map<String, String> properties = new HashMap<String, String>();
    properties.put("foo", "bar");
    f.setProperties(JAXBUtils.xPropertiesFromMap(properties));

    UpdatePeriods upd = cubeObjectFactory.createUpdatePeriods();

    for (int i = 0; i < storages.length; i++) {
      UpdatePeriodElement uel = cubeObjectFactory.createUpdatePeriodElement();
      XStorage xs = createXStorage(storages[i]);
      Column dt = cubeObjectFactory.createColumn();
      dt.setName("dt");
      dt.setType("string");
      dt.setComment("default partition column for fact");
      xs.getPartCols().add(dt);
      uel.setStorageAttr(xs);
      uel.setUpdatePeriod(updatePeriods[i]);
      upd.getUpdatePeriodElement().add(uel);
    }

    f.setUpdatePeriods(upd);
    return f;
  }
    
  @Test
  public void testCreateFactTable() throws Exception {
    final String table = "testCreateFactTable";
    final String DB = "testCreateFactTable_DB";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);
    try {

      FactTable f = createFactTable(table, new String[] {"S1", "S2"},  new String[] {"HOURLY", "DAILY"});
      // Create the FACT table
      APIResult result = target().path("metastore").path("facts").path(table)
          .request(mediaType)
          .post(Entity.xml(cubeObjectFactory.createFactTable(f)), APIResult.class);

      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Get all fact names, this should contain the fact table
      StringList factNames = target().path("metastore/facts")
        .request(mediaType).get(StringList.class);
      boolean contains = false;
      for (String fn : factNames.getElements()) {
        if (fn.equalsIgnoreCase(table)) {
          contains = true;
          break;
        }
      }
      assertTrue(contains);

      // Get the created table
      JAXBElement<FactTable> gotFactElement = target().path("metastore/facts").path(table)
          .request(mediaType)
          .get(new GenericType<JAXBElement<FactTable>>() {});
      FactTable gotFact = gotFactElement.getValue();
      assertTrue(gotFact.getName().equalsIgnoreCase(table));
      assertEquals(gotFact.getWeight(), 10.0);
      CubeFactTable cf = JAXBUtils.cubeFactFromFactTable(gotFact);

      // Check for a column
      boolean foundC1 = false;
      for (FieldSchema fs : cf.getColumns()) {
        if (fs.getName().equalsIgnoreCase("c1") && fs.getType().equalsIgnoreCase("string")) {
          foundC1 = true;
          break;
        }
      }

      assertTrue(foundC1);
      assertEquals(cf.getProperties().get("foo"), "bar");
      assertTrue(cf.getStorages().contains("S1"));
      assertTrue(cf.getStorages().contains("S2"));
      assertTrue(cf.getUpdatePeriods().get("S1").contains(UpdatePeriod.HOURLY));
      assertTrue(cf.getUpdatePeriods().get("S2").contains(UpdatePeriod.DAILY));

      // Do some changes to test update
      cf.addUpdatePeriod("S2", UpdatePeriod.MONTHLY);
      cf.alterWeight(20.0);
      cf.alterColumn(new FieldSchema("c2", "int", "changed to int"));

      FactTable update = JAXBUtils.factTableFromCubeFactTable(cf);

      // Update
      result = target().path("metastore").path("facts").path(table)
          .request(mediaType)
          .put(Entity.xml(cubeObjectFactory.createFactTable(update)), APIResult.class);
      assertEquals(result.getStatus(), Status.SUCCEEDED);

      // Get the updated table
      gotFactElement = target().path("metastore/facts").path(table)
          .request(mediaType)
          .get(new GenericType<JAXBElement<FactTable>>() {});
      gotFact = gotFactElement.getValue();
      CubeFactTable ucf = JAXBUtils.cubeFactFromFactTable(gotFact);

      assertEquals(ucf.weight(), 20.0);
      assertTrue(ucf.getUpdatePeriods().get("S2").contains(UpdatePeriod.MONTHLY));

      boolean foundC2 = false;
      for (FieldSchema fs : cf.getColumns()) {
        if (fs.getName().equalsIgnoreCase("c2") && fs.getType().equalsIgnoreCase("int")) {
          foundC2 = true;
          break;
        }
      }
      assertTrue(foundC2);

      // Finally, drop the fact table
      result = target().path("metastore").path("facts").path(table)
          .queryParam("cascade", "true")
          .request(mediaType)
          .delete(APIResult.class);

      assertEquals(result.getStatus(), Status.SUCCEEDED);

      // Drop again, this time it should give a 404
      try {
        result = target().path("metastore").path("facts").path(table)
          .queryParam("cascade", "true")
          .request(mediaType)
          .delete(APIResult.class);
        fail("Expected 404");
      } catch (NotFoundException nfe) {
        // PASS
      }
    } finally {
      setCurrentDatabase(prevDb);
      dropDatabase(DB);
    }
  }

  @Test
  public void testFactStorages() throws Exception {
    final String table = "testFactStorages";
    final String DB = "testFactStorages_DB";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);

    try {
      String [] storages = {"S1", "S2"};
      String [] updatePeriods = {"HOURLY", "DAILY"};
      FactTable f = createFactTable(table, storages, updatePeriods);

      APIResult result = target().path("metastore/facts").path(table)
        .request(mediaType)
        .post(Entity.xml(cubeObjectFactory.createFactTable(f)), APIResult.class);

      assertEquals(result.getStatus(), Status.SUCCEEDED);

      // Test get storages
      StringList storageList = target().path("metastore/facts").path(table).path("storages")
        .request(mediaType).get(StringList.class);
      assertEquals(storageList.getElements().size(), 2);
      assertTrue(storageList.getElements().contains("S1"));
      assertTrue(storageList.getElements().contains("S2"));

      XStorage s3 = createXStorage("S3");
      FactStorage fs = cubeObjectFactory.createFactStorage();
      fs.setStorage(createXStorage("S3"));
      StorageUpdatePeriod sup1 = new StorageUpdatePeriod();
      sup1.setUpdatePeriod("HOURLY");

      StorageUpdatePeriod sup2 = new StorageUpdatePeriod();
      sup2.setUpdatePeriod("DAILY");

      StorageUpdatePeriod sup3 = new StorageUpdatePeriod();
      sup3.setUpdatePeriod("MONTHLY");

      fs.getStorageUpdatePeriod().add(sup1);
      fs.getStorageUpdatePeriod().add(sup2);
      fs.getStorageUpdatePeriod().add(sup3);

      result = target().path("metastore/facts").path(table).path("storages")
        .request(mediaType)
        .post(Entity.xml(cubeObjectFactory.createFactStorage(fs)), APIResult.class);
      assertEquals(result.getStatus(), Status.SUCCEEDED);

      // Get the fact storage
      FactStorage got = target().path("metastore/facts").path(table).path("storages/S3")
        .request(mediaType)
        .get(FactStorage.class);
      assertNotNull(got);
      assertEquals(got.getStorage().getName(), "S3");
      Set<String> gotStorages = new HashSet<String>();
      for (StorageUpdatePeriod sup : got.getStorageUpdatePeriod()) {
        gotStorages.add(sup.getUpdatePeriod());
      }

      assertEquals(gotStorages, new HashSet<String>(Arrays.asList("HOURLY", "DAILY", "MONTHLY")));

      // Check new storage is added
      storageList = target().path("metastore/facts").path(table).path("storages")
        .request(mediaType).get(StringList.class);
      assertEquals(storageList.getElements().size(), 3);
      assertTrue(storageList.getElements().contains("S3"));

      // Drop new storage
      result = target().path("metastore/facts").path(table).path("storages").path("S3")
        .request(mediaType).delete(APIResult.class);
      assertEquals(result.getStatus(), Status.SUCCEEDED);

      // Now S3 should not be available
      storageList = null;
      storageList = target().path("metastore/facts").path(table).path("storages")
        .request(mediaType).get(StringList.class);
      assertEquals(storageList.getElements().size(), 2);
      assertFalse(storageList.getElements().contains("S3"));

      // Change update period of S2
      StorageUpdatePeriodList supList = cubeObjectFactory.createStorageUpdatePeriodList();
      StorageUpdatePeriod s2Sup1 = cubeObjectFactory.createStorageUpdatePeriod();
      s2Sup1.setUpdatePeriod("MONTHLY");
      StorageUpdatePeriod s2Sup2 = cubeObjectFactory.createStorageUpdatePeriod();
      s2Sup2.setUpdatePeriod("QUARTERLY");

      supList.getStorageUpdatePeriod().add(s2Sup1);
      supList.getStorageUpdatePeriod().add(s2Sup2);

      APIResult updateResult = target().path("metastore/facts").path(table).path("storages/S2")
        .request(mediaType)
        .put(Entity.xml(cubeObjectFactory.createStorageUpdatePeriodList(supList)), APIResult.class);
      assertEquals(updateResult.getStatus(), Status.SUCCEEDED);

      FactStorage s2 = target().path("metastore/facts").path(table).path("storages/S2")
        .request(mediaType)
        .get(FactStorage.class);
      EnumSet<UpdatePeriod> s2Periods = EnumSet.noneOf(UpdatePeriod.class);
      for (StorageUpdatePeriod sup : s2.getStorageUpdatePeriod()) {
        s2Periods.add(UpdatePeriod.valueOf(sup.getUpdatePeriod().toUpperCase()));
      }

      assertEquals(s2Periods, EnumSet.of(UpdatePeriod.MONTHLY, UpdatePeriod.QUARTERLY));

    } finally {
      setCurrentDatabase(prevDb);
      dropDatabase(DB);
    }
  }

  private XPartition createPartition(Date partDate) {
    XPartition xp = cubeObjectFactory.createXPartition();
    xp.setName("test_part");
    xp.setLocation("/tmp/part/test_part");
    xp.setDataLocation("file:///tmp/part/test_part");

    Map<String, String> partitionSpec = new HashMap<String, String>();
    partitionSpec.put("foo", "bar");
    for (Map.Entry<String, String> e : partitionSpec.entrySet()) {
      PartitionSpec ps = cubeObjectFactory.createPartitionSpec();
      ps.setKey(e.getKey());
      ps.setValue(e.getValue());
      xp.getPartitionSpec().add(ps);
    }

    PartitionTimeStamp pts = cubeObjectFactory.createPartitionTimeStamp();
    pts.setColumn("dt");
    pts.setDate(JAXBUtils.getXMLGregorianCalendar(partDate));
    xp.getPartitionTimeStamp().add(pts);

    StorageUpdatePeriod sup = cubeObjectFactory.createStorageUpdatePeriod();
    sup.setUpdatePeriod("HOURLY");
    xp.setUpdatePeriod(sup);

    return xp;
  }

  @Test
  public void testFactStoragePartitions() throws Exception {
    final String table = "testFactStoragePartitions";
    final String DB = "testFactStoragePartitions_DB";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);

    try {
      String [] storages = {"S1", "S2"};
      String [] updatePeriods = {"HOURLY", "DAILY"};
      FactTable f = createFactTable(table, storages, updatePeriods);

      APIResult result = target().path("metastore/facts").path(table)
        .request(mediaType)
        .post(Entity.xml(cubeObjectFactory.createFactTable(f)), APIResult.class);

      assertEquals(result.getStatus(), Status.SUCCEEDED);

      // Add a partition
      final Date partDate = new Date();
      XPartition xp = createPartition(partDate);
      APIResult partAddResult = target().path("metastore/facts/").path(table).path("storages/S2/partitions")
        .request(mediaType)
        .post(Entity.xml(cubeObjectFactory.createXPartition(xp)), APIResult.class);
      assertEquals(partAddResult.getStatus(), Status.SUCCEEDED);

      JAXBElement<PartitionList> partitionsElement = target().path("metastore/facts").path(table).path("storages/S2/partitions")
        .request(mediaType)
        .get(new GenericType<JAXBElement<PartitionList>>() {});

      PartitionList partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getXPartition().size(), 1);

      // Drop the partitions
      APIResult dropResult = target().path("metastore/facts").path(table).path("storages/S2/partitions")
        .request(mediaType)
        .delete(APIResult.class);

      assertEquals(dropResult.getStatus(), Status.SUCCEEDED);

      // Verify partition was dropped
      partitionsElement = target().path("metastore/facts").path(table).path("storages/S2/partitions")
        .request(mediaType)
        .get(new GenericType<JAXBElement<PartitionList>>() {});

      partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getXPartition().size(), 0);

      // Add again
      partAddResult = target().path("metastore/facts/").path(table).path("storages/S2/partitions")
        .request(mediaType)
        .post(Entity.xml(cubeObjectFactory.createXPartition(xp)), APIResult.class);
      assertEquals(partAddResult.getStatus(), Status.SUCCEEDED);

      // Verify partition was added
      partitionsElement = target().path("metastore/facts").path(table).path("storages/S2/partitions")
        .request(mediaType)
        .get(new GenericType<JAXBElement<PartitionList>>() {});

      partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getXPartition().size(), 1);

      // Drop again by values
      String val[] = new String[] {UpdatePeriod.HOURLY.format().format(partDate)};
      dropResult = target().path("metastore/facts").path(table).path("storages/S2/partition")
        .path(StringUtils.join(val, ","))
        .request(mediaType)
        .delete(APIResult.class);

      assertEquals(dropResult.getStatus(), Status.SUCCEEDED);

      // Verify partition was dropped
      partitionsElement = target().path("metastore/facts").path(table).path("storages/S2/partitions")
        .request(mediaType)
        .get(new GenericType<JAXBElement<PartitionList>>() {});

      partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getXPartition().size(), 0);

    } finally {
      setCurrentDatabase(prevDb);
      dropDatabase(DB);
    }
  }
}

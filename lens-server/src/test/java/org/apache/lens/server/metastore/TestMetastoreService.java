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
package org.apache.lens.server.metastore;

import java.util.*;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.*;
import javax.ws.rs.core.*;
import javax.xml.bind.JAXBElement;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.lens.api.metastore.*;
import org.apache.lens.cube.metadata.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.lens.api.APIResult;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.StringList;
import org.apache.lens.api.APIResult.Status;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.LensTestUtil;
import org.apache.lens.server.metastore.CubeMetastoreServiceImpl;
import org.apache.lens.server.metastore.JAXBUtils;
import org.apache.lens.server.metastore.MetastoreApp;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import static org.testng.Assert.*;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Test(groups="unit-test")
public class TestMetastoreService extends LensJerseyTest {
  public static final Logger LOG = LogManager.getLogger(TestMetastoreService.class);
  private ObjectFactory cubeObjectFactory;
  protected String mediaType = MediaType.APPLICATION_XML;
  protected MediaType medType = MediaType.APPLICATION_XML_TYPE;
  protected String dbPFX = "TestMetastoreService_";
  CubeMetastoreServiceImpl metastoreService;
  LensSessionHandle lensSessionId;

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
    BasicConfigurator.configure();
    cubeObjectFactory = new ObjectFactory();
    metastoreService = (CubeMetastoreServiceImpl)LensServices.get().getService("metastore");
    lensSessionId = metastoreService.openSession("foo", "bar", new HashMap<String, String>());

  }

  @AfterTest
  public void tearDown() throws Exception {
    metastoreService.closeSession(lensSessionId);
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
    WebTarget dbTarget = target().path("metastore").path("databases/current");
    String dbName = "test_set_db";
    try {
      dbTarget.queryParam("sessionid", lensSessionId).request(mediaType).put(Entity.xml(dbName), APIResult.class);
      fail("Should get 404");
    } catch (NotFoundException e) {
      // expected
    }

    // create
    APIResult result = target().path("metastore").path("databases")
        .queryParam("sessionid", lensSessionId).request(mediaType).post(Entity.xml(dbName), APIResult.class);
    assertNotNull(result);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    // set
    result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType).put(Entity.xml(dbName), APIResult.class);
    assertNotNull(result);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    // set without session id, we should get bad request
    try {
      result = dbTarget.request(mediaType).put(Entity.xml(dbName), APIResult.class);
      fail("Should have thrown bad request exception");
    } catch (BadRequestException badReq) {
      // expected
    }

    String current = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType).get(String.class);
    assertEquals(current, dbName);
  }

  @Test
  public void testCreateDatabase() throws Exception {
    final String newDb = dbPFX + "new_db";
    WebTarget dbTarget = target().path("metastore").path("databases");

    APIResult result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType).post(Entity.xml(newDb), APIResult.class);
    assertNotNull(result);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    // Create again
    result = dbTarget.queryParam("sessionid", lensSessionId).queryParam("ignoreIfExisting", false).request(mediaType).post(Entity.xml(newDb), APIResult.class);
    assertEquals(result.getStatus(), APIResult.Status.FAILED);
    LOG.info(">> Result message " + result.getMessage());

    // Drop
    dbTarget.path(newDb).queryParam("sessionid", lensSessionId).request().delete();
  }

  @Test
  public void testDropDatabase() throws Exception {
    final String dbName = dbPFX + "del_db";
    final WebTarget dbTarget = target().path("metastore").path("databases");
    // First create the database
    APIResult create = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType).post(Entity.xml(dbName), APIResult.class);
    assertEquals(create.getStatus(), APIResult.Status.SUCCEEDED);

    // Now drop it
    APIResult drop = dbTarget.path(dbName)
        .queryParam("cascade", "true")
        .queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
    assertEquals(drop.getStatus(), APIResult.Status.SUCCEEDED);
  }

  @Test
  public void testGetAllDatabases() throws Exception {
    final String[] dbsToCreate = {"db_1", "db_2", "db_3"};
    final WebTarget dbTarget = target().path("metastore").path("databases");

    for (String name : dbsToCreate) {
      dbTarget.queryParam("sessionid", lensSessionId).request(mediaType).post(Entity.xml(name));
    }


    StringList allDbs = target().path("metastore").path("databases")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(StringList.class);
    System.out.println("ALL DBs:" + allDbs.getElements());
    assertEquals(allDbs.getElements().size(), 4);

    List<String> expected = new ArrayList<String>(Arrays.asList(dbsToCreate));
    // Default is always there
    expected.add("default");

    assertEquals(allDbs.getElements(), expected);

    for (String name : dbsToCreate) {
      dbTarget.path(name).queryParam("cascade", "true").queryParam("sessionid", lensSessionId).request().delete();
    }
  }

  private void createDatabase(String dbName) throws Exception {
    WebTarget dbTarget = target().path("metastore").path("databases");

    APIResult result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType).post(Entity.xml(dbName), APIResult.class);
    assertNotNull(result);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  private void createStorage(String storageName) throws Exception {
    WebTarget target = target().path("metastore").path("storages");

    XStorage xs = new XStorage();
    xs.setName(storageName);
    xs.setClassname(HDFSStorage.class.getCanonicalName());
    XProperties props = cubeObjectFactory.createXProperties();
    XProperty prop = cubeObjectFactory.createXProperty();
    prop.setName("prop1.name");
    prop.setValue("prop1.value");
    props.getProperties().add(prop);
    xs.setProperties(props);

    APIResult result = target.queryParam("sessionid", lensSessionId).request(mediaType).post(Entity.xml(cubeObjectFactory.createXStorage(xs)), APIResult.class);
    assertNotNull(result);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  private void dropStorage(String storageName) throws Exception {
    WebTarget target = target().path("metastore").path("storages").path(storageName);

    APIResult result = target
        .queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  private void dropDatabase(String dbName) throws Exception {
    WebTarget dbTarget = target().path("metastore").path("databases").path(dbName);

    APIResult result = dbTarget.queryParam("cascade", "true")
        .queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  private void setCurrentDatabase(String dbName) throws Exception {
    WebTarget dbTarget = target().path("metastore").path("databases/current");
    APIResult result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType).put(Entity.xml(dbName), APIResult.class);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  private String getCurrentDatabase() throws Exception {
    WebTarget dbTarget = target().path("metastore").path("databases/current");
    Invocation.Builder builder = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType);
    String response = builder.get(String.class);
    return response;
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
    XDimAttributes xdims = cubeObjectFactory.createXDimAttributes();

    XDimAttribute xd1 = cubeObjectFactory.createXDimAttribute();
    xd1.setName("dim1");
    xd1.setType("string");
    xd1.setDescription("first dimension");
    xd1.setDisplayString("Dimension1");
    xd1.setStartTime(startDate);
    // Don't set endtime on this dim to validate null handling on server side
    xd1.setCost(10.0);

    XDimAttribute xd2 = cubeObjectFactory.createXDimAttribute();
    xd2.setName("dim2");
    xd2.setType("int");
    xd2.setDescription("second dimension");
    xd2.setDisplayString("Dimension2");
    // Don't set start time on this dim to validate null handling on server side
    xd2.setEndTime(endDate);
    xd2.setCost(5.0);

    xdims.getDimAttributes().add(xd1);
    xdims.getDimAttributes().add(xd2);
    cube.setDimAttributes(xdims);


    XMeasures measures = cubeObjectFactory.createXMeasures();

    XMeasure xm1 = new XMeasure();
    xm1.setName("msr1");
    xm1.setType("double");
    xm1.setDescription("first measure");
    xm1.setDisplayString("Measure1");
    xm1.setCost(10.0);
    // Don't set start time and end time to validate null handling on server side.
    //xm1.setStarttime(startDate);
    //xm1.setEndtime(endDate);
    xm1.setDefaultAggr("sum");

    XMeasure xm2 = new XMeasure();
    xm2.setName("msr2");
    xm2.setType("int");
    xm2.setDescription("second measure");
    xm2.setDisplayString("Measure2");
    xm2.setCost(10.0);
    xm2.setStartTime(startDate);
    xm2.setEndTime(endDate);
    xm2.setDefaultAggr("max");

    measures.getMeasures().add(xm1);
    measures.getMeasures().add(xm2);
    cube.setMeasures(measures);

    XExpressions expressions = cubeObjectFactory.createXExpressions();

    XExprColumn xe1 = new XExprColumn();
    xe1.setName("expr1");
    xe1.setType("double");
    xe1.setDescription("first expression");
    xe1.setDisplayString("Expression1");
    xe1.setExpr("msr1/1000");

    expressions.getExpressions().add(xe1);
    cube.setExpressions(expressions);

    XProperties properties = cubeObjectFactory.createXProperties();
    XProperty xp1 = cubeObjectFactory.createXProperty();
    xp1.setName("foo");
    xp1.setValue("bar");
    properties.getProperties().add(xp1);

    cube.setProperties(properties);
    return cube;
  }

  private XCube createDerivedCube(String cubeName, String parent) throws Exception {
    XCube cube = cubeObjectFactory.createXCube();
    cube.setName(cubeName);
    cube.setWeight(50.0);
    XDimAttrNames dimNames = cubeObjectFactory.createXDimAttrNames();
    dimNames.getDimAttrNames().add("dim1");
    cube.setDimAttrNames(dimNames);

    XMeasureNames msrNames = cubeObjectFactory.createXMeasureNames();
    msrNames.getMeasures().add("msr1");
    cube.setMeasureNames(msrNames);

    XProperties properties = cubeObjectFactory.createXProperties();
    XProperty xp1 = cubeObjectFactory.createXProperty();
    xp1.setName("derived.foo");
    xp1.setValue("derived.bar");
    properties.getProperties().add(xp1);

    cube.setProperties(properties);
    cube.setParent(parent);
    cube.setDerived(true);
    return cube;
  }

  @Test
  public void testCreateCube() throws Exception {
    final String DB = dbPFX + "test_create_cube";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);
    try {
      final XCube cube = createTestCube("testCube1");
      final WebTarget target = target().path("metastore").path("cubes");
      APIResult result = null;
      try {
        // first try without a session id
        result = target.request(mediaType).post(Entity.xml(cubeObjectFactory.createXCube(cube)), APIResult.class);
        fail("Should have thrown bad request exception");
      } catch (BadRequestException badReq) {
        // expected
      }
      result = target.queryParam("sessionid", lensSessionId).request(mediaType).post(Entity.xml(cubeObjectFactory.createXCube(cube)), APIResult.class);
      assertNotNull(result);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      StringList cubes = target().path("metastore/cubes").queryParam("sessionid", lensSessionId).request(mediaType).get(StringList.class);
      boolean foundcube = false;
      for (String c : cubes.getElements()) {
        if (c.equalsIgnoreCase("testCube1")) {
          foundcube = true;
          break;
        }
      }

      assertTrue(foundcube);

      // create derived cube
      final XCube dcube = createDerivedCube("testderived", "testCube1");
      result = target.queryParam("sessionid", lensSessionId).request(
          mediaType).post(Entity.xml(cubeObjectFactory.createXCube(dcube)), APIResult.class);
      assertNotNull(result);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      cubes = target().path("metastore/cubes").queryParam("sessionid", lensSessionId).request(mediaType).get(StringList.class);
      boolean foundDcube = false;
      foundcube = false;
      for (String c : cubes.getElements()) {
        if (c.equalsIgnoreCase("testCube1")) {
          foundcube = true;
        }
        if (c.equalsIgnoreCase("testderived")) {
          foundDcube = true;
        }
      }

      assertTrue(foundcube);
      assertTrue(foundDcube);

      // get all base cubes
      cubes = target().path("metastore/cubes").queryParam("sessionid", lensSessionId)
          .queryParam("type", "base").request(mediaType).get(StringList.class);
      foundDcube = false;
      foundcube = false;
      for (String c : cubes.getElements()) {
        if (c.equalsIgnoreCase("testCube1")) {
          foundcube = true;
        }
        if (c.equalsIgnoreCase("testderived")) {
          foundDcube = true;
        }
      }

      assertTrue(foundcube);
      assertFalse(foundDcube);

      // get all derived cubes
      cubes = target().path("metastore/cubes").queryParam("sessionid", lensSessionId)
          .queryParam("type", "derived").request(mediaType).get(StringList.class);
      foundDcube = false;
      foundcube = false;
      for (String c : cubes.getElements()) {
        if (c.equalsIgnoreCase("testCube1")) {
          foundcube = true;
        }
        if (c.equalsIgnoreCase("testderived")) {
          foundDcube = true;
        }
      }

      assertFalse(foundcube);
      assertTrue(foundDcube);

      // Create a non queryable cube
      final XCube qcube = createTestCube("testNoQueryCube");
      XProperty xp = new XProperty();
      xp.setName(MetastoreConstants.CUBE_ALL_FIELDS_QUERIABLE);
      xp.setValue("false");
      qcube.getProperties().getProperties().add(xp);

      result = target.queryParam("sessionid", lensSessionId).request(
          mediaType).post(Entity.xml(cubeObjectFactory.createXCube(qcube)), APIResult.class);
      assertNotNull(result);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // get all cubes
      cubes = target().path("metastore/cubes").queryParam("sessionid", lensSessionId)
          .queryParam("type", "all").request(mediaType).get(StringList.class);
      foundDcube = false;
      foundcube = false;
      boolean foundQCube = false;
      for (String c : cubes.getElements()) {
        if (c.equalsIgnoreCase("testCube1")) {
          foundcube = true;
        }
        if (c.equalsIgnoreCase("testderived")) {
          foundDcube = true;
        }
        if (c.equalsIgnoreCase("testNoQueryCube")) {
          foundQCube = true;
        }
      }

      assertTrue(foundcube);
      assertTrue(foundDcube);
      assertTrue(foundQCube);

      // get queryable cubes
      cubes = target().path("metastore/cubes").queryParam("sessionid", lensSessionId)
          .queryParam("type", "queryable").request(mediaType).get(StringList.class);
      foundDcube = false;
      foundcube = false;
      foundQCube = false;
      for (String c : cubes.getElements()) {
        if (c.equalsIgnoreCase("testCube1")) {
          foundcube = true;
        }
        if (c.equalsIgnoreCase("testderived")) {
          foundDcube = true;
        }
        if (c.equalsIgnoreCase("testNoQueryCube")) {
          foundQCube = true;
        }
      }

      assertTrue(foundcube);
      assertTrue(foundDcube);
      assertFalse(foundQCube);
    }
    finally {
      dropDatabase(DB);
      setCurrentDatabase(prevDb);
    }
  }

  @Test
  public void testGetCube() throws Exception {
    final String DB = dbPFX + "test_get_cube";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);

    try {
      final XCube cube = createTestCube("testGetCube");
      // Create this cube first
      WebTarget target = target().path("metastore").path("cubes");
      JAXBElement<XCube> element = cubeObjectFactory.createXCube(cube);
      APIResult result =
          target.queryParam("sessionid", lensSessionId).request(mediaType).post(Entity.xml(element), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Now get
      target = target().path("metastore").path("cubes").path("testGetCube");
      JAXBElement<XCube> actualElement =
          target.queryParam("sessionid", lensSessionId).request(mediaType).get(new GenericType<JAXBElement<XCube>>() {});
      XCube actual = actualElement.getValue();
      assertNotNull(actual);

      assertTrue(cube.getName().equalsIgnoreCase(actual.getName()));
      assertNotNull(actual.getMeasures());
      assertEquals(actual.getMeasures().getMeasures().size(), cube.getMeasures().getMeasures().size());
      assertEquals(actual.getDimAttributes().getDimAttributes().size(), cube.getDimAttributes().getDimAttributes().size());
      assertEquals(actual.getExpressions().getExpressions().size(), cube.getExpressions().getExpressions().size());
      assertEquals(actual.getWeight(), 100.0d);
      assertFalse(actual.isDerived());
      assertNull(actual.getParent());
      Cube hcube = (Cube) JAXBUtils.hiveCubeFromXCube(actual, null);
      assertNotNull(hcube.getDimAttributeByName("dim1"));
      assertEquals(hcube.getDimAttributeByName("dim1").getDescription(), "first dimension");
      assertEquals(hcube.getDimAttributeByName("dim1").getDisplayString(), "Dimension1");
      assertNotNull(hcube.getMeasureByName("msr1"));
      assertEquals(hcube.getMeasureByName("msr1").getDescription(), "first measure");
      assertEquals(hcube.getMeasureByName("msr1").getDisplayString(), "Measure1");
      assertNotNull(hcube.getExpressionByName("expr1"));
      assertEquals(hcube.getExpressionByName("expr1").getDescription(), "first expression");
      assertEquals(hcube.getExpressionByName("expr1").getDisplayString(), "Expression1");

      final XCube dcube = createDerivedCube("testGetDerivedCube", "testGetCube");
      target = target().path("metastore").path("cubes");
      // Create this cube first
      element = cubeObjectFactory.createXCube(dcube);
      result =
          target.queryParam("sessionid", lensSessionId).request(mediaType).post(Entity.xml(element), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Now get
      target = target().path("metastore").path("cubes").path("testGetDerivedCube");
      actualElement =
          target.queryParam("sessionid", lensSessionId).request(mediaType).get(new GenericType<JAXBElement<XCube>>() {});
      actual = actualElement.getValue();
      assertNotNull(actual);

      assertTrue(dcube.getName().equalsIgnoreCase(actual.getName()));
      assertTrue(actual.isDerived());
      assertEquals(actual.getParent(), "testGetCube".toLowerCase());
      assertEquals(actual.getWeight(), 50.0d);
      assertEquals(actual.getMeasureNames().getMeasures().size(), dcube.getMeasureNames().getMeasures().size());
      assertEquals(actual.getDimAttrNames().getDimAttrNames().size(), dcube.getDimAttrNames().getDimAttrNames().size());
    } finally {
      dropDatabase(DB);
      setCurrentDatabase(prevDb);
    }
  }

  @Test
  public void testDropCube() throws Exception {
    final String DB = dbPFX + "test_drop_cube";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);

    try {
      final XCube cube = createTestCube("test_drop_cube");
      // Create this cube first
      WebTarget target = target().path("metastore").path("cubes");
      JAXBElement<XCube> element = cubeObjectFactory.createXCube(cube);
      APIResult result =
          target.queryParam("sessionid", lensSessionId).request(mediaType).post(Entity.xml(element), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      final XCube dcube = createDerivedCube("test_drop_derived_cube", "test_drop_cube");
      target = target().path("metastore").path("cubes");
      // Create this cube first
      element = cubeObjectFactory.createXCube(dcube);
      result =
          target.queryParam("sessionid", lensSessionId).request(mediaType).post(Entity.xml(element), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      target = target().path("metastore").path("cubes").path("test_drop_derived_cube");
      result = target.queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Now get should give 404
      try {
        JAXBElement<XCube> got =
            target.queryParam("sessionid", lensSessionId).request(mediaType).get(new GenericType<JAXBElement<XCube>>() {});
        fail("Should have thrown 404");
      } catch (NotFoundException ex) {
        ex.printStackTrace();
      }

      target = target().path("metastore").path("cubes").path("test_drop_cube");
      result = target.queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Now get should give 404
      try {
        JAXBElement<XCube> got =
            target.queryParam("sessionid", lensSessionId).request(mediaType).get(new GenericType<JAXBElement<XCube>>() {});
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
    final String DB = dbPFX + "test_update_cube";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);

    try {
      final XCube cube = createTestCube(cubeName);
      // Create this cube first
      WebTarget target = target().path("metastore").path("cubes");
      JAXBElement<XCube> element = cubeObjectFactory.createXCube(cube);
      APIResult result =
          target.queryParam("sessionid", lensSessionId).request(mediaType).post(Entity.xml(element), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Update something
      cube.setWeight(200.0);
      // Add a measure and dimension
      XMeasure xm2 = new XMeasure();
      xm2.setName("msr3");
      xm2.setType("double");
      xm2.setCost(20.0);
      xm2.setDefaultAggr("sum");
      cube.getMeasures().getMeasures().add(xm2);

      XDimAttribute xd2 = cubeObjectFactory.createXDimAttribute();
      xd2.setName("dim3");
      xd2.setType("string");
      xd2.setCost(55.0);
      cube.getDimAttributes().getDimAttributes().add(xd2);

      XProperty xp = new XProperty();
      xp.setName("foo2");
      xp.setValue("bar2");
      cube.getProperties().getProperties().add(xp);


      element = cubeObjectFactory.createXCube(cube);
      result = target.path(cubeName)
          .queryParam("sessionid", lensSessionId).request(mediaType).put(Entity.xml(element), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      JAXBElement<XCube> got =
          target.path(cubeName)
          .queryParam("sessionid", lensSessionId).request(mediaType).get(new GenericType<JAXBElement<XCube>>() {});
      XCube actual = got.getValue();
      assertEquals(actual.getWeight(), 200.0);
      assertEquals(actual.getDimAttributes().getDimAttributes().size(), 3);
      assertEquals(actual.getMeasures().getMeasures().size(), 3);

      CubeInterface hcube = JAXBUtils.hiveCubeFromXCube(actual, null);
      assertTrue(hcube instanceof Cube);
      assertTrue(hcube.getMeasureByName("msr3").getAggregate().equals("sum"));
      assertEquals(hcube.getMeasureByName("msr3").getCost(), 20.0);
      assertNotNull(hcube.getDimAttributeByName("dim3"));
      assertEquals(((AbstractCubeTable)hcube).getProperties().get("foo2"), "bar2");

      final XCube dcube = createDerivedCube("test_update_derived", cubeName);
      // Create this cube first
      element = cubeObjectFactory.createXCube(dcube);
      result =
          target.queryParam("sessionid", lensSessionId).request(mediaType).post(Entity.xml(element), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Update something
      dcube.setWeight(80.0);
      // Add a measure and dimension
      dcube.getMeasureNames().getMeasures().add("msr3");
      dcube.getDimAttrNames().getDimAttrNames().add("dim3");

      xp = new XProperty();
      xp.setName("foo.derived2");
      xp.setValue("bar.derived2");
      dcube.getProperties().getProperties().add(xp);


      element = cubeObjectFactory.createXCube(dcube);
      result = target.path("test_update_derived")
          .queryParam("sessionid", lensSessionId).request(mediaType).put(Entity.xml(element), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      got = target.path("test_update_derived")
          .queryParam("sessionid", lensSessionId).request(mediaType).get(new GenericType<JAXBElement<XCube>>() {});
      actual = got.getValue();
      assertEquals(actual.getWeight(), 80.0);
      assertEquals(actual.getDimAttrNames().getDimAttrNames().size(), 2);
      assertEquals(actual.getMeasureNames().getMeasures().size(), 2);
      assertTrue(actual.getMeasureNames().getMeasures().contains("msr3"));
      assertTrue(actual.getDimAttrNames().getDimAttrNames().contains("dim3"));

      CubeInterface hdcube = JAXBUtils.hiveCubeFromXCube(actual, (Cube)hcube);
      assertTrue(hdcube instanceof DerivedCube);
      assertTrue(hdcube.getMeasureByName("msr3").getAggregate().equals("sum"));
      assertEquals(hdcube.getMeasureByName("msr3").getCost(), 20.0);
      assertNotNull(hdcube.getDimAttributeByName("dim3"));
      assertEquals(((AbstractCubeTable)hdcube).getProperties().get("foo.derived2"), "bar.derived2");

    } finally {
      dropDatabase(DB);
      setCurrentDatabase(prevDb);
    }
  }

  @Test
  public void testStorage() throws Exception {
    final String DB = dbPFX + "test_storage";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);
    try {
      createStorage("store1");
      final WebTarget target = target().path("metastore").path("storages");

      StringList storages = target.queryParam("sessionid", lensSessionId).request(mediaType).get(StringList.class);
      boolean foundStorage = false;
      for (String c : storages.getElements()) {
        if (c.equalsIgnoreCase("store1")) {
          foundStorage = true;
          break;
        }
      }

      assertTrue(foundStorage);

      XStorage store1 = target.path("store1").queryParam("sessionid", lensSessionId).request(mediaType).get(XStorage.class);
      assertEquals(store1.getName(), "store1");
      assertEquals(store1.getClassname(), HDFSStorage.class.getCanonicalName());
      assertTrue(store1.getProperties().getProperties().size() >= 1);
      assertTrue(JAXBUtils.mapFromXProperties(store1.getProperties()).containsKey("prop1.name"));
      assertEquals(JAXBUtils.mapFromXProperties(store1.getProperties()).get("prop1.name"), "prop1.value");

      // alter storage
      XProperty prop = cubeObjectFactory.createXProperty();
      prop.setName("prop2.name");
      prop.setValue("prop2.value");
      store1.getProperties().getProperties().add(prop);

      APIResult result = target.path("store1")
          .queryParam("sessionid", lensSessionId).queryParam("storage", "store1")
          .request(mediaType).put(Entity.xml(cubeObjectFactory.createXStorage(store1)), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      store1 = target.path("store1").queryParam("sessionid", lensSessionId).request(mediaType).get(XStorage.class);
      assertEquals(store1.getName(), "store1");
      assertEquals(store1.getClassname(), HDFSStorage.class.getCanonicalName());
      assertTrue(store1.getProperties().getProperties().size() >= 2);
      assertTrue(JAXBUtils.mapFromXProperties(store1.getProperties()).containsKey("prop1.name"));
      assertEquals(JAXBUtils.mapFromXProperties(store1.getProperties()).get("prop1.name"), "prop1.value");
      assertTrue(JAXBUtils.mapFromXProperties(store1.getProperties()).containsKey("prop2.name"));
      assertEquals(JAXBUtils.mapFromXProperties(store1.getProperties()).get("prop2.name"), "prop2.value");

      // drop the storage
      dropStorage("store1");
    }
    finally {
      dropDatabase(DB);
      setCurrentDatabase(prevDb);
    }
  }

  private XStorageTableDesc createStorageTableDesc(String name) {
    XStorageTableDesc xs1 = cubeObjectFactory.createXStorageTableDesc();
    xs1.setCollectionDelimiter(",");
    xs1.setEscapeChar("\\");
    xs1.setFieldDelimiter("");
    xs1.setFieldDelimiter("\t");
    xs1.setLineDelimiter("\n");
    xs1.setMapKeyDelimiter("\r");
    xs1.setTableLocation("/tmp/" + name);
    xs1.setExternal(true);
    Columns partCols = cubeObjectFactory.createColumns();
    Column dt = cubeObjectFactory.createColumn();
    dt.setName("dt");
    dt.setType("string");
    dt.setComment("default partition column");
    partCols.getColumns().add(dt);
    xs1.setPartCols(partCols);
    return xs1;
  }

  private XStorageTableElement createStorageTblElement(String storageName, String table, String... updatePeriod) {
    XStorageTableElement tbl = cubeObjectFactory.createXStorageTableElement();
    tbl.setStorageName(storageName);
    if (updatePeriod != null) {
      for (String p : updatePeriod) {
        tbl.getUpdatePeriods().add(p);
      }
    }
    tbl.setTableDesc(createStorageTableDesc(table));
    return tbl;
  }

  private DimensionTable createDimTable(String dimName, String table) {
    DimensionTable dt = cubeObjectFactory.createDimensionTable();
    dt.setDimName(dimName);
    dt.setTableName(table);
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
    properties.getProperties().add(p1);
    dt.setProperties(properties);

    UpdatePeriods periods = cubeObjectFactory.createUpdatePeriods();
    UpdatePeriodElement ue1 = cubeObjectFactory.createUpdatePeriodElement();
    ue1.setStorageName("test");
    ue1.getUpdatePeriods().add("HOURLY");
    periods.getUpdatePeriodElement().add(ue1);
    dt.setStorageDumpPeriods(periods);
    return dt;
  }

  private DimensionTable createDimTable(String dimTableName) throws Exception {
    DimensionTable dt = createDimTable("testdim", dimTableName);
    XStorageTables storageTables = cubeObjectFactory.createXStorageTables();
    storageTables.getStorageTables().add(createStorageTblElement("test", dimTableName, "HOURLY"));
    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        lensSessionId, medType));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("dimensionTable").fileName("dimtable").build(),
        cubeObjectFactory.createDimensionTable(dt), medType));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("storageTables").fileName("storagetables").build(),
        cubeObjectFactory.createXStorageTables(storageTables), medType));
    APIResult result = target()
        .path("metastore")
        .path("dimtables")
        .request(mediaType)
        .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    return dt;
  }

  private XDimension createDimension(String dimName) throws Exception {
    GregorianCalendar c = new GregorianCalendar();
    c.setTime(new Date());
    final XMLGregorianCalendar startDate = DatatypeFactory.newInstance().newXMLGregorianCalendar(c);
    c.add(GregorianCalendar.DAY_OF_MONTH, 7);
    final XMLGregorianCalendar endDate = DatatypeFactory.newInstance().newXMLGregorianCalendar(c);

    XDimension dimension = cubeObjectFactory.createXDimension();
    dimension.setName(dimName);
    dimension.setWeight(100.0);
    XDimAttributes xdims = cubeObjectFactory.createXDimAttributes();

    XDimAttribute xd1 = cubeObjectFactory.createXDimAttribute();
    xd1.setName("col1");
    xd1.setType("string");
    xd1.setDescription("first column");
    xd1.setDisplayString("Column1");
    xd1.setStartTime(startDate);
    // Don't set endtime on this dim to validate null handling on server side
    xd1.setCost(10.0);

    XDimAttribute xd2 = cubeObjectFactory.createXDimAttribute();
    xd2.setName("col2");
    xd2.setType("int");
    xd2.setDescription("second column");
    xd2.setDisplayString("Column2");
    // Don't set start time on this dim to validate null handling on server side
    xd2.setEndTime(endDate);
    xd2.setCost(5.0);

    xdims.getDimAttributes().add(xd1);
    xdims.getDimAttributes().add(xd2);
    dimension.setAttributes(xdims);

    XExpressions expressions = cubeObjectFactory.createXExpressions();

    XExprColumn xe1 = new XExprColumn();
    xe1.setName("dimexpr");
    xe1.setType("string");
    xe1.setDescription("dimension expression");
    xe1.setDisplayString("Dim Expression");
    xe1.setExpr("substr(col1, 3)");

    expressions.getExpressions().add(xe1);
    dimension.setExpressions(expressions);

    XProperties properties = cubeObjectFactory.createXProperties();
    XProperty xp1 = cubeObjectFactory.createXProperty();
    xp1.setName("dimension.foo");
    xp1.setValue("dim.bar");
    properties.getProperties().add(xp1);

    dimension.setProperties(properties);
    return dimension;
  }

  @Test
  public void testDimension() throws Exception {
    final String DB = dbPFX + "test_dimension";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);
    try {
      XDimension dimension = createDimension("testdim");
      final WebTarget target = target().path("metastore").path("dimensions");

      // create
      APIResult result = target.queryParam("sessionid", lensSessionId).request(
          mediaType).post(Entity.xml(cubeObjectFactory.createXDimension(dimension)), APIResult.class);
      assertNotNull(result);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // getall
      StringList dimensions = target.queryParam("sessionid", lensSessionId).request(mediaType).get(StringList.class);
      boolean foundDim = false;
      for (String c : dimensions.getElements()) {
        if (c.equalsIgnoreCase("testdim")) {
          foundDim = true;
          break;
        }
      }

      assertTrue(foundDim);

      // get
      XDimension testDim = target.path("testdim").queryParam("sessionid", lensSessionId).request(mediaType).get(XDimension.class);
      assertEquals(testDim.getName(), "testdim");
      assertTrue(testDim.getProperties().getProperties().size() >= 1);
      assertTrue(JAXBUtils.mapFromXProperties(testDim.getProperties()).containsKey("dimension.foo"));
      assertEquals(JAXBUtils.mapFromXProperties(testDim.getProperties()).get("dimension.foo"), "dim.bar");
      assertEquals(testDim.getWeight(), 100.0);
      assertEquals(testDim.getAttributes().getDimAttributes().size(), 2);
      assertEquals(testDim.getExpressions().getExpressions().size(), 1);

      Dimension dim = JAXBUtils.dimensionFromXDimension(dimension);
      assertNotNull(dim.getAttributeByName("col1"));
      assertEquals(dim.getAttributeByName("col1").getDescription(), "first column");
      assertEquals(dim.getAttributeByName("col1").getDisplayString(), "Column1");
      assertNotNull(dim.getAttributeByName("col2"));
      assertEquals(dim.getAttributeByName("col2").getDescription(), "second column");
      assertEquals(dim.getAttributeByName("col2").getDisplayString(), "Column2");
      assertNotNull(dim.getExpressionByName("dimexpr"));
      assertEquals(dim.getExpressionByName("dimexpr").getDescription(), "dimension expression");
      assertEquals(dim.getExpressionByName("dimexpr").getDisplayString(), "Dim Expression");

      // alter dimension
      XProperty prop = cubeObjectFactory.createXProperty();
      prop.setName("dim.prop2.name");
      prop.setValue("dim.prop2.value");
      dimension.getProperties().getProperties().add(prop);

      dimension.getAttributes().getDimAttributes().remove(1);
      XDimAttribute xd1 = cubeObjectFactory.createXDimAttribute();
      xd1.setName("col3");
      xd1.setType("string");
      dimension.getAttributes().getDimAttributes().add(xd1);
      dimension.setWeight(200.0);

      result = target.path("testdim")
          .queryParam("sessionid", lensSessionId)
          .request(mediaType).put(Entity.xml(cubeObjectFactory.createXDimension(dimension)), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      testDim = target.path("testdim").queryParam("sessionid", lensSessionId).request(mediaType).get(XDimension.class);
      assertEquals(testDim.getName(), "testdim");
      assertTrue(testDim.getProperties().getProperties().size() >= 2);
      assertTrue(JAXBUtils.mapFromXProperties(testDim.getProperties()).containsKey("dim.prop2.name"));
      assertEquals(JAXBUtils.mapFromXProperties(testDim.getProperties()).get("dim.prop2.name"), "dim.prop2.value");
      assertTrue(JAXBUtils.mapFromXProperties(testDim.getProperties()).containsKey("dimension.foo"));
      assertEquals(JAXBUtils.mapFromXProperties(testDim.getProperties()).get("dimension.foo"), "dim.bar");
      assertEquals(testDim.getWeight(), 200.0);
      assertEquals(testDim.getAttributes().getDimAttributes().size(), 2);

      dim = JAXBUtils.dimensionFromXDimension(testDim);
      System.out.println("Attributes:" + dim.getAttributes());
      assertNotNull(dim.getAttributeByName("col3"));
      assertNull(dim.getAttributeByName("col2"));
      assertNotNull(dim.getAttributeByName("col1"));

      // drop the dimension
      result = target.path("testdim")
          .queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Now get should give 404
      try {
        JAXBElement<XDimension> got =
            target.path("testdim").queryParam("sessionid", lensSessionId).request(
                mediaType).get(new GenericType<JAXBElement<XDimension>>() {});
        fail("Should have thrown 404, but got" + got.getValue().getName());
      } catch (NotFoundException ex) {
        ex.printStackTrace();
      }

      try {
        result = target.path("testdim")
            .queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
        fail("Should have thrown 404, but got" + result.getStatus());
      } catch (NotFoundException ex) {
        ex.printStackTrace();
      }
    }
    finally {
      dropDatabase(DB);
      setCurrentDatabase(prevDb);
    }
  }

  @Test
  public void testCreateAndDropDimensionTable() throws Exception {
    final String table = "test_create_dim";
    final String DB = dbPFX + "test_dim_db";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);
    createStorage("test");

    try {
      createDimTable(table);

      // Drop the table now
      APIResult result =
          target().path("metastore/dimtables").path(table)
          .queryParam("cascade", "true")
          .queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Drop again, should get 404 now
      try {
        result = target().path("metastore/dimtables").path(table)
            .queryParam("cascade", "true")
            .queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
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
    final String DB = dbPFX + "test_get_dim_db";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);
    createStorage("test");

    try {
      DimensionTable dt1 = createDimTable(table);

      JAXBElement<DimensionTable> dtElement = target().path("metastore/dimtables").path(table)
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .get(new GenericType<JAXBElement<DimensionTable>>() {});
      DimensionTable dt2 = dtElement.getValue();
      assertTrue (dt1 != dt2);
      assertEquals(dt2.getDimName(), dt1.getDimName());
      assertEquals(dt2.getTableName(), table);
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
      APIResult result = target().path("metastore/dimtables")
          .path(table)
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .put(Entity.xml(cubeObjectFactory.createDimensionTable(dt2)), APIResult.class);
      assertEquals(result.getStatus(), Status.SUCCEEDED);

      // Get the updated table
      JAXBElement<DimensionTable> dtElement2 = target().path("metastore/dimtables").path(table)
          .queryParam("sessionid", lensSessionId).request(mediaType)
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
          target().path("metastore/dimtables").path(table)
          .queryParam("cascade", "true")
          .queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    } finally {
      setCurrentDatabase(prevDb);
      dropDatabase(DB);
    }
  }

  @Test
  public void testGetDimensionStorages() throws Exception {
    final String table = "test_get_storage";
    final String DB = dbPFX + "test_get_dim_storage_db";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);
    createStorage("test");

    try {
      DimensionTable dt1 = createDimTable(table);
      StringList storages = target().path("metastore").path("dimtables")
          .path(table).path("storages")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .get(StringList.class);
      assertEquals(storages.getElements().size(), 1);
      assertTrue(storages.getElements().contains("test"));
    } finally {
      setCurrentDatabase(prevDb);
      dropDatabase(DB);
    }
  }

  @Test
  public void testAddAndDropDimensionStorages() throws Exception {
    final String table = "test_add_drop_storage";
    final String DB = dbPFX + "test_add_drop_dim_storage_db";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);
    createStorage("test");
    createStorage("test2");
    createStorage("test3");
    try {
      DimensionTable dt1 = createDimTable(table);

      XStorageTableElement sTbl = createStorageTblElement("test2", table, "DAILY");
      APIResult result = target().path("metastore/dimtables").path(table).path("/storages")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .post(Entity.xml(cubeObjectFactory.createXStorageTableElement(sTbl)), APIResult.class);
      assertEquals(result.getStatus(), Status.SUCCEEDED);

      StringList storages = target().path("metastore").path("dimtables")
          .path(table).path("storages")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .get(StringList.class);
      assertEquals(storages.getElements().size(), 2);
      assertTrue(storages.getElements().contains("test"));
      assertTrue(storages.getElements().contains("test2"));

      // Check get table also contains the storage
      JAXBElement<DimensionTable> dt = target().path("metastore/dimtables").path(table)
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .get(new GenericType<JAXBElement<DimensionTable>>() {});
      DimensionTable dimTable = dt.getValue();
      CubeDimensionTable cdim = JAXBUtils.cubeDimTableFromDimTable(dimTable);
      assertTrue(cdim.getStorages().contains("test"));
      assertTrue(cdim.getStorages().contains("test2"));
      assertEquals(cdim.getSnapshotDumpPeriods().get("test2"), UpdatePeriod.DAILY);
      assertEquals(cdim.getSnapshotDumpPeriods().get("test"), UpdatePeriod.HOURLY);

      result = target().path("metastore/dimtables/").path(table).path("storages").path("test")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .delete(APIResult.class);
      assertEquals(result.getStatus(), Status.SUCCEEDED);

      storages = target().path("metastore").path("dimtables")
          .path(table).path("storages")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .get(StringList.class);
      assertEquals(storages.getElements().size(), 1);
      assertFalse(storages.getElements().contains("test"));
      assertTrue(storages.getElements().contains("test2"));

      // Check get table also contains the storage
      dt = target().path("metastore/dimtables").path(table)
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .get(new GenericType<JAXBElement<DimensionTable>>() {});
      dimTable = dt.getValue();
      cdim = JAXBUtils.cubeDimTableFromDimTable(dimTable);
      assertFalse(cdim.getStorages().contains("test"));
      assertTrue(cdim.getStorages().contains("test2"));
      assertEquals(cdim.getSnapshotDumpPeriods().get("test2"), UpdatePeriod.DAILY);

      // add another storage without dump period
      sTbl = createStorageTblElement("test3", table, null);
      result = target().path("metastore/dimtables").path(table).path("/storages")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .post(Entity.xml(cubeObjectFactory.createXStorageTableElement(sTbl)), APIResult.class);
      assertEquals(result.getStatus(), Status.SUCCEEDED);

      storages = target().path("metastore").path("dimtables")
          .path(table).path("storages")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .get(StringList.class);
      assertEquals(storages.getElements().size(), 2);
      assertTrue(storages.getElements().contains("test2"));
      assertTrue(storages.getElements().contains("test3"));

      // Check get table also contains the storage
      dt = target().path("metastore/dimtables").path(table)
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .get(new GenericType<JAXBElement<DimensionTable>>() {});
      dimTable = dt.getValue();
      cdim = JAXBUtils.cubeDimTableFromDimTable(dimTable);
      assertTrue(cdim.getStorages().contains("test2"));
      assertTrue(cdim.getStorages().contains("test3"));
      assertNull(cdim.getSnapshotDumpPeriods().get("test3"));
    } finally {
      setCurrentDatabase(prevDb);
      dropDatabase(DB);
    }
  }

  @Test
  public void testAddDropAllDimStorages() throws Exception {
    final String table = "testAddDropAllDimStorages";
    final String DB = dbPFX + "testAddDropAllDimStorages_db";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);
    createStorage("test");
    createStorage("test2");

    try {
      DimensionTable dt1 = createDimTable(table);
      XStorageTableElement sTbl = createStorageTblElement("test2", table, "DAILY");
      APIResult result = target().path("metastore/dimtables").path(table).path("/storages")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .post(Entity.xml(cubeObjectFactory.createXStorageTableElement(sTbl)), APIResult.class);
      assertEquals(result.getStatus(), Status.SUCCEEDED);

      result = target().path("metastore/dimtables/").path(table).path("storages")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .delete(APIResult.class);
      assertEquals(result.getStatus(), Status.SUCCEEDED);


      JAXBElement<DimensionTable> dt = target().path("metastore/dimtables").path(table)
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .get(new GenericType<JAXBElement<DimensionTable>>() {});
      DimensionTable dimTable = dt.getValue();
      CubeDimensionTable cdim = JAXBUtils.cubeDimTableFromDimTable(dimTable);
      assertTrue(cdim.getStorages().isEmpty());
      assertTrue(cdim.getSnapshotDumpPeriods().isEmpty());
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
      uel.setStorageName(storages[i]);
      uel.getUpdatePeriods().add(updatePeriods[i]);
      upd.getUpdatePeriodElement().add(uel);
    }

    f.setStorageUpdatePeriods(upd);
    return f;
  }

  @Test
  public void testCreateFactTable() throws Exception {
    final String table = "testCreateFactTable";
    final String DB = dbPFX + "testCreateFactTable_DB";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);
    createStorage("S1");
    createStorage("S2");
    try {

      FactTable f = createFactTable(table, new String[] {"S1", "S2"},  new String[] {"HOURLY", "DAILY"});
      XStorageTables storageTables = cubeObjectFactory.createXStorageTables();
      storageTables.getStorageTables().add(createStorageTblElement("S1", table, "HOURLY"));
      storageTables.getStorageTables().add(createStorageTblElement("S2", table, "DAILY"));
      final FormDataMultiPart mp = new FormDataMultiPart();
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
          lensSessionId, medType));
      mp.bodyPart(new FormDataBodyPart(
          FormDataContentDisposition.name("fact").fileName("fact").build(),
          cubeObjectFactory.createFactTable(f), medType));
      mp.bodyPart(new FormDataBodyPart(
          FormDataContentDisposition.name("storageTables").fileName("storagetables").build(),
          cubeObjectFactory.createXStorageTables(storageTables), medType));
      APIResult result = target()
          .path("metastore")
          .path("facts")
          .request(mediaType)
          .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Get all fact names, this should contain the fact table
      StringList factNames = target().path("metastore/facts")
          .queryParam("sessionid", lensSessionId).request(mediaType).get(StringList.class);
      assertTrue(factNames.getElements().contains(table.toLowerCase()));

      // Get the created table
      JAXBElement<FactTable> gotFactElement = target().path("metastore/facts").path(table)
          .queryParam("sessionid", lensSessionId).request(mediaType)
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
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .put(Entity.xml(cubeObjectFactory.createFactTable(update)), APIResult.class);
      assertEquals(result.getStatus(), Status.SUCCEEDED);

      // Get the updated table
      gotFactElement = target().path("metastore/facts").path(table)
          .queryParam("sessionid", lensSessionId).request(mediaType)
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
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .delete(APIResult.class);

      assertEquals(result.getStatus(), Status.SUCCEEDED);

      // Drop again, this time it should give a 404
      try {
        result = target().path("metastore").path("facts").path(table)
            .queryParam("cascade", "true")
            .queryParam("sessionid", lensSessionId).request(mediaType)
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
    final String DB = dbPFX + "testFactStorages_DB";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);
    createStorage("S1");
    createStorage("S2");
    createStorage("S3");

    try {
      String [] storages = {"S1", "S2"};
      String [] updatePeriods = {"HOURLY", "DAILY"};
      FactTable f = createFactTable(table, storages, updatePeriods);
      XStorageTables storageTables = cubeObjectFactory.createXStorageTables();
      storageTables.getStorageTables().add(createStorageTblElement("S1", table, "HOURLY"));
      storageTables.getStorageTables().add(createStorageTblElement("S2", table, "DAILY"));
      final FormDataMultiPart mp = new FormDataMultiPart();
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
          lensSessionId, medType));
      mp.bodyPart(new FormDataBodyPart(
          FormDataContentDisposition.name("fact").fileName("fact").build(),
          cubeObjectFactory.createFactTable(f), medType));
      mp.bodyPart(new FormDataBodyPart(
          FormDataContentDisposition.name("storageTables").fileName("storagetables").build(),
          cubeObjectFactory.createXStorageTables(storageTables), medType));
      APIResult result = target()
          .path("metastore")
          .path("facts")
          .request(mediaType)
          .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Test get storages
      StringList storageList = target().path("metastore/facts").path(table).path("storages")
          .queryParam("sessionid", lensSessionId).request(mediaType).get(StringList.class);
      assertEquals(storageList.getElements().size(), 2);
      assertTrue(storageList.getElements().contains("S1"));
      assertTrue(storageList.getElements().contains("S2"));

      XStorageTableElement sTbl = createStorageTblElement("S3", table, "HOURLY", "DAILY", "MONTHLY");
      result = target().path("metastore/facts").path(table).path("storages")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .post(Entity.xml(cubeObjectFactory.createXStorageTableElement(sTbl)), APIResult.class);
      assertEquals(result.getStatus(), Status.SUCCEEDED);

      // Get the fact storage
      StringList got = target().path("metastore/facts").path(table).path("storages")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .get(StringList.class);
      assertNotNull(got);
      assertEquals(got.getElements().size(), 3);
      assertTrue(got.getElements().contains("S1"));
      assertTrue(got.getElements().contains("S2"));
      assertTrue(got.getElements().contains("S3"));

      JAXBElement<FactTable> gotFactElement = target().path("metastore/facts").path(table)
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .get(new GenericType<JAXBElement<FactTable>>() {});
      FactTable gotFact = gotFactElement.getValue();
      CubeFactTable ucf = JAXBUtils.cubeFactFromFactTable(gotFact);

      assertTrue(ucf.getUpdatePeriods().get("S3").contains(UpdatePeriod.MONTHLY));
      assertTrue(ucf.getUpdatePeriods().get("S3").contains(UpdatePeriod.DAILY));
      assertTrue(ucf.getUpdatePeriods().get("S3").contains(UpdatePeriod.HOURLY));

      // Drop new storage
      result = target().path("metastore/facts").path(table).path("storages").path("S3")
          .queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
      assertEquals(result.getStatus(), Status.SUCCEEDED);

      // Now S3 should not be available
      storageList = null;
      storageList = target().path("metastore/facts").path(table).path("storages")
          .queryParam("sessionid", lensSessionId).request(mediaType).get(StringList.class);
      assertEquals(storageList.getElements().size(), 2);
      assertFalse(storageList.getElements().contains("S3"));
    } finally {
      setCurrentDatabase(prevDb);
      dropDatabase(DB);
    }
  }

  private XPartition createPartition(String cubeTableName, Date partDate) {
    XPartition xp = cubeObjectFactory.createXPartition();
    xp.setLocation("file:///tmp/part/test_part");
    xp.setCubeTableName(cubeTableName);
    XTimePartSpec timeSpec = cubeObjectFactory.createXTimePartSpec();
    XTimePartSpecElement timePart = cubeObjectFactory.createXTimePartSpecElement();
    timePart.setKey("dt");
    timePart.setValue(JAXBUtils.getXMLGregorianCalendar(partDate));
    timeSpec.getPartSpecElement().add(timePart);
    xp.setTimePartitionSpec(timeSpec);
    xp.setUpdatePeriod("HOURLY");
    return xp;
  }

  @Test
  public void testFactStoragePartitions() throws Exception {
    final String table = "testFactStoragePartitions";
    final String DB = dbPFX + "testFactStoragePartitions_DB";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);
    createStorage("S1");
    createStorage("S2");

    try {
      String [] storages = {"S1", "S2"};
      String [] updatePeriods = {"HOURLY", "DAILY"};
      FactTable f = createFactTable(table, storages, updatePeriods);
      XStorageTables storageTables = cubeObjectFactory.createXStorageTables();
      storageTables.getStorageTables().add(createStorageTblElement("S1", table, "HOURLY"));
      storageTables.getStorageTables().add(createStorageTblElement("S2", table, "DAILY"));
      final FormDataMultiPart mp = new FormDataMultiPart();
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
          lensSessionId, medType));
      mp.bodyPart(new FormDataBodyPart(
          FormDataContentDisposition.name("fact").fileName("fact").build(),
          cubeObjectFactory.createFactTable(f), medType));
      mp.bodyPart(new FormDataBodyPart(
          FormDataContentDisposition.name("storageTables").fileName("storagetables").build(),
          cubeObjectFactory.createXStorageTables(storageTables), medType));
      APIResult result = target()
          .path("metastore")
          .path("facts")
          .request(mediaType)
          .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Add a partition
      final Date partDate = new Date();
      XPartition xp = createPartition(table, partDate);
      APIResult partAddResult = target().path("metastore/facts/").path(table).path("storages/S2/partitions")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .post(Entity.xml(cubeObjectFactory.createXPartition(xp)), APIResult.class);
      assertEquals(partAddResult.getStatus(), Status.SUCCEEDED);

      JAXBElement<PartitionList> partitionsElement = target().path("metastore/facts").path(table).path("storages/S2/partitions")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .get(new GenericType<JAXBElement<PartitionList>>() {});

      PartitionList partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getXPartition().size(), 1);

      // Drop the partitions
      APIResult dropResult = target().path("metastore/facts").path(table).path("storages/S2/partitions")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .delete(APIResult.class);

      assertEquals(dropResult.getStatus(), Status.SUCCEEDED);

      // Verify partition was dropped
      partitionsElement = target().path("metastore/facts").path(table).path("storages/S2/partitions")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .get(new GenericType<JAXBElement<PartitionList>>() {});

      partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getXPartition().size(), 0);

      // Add again
      partAddResult = target().path("metastore/facts/").path(table).path("storages/S2/partitions")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .post(Entity.xml(cubeObjectFactory.createXPartition(xp)), APIResult.class);
      assertEquals(partAddResult.getStatus(), Status.SUCCEEDED);

      // Verify partition was added
      partitionsElement = target().path("metastore/facts").path(table).path("storages/S2/partitions")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .get(new GenericType<JAXBElement<PartitionList>>() {});

      partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getXPartition().size(), 1);

      // Drop again by values
      String val[] = new String[] {UpdatePeriod.HOURLY.format().format(partDate)};
      dropResult = target().path("metastore/facts").path(table).path("storages/S2/partition")
          .queryParam("values", StringUtils.join(val, ","))
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .delete(APIResult.class);
      assertEquals(dropResult.getStatus(), Status.SUCCEEDED);

      // Verify partition was dropped
      partitionsElement = target().path("metastore/facts").path(table).path("storages/S2/partitions")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .get(new GenericType<JAXBElement<PartitionList>>() {});

      partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getXPartition().size(), 0);
    } finally {
      setCurrentDatabase(prevDb);
      dropDatabase(DB);
    }
  }

  @Test
  public void testDimStoragePartitions() throws Exception {
    final String table = "testDimStoragePartitions";
    final String DB = dbPFX + "testDimStoragePartitions_DB";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);
    createStorage("S1");
    createStorage("S2");

    try {
      createDimTable(table);
      // Add a partition
      final Date partDate = new Date();
      XPartition xp = createPartition(table, partDate);
      APIResult partAddResult = target().path("metastore/dimtables/").path(table).path("storages/test/partitions")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .post(Entity.xml(cubeObjectFactory.createXPartition(xp)), APIResult.class);
      assertEquals(partAddResult.getStatus(), Status.SUCCEEDED);

      JAXBElement<PartitionList> partitionsElement = target().path("metastore/dimtables").path(table).path("storages/test/partitions")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .get(new GenericType<JAXBElement<PartitionList>>() {});

      PartitionList partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getXPartition().size(), 1);

      // Drop the partitions
      APIResult dropResult = target().path("metastore/dimtables").path(table).path("storages/test/partitions")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .delete(APIResult.class);

      assertEquals(dropResult.getStatus(), Status.SUCCEEDED);

      // Verify partition was dropped
      partitionsElement = target().path("metastore/dimtables").path(table).path("storages/test/partitions")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .get(new GenericType<JAXBElement<PartitionList>>() {});

      partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getXPartition().size(), 0);

      // Add again
      partAddResult = target().path("metastore/dimtables/").path(table).path("storages/test/partitions")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .post(Entity.xml(cubeObjectFactory.createXPartition(xp)), APIResult.class);
      assertEquals(partAddResult.getStatus(), Status.SUCCEEDED);

      // Verify partition was added
      partitionsElement = target().path("metastore/dimtables").path(table).path("storages/test/partitions")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .get(new GenericType<JAXBElement<PartitionList>>() {});

      partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getXPartition().size(), 1);

      // Drop again by values
      String val[] = new String[] {UpdatePeriod.HOURLY.format().format(partDate)};
      dropResult = target().path("metastore/dimtables").path(table).path("storages/test/partition")
          .queryParam("values", StringUtils.join(val, ","))
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .delete(APIResult.class);
      assertEquals(dropResult.getStatus(), Status.SUCCEEDED);

      // Verify partition was dropped
      partitionsElement = target().path("metastore/dimtables").path(table).path("storages/test/partitions")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .get(new GenericType<JAXBElement<PartitionList>>() {});

      partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getXPartition().size(), 0);
    } finally {
      setCurrentDatabase(prevDb);
      dropDatabase(DB);
    }
  }

  @Test
  public void testNativeTables() throws Exception {
    final String DB = dbPFX + "test_native_tables";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);

    try {
      // create hive table
      String tableName = "test_simple_table";
      SessionState.get().setCurrentDatabase(DB);
      LensTestUtil.createHiveTable(tableName);

      WebTarget target = target().path("metastore").path("nativetables");
      // get all native tables
      StringList nativetables = target.queryParam("sessionid", lensSessionId).request(mediaType).get(StringList.class);
      assertEquals(nativetables.getElements().size(), 1);
      assertEquals(nativetables.getElements().get(0), tableName);

      // test current option
      nativetables = target.queryParam("sessionid", lensSessionId)
          .queryParam("dbOption", "current").request(mediaType).get(StringList.class);
      assertEquals(nativetables.getElements().size(), 1);
      assertEquals(nativetables.getElements().get(0), tableName);

      // test all option
      nativetables = target.queryParam("sessionid", lensSessionId)
          .queryParam("dbOption", "all").request(mediaType).get(StringList.class);
      assertTrue(nativetables.getElements().size()>= 1);
      assertTrue(nativetables.getElements().contains(DB.toLowerCase() + "." + tableName));

      // test dbname option
      nativetables = target.queryParam("sessionid", lensSessionId)
          .queryParam("dbName", DB).request(mediaType).get(StringList.class);
      assertEquals(nativetables.getElements().size(), 1);
      assertEquals(nativetables.getElements().get(0), tableName);

      // test dbname option with dboption
      nativetables = target.queryParam("sessionid", lensSessionId).queryParam("dbName", DB)
          .queryParam("dbOption", "current").request(mediaType).get(StringList.class);
      assertEquals(nativetables.getElements().size(), 1);
      assertEquals(nativetables.getElements().get(0), tableName);

      // Now get the table
      JAXBElement<NativeTable> actualElement = target.path(tableName).queryParam(
          "sessionid", lensSessionId).request(mediaType).get(new GenericType<JAXBElement<NativeTable>>() {});
      NativeTable actual = actualElement.getValue();
      assertNotNull(actual);

      assertTrue(tableName.equalsIgnoreCase(actual.getName()));
      assertEquals(actual.getColumns().getColumns().size(), 1);
      assertEquals(actual.getColumns().getColumns().get(0).getName(), "col1");
      assertEquals(actual.getStorageDescriptor().getPartCols().getColumns().size(), 1);
      assertEquals(actual.getStorageDescriptor().getPartCols().getColumns().get(0).getName(), "pcol1");
      assertEquals(actual.getType(), TableType.MANAGED_TABLE.name());
      assertFalse(actual.getStorageDescriptor().isExternal());
      boolean foundProp = false;
      for (XProperty prop : actual.getStorageDescriptor().getTableParameters().getProperties()) {
        if (prop.getName().equals("test.hive.table.prop")) {
          assertTrue(prop.getValue().equals("tvalue"));
          foundProp = true;
        }
      }
      assertTrue(foundProp);

      final XCube cube = createTestCube("testhiveCube");
      // Create a cube
      JAXBElement<XCube> element = cubeObjectFactory.createXCube(cube);
      APIResult result =
          target().path("metastore").path("cubes").queryParam("sessionid",
              lensSessionId).request(mediaType).post(Entity.xml(element), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // get a cube table
      Response response = target.path("testhiveCube").queryParam(
          "sessionid", lensSessionId).request(mediaType).get(Response.class);
      assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

      // get a non existing table
      response = target.path("nonexisting").queryParam(
          "sessionid", lensSessionId).request(mediaType).get(Response.class);
      assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());

      // get all tables in default db
      nativetables = target.queryParam("sessionid", lensSessionId)
          .queryParam("dbName", "default").request(mediaType).get(StringList.class);
      assertNotNull(nativetables);

      // get all tables in non existing db
      response = target.queryParam("sessionid", lensSessionId)
          .queryParam("dbName", "nonexisting").request(mediaType).get(Response.class);
      assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    } finally {
      dropDatabase(DB);
      setCurrentDatabase(prevDb);
    }
  }

  @Test
  public void testFlattenedView() throws Exception {
    final String DB = dbPFX + "test_native_tables";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);

    try {
      XCube flatTestCube = createTestCube("flatTestCube");
      XDimAttributes dimAttrs = flatTestCube.getDimAttributes();

      GregorianCalendar c = new GregorianCalendar();
      c.setTime(new Date());
      final XMLGregorianCalendar startDate = DatatypeFactory.newInstance().newXMLGregorianCalendar(c);

      // Add link from cube to dim1
      XDimAttribute cubeToDim1Ref = cubeObjectFactory.createXDimAttribute();
      cubeToDim1Ref.setName("cubeToDim1Ref");
      cubeToDim1Ref.setType("string");
      cubeToDim1Ref.setDescription("test ref cube to dim1");
      cubeToDim1Ref.setDisplayString("cubeToDim1Ref");
      cubeToDim1Ref.setStartTime(startDate);
      // Don't set endtime on this dim to validate null handling on server side
      cubeToDim1Ref.setCost(10.0);
      XTablereferences xTablereference = cubeObjectFactory.createXTablereferences();
      cubeToDim1Ref.setReferences(xTablereference);
      List<XTablereference> refDimRefs = cubeToDim1Ref.getReferences().getTableReferences();
      XTablereference r = cubeObjectFactory.createXTablereference();
      r.setDestTable("flatTestDim1");
      r.setDestColumn("col1");
      refDimRefs.add(r);
      dimAttrs.getDimAttributes().add(cubeToDim1Ref);

      // Link from dim1 to dim2
      XDimension dim1 = createDimension("flatTestDim1");
      XDimAttributes dim1Attrs = dim1.getAttributes();
      XDimAttribute dim1ToDim2Ref = cubeObjectFactory.createXDimAttribute();
      dim1ToDim2Ref.setName("dim1ToDim2Ref");
      dim1ToDim2Ref.setType("string");
      dim1ToDim2Ref.setDescription("test ref dim1 to dim2");
      dim1ToDim2Ref.setDisplayString("dim1ToDim2Ref");
      dim1ToDim2Ref.setStartTime(startDate);
      // Don't set endtime on this dim to validate null handling on server side
      dim1ToDim2Ref.setCost(10.0);
      XTablereferences xTablereferences2 = cubeObjectFactory.createXTablereferences();
      dim1ToDim2Ref.setReferences(xTablereferences2);
      List<XTablereference> refs = dim1ToDim2Ref.getReferences().getTableReferences();
      XTablereference r2 = cubeObjectFactory.createXTablereference();
      r2.setDestTable("flatTestDim2");
      r2.setDestColumn("col1");
      refs.add(r2);
      dim1Attrs.getDimAttributes().add(dim1ToDim2Ref);

      XDimension dim2 = createDimension("flatTestDim2");

      // Create the tables

      // Create cube
      final WebTarget cubeTarget = target().path("metastore").path("cubes");
      APIResult result =
          cubeTarget.queryParam("sessionid", lensSessionId).request(mediaType)
          .post(Entity.xml(cubeObjectFactory.createXCube(flatTestCube)), APIResult.class);
      assertNotNull(result);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Dim1
      final WebTarget dimTarget = target().path("metastore").path("dimensions");
      result = dimTarget.queryParam("sessionid", lensSessionId).request(
          mediaType).post(Entity.xml(cubeObjectFactory.createXDimension(dim1)), APIResult.class);
      assertNotNull(result);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Dim2
      result = dimTarget.queryParam("sessionid", lensSessionId).request(
          mediaType).post(Entity.xml(cubeObjectFactory.createXDimension(dim2)), APIResult.class);
      assertNotNull(result);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Now test flattened view
      final WebTarget flatCubeTarget = target().path("metastore").path("flattened").path("flattestcube");
      FlattenedColumns flattenedColumns = null;
      try {
        flattenedColumns =
            flatCubeTarget.queryParam("sessionid", lensSessionId).request().get(FlattenedColumns.class);
      } catch (Exception exc) {
        exc.printStackTrace();
        throw exc;
      }
      assertNotNull(flattenedColumns);

      List<Object> columns = flattenedColumns.getMeasureOrExpressionOrDimAttribute();
      assertNotNull(columns);
      assertTrue(!columns.isEmpty());
      int i = 0;

      Set<String> tables = new HashSet<String>();
      Set<String> colSet = new HashSet<String>();
      for (Object colObject : columns) {
        assertTrue(colObject instanceof XMeasure || colObject instanceof XDimAttribute || colObject instanceof XExprColumn);
        if (colObject instanceof XMeasure) {
          tables.add(((XMeasure) colObject).getCubeTable());
          colSet.add(((XMeasure) colObject).getCubeTable() + "." + ((XMeasure) colObject).getName() );
        } else if (colObject instanceof XDimAttribute) {
          tables.add(((XDimAttribute) colObject).getCubeTable());
          colSet.add(((XDimAttribute) colObject).getCubeTable() + "." + ((XDimAttribute) colObject).getName() );
        } else {
          tables.add(((XExprColumn) colObject).getCubeTable());
          colSet.add(((XExprColumn) colObject).getCubeTable() + "." + ((XExprColumn) colObject).getName() );
        }
      }

      assertEquals(tables, new HashSet<String>(Arrays.asList("flattestcube", "flattestdim1", "flattestdim2")));
      assertEquals(colSet,new HashSet<String>(Arrays.asList(
          "flattestcube.msr1",
          "flattestcube.msr2",
          "flattestcube.dim1",
          "flattestcube.dim2",
          "flattestcube.cubetodim1ref",
          "flattestcube.expr1",
          "flattestdim1.dim1todim2ref",
          "flattestdim1.col2",
          "flattestdim1.col1",
          "flattestdim2.col2",
          "flattestdim2.col1")));
    } finally {
      dropDatabase(DB);
      setCurrentDatabase(prevDb);
    }
  }
}

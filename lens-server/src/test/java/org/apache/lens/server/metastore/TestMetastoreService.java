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

import static org.testng.Assert.*;

import java.util.*;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBElement;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.APIResult.Status;
import org.apache.lens.api.DateTime;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.StringList;
import org.apache.lens.api.metastore.*;
import org.apache.lens.cube.metadata.*;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.LensTestUtil;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Test(groups = "unit-test")
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
    metastoreService = (CubeMetastoreServiceImpl) LensServices.get().getService("metastore");
    lensSessionId = metastoreService.openSession("foo", "bar", new HashMap<String, String>());

  }

  @AfterTest
  public void tearDown() throws Exception {
    metastoreService.closeSession(lensSessionId);
    super.tearDown();
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
    result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType)
      .put(Entity.xml(dbName), APIResult.class);
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

    APIResult result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType)
      .post(Entity.xml(newDb), APIResult.class);
    assertNotNull(result);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    // Create again
    result = dbTarget.queryParam("sessionid", lensSessionId).queryParam("ignoreIfExisting", false)
      .request(mediaType).post(Entity.xml(newDb), APIResult.class);
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
    APIResult create = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType)
      .post(Entity.xml(dbName), APIResult.class);
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

    for (String db : dbsToCreate) {
      Assert.assertTrue(allDbs.getElements().contains(db));
    }

    List<String> expected = new ArrayList<String>(Arrays.asList(dbsToCreate));
    // Default is always there
    expected.add("default");

    assertTrue(allDbs.getElements().containsAll(expected));

    for (String name : dbsToCreate) {
      dbTarget.path(name).queryParam("cascade", "true").queryParam("sessionid", lensSessionId).request().delete();
    }
  }

  private void createDatabase(String dbName) throws Exception {
    WebTarget dbTarget = target().path("metastore").path("databases");

    APIResult result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType)
      .post(Entity.xml(dbName), APIResult.class);
    assertNotNull(result);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  private void createStorage(String storageName) throws Exception {
    WebTarget target = target().path("metastore").path("storages");

    XStorage xs = new XStorage();
    xs.setProperties(new XProperties());
    xs.setName(storageName);
    xs.setClassname(HDFSStorage.class.getCanonicalName());
    XProperty prop = cubeObjectFactory.createXProperty();
    prop.setName("prop1.name");
    prop.setValue("prop1.value");
    xs.getProperties().getProperty().add(prop);

    APIResult result = target.queryParam("sessionid", lensSessionId).request(mediaType).post(Entity.xml(
      cubeObjectFactory.createXStorage(xs)), APIResult.class);
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
    APIResult result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType).put(Entity.xml(dbName),
      APIResult.class);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  private String getCurrentDatabase() throws Exception {
    WebTarget dbTarget = target().path("metastore").path("databases/current");
    Invocation.Builder builder = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType);
    String response = builder.get(String.class);
    return response;
  }

  private XBaseCube createTestCube(String cubeName) throws Exception {
    GregorianCalendar c = new GregorianCalendar();
    c.setTime(new Date());
    final XMLGregorianCalendar startDate = DatatypeFactory.newInstance().newXMLGregorianCalendar(c);
    c.add(GregorianCalendar.DAY_OF_MONTH, 7);
    final XMLGregorianCalendar endDate = DatatypeFactory.newInstance().newXMLGregorianCalendar(c);

    XBaseCube cube = cubeObjectFactory.createXBaseCube();
    cube.setName(cubeName);
    cube.setDimAttributes(new XDimAttributes());
    cube.setExpressions(new XExpressions());
    cube.setMeasures(new XMeasures());
    cube.setJoinChains(new XJoinChains());
    cube.setProperties(new XProperties());

    XDimAttribute xd1 = cubeObjectFactory.createXDimAttribute();
    xd1.setName("dim1");
    xd1.setType("STRING");
    xd1.setDescription("first dimension");
    xd1.setDisplayString("Dimension1");
    // Don't set endtime on this dim to validate null handling on server side
    xd1.setStartTime(startDate);

    XDimAttribute xd2 = cubeObjectFactory.createXDimAttribute();
    xd2.setName("dim2");
    xd2.setType("INT");
    xd2.setDescription("second dimension");
    xd2.setDisplayString("Dimension2");
    // Don't set start time on this dim to validate null handling on server side
    xd2.setEndTime(endDate);

    XDimAttribute xd3 = cubeObjectFactory.createXDimAttribute();
    xd3.setName("testdim2col2");
    xd3.setType("STRING");
    xd3.setDescription("ref chained dimension");
    xd3.setDisplayString("Chained Dimension");
    XChainColumn xcc = new XChainColumn();
    xcc.setChainName("chain1");
    xcc.setRefCol("col2");
    xd3.setRefSpec(cubeObjectFactory.createXDimAttributeRefSpec());
    xd3.getRefSpec().setChainRefColumn(xcc);

    // add attribute with complex type
    XDimAttribute xd4 = cubeObjectFactory.createXDimAttribute();
    xd4.setName("dim4");
    xd4.setType("struct<a:INT,b:array<string>,c:map<int,array<struct<x:int,y:array<int>>>");
    xd4.setDescription("complex attribute");
    xd4.setDisplayString("Complex Attribute");

    cube.getDimAttributes().getDimAttribute().add(xd1);
    cube.getDimAttributes().getDimAttribute().add(xd2);
    cube.getDimAttributes().getDimAttribute().add(xd3);
    cube.getDimAttributes().getDimAttribute().add(xd4);

    XMeasure xm1 = new XMeasure();
    xm1.setName("msr1");
    xm1.setType(XMeasureType.DOUBLE);
    xm1.setDescription("first measure");
    xm1.setDisplayString("Measure1");
    // Don't set start time and end time to validate null handling on server side.
    //xm1.setStarttime(startDate);
    //xm1.setEndtime(endDate);
    xm1.setDefaultAggr("sum");

    XMeasure xm2 = new XMeasure();
    xm2.setName("msr2");
    xm2.setType(XMeasureType.INT);
    xm2.setDescription("second measure");
    xm2.setDisplayString("Measure2");
    xm2.setStartTime(startDate);
    xm2.setEndTime(endDate);
    xm2.setDefaultAggr("max");

    cube.getMeasures().getMeasure().add(xm1);
    cube.getMeasures().getMeasure().add(xm2);

    XJoinChain xj1 = new XJoinChain();
    xj1.setName("chain1");
    xj1.setDescription("first chain");
    xj1.setDisplayString("Chain-1");
    xj1.setPaths(new XJoinPaths());
    XJoinPath path1 = cubeObjectFactory.createXJoinPath();
    path1.setEdges(new XJoinEdges());
    XTableReference link1 = new XTableReference();
    link1.setTable(cubeName);
    link1.setColumn("col1");
    XTableReference link2 = new XTableReference();
    link2.setTable("testdim");
    link2.setColumn("col1");
    XJoinEdge edge1 = cubeObjectFactory.createXJoinEdge();
    edge1.setFrom(link1);
    edge1.setTo(link2);
    path1.getEdges().getEdge().add(edge1);
    xj1.getPaths().getPath().add(path1);
    cube.getJoinChains().getJoinChain().add(xj1);

    XJoinChain xj2 = new XJoinChain();
    xj2.setName("dim2chain");
    xj2.setDescription("testdim2 chain");
    xj2.setDisplayString("Chain-2");
    xj2.setPaths(new XJoinPaths());
    XJoinPath path = cubeObjectFactory.createXJoinPath();
    path.setEdges(new XJoinEdges());
    path.getEdges().getEdge().add(edge1);
    XJoinEdge edge2 = cubeObjectFactory.createXJoinEdge();
    XTableReference link3 = new XTableReference();
    link3.setTable("testdim");
    link3.setColumn("col2");
    XTableReference link4 = new XTableReference();
    link4.setTable("testdim2");
    link4.setColumn("col1");
    edge2.setFrom(link3);
    edge2.setTo(link4);
    path.getEdges().getEdge().add(edge2);
    xj2.getPaths().getPath().add(path);
    cube.getJoinChains().getJoinChain().add(xj2);

    XExprColumn xe1 = new XExprColumn();
    xe1.setName("expr1");
    xe1.setType("DOUBLE");
    xe1.setDescription("first expression");
    xe1.setDisplayString("Expression1");
    xe1.setExpr("msr1/1000");

    cube.getExpressions().getExpression().add(xe1);

    XProperty xp1 = cubeObjectFactory.createXProperty();
    xp1.setName("foo");
    xp1.setValue("bar");
    cube.getProperties().getProperty().add(xp1);
    return cube;
  }

  private XDerivedCube createDerivedCube(String cubeName, String parent) throws Exception {
    XDerivedCube cube = cubeObjectFactory.createXDerivedCube();
    cube.setName(cubeName);
    cube.setDimAttrNames(new XDimAttrNames());
    cube.setMeasureNames(new XMeasureNames());
    cube.setProperties(new XProperties());

    cube.getDimAttrNames().getAttrName().add("dim1");
    cube.getMeasureNames().getMeasureName().add("msr1");

    XProperty xp1 = cubeObjectFactory.createXProperty();
    xp1.setName("derived.foo");
    xp1.setValue("derived.bar");
    cube.getProperties().getProperty().add(xp1);

    cube.setParent(parent);
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
      result = target.queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.xml(cubeObjectFactory.createXCube(cube)), APIResult.class);
      assertNotNull(result);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      StringList cubes = target().path("metastore/cubes").queryParam("sessionid", lensSessionId).request(mediaType)
        .get(StringList.class);
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

      cubes = target().path("metastore/cubes").queryParam("sessionid", lensSessionId).request(mediaType)
        .get(StringList.class);
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
      qcube.getProperties().getProperty().add(xp);

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
    } finally {
      dropDatabase(DB);
      setCurrentDatabase(prevDb);
    }
  }

  @Test
  public void testMeasureJaxBConversion() throws Exception {
    CubeMeasure cubeMeasure =
      new ColumnMeasure(new FieldSchema("msr1", "int", "first measure"), null, null, null, null, null, null, null,
        0.0, 9999.0);
    XMeasure measure = JAXBUtils.xMeasureFromHiveMeasure(cubeMeasure);
    CubeMeasure actualMeasure = JAXBUtils.hiveMeasureFromXMeasure(measure);
    assertEquals(actualMeasure, cubeMeasure);
    assertEquals(actualMeasure.getMin(), measure.getMin());
    assertEquals(actualMeasure.getMax(), measure.getMax());
  }

  @Test
  public void testGetCube() throws Exception {
    final String DB = dbPFX + "test_get_cube";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);

    try {
      final XBaseCube cube = createTestCube("testGetCube");
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
      XBaseCube actual = (XBaseCube) actualElement.getValue();
      assertNotNull(actual);

      assertTrue(cube.getName().equalsIgnoreCase(actual.getName()));
      assertNotNull(actual.getMeasures());
      assertEquals(actual.getMeasures().getMeasure().size(), cube.getMeasures().getMeasure().size());
      assertEquals(actual.getDimAttributes().getDimAttribute().size(),
        cube.getDimAttributes().getDimAttribute().size());
      assertEquals(actual.getExpressions().getExpression().size(), cube.getExpressions().getExpression().size());
      assertEquals(actual.getJoinChains().getJoinChain().size(), cube.getJoinChains().getJoinChain().size());
      Map<String, XJoinChain> chains = new HashMap<String, XJoinChain>();
      for (XJoinChain xjc : actual.getJoinChains().getJoinChain()) {
        chains.put(xjc.getName(), xjc);
      }
      assertEquals(chains.get("chain1").getDestTable(), "testdim");
      assertEquals(chains.get("dim2chain").getDestTable(), "testdim2");
      boolean chainValidated = false;
      for (XDimAttribute attr : actual.getDimAttributes().getDimAttribute()) {
        if (attr.getName().equalsIgnoreCase("testdim2col2")) {
          assertEquals(attr.getRefSpec().getChainRefColumn().getDestTable(), "testdim");
          chainValidated = true;
          break;
        }
      }
      assertTrue(chainValidated);
      Cube hcube = (Cube) JAXBUtils.hiveCubeFromXCube(actual, null);
      assertEquals(hcube.getDimAttributeByName("dim1").getDescription(), "first dimension");
      assertEquals(hcube.getDimAttributeByName("dim1").getDisplayString(), "Dimension1");
      assertNotNull(hcube.getDimAttributeByName("testdim2col2"));
      assertEquals(hcube.getDimAttributeByName("testdim2col2").getDisplayString(), "Chained Dimension");
      assertEquals(hcube.getDimAttributeByName("testdim2col2").getDescription(), "ref chained dimension");
      assertEquals(((BaseDimAttribute) hcube.getDimAttributeByName("dim4")).getType(),
        "struct<a:int,b:array<string>,c:map<int,array<struct<x:int,y:array<int>>>");
      assertEquals(((ReferencedDimAtrribute) hcube.getDimAttributeByName("testdim2col2")).getType(), "string");
      assertEquals(((ReferencedDimAtrribute) hcube.getDimAttributeByName("testdim2col2")).getChainName(), "chain1");
      assertEquals(((ReferencedDimAtrribute) hcube.getDimAttributeByName("testdim2col2")).getRefColumn(), "col2");
      assertNotNull(hcube.getMeasureByName("msr1"));
      assertEquals(hcube.getMeasureByName("msr1").getDescription(), "first measure");
      assertEquals(hcube.getMeasureByName("msr1").getDisplayString(), "Measure1");
      assertNotNull(hcube.getExpressionByName("expr1"));
      assertEquals(hcube.getExpressionByName("expr1").getDescription(), "first expression");
      assertEquals(hcube.getExpressionByName("expr1").getDisplayString(), "Expression1");
      Assert.assertFalse(hcube.getJoinChains().isEmpty());
      Assert.assertEquals(hcube.getJoinChains().size(), 2);
      Assert.assertTrue(hcube.getJoinChainNames().contains("chain1"));
      JoinChain chain1 = hcube.getChainByName("chain1");
      Assert.assertEquals(chain1.getDisplayString(), "Chain-1");
      Assert.assertEquals(chain1.getDescription(), "first chain");
      Assert.assertEquals(chain1.getPaths().size(), 1);
      List<TableReference> links = chain1.getPaths().get(0).getReferences();
      Assert.assertEquals(links.size(), 2);
      Assert.assertEquals(links.get(0).toString(), "testgetcube.col1");
      Assert.assertEquals(links.get(1).toString(), "testdim.col1");

      final XDerivedCube dcube = createDerivedCube("testGetDerivedCube", "testGetCube");
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
      XDerivedCube actual2 = (XDerivedCube) actualElement.getValue();
      assertNotNull(actual2);
      assertTrue(dcube.getName().equalsIgnoreCase(actual2.getName()));
      assertEquals(actual2.getParent(), "testGetCube".toLowerCase());
      assertEquals(actual2.getMeasureNames().getMeasureName().size(), dcube.getMeasureNames().getMeasureName().size());
      assertEquals(actual2.getDimAttrNames().getAttrName().size(), dcube.getDimAttrNames().getAttrName().size());
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
          target.queryParam("sessionid", lensSessionId).request(mediaType)
            .get(new GenericType<JAXBElement<XCube>>() {});
        fail("Should have thrown 404, got:" + got);
      } catch (NotFoundException ex) {
        ex.printStackTrace();
      }

      target = target().path("metastore").path("cubes").path("test_drop_cube");
      result = target.queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Now get should give 404
      try {
        JAXBElement<XCube> got =
          target.queryParam("sessionid", lensSessionId).request(mediaType)
            .get(new GenericType<JAXBElement<XCube>>() {});
        fail("Should have thrown 404, got :" + got);
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
      final XBaseCube cube = createTestCube(cubeName);
      // Create this cube first
      WebTarget target = target().path("metastore").path("cubes");
      JAXBElement<XCube> element = cubeObjectFactory.createXCube(cube);
      APIResult result =
        target.queryParam("sessionid", lensSessionId).request(mediaType).post(Entity.xml(element), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Update something
      // Add a measure and dimension
      XMeasure xm2 = new XMeasure();
      xm2.setName("msr3");
      xm2.setType(XMeasureType.DOUBLE);
      xm2.setDefaultAggr("sum");
      cube.getMeasures().getMeasure().add(xm2);

      XDimAttribute xd2 = cubeObjectFactory.createXDimAttribute();
      xd2.setName("dim3");
      xd2.setType("STRING");
      cube.getDimAttributes().getDimAttribute().add(xd2);

      XProperty xp = new XProperty();
      xp.setName("foo2");
      xp.setValue("bar2");
      cube.getProperties().getProperty().add(xp);


      element = cubeObjectFactory.createXCube(cube);
      result = target.path(cubeName)
        .queryParam("sessionid", lensSessionId).request(mediaType).put(Entity.xml(element), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      JAXBElement<XCube> got =
        target.path(cubeName)
          .queryParam("sessionid", lensSessionId).request(mediaType).get(new GenericType<JAXBElement<XCube>>() {});
      XBaseCube actual = (XBaseCube) got.getValue();
      assertEquals(actual.getDimAttributes().getDimAttribute().size(), 5);
      assertEquals(actual.getMeasures().getMeasure().size(), 3);

      CubeInterface hcube = JAXBUtils.hiveCubeFromXCube(actual, null);
      assertTrue(hcube instanceof Cube);
      assertTrue(hcube.getMeasureByName("msr3").getAggregate().equals("sum"));
      assertNotNull(hcube.getDimAttributeByName("dim3"));
      assertEquals(((AbstractCubeTable) hcube).getProperties().get("foo2"), "bar2");

      final XDerivedCube dcube = createDerivedCube("test_update_derived", cubeName);
      // Create this cube first
      element = cubeObjectFactory.createXCube(dcube);
      result =
        target.queryParam("sessionid", lensSessionId).request(mediaType).post(Entity.xml(element), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // Update something
      // Add a measure and dimension
      dcube.getMeasureNames().getMeasureName().add("msr3");
      dcube.getDimAttrNames().getAttrName().add("dim3");

      xp = new XProperty();
      xp.setName("foo.derived2");
      xp.setValue("bar.derived2");
      dcube.getProperties().getProperty().add(xp);


      element = cubeObjectFactory.createXCube(dcube);
      result = target.path("test_update_derived")
        .queryParam("sessionid", lensSessionId).request(mediaType).put(Entity.xml(element), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      got = target.path("test_update_derived")
        .queryParam("sessionid", lensSessionId).request(mediaType).get(new GenericType<JAXBElement<XCube>>() {});
      XDerivedCube actual2 = (XDerivedCube) got.getValue();
      assertEquals(actual2.getDimAttrNames().getAttrName().size(), 2);
      assertEquals(actual2.getMeasureNames().getMeasureName().size(), 2);
      assertTrue(actual2.getMeasureNames().getMeasureName().contains("msr3"));
      assertTrue(actual2.getDimAttrNames().getAttrName().contains("dim3"));

      CubeInterface hdcube = JAXBUtils.hiveCubeFromXCube(actual2, (Cube) hcube);
      assertTrue(hdcube instanceof DerivedCube);
      assertTrue(hdcube.getMeasureByName("msr3").getAggregate().equals("sum"));
      assertNotNull(hdcube.getDimAttributeByName("dim3"));
      assertEquals(((AbstractCubeTable) hdcube).getProperties().get("foo.derived2"), "bar.derived2");

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

      XStorage store1 = target.path("store1").queryParam("sessionid", lensSessionId).request(mediaType)
        .get(XStorage.class);
      assertEquals(store1.getName(), "store1");
      assertEquals(store1.getClassname(), HDFSStorage.class.getCanonicalName());
      assertTrue(store1.getProperties().getProperty().size() >= 1);
      assertTrue(JAXBUtils.mapFromXProperties(store1.getProperties()).containsKey("prop1.name"));
      assertEquals(JAXBUtils.mapFromXProperties(store1.getProperties()).get("prop1.name"), "prop1.value");

      // alter storage
      XProperty prop = cubeObjectFactory.createXProperty();
      prop.setName("prop2.name");
      prop.setValue("prop2.value");
      store1.getProperties().getProperty().add(prop);

      APIResult result = target.path("store1")
        .queryParam("sessionid", lensSessionId).queryParam("storage", "store1")
        .request(mediaType).put(Entity.xml(cubeObjectFactory.createXStorage(store1)), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      store1 = target.path("store1").queryParam("sessionid", lensSessionId).request(mediaType).get(XStorage.class);
      assertEquals(store1.getName(), "store1");
      assertEquals(store1.getClassname(), HDFSStorage.class.getCanonicalName());
      assertTrue(store1.getProperties().getProperty().size() >= 2);
      assertTrue(JAXBUtils.mapFromXProperties(store1.getProperties()).containsKey("prop1.name"));
      assertEquals(JAXBUtils.mapFromXProperties(store1.getProperties()).get("prop1.name"), "prop1.value");
      assertTrue(JAXBUtils.mapFromXProperties(store1.getProperties()).containsKey("prop2.name"));
      assertEquals(JAXBUtils.mapFromXProperties(store1.getProperties()).get("prop2.name"), "prop2.value");

      // drop the storage
      dropStorage("store1");
    } finally {
      dropDatabase(DB);
      setCurrentDatabase(prevDb);
    }
  }

  private XStorageTableDesc createStorageTableDesc(String name, final String[] timePartColNames) {
    XStorageTableDesc xs1 = cubeObjectFactory.createXStorageTableDesc();
    xs1.setCollectionDelimiter(",");
    xs1.setEscapeChar("\\");
    xs1.setFieldDelimiter("");
    xs1.setFieldDelimiter("\t");
    xs1.setLineDelimiter("\n");
    xs1.setMapKeyDelimiter("\r");
    xs1.setTableLocation("/tmp/" + name);
    xs1.setExternal(true);
    xs1.setPartCols(new XColumns());
    xs1.setTableParameters(new XProperties());
    xs1.setSerdeParameters(new XProperties());

    for (String timePartColName : timePartColNames) {
      XColumn partCol = cubeObjectFactory.createXColumn();
      partCol.setName(timePartColName);
      partCol.setType("STRING");
      partCol.setComment("partition column");
      xs1.getPartCols().getColumn().add(partCol);
      xs1.getTimePartCols().add(timePartColName);
    }
    return xs1;
  }

  private XStorageTableElement createStorageTblElement(String storageName, String table, String... updatePeriod) {
    final String[] timePartColNames = {"dt"};
    return createStorageTblElement(storageName, table, timePartColNames, updatePeriod);
  }

  private XStorageTableElement createStorageTblElement(String storageName, String table,
    final String[] timePartColNames, String... updatePeriod) {
    XStorageTableElement tbl = cubeObjectFactory.createXStorageTableElement();
    tbl.setUpdatePeriods(new XUpdatePeriods());
    tbl.setStorageName(storageName);
    if (updatePeriod != null) {
      for (String p : updatePeriod) {
        tbl.getUpdatePeriods().getUpdatePeriod().add(XUpdatePeriod.valueOf(p));
      }
    }
    tbl.setTableDesc(createStorageTableDesc(table, timePartColNames));
    return tbl;
  }

  private XDimensionTable createDimTable(String dimName, String table) {
    XDimensionTable dt = cubeObjectFactory.createXDimensionTable();
    dt.setDimensionName(dimName);
    dt.setTableName(table);
    dt.setWeight(15.0);
    dt.setColumns(new XColumns());
    dt.setProperties(new XProperties());
    dt.setStorageTables(new XStorageTables());

    XColumn c1 = cubeObjectFactory.createXColumn();
    c1.setName("col1");
    c1.setType("STRING");
    c1.setComment("Fisrt column");
    dt.getColumns().getColumn().add(c1);
    XColumn c2 = cubeObjectFactory.createXColumn();
    c2.setName("col2");
    c2.setType("INT");
    c2.setComment("Second column");
    dt.getColumns().getColumn().add(c2);

    XProperty p1 = cubeObjectFactory.createXProperty();
    p1.setName("foodim");
    p1.setValue("bardim");
    dt.getProperties().getProperty().add(p1);

    return dt;
  }

  private XDimensionTable createDimTable(String dimTableName) throws Exception {
    XDimensionTable dt = createDimTable("testdim", dimTableName);
    dt.getStorageTables().getStorageTable().add(createStorageTblElement("test", dimTableName, "HOURLY"));
    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
      lensSessionId, medType));
    mp.bodyPart(new FormDataBodyPart(
      FormDataContentDisposition.name("dimensionTable").fileName("dimtable").build(),
      cubeObjectFactory.createXDimensionTable(dt), medType));
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
    dimension.setAttributes(new XDimAttributes());
    dimension.setExpressions(new XExpressions());
    dimension.setJoinChains(new XJoinChains());
    dimension.setProperties(new XProperties());

    XDimAttribute xd1 = cubeObjectFactory.createXDimAttribute();
    xd1.setName("col1");
    xd1.setType("STRING");
    xd1.setDescription("first column");
    xd1.setDisplayString("Column1");
    // Don't set endtime on this dim to validate null handling on server side
    xd1.setStartTime(startDate);

    XDimAttribute xd2 = cubeObjectFactory.createXDimAttribute();
    xd2.setName("col2");
    xd2.setType("INT");
    xd2.setDescription("second column");
    xd2.setDisplayString("Column2");
    // Don't set start time on this dim to validate null handling on server side
    xd2.setEndTime(endDate);

    dimension.getAttributes().getDimAttribute().add(xd1);
    dimension.getAttributes().getDimAttribute().add(xd2);


    XExprColumn xe1 = new XExprColumn();
    xe1.setName("dimexpr");
    xe1.setType("STRING");
    xe1.setDescription("dimension expression");
    xe1.setDisplayString("Dim Expression");
    xe1.setExpr("substr(col1, 3)");
    dimension.getExpressions().getExpression().add(xe1);

    XProperty xp1 = cubeObjectFactory.createXProperty();
    xp1.setName("dimension.foo");
    xp1.setValue("dim.bar");
    dimension.getProperties().getProperty().add(xp1);
    return dimension;
  }

  private void createdChainedDimensions() throws Exception {
    XDimension dimension = createDimension("testdim");
    XDimension dimension2 = createDimension("testdim2");

    XJoinChain xj1 = new XJoinChain();
    xj1.setName("chain1");
    xj1.setDescription("first chain");
    xj1.setDisplayString("Chain-1");
    xj1.setPaths(new XJoinPaths());
    XJoinPath path1 = cubeObjectFactory.createXJoinPath();
    path1.setEdges(new XJoinEdges());
    XTableReference link1 = new XTableReference();
    link1.setTable("testdim");
    link1.setColumn("col1");
    XTableReference link2 = new XTableReference();
    link2.setTable("testdim2");
    link2.setColumn("col1");
    XJoinEdge edge1 = cubeObjectFactory.createXJoinEdge();
    edge1.setFrom(link1);
    edge1.setTo(link2);
    path1.getEdges().getEdge().add(edge1);
    xj1.getPaths().getPath().add(path1);
    dimension.getJoinChains().getJoinChain().add(xj1);

    final WebTarget target = target().path("metastore").path("dimensions");

    // create
    APIResult result = target.queryParam("sessionid", lensSessionId).request(
      mediaType).post(Entity.xml(cubeObjectFactory.createXDimension(dimension)), APIResult.class);
    assertNotNull(result);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    // create
    result = target.queryParam("sessionid", lensSessionId).request(
      mediaType).post(Entity.xml(cubeObjectFactory.createXDimension(dimension2)), APIResult.class);
    assertNotNull(result);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

  }

  @Test
  public void testDimension() throws Exception {
    final String DB = dbPFX + "test_dimension";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);
    try {
      createdChainedDimensions();

      final WebTarget target = target().path("metastore").path("dimensions");

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
      XDimension testDim = target.path("testdim").queryParam("sessionid", lensSessionId).request(mediaType)
        .get(XDimension.class);
      assertEquals(testDim.getName(), "testdim");
      assertTrue(testDim.getProperties().getProperty().size() >= 1);
      assertTrue(JAXBUtils.mapFromXProperties(testDim.getProperties()).containsKey("dimension.foo"));
      assertEquals(JAXBUtils.mapFromXProperties(testDim.getProperties()).get("dimension.foo"), "dim.bar");
      assertEquals(testDim.getAttributes().getDimAttribute().size(), 2);
      assertEquals(testDim.getExpressions().getExpression().size(), 1);
      assertEquals(testDim.getJoinChains().getJoinChain().size(), 1);
      assertEquals(testDim.getJoinChains().getJoinChain().get(0).getPaths().getPath().size(), 1);
      assertEquals(
        testDim.getJoinChains().getJoinChain().get(0).getPaths().getPath().get(0).getEdges().getEdge().size(), 1);
      assertEquals(testDim.getJoinChains().getJoinChain().get(0).getDescription(), "first chain");
      assertEquals(testDim.getJoinChains().getJoinChain().get(0).getDisplayString(), "Chain-1");

      Dimension dim = JAXBUtils.dimensionFromXDimension(testDim);
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
      testDim.getProperties().getProperty().add(prop);

      testDim.getAttributes().getDimAttribute().remove(1);
      XDimAttribute xd1 = cubeObjectFactory.createXDimAttribute();
      xd1.setName("col3");
      xd1.setType("STRING");
      testDim.getAttributes().getDimAttribute().add(xd1);

      APIResult result = target.path("testdim")
        .queryParam("sessionid", lensSessionId)
        .request(mediaType).put(Entity.xml(cubeObjectFactory.createXDimension(testDim)), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      XDimension altered = target.path("testdim").queryParam("sessionid", lensSessionId).request(mediaType).get(
        XDimension.class);
      assertEquals(altered.getName(), "testdim");
      assertTrue(altered.getProperties().getProperty().size() >= 2);
      assertTrue(JAXBUtils.mapFromXProperties(altered.getProperties()).containsKey("dim.prop2.name"));
      assertEquals(JAXBUtils.mapFromXProperties(altered.getProperties()).get("dim.prop2.name"), "dim.prop2.value");
      assertTrue(JAXBUtils.mapFromXProperties(altered.getProperties()).containsKey("dimension.foo"));
      assertEquals(JAXBUtils.mapFromXProperties(altered.getProperties()).get("dimension.foo"), "dim.bar");
      assertEquals(testDim.getAttributes().getDimAttribute().size(), 2);

      dim = JAXBUtils.dimensionFromXDimension(altered);
      assertNotNull(dim.getAttributeByName("col3"));
      assertTrue(dim.getAttributeByName("col2") == null || dim.getAttributeByName("col1") == null);

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
    } finally {
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
      XDimensionTable dt1 = createDimTable(table);

      JAXBElement<XDimensionTable> dtElement = target().path("metastore/dimtables").path(table)
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XDimensionTable>>() {});
      XDimensionTable dt2 = dtElement.getValue();
      assertTrue(dt1 != dt2);
      assertEquals(dt2.getDimensionName(), dt1.getDimensionName());
      assertEquals(dt2.getTableName(), table);
      assertEquals(dt2.getWeight(), dt1.getWeight());
      Map<String, String> props = JAXBUtils.mapFromXProperties(dt2.getProperties());
      assertTrue(props.containsKey("foodim"));
      assertEquals(props.get("foodim"), "bardim");


      // Update a property
      props.put("foodim", "bardim1");
      dt2.getProperties().getProperty().addAll(JAXBUtils.xPropertiesFromMap(props));
      dt2.setWeight(200.0);
      // Add a column
      XColumn c = cubeObjectFactory.createXColumn();
      c.setName("col3");
      c.setType("STRING");
      c.setComment("Added column");
      dt2.getColumns().getColumn().add(c);

      // Update the table
      APIResult result = target().path("metastore/dimtables")
        .path(table)
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .put(Entity.xml(cubeObjectFactory.createXDimensionTable(dt2)), APIResult.class);
      assertEquals(result.getStatus(), Status.SUCCEEDED);

      // Get the updated table
      JAXBElement<XDimensionTable> dtElement2 = target().path("metastore/dimtables").path(table)
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XDimensionTable>>() {});
      XDimensionTable dt3 = dtElement2.getValue();
      assertEquals(dt3.getWeight(), 200.0);

      List<XColumn> colList = dt3.getColumns().getColumn();
      boolean foundCol = false;
      for (XColumn col : colList) {
        if (col.getName().equals("col3") && col.getType().equals("string")
          && "Added column".equalsIgnoreCase(col.getComment())) {
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
      createDimTable(table);
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
      createDimTable(table);

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
      JAXBElement<XDimensionTable> dt = target().path("metastore/dimtables").path(table)
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XDimensionTable>>() {});
      XDimensionTable dimTable = dt.getValue();
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
        .get(new GenericType<JAXBElement<XDimensionTable>>() {});
      dimTable = dt.getValue();
      cdim = JAXBUtils.cubeDimTableFromDimTable(dimTable);
      assertFalse(cdim.getStorages().contains("test"));
      assertTrue(cdim.getStorages().contains("test2"));
      assertEquals(cdim.getSnapshotDumpPeriods().get("test2"), UpdatePeriod.DAILY);

      // add another storage without dump period
      sTbl = createStorageTblElement("test3", table, (String[]) null);
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
        .get(new GenericType<JAXBElement<XDimensionTable>>() {});
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
      createDimTable(table);
      XStorageTableElement sTbl = createStorageTblElement("test2", table, "DAILY");
      APIResult result = target().path("metastore/dimtables").path(table).path("/storages")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.xml(cubeObjectFactory.createXStorageTableElement(sTbl)), APIResult.class);
      assertEquals(result.getStatus(), Status.SUCCEEDED);

      result = target().path("metastore/dimtables/").path(table).path("storages")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .delete(APIResult.class);
      assertEquals(result.getStatus(), Status.SUCCEEDED);


      JAXBElement<XDimensionTable> dt = target().path("metastore/dimtables").path(table)
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XDimensionTable>>() {});
      XDimensionTable dimTable = dt.getValue();
      CubeDimensionTable cdim = JAXBUtils.cubeDimTableFromDimTable(dimTable);
      assertTrue(cdim.getStorages().isEmpty());
      assertTrue(cdim.getSnapshotDumpPeriods().isEmpty());
    } finally {
      setCurrentDatabase(prevDb);
      dropDatabase(DB);
    }
  }

  private XFactTable createFactTable(String factName) {
    return createFactTable(factName, "testCube");
  }

  private XFactTable createFactTable(String factName, final String cubeName) {
    XFactTable f = cubeObjectFactory.createXFactTable();
    f.setColumns(new XColumns());
    f.setProperties(new XProperties());
    f.setStorageTables(new XStorageTables());
    f.setName(factName);
    f.setWeight(10.0);
    f.setCubeName(cubeName);

    XColumn c1 = cubeObjectFactory.createXColumn();
    c1.setName("c1");
    c1.setType("STRING");
    c1.setComment("col1");
    f.getColumns().getColumn().add(c1);

    XColumn c2 = cubeObjectFactory.createXColumn();
    c2.setName("c2");
    c2.setType("STRING");
    c2.setComment("col2");
    f.getColumns().getColumn().add(c2);


    Map<String, String> properties = new HashMap<String, String>();
    properties.put("foo", "bar");
    f.getProperties().getProperty().addAll(JAXBUtils.xPropertiesFromMap(properties));
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

      XFactTable f = createFactTable(table);
      f.getStorageTables().getStorageTable().add(createStorageTblElement("S1", table, "HOURLY"));
      f.getStorageTables().getStorageTable().add(createStorageTblElement("S2", table, "DAILY"));
      final FormDataMultiPart mp = new FormDataMultiPart();
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        lensSessionId, medType));
      mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("fact").fileName("fact").build(),
        cubeObjectFactory.createXFactTable(f), medType));
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
      JAXBElement<XFactTable> gotFactElement = target().path("metastore/facts").path(table)
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XFactTable>>() {});
      XFactTable gotFact = gotFactElement.getValue();
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
      cf.alterWeight(20.0);
      cf.alterColumn(new FieldSchema("c2", "int", "changed to int"));

      XFactTable update = JAXBUtils.factTableFromCubeFactTable(cf);
      update.getStorageTables().getStorageTable().add(createStorageTblElement("S1", table, "HOURLY"));
      update.getStorageTables().getStorageTable().add(createStorageTblElement("S2", table, "MONTHLY"));

      // Update
      result = target().path("metastore").path("facts").path(table)
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .put(Entity.xml(cubeObjectFactory.createXFactTable(update)), APIResult.class);
      assertEquals(result.getStatus(), Status.SUCCEEDED);

      // Get the updated table
      gotFactElement = target().path("metastore/facts").path(table)
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XFactTable>>() {});
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
      XFactTable f = createFactTable(table);
      f.getStorageTables().getStorageTable().add(createStorageTblElement("S1", table, "HOURLY"));
      f.getStorageTables().getStorageTable().add(createStorageTblElement("S2", table, "DAILY"));
      final FormDataMultiPart mp = new FormDataMultiPart();
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        lensSessionId, medType));
      mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("fact").fileName("fact").build(),
        cubeObjectFactory.createXFactTable(f), medType));
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

      JAXBElement<XFactTable> gotFactElement = target().path("metastore/facts").path(table)
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XFactTable>>() {});
      XFactTable gotFact = gotFactElement.getValue();
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
    return createPartition(cubeTableName, partDate, "dt");
  }

  private XPartition createPartition(String cubeTableName, Date partDate, final String timeDimension) {

    XTimePartSpecElement timePart = cubeObjectFactory.createXTimePartSpecElement();
    timePart.setKey(timeDimension);
    timePart.setValue(JAXBUtils.getXMLGregorianCalendar(partDate));

    return createPartition(cubeTableName, Arrays.asList(timePart));
  }

  private XPartition createPartition(String cubeTableName, final List<XTimePartSpecElement> timePartSpecs) {

    XPartition xp = cubeObjectFactory.createXPartition();
    xp.setLocation("file:///tmp/part/test_part");
    xp.setFactOrDimensionTableName(cubeTableName);
    xp.setNonTimePartitionSpec(new XPartSpec());
    xp.setTimePartitionSpec(new XTimePartSpec());
    xp.setPartitionParameters(new XProperties());
    xp.setSerdeParameters(new XProperties());
    for (XTimePartSpecElement timePartSpec : timePartSpecs) {
      xp.getTimePartitionSpec().getPartSpecElement().add(timePartSpec);
    }
    xp.setUpdatePeriod(XUpdatePeriod.HOURLY);
    return xp;
  }

  @Test
  public void testLatestDateWithInputTimeDimAbsentFromAtleastOneFactPartition() throws Exception {

    final String dbName = dbPFX + getUniqueDbName();
    String prevDb = getCurrentDatabase();

    try {

      // Begin: Setup
      createDatabase(dbName);
      setCurrentDatabase(dbName);

      String[] storages = {"S1"};
      for (String storage : storages) {
        createStorage(storage);
      }

      // Create a cube with name testCube
      final String cubeName = "testCube";
      final XCube cube = createTestCube(cubeName);
      APIResult result = target().path("metastore").path("cubes").queryParam("sessionid", lensSessionId)
        .request(mediaType).post(Entity.xml(cubeObjectFactory.createXCube(cube)), APIResult.class);
      if (!result.getStatus().equals(APIResult.Status.SUCCEEDED)) {
        throw new RuntimeException("Setup failure: Cube Creation failed : " + result.getMessage());
      }

      // Create two facts and fact storage tables with one of the facts
      // not having one of the time dimensions in the partition

      final String timeDimensionPresentInPartitionOfAllFacts = "it";
      final String timeDimOnlyPresentInPartitionOfFact1 = "et";
      String fact1TableName = "fact1";
      String[] fact1TimePartColNames = {
        timeDimensionPresentInPartitionOfAllFacts,
        timeDimOnlyPresentInPartitionOfFact1,
      };

      String fact2TableName = "fact2";
      String[] fact2TimePartColNames = {timeDimensionPresentInPartitionOfAllFacts};

      createTestFactAndStorageTable(cubeName, storages, fact1TableName, fact1TimePartColNames);
      createTestFactAndStorageTable(cubeName, storages, fact2TableName, fact2TimePartColNames);

      // Add partition to fact storage table of the fact whose partition has all time dimension

      // Prepare Partition spec elements
      final Date currentDate = new Date();
      final Date expectedLatestDate = DateUtils.addHours(currentDate, 2);

      XTimePartSpecElement timePartSpecElement1 = cubeObjectFactory.createXTimePartSpecElement();
      timePartSpecElement1.setKey(timeDimensionPresentInPartitionOfAllFacts);
      timePartSpecElement1.setValue(JAXBUtils.getXMLGregorianCalendar(currentDate));

      XTimePartSpecElement timePartSpecElement2 = cubeObjectFactory.createXTimePartSpecElement();
      timePartSpecElement2.setKey(timeDimOnlyPresentInPartitionOfFact1);
      timePartSpecElement2.setValue(JAXBUtils.getXMLGregorianCalendar(expectedLatestDate));

      // Create Partition with prepared partition spec elements
      XPartition xp = createPartition(fact1TableName, Arrays.asList(timePartSpecElement1, timePartSpecElement2));

      APIResult partAddResult = target().path("metastore/facts/").path(fact1TableName)
        .path("storages/" + storages[0] + "/partition")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.xml(cubeObjectFactory.createXPartition(xp)), APIResult.class);
      if (!partAddResult.getStatus().equals(APIResult.Status.SUCCEEDED)) {
        throw new RuntimeException("Setup failure: Partition Creation failed : " + partAddResult.getMessage());
      }

      // End: Setup

      // Begin: Test Execution
      // Get latest date for Cube using timeDimOnlyPresentInPartitionOfFact1
      DateTime retrievedLatestDate = target().path("metastore/cubes").path(cubeName).path("latestdate").queryParam(
        "timeDimension", timeDimOnlyPresentInPartitionOfFact1)
        .queryParam("sessionid", lensSessionId).request(mediaType).get(DateTime.class);
      // End: Test Execution

      // Begin: Verification
      assertEquals(retrievedLatestDate.getDate(), DateUtils.truncate(expectedLatestDate, Calendar.HOUR));
      // End: Verification

    } finally {
      // Cleanup
      setCurrentDatabase(prevDb);
      dropDatabase(dbName);
    }
  }


  @SuppressWarnings("deprecation")
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

      final XCube cube = createTestCube("testCube");
      target().path("metastore").path("cubes").queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.xml(cubeObjectFactory.createXCube(cube)), APIResult.class);

      XFactTable f = createFactTable(table);
      f.getStorageTables().getStorageTable().add(createStorageTblElement("S1", table, "HOURLY"));
      f.getStorageTables().getStorageTable().add(createStorageTblElement("S2", table, "DAILY"));
      final FormDataMultiPart mp = new FormDataMultiPart();
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        lensSessionId, medType));
      mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("fact").fileName("fact").build(),
        cubeObjectFactory.createXFactTable(f), medType));
      APIResult result = target()
        .path("metastore")
        .path("facts")
        .request(mediaType)
        .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      APIResult partAddResult;
      // Add null partition
      Response resp = target().path("metastore/facts/").path(table).path("storages/S2/partition")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(null);
      Assert.assertEquals(resp.getStatus(), 400);

      // Add a partition
      final Date partDate = new Date();
      XPartition xp = createPartition(table, partDate);
      partAddResult = target().path("metastore/facts/").path(table).path("storages/S2/partition")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.xml(cubeObjectFactory.createXPartition(xp)), APIResult.class);
      assertEquals(partAddResult.getStatus(), Status.SUCCEEDED);

      JAXBElement<XPartitionList> partitionsElement = target().path("metastore/facts").path(table)
        .path("storages/S2/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XPartitionList>>() {});

      XPartitionList partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getPartition().size(), 2);

      DateTime date =
        target().path("metastore/cubes").path("testCube").path("latestdate").queryParam("timeDimension", "dt")
          .queryParam("sessionid", lensSessionId).request(mediaType).get(DateTime.class);

      partDate.setMinutes(0);
      partDate.setSeconds(0);
      partDate.setTime(partDate.getTime() - partDate.getTime() % 1000);
      assertEquals(date.getDate(), partDate);

      // Drop the partitions
      APIResult dropResult = target().path("metastore/facts").path(table).path("storages/S2/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .delete(APIResult.class);

      assertEquals(dropResult.getStatus(), Status.SUCCEEDED);

      // Verify partition was dropped
      partitionsElement = target().path("metastore/facts").path(table).path("storages/S2/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XPartitionList>>() {});

      partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getPartition().size(), 0);
      // add null in batch
      resp = target().path("metastore/facts/").path(table).path("storages/S2/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(null);
      Assert.assertEquals(resp.getStatus(), 400);
      // Add again, in batch this time
      partAddResult = target().path("metastore/facts/").path(table).path("storages/S2/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.xml(cubeObjectFactory.createXPartitionList(toXPartitionList(xp))),
          APIResult.class);
      assertEquals(partAddResult.getStatus(), Status.SUCCEEDED);

      // Verify partition was added
      partitionsElement = target().path("metastore/facts").path(table).path("storages/S2/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XPartitionList>>() {});

      partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getPartition().size(), 2);

      // Drop again by values
      String[] val = new String[]{UpdatePeriod.HOURLY.format().format(partDate)};
      dropResult = target().path("metastore/facts").path(table).path("storages/S2/partition")
        .queryParam("values", StringUtils.join(val, ","))
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .delete(APIResult.class);
      assertEquals(dropResult.getStatus(), Status.SUCCEEDED);

      // Verify partition was dropped
      partitionsElement = target().path("metastore/facts").path(table).path("storages/S2/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XPartitionList>>() {});

      partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getPartition().size(), 0);
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
    createStorage("test");

    try {
      createDimTable(table);
      APIResult partAddResult;
      // Add null partition
      Response resp = target().path("metastore/dimtables/").path(table).path("storages/test/partition")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(null);
      Assert.assertEquals(resp.getStatus(), 400);

      // Add a partition
      final Date partDate = new Date();
      XPartition xp = createPartition(table, partDate);
      partAddResult = target().path("metastore/dimtables/").path(table).path("storages/test/partition")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.xml(cubeObjectFactory.createXPartition(xp)), APIResult.class);
      assertEquals(partAddResult.getStatus(), Status.SUCCEEDED);

      JAXBElement<XPartitionList> partitionsElement = target().path("metastore/dimtables").path(table)
        .path("storages/test/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XPartitionList>>() {});

      XPartitionList partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getPartition().size(), 2);

      // Drop the partitions
      APIResult dropResult = target().path("metastore/dimtables").path(table).path("storages/test/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .delete(APIResult.class);

      assertEquals(dropResult.getStatus(), Status.SUCCEEDED);

      // Verify partition was dropped
      partitionsElement = target().path("metastore/dimtables").path(table).path("storages/test/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XPartitionList>>() {});

      partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getPartition().size(), 0);

      // add null in batch
      resp = target().path("metastore/dimtables/").path(table).path("storages/test/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(null);
      Assert.assertEquals(resp.getStatus(), 400);
      // Add again, this time in batch
      partAddResult = target().path("metastore/dimtables/").path(table).path("storages/test/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.xml(cubeObjectFactory.createXPartitionList(toXPartitionList(xp))),
          APIResult.class);
      assertEquals(partAddResult.getStatus(), Status.SUCCEEDED);

      // Verify partition was added
      partitionsElement = target().path("metastore/dimtables").path(table).path("storages/test/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XPartitionList>>() {});

      partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getPartition().size(), 2);

      // Drop again by values
      String[] val = new String[]{UpdatePeriod.HOURLY.format().format(partDate)};
      dropResult = target().path("metastore/dimtables").path(table).path("storages/test/partition")
        .queryParam("values", StringUtils.join(val, ","))
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .delete(APIResult.class);
      assertEquals(dropResult.getStatus(), Status.SUCCEEDED);

      // Verify partition was dropped
      partitionsElement = target().path("metastore/dimtables").path(table).path("storages/test/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XPartitionList>>() {});

      partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getPartition().size(), 0);
    } finally {
      setCurrentDatabase(prevDb);
      dropDatabase(DB);
    }
  }

  private XPartitionList toXPartitionList(final XPartition... xps) {
    XPartitionList ret = new XPartitionList();
    Collections.addAll(ret.getPartition(), xps);
    return ret;
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
      assertTrue(nativetables.getElements().size() >= 1);
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
      JAXBElement<XNativeTable> actualElement = target.path(tableName).queryParam(
        "sessionid", lensSessionId).request(mediaType).get(new GenericType<JAXBElement<XNativeTable>>() {});
      XNativeTable actual = actualElement.getValue();
      assertNotNull(actual);

      assertTrue(tableName.equalsIgnoreCase(actual.getName()));
      assertEquals(actual.getColumns().getColumn().size(), 1);
      assertEquals(actual.getColumns().getColumn().get(0).getName(), "col1");
      assertEquals(actual.getStorageDescriptor().getPartCols().getColumn().size(), 1);
      assertEquals(actual.getStorageDescriptor().getPartCols().getColumn().get(0).getName(), "pcol1");
      assertEquals(actual.getTableType(), TableType.MANAGED_TABLE.name());
      assertFalse(actual.getStorageDescriptor().isExternal());
      boolean foundProp = false;
      for (XProperty prop : actual.getStorageDescriptor().getTableParameters().getProperty()) {
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

  private void populateActualTablesAndCols(List<XFlattenedColumn> columns, Set<String> tables, Set<String> colSet) {
    for (XFlattenedColumn colObject : columns) {
      String colStr;
      tables.add(colObject.getTableName());
      if (colObject.getMeasure() != null) {
        colStr = colObject.getTableName() + "." + colObject.getMeasure().getName();
      } else if (colObject.getDimAttribute() != null) {
        colStr = (colObject.getChainName() != null ? colObject.getChainName() + "-" : "") + colObject.getTableName()
          + "." + colObject.getDimAttribute().getName();
      } else { // it will be expression
        colStr = (colObject.getChainName() != null ? colObject.getChainName() + "-" : "") + colObject.getTableName()
          + "." + colObject.getExpression().getName();
      }
      colSet.add(colStr);
    }
  }

  @Test
  public void testFlattenedView() throws Exception {
    final String DB = dbPFX + "test_flattened_view";
    String prevDb = getCurrentDatabase();
    createDatabase(DB);
    setCurrentDatabase(DB);

    try {
      // Create the tables
      XCube flatTestCube = createTestCube("flatTestCube");
      // Create cube
      final WebTarget cubeTarget = target().path("metastore").path("cubes");
      APIResult result =
        cubeTarget.queryParam("sessionid", lensSessionId).request(mediaType)
          .post(Entity.xml(cubeObjectFactory.createXCube(flatTestCube)), APIResult.class);
      assertNotNull(result);
      assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

      // create chained dimensions - testdim and testdim2
      createdChainedDimensions();

      // Now test flattened view
      final WebTarget flatCubeTarget = target().path("metastore").path("flattened").path("flattestcube");
      XFlattenedColumns flattenedColumns = null;
      JAXBElement<XFlattenedColumns> actualElement = flatCubeTarget.queryParam("sessionid", lensSessionId).request()
        .get(new GenericType<JAXBElement<XFlattenedColumns>>() {});
      flattenedColumns = actualElement.getValue();
      assertNotNull(flattenedColumns);

      List<XFlattenedColumn> columns = flattenedColumns.getFlattenedColumn();
      assertNotNull(columns);
      assertTrue(!columns.isEmpty());

      Set<String> tables = new HashSet<String>();
      Set<String> colSet = new HashSet<String>();
      populateActualTablesAndCols(columns, tables, colSet);

      assertEquals(tables, new HashSet<String>(Arrays.asList("flattestcube", "testdim", "testdim2")));
      assertEquals(colSet, new HashSet<String>(Arrays.asList(
        "flattestcube.msr1",
        "flattestcube.msr2",
        "flattestcube.dim1",
        "flattestcube.dim2",
        "flattestcube.testdim2col2",
        "flattestcube.dim4",
        "flattestcube.expr1",
        "chain1-testdim.col2",
        "chain1-testdim.col1",
        "chain1-testdim.dimexpr",
        "dim2chain-testdim2.col2",
        "dim2chain-testdim2.col1",
        "dim2chain-testdim2.dimexpr"
      )));

      // Now test flattened view for dimension
      final WebTarget flatDimTarget = target().path("metastore").path("flattened").path("testdim");
      actualElement = flatDimTarget.queryParam("sessionid", lensSessionId).request()
        .get(new GenericType<JAXBElement<XFlattenedColumns>>() {});
      flattenedColumns = actualElement.getValue();
      assertNotNull(flattenedColumns);

      columns = flattenedColumns.getFlattenedColumn();
      assertNotNull(columns);
      assertTrue(!columns.isEmpty());

      tables = new HashSet<String>();
      colSet = new HashSet<String>();
      populateActualTablesAndCols(columns, tables, colSet);

      assertEquals(tables, new HashSet<String>(Arrays.asList("testdim", "testdim2")));
      assertEquals(colSet, new HashSet<String>(Arrays.asList(
        "testdim.col2",
        "testdim.col1",
        "testdim.dimexpr",
        "chain1-testdim2.col2",
        "chain1-testdim2.col1",
        "chain1-testdim2.dimexpr"
      )));

    } finally {
      dropDatabase(DB);
      setCurrentDatabase(prevDb);
    }
  }

  private void createTestFactAndStorageTable(final String cubeName, final String[] storages, final String tableName,
    final String[] timePartColNames) {

    // Create a fact table object linked to cubeName
    XFactTable f = createFactTable(tableName, cubeName);
    // Create a storage tables
    f.getStorageTables().getStorageTable().add(createStorageTblElement("S1", tableName, timePartColNames, "HOURLY"));


    // Call API to create a fact table and storage table
    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
      lensSessionId, medType));
    mp.bodyPart(new FormDataBodyPart(
      FormDataContentDisposition.name("fact").fileName("fact").build(),
      cubeObjectFactory.createXFactTable(f), medType));
    APIResult result = target()
      .path("metastore")
      .path("facts")
      .request(mediaType)
      .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    if (!result.getStatus().equals(APIResult.Status.SUCCEEDED)) {
      throw new RuntimeException("Fact/Storage Table Creation failed");
    }
  }

  private String getUniqueDbName() {
    // hyphens replaced with underscore to create a valid db name
    return java.util.UUID.randomUUID().toString().replaceAll("-", "_");
  }
}

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

import static org.apache.lens.cube.metadata.UpdatePeriod.*;

import static org.testng.Assert.*;

import java.io.File;
import java.util.*;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.*;
import javax.xml.bind.JAXBElement;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.APIResult.Status;
import org.apache.lens.api.DateTime;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.StringList;
import org.apache.lens.api.error.LensCommonErrorCode;
import org.apache.lens.api.metastore.*;
import org.apache.lens.api.metastore.ObjectFactory;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.cube.metadata.*;
import org.apache.lens.cube.metadata.ExprColumn.ExprSpec;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServerTestUtil;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.metastore.CubeMetastoreService;
import org.apache.lens.server.api.util.LensUtil;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import org.glassfish.jersey.test.TestProperties;

import org.testng.Assert;
import org.testng.annotations.*;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Test(groups = "unit-test")
public class TestMetastoreService extends LensJerseyTest {
  private ObjectFactory cubeObjectFactory;
  protected String dbPFX = "TestMetastoreService_";
  CubeMetastoreServiceImpl metastoreService;
  LensSessionHandle lensSessionId;

  private void assertSuccess(APIResult result) {
    assertEquals(result.getStatus(), Status.SUCCEEDED, String.valueOf(result));
  }

  @BeforeMethod
  public void setUp() throws Exception {
    super.setUp();
    cubeObjectFactory = new ObjectFactory();
    metastoreService = LensServices.get().getService(CubeMetastoreService.NAME);
    lensSessionId = metastoreService.openSession("foo", "bar", new HashMap<String, String>());

  }

  @AfterMethod
  public void tearDown() throws Exception {
    metastoreService.closeSession(lensSessionId);
    super.tearDown();
  }

  @Override
  protected Application configure() {
    enable(TestProperties.LOG_TRAFFIC);
    enable(TestProperties.DUMP_ENTITY);
    return new MetastoreApp();
  }

  @Test(dataProvider = "mediaTypeData")
  public void testSetDatabase(MediaType mediaType) throws Exception {
    String prevDb = getCurrentDatabase(mediaType);
    String dbName = "test_set_db" + mediaType.getSubtype();
    try {
      WebTarget dbTarget = target().path("metastore").path("databases/current");
      try {
        dbTarget.queryParam("sessionid", lensSessionId).request(mediaType).put(getEntityForString(dbName, mediaType),
          APIResult.class);
        fail("Should get 404");
      } catch (NotFoundException e) {
        // expected
      }

      // create
      APIResult result = target().path("metastore").path("databases")
        .queryParam("sessionid", lensSessionId).request(mediaType).post(getEntityForString(dbName, mediaType), APIResult
          .class);
      assertNotNull(result);
      assertSuccess(result);

      // set
      result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType)
        .put(getEntityForString(dbName, mediaType), APIResult.class);
      assertNotNull(result);
      assertSuccess(result);

      // set without session id, we should get bad request
      try {
        dbTarget.request(mediaType).put(getEntityForString(dbName, mediaType), APIResult.class);
        fail("Should have thrown bad request exception");
      } catch (BadRequestException badReq) {
        // expected
      }

      String current = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType).get(String.class);
      assertEquals(current, dbName);
    } finally {
      setCurrentDatabase(prevDb, mediaType);
      dropDatabase(dbName, mediaType);
    }
  }

  @Test(dataProvider = "mediaTypeData")
  public void testCreateDatabase(MediaType mediaType) throws Exception {
    final String newDb = dbPFX + "new_db" + mediaType.getSubtype();
    WebTarget dbTarget = target().path("metastore").path("databases");

    APIResult result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType)
      .post(getEntityForString(newDb, mediaType), APIResult.class);
    assertNotNull(result);
    assertSuccess(result);

    // Create again
    result = dbTarget.queryParam("sessionid", lensSessionId).queryParam("ignoreIfExisting", false)
      .request(mediaType).post(getEntityForString(newDb, mediaType), APIResult.class);
    assertEquals(result.getStatus(), APIResult.Status.FAILED);
    log.info(">> Result message " + result.getMessage());

    // Drop
    dbTarget.path(newDb).queryParam("sessionid", lensSessionId).request().delete();
  }

  @Test(dataProvider = "mediaTypeData")
  public void testDropDatabase(MediaType mediaType) throws Exception {
    final String dbName = dbPFX + "del_db" + mediaType.getSubtype();
    final WebTarget dbTarget = target().path("metastore").path("databases");
    // First create the database
    APIResult create = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType)
      .post(getEntityForString(dbName, mediaType), APIResult.class);
    assertSuccess(create);

    // Now drop it
    APIResult drop = dbTarget.path(dbName)
      .queryParam("cascade", "true")
      .queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
    assertSuccess(drop);
  }

  @Test(dataProvider = "mediaTypeData")
  public void testGetAllDatabases(MediaType mediaType) throws Exception {
    final String[] dbsToCreate = {"db_1" + mediaType.getSubtype(),
      "db_2" + mediaType.getSubtype(), "db_3" + mediaType.getSubtype(), };
    final WebTarget dbTarget = target().path("metastore").path("databases");

    for (String name : dbsToCreate) {
      dbTarget.queryParam("sessionid", lensSessionId).request(mediaType).post(getEntityForString(name, mediaType));
    }

    StringList allDbs = target().path("metastore").path("databases")
      .queryParam("sessionid", lensSessionId).request(mediaType)
      .get(StringList.class);
    System.out.println("ALL DBs:" + allDbs.getElements());

    for (String db : dbsToCreate) {
      Assert.assertTrue(allDbs.getElements().contains(db));
    }

    List<String> expected = Lists.newArrayList(dbsToCreate);
    // Default is always there
    expected.add("default");

    assertTrue(allDbs.getElements().containsAll(expected));

    for (String name : dbsToCreate) {
      dbTarget.path(name).queryParam("cascade", "true").queryParam("sessionid", lensSessionId).request().delete();
    }
  }

  private void createDatabase(String dbName, MediaType mediaType) throws Exception {
    WebTarget dbTarget = target().path("metastore").path("databases");

    APIResult result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType)
      .post(getEntityForString(dbName, mediaType), APIResult.class);
    assertNotNull(result);
    assertSuccess(result);
  }

  private void createStorage(String storageName, MediaType mediaType) throws Exception {
    WebTarget target = target().path("metastore").path("storages");

    XStorage xs = new XStorage();
    xs.setProperties(new XProperties());
    xs.setName(storageName);
    xs.setClassname(HDFSStorage.class.getCanonicalName());
    XProperty prop = cubeObjectFactory.createXProperty();
    prop.setName("prop1.name");
    prop.setValue("prop1.value");
    xs.getProperties().getProperty().add(prop);

    APIResult result = target.queryParam("sessionid", lensSessionId).request(mediaType).post(
      Entity.entity(new GenericEntity<JAXBElement<XStorage>>(cubeObjectFactory.createXStorage(xs)) {
      }, mediaType),
      APIResult.class);
    assertNotNull(result);
    assertSuccess(result);
  }

  private void dropStorage(String storageName, MediaType mediaType) throws Exception {
    WebTarget target = target().path("metastore").path("storages").path(storageName);

    APIResult result = target
      .queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
    assertSuccess(result);
  }

  private void dropDatabase(String dbName, MediaType mediaType) throws Exception {
    WebTarget dbTarget = target().path("metastore").path("databases").path(dbName);

    APIResult result = dbTarget.queryParam("cascade", "true")
      .queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
    assertSuccess(result);
  }

  private void setCurrentDatabase(String dbName, MediaType mediaType) throws Exception {
    WebTarget dbTarget = target().path("metastore").path("databases/current");
    APIResult result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType)
      .put(getEntityForString(dbName, mediaType), APIResult.class);
    assertSuccess(result);
  }

  private String getCurrentDatabase(MediaType mediaType) throws Exception {
    return target().path("metastore").path("databases/current")
      .queryParam("sessionid", lensSessionId).request(mediaType).get(String.class);
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
    xd1.setNumDistinctValues(2000L);

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
    xd3.getChainRefColumn().add(xcc);
    xd3.setNumDistinctValues(1000L);

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
    link2.setMapsToMany(true);
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
    XExprSpec es = new XExprSpec();
    es.setExpr("msr1/1000");
    xe1.getExprSpec().add(es);

    XExprColumn xe2 = new XExprColumn();
    xe2.setName("expr2");
    xe2.setType("float");
    xe2.setDescription("multi expression");
    xe2.setDisplayString("Expression2");
    XExprSpec es1 = new XExprSpec();
    es1.setExpr("msr1/1000");
    xe2.getExprSpec().add(es1);
    XExprSpec es2 = new XExprSpec();
    es2.setExpr("(msr1/1000) + 0.01");
    es2.setStartTime(startDate);
    xe2.getExprSpec().add(es2);
    XExprSpec es3 = new XExprSpec();
    es3.setExpr("(msr1/1000) + 0.03");
    es3.setEndTime(endDate);
    xe2.getExprSpec().add(es3);
    XExprSpec es4 = new XExprSpec();
    es4.setExpr("(msr1/1000) - 0.01");
    es4.setStartTime(startDate);
    es4.setEndTime(endDate);
    xe2.getExprSpec().add(es4);

    cube.getExpressions().getExpression().add(xe1);
    cube.getExpressions().getExpression().add(xe2);

    XProperty xp1 = cubeObjectFactory.createXProperty();
    xp1.setName("foo");
    xp1.setValue("bar");
    cube.getProperties().getProperty().add(xp1);
    return cube;
  }

  private XDerivedCube createDerivedCube(String cubeName, String parent, boolean addExtraFields) throws Exception {
    XDerivedCube cube = cubeObjectFactory.createXDerivedCube();
    cube.setName(cubeName);
    cube.setDimAttrNames(new XDimAttrNames());
    cube.setMeasureNames(new XMeasureNames());
    cube.setProperties(new XProperties());

    cube.getDimAttrNames().getAttrName().add("dim1");
    cube.getMeasureNames().getMeasureName().add("msr1");
    if (addExtraFields) {
      cube.getDimAttrNames().getAttrName().add("random_dim");
      cube.getMeasureNames().getMeasureName().add("random_measure");
    }
    XProperty xp1 = cubeObjectFactory.createXProperty();
    xp1.setName("derived.foo");
    xp1.setValue("derived.bar");
    cube.getProperties().getProperty().add(xp1);

    cube.setParent(parent);
    return cube;
  }

  private void assertCubesExistence(List<String> cubes, Map<String, Boolean> expectedExistence) {
    for (String cube : cubes) {
      for (Map.Entry<String, Boolean> expectedCubeEntry : expectedExistence.entrySet()) {
        if (cube.equalsIgnoreCase(expectedCubeEntry.getKey())) {
          assertTrue(expectedCubeEntry.getValue(), expectedCubeEntry.getKey() + " is not supposed to be in the list");
        }
      }
    }
  }

  @Test(dataProvider = "mediaTypeData")
  public void testCreateCube(MediaType mediaType) throws Exception {
    final String DB = dbPFX + "test_create_cube" + mediaType.getSubtype();
    String prevDb = getCurrentDatabase(mediaType);
    createDatabase(DB, mediaType);
    setCurrentDatabase(DB, mediaType);
    try {
      final XCube cube = createTestCube("testCube1");
      final WebTarget target = target().path("metastore").path("cubes");
      APIResult result;
      try {
        // first try without a session id
        target.request(mediaType).post(Entity.entity(new GenericEntity<JAXBElement<XCube>>(
          cubeObjectFactory.createXCube(cube)){}, mediaType), APIResult.class);
        fail("Should have thrown bad request exception");
      } catch (BadRequestException badReq) {
        // expected
      }
      result = target.queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(new GenericEntity<JAXBElement<XCube>>(cubeObjectFactory.createXCube(cube)){}, mediaType),
          APIResult.class);
      assertNotNull(result);
      assertSuccess(result);

      StringList cubes = target().path("metastore/cubes").queryParam("sessionid", lensSessionId).request(mediaType)
        .get(StringList.class);
      assertCubesExistence(cubes.getElements(), LensUtil.<String, Boolean>getHashMap("testCube1", Boolean.TRUE));
      // create invalid derived cube
      XCube dcube = createDerivedCube("testderived", "testCube1", true);
      result = target.queryParam("sessionid", lensSessionId).request(
        mediaType).post(Entity.entity(new GenericEntity<JAXBElement<XCube>>(cubeObjectFactory.createXCube(dcube)){},
        mediaType), APIResult.class);
      assertEquals(result.getStatus(), Status.FAILED);
      assertEquals(result.getMessage(), "Problem in submitting entity: Derived cube invalid: Measures "
        + "[random_measure] and Dim Attributes [random_dim] were not present in parent cube testcube1");
      // create derived cube
      dcube = createDerivedCube("testderived", "testCube1", false);
      result = target.queryParam("sessionid", lensSessionId).request(
        mediaType).post(Entity.entity(new GenericEntity<JAXBElement<XCube>>(cubeObjectFactory.createXCube(dcube)){},
        mediaType), APIResult.class);
      assertNotNull(result);
      assertSuccess(result);

      cubes = target().path("metastore/cubes").queryParam("sessionid", lensSessionId).request(mediaType)
        .get(StringList.class);

      assertCubesExistence(cubes.getElements(),
        LensUtil.<String, Boolean>getHashMap("testCube1", true, "testderived", true));
      // get all base cubes
      cubes = target().path("metastore/cubes").queryParam("sessionid", lensSessionId)
        .queryParam("type", "base").request(mediaType).get(StringList.class);

      assertCubesExistence(cubes.getElements(),
        LensUtil.<String, Boolean>getHashMap("testCube1", true, "testderived", false));

      // get all derived cubes
      cubes = target().path("metastore/cubes").queryParam("sessionid", lensSessionId)
        .queryParam("type", "derived").request(mediaType).get(StringList.class);

      assertCubesExistence(cubes.getElements(),
        LensUtil.<String, Boolean>getHashMap("testCube1", false, "testderived", true));

      // Create a non queryable cube
      final XCube qcube = createTestCube("testNoQueryCube");
      XProperty xp = new XProperty();
      xp.setName(MetastoreConstants.CUBE_ALL_FIELDS_QUERIABLE);
      xp.setValue("false");
      qcube.getProperties().getProperty().add(xp);

      result = target.queryParam("sessionid", lensSessionId).request(
        mediaType).post(Entity.entity(new GenericEntity<JAXBElement<XCube>>(cubeObjectFactory.createXCube(qcube)){},
        mediaType), APIResult.class);
      assertNotNull(result);
      assertSuccess(result);

      // get all cubes
      cubes = target().path("metastore/cubes").queryParam("sessionid", lensSessionId)
        .queryParam("type", "all").request(mediaType).get(StringList.class);

      assertCubesExistence(cubes.getElements(),
        LensUtil.<String, Boolean>getHashMap("testCube1", true, "testderived", true, "testNoQueryCube", true));

      // get queryable cubes
      cubes = target().path("metastore/cubes").queryParam("sessionid", lensSessionId)
        .queryParam("type", "queryable").request(mediaType).get(StringList.class);
      assertCubesExistence(cubes.getElements(),
        LensUtil.<String, Boolean>getHashMap("testCube1", true, "testderived", true, "testNoQueryCube", false));

    } finally {
      dropDatabase(DB, mediaType);
      setCurrentDatabase(prevDb, mediaType);
    }
  }

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

  @Test(dataProvider = "mediaTypeData")
  public void testGetCube(MediaType mediaType) throws Exception {
    final String DB = dbPFX + "test_get_cube" + mediaType.getSubtype();
    String prevDb = getCurrentDatabase(mediaType);
    createDatabase(DB, mediaType);
    setCurrentDatabase(DB, mediaType);

    try {
      final XBaseCube cube = createTestCube("testGetCube");
      // Create this cube first
      WebTarget target = target().path("metastore").path("cubes");
      JAXBElement<XCube> element = cubeObjectFactory.createXCube(cube);
      APIResult result =
        target.queryParam("sessionid", lensSessionId).request(mediaType).post(Entity.entity(
          new GenericEntity<JAXBElement<XCube>>(element){}, mediaType), APIResult.class);
      assertSuccess(result);

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
      Map<String, XJoinChain> chains = new HashMap<>();
      for (XJoinChain xjc : actual.getJoinChains().getJoinChain()) {
        chains.put(xjc.getName(), xjc);
      }
      assertEquals(chains.get("chain1").getDestTable(), "testdim");
      assertEquals(chains.get("dim2chain").getDestTable(), "testdim2");
      boolean chainValidated = false;
      for (XDimAttribute attr : actual.getDimAttributes().getDimAttribute()) {
        if (attr.getName().equalsIgnoreCase("testdim2col2")) {
          assertEquals(attr.getChainRefColumn().get(0).getDestTable(), "testdim");
          chainValidated = true;
          break;
        }
      }
      assertTrue(chainValidated);
      Cube hcube = (Cube) JAXBUtils.hiveCubeFromXCube(actual, null);
      assertEquals(hcube.getDimAttributeByName("dim1").getDescription(), "first dimension");
      assertEquals(hcube.getDimAttributeByName("dim1").getDisplayString(), "Dimension1");
      assertEquals((((BaseDimAttribute) hcube.getDimAttributeByName("dim1")).getNumOfDistinctValues().get()),
        Long.valueOf(2000));

      assertNotNull(hcube.getDimAttributeByName("testdim2col2"));
      assertEquals(hcube.getDimAttributeByName("testdim2col2").getDisplayString(), "Chained Dimension");
      assertEquals(hcube.getDimAttributeByName("testdim2col2").getDescription(), "ref chained dimension");
      assertEquals(((BaseDimAttribute) hcube.getDimAttributeByName("dim4")).getType(),
        "struct<a:int,b:array<string>,c:map<int,array<struct<x:int,y:array<int>>>");
      ReferencedDimAttribute testdim2col2 = (ReferencedDimAttribute) hcube.getDimAttributeByName("testdim2col2");
      assertEquals(testdim2col2.getType(), "string");
      assertEquals(testdim2col2.getChainRefColumns().get(0).getChainName(), "chain1");
      assertEquals(testdim2col2.getChainRefColumns().get(0).getRefColumn(), "col2");
      assertEquals(testdim2col2.getNumOfDistinctValues().get(), Long.valueOf(1000));
      assertEquals((testdim2col2.getNumOfDistinctValues().get()), Long.valueOf(1000));

      assertEquals(((BaseDimAttribute) hcube.getDimAttributeByName("dim2")).getNumOfDistinctValues().isPresent(),
        false);

      assertNotNull(hcube.getMeasureByName("msr1"));
      assertEquals(hcube.getMeasureByName("msr1").getDescription(), "first measure");
      assertEquals(hcube.getMeasureByName("msr1").getDisplayString(), "Measure1");
      assertNotNull(hcube.getExpressionByName("expr1"));
      assertEquals(hcube.getExpressionByName("expr1").getDescription(), "first expression");
      assertEquals(hcube.getExpressionByName("expr1").getDisplayString(), "Expression1");
      assertNotNull(hcube.getExpressionByName("expr2"));
      assertEquals(hcube.getExpressionByName("expr2").getExpressions().size(), 4);
      ExprColumn expr2 = hcube.getExpressionByName("expr2");
      Iterator<ExprSpec> esIter = expr2.getExpressionSpecs().iterator();
      ExprSpec first = esIter.next();
      assertEquals(first.getExpr(), "msr1/1000");
      assertNull(first.getStartTime());
      assertNull(first.getEndTime());
      ExprSpec second = esIter.next();
      assertEquals(second.getExpr(), "(msr1/1000) + 0.01");
      assertNotNull(second.getStartTime());
      assertNull(second.getEndTime());
      ExprSpec third = esIter.next();
      assertEquals(third.getExpr(), "(msr1/1000) + 0.03");
      assertNull(third.getStartTime());
      assertNotNull(third.getEndTime());
      ExprSpec last = esIter.next();
      assertEquals(last.getExpr(), "(msr1/1000) - 0.01");
      assertNotNull(last.getStartTime());
      assertNotNull(last.getEndTime());
      assertFalse(esIter.hasNext());
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
      Assert.assertTrue(links.get(1).isMapsToMany());
      Assert.assertEquals(links.get(1).toString(), "testdim.col1[n]");

      final XDerivedCube dcube = createDerivedCube("testGetDerivedCube", "testGetCube", false);
      target = target().path("metastore").path("cubes");
      // Create this cube first
      element = cubeObjectFactory.createXCube(dcube);
      result =
        target.queryParam("sessionid", lensSessionId).request(mediaType).post(
          Entity.entity(new GenericEntity<JAXBElement<XCube>>(element){}, mediaType), APIResult.class);
      assertSuccess(result);

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
      dropDatabase(DB, mediaType);
      setCurrentDatabase(prevDb, mediaType);
    }
  }

  @Test(dataProvider = "mediaTypeData")
  public void testDropCube(MediaType mediaType) throws Exception {
    final String DB = dbPFX + "test_drop_cube" + mediaType.getSubtype();
    String prevDb = getCurrentDatabase(mediaType);
    createDatabase(DB, mediaType);
    setCurrentDatabase(DB, mediaType);

    try {
      final XCube cube = createTestCube("test_drop_cube");
      // Create this cube first
      WebTarget target = target().path("metastore").path("cubes");
      JAXBElement<XCube> element = cubeObjectFactory.createXCube(cube);
      APIResult result =
        target.queryParam("sessionid", lensSessionId).request(mediaType).post(
          Entity.entity(new GenericEntity<JAXBElement<XCube>>(element){}, mediaType), APIResult.class);
      assertSuccess(result);

      final XCube dcube = createDerivedCube("test_drop_derived_cube", "test_drop_cube", false);
      target = target().path("metastore").path("cubes");
      // Create this cube first
      element = cubeObjectFactory.createXCube(dcube);
      result =
        target.queryParam("sessionid", lensSessionId).request(mediaType).post(
          Entity.entity(new GenericEntity<JAXBElement<XCube>>(element){}, mediaType), APIResult.class);
      assertSuccess(result);

      target = target().path("metastore").path("cubes").path("test_drop_derived_cube");
      result = target.queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
      assertSuccess(result);

      // Now get should give 404
      try {
        JAXBElement<XCube> got =
          target.queryParam("sessionid", lensSessionId).request(mediaType)
            .get(new GenericType<JAXBElement<XCube>>() {});
        fail("Should have thrown 404, got:" + got);
      } catch (NotFoundException ex) {
        log.error("Resource not found.", ex);
      }

      target = target().path("metastore").path("cubes").path("test_drop_cube");
      result = target.queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
      assertSuccess(result);

      // Now get should give 404
      try {
        JAXBElement<XCube> got =
          target.queryParam("sessionid", lensSessionId).request(mediaType)
            .get(new GenericType<JAXBElement<XCube>>() {});
        fail("Should have thrown 404, got :" + got);
      } catch (NotFoundException ex) {
        log.error("Resource not found.", ex);
      }
    } finally {
      dropDatabase(DB, mediaType);
      setCurrentDatabase(prevDb, mediaType);
    }
  }

  @Test(dataProvider = "mediaTypeData")
  public void testUpdateCube(MediaType mediaType) throws Exception {
    final String cubeName = "test_update";
    final String DB = dbPFX + "test_update_cube" + mediaType.getSubtype();
    String prevDb = getCurrentDatabase(mediaType);
    createDatabase(DB, mediaType);
    setCurrentDatabase(DB, mediaType);

    try {
      final XBaseCube cube = createTestCube(cubeName);
      // Create this cube first
      WebTarget target = target().path("metastore").path("cubes");
      JAXBElement<XCube> element = cubeObjectFactory.createXCube(cube);
      APIResult result =
        target.queryParam("sessionid", lensSessionId).request(mediaType).post(Entity.entity(
          new GenericEntity<JAXBElement<XCube>>(element){}, mediaType), APIResult.class);
      assertSuccess(result);

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
        .queryParam("sessionid", lensSessionId).request(mediaType).put(Entity.entity(
          new GenericEntity<JAXBElement<XCube>>(element){}, mediaType), APIResult.class);
      assertSuccess(result);

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

      XDerivedCube dcube = createDerivedCube("test_update_derived", cubeName, true);
      element = cubeObjectFactory.createXCube(dcube);
      result =
        target.queryParam("sessionid", lensSessionId).request(mediaType).post(Entity.entity(
          new GenericEntity<JAXBElement<XCube>>(element){}, mediaType), APIResult.class);
      assertEquals(result.getStatus(), Status.FAILED);
      assertEquals(result.getMessage(), "Problem in submitting entity: Derived cube invalid: Measures "
        + "[random_measure] and Dim Attributes [random_dim] were not present in parent cube test_update");
      dcube = createDerivedCube("test_update_derived", cubeName, false);
      // Create this cube first
      element = cubeObjectFactory.createXCube(dcube);
      result =
        target.queryParam("sessionid", lensSessionId).request(mediaType).post(
          Entity.entity(new GenericEntity<JAXBElement<XCube>>(element){}, mediaType), APIResult.class);
      assertSuccess(result);

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
        .queryParam("sessionid", lensSessionId).request(mediaType).put(Entity.entity(new
          GenericEntity<JAXBElement<XCube>>(element){}, mediaType), APIResult.class);
      assertSuccess(result);

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
      dropDatabase(DB, mediaType);
      setCurrentDatabase(prevDb, mediaType);
    }
  }

  @Test(dataProvider = "mediaTypeData")
  public void testStorage(MediaType mediaType) throws Exception {
    final String DB = dbPFX + "test_storage" + mediaType.getSubtype();
    String prevDb = getCurrentDatabase(mediaType);
    createDatabase(DB, mediaType);
    setCurrentDatabase(DB, mediaType);
    try {
      createStorage("store1", mediaType);
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
        .request(mediaType).put(Entity.entity(new GenericEntity<JAXBElement<XStorage>>(cubeObjectFactory
          .createXStorage(store1)){}, mediaType), APIResult.class);
      assertSuccess(result);

      store1 = target.path("store1").queryParam("sessionid", lensSessionId).request(mediaType).get(XStorage.class);
      assertEquals(store1.getName(), "store1");
      assertEquals(store1.getClassname(), HDFSStorage.class.getCanonicalName());
      assertTrue(store1.getProperties().getProperty().size() >= 2);
      assertTrue(JAXBUtils.mapFromXProperties(store1.getProperties()).containsKey("prop1.name"));
      assertEquals(JAXBUtils.mapFromXProperties(store1.getProperties()).get("prop1.name"), "prop1.value");
      assertTrue(JAXBUtils.mapFromXProperties(store1.getProperties()).containsKey("prop2.name"));
      assertEquals(JAXBUtils.mapFromXProperties(store1.getProperties()).get("prop2.name"), "prop2.value");

      // drop the storage
      dropStorage("store1", mediaType);
    } finally {
      dropDatabase(DB, mediaType);
      setCurrentDatabase(prevDb, mediaType);
    }
  }

  private XStorageTableDesc createStorageTableDesc(String name, final String[] timePartColNames) {
    XStorageTableDesc xs1 = cubeObjectFactory.createXStorageTableDesc();
    XProperties props = cubeObjectFactory.createXProperties();
    XProperty propStartTime = cubeObjectFactory.createXProperty();
    propStartTime.setName(MetastoreUtil.getStoragetableStartTimesKey());
    propStartTime.setValue("now -10 days");
    XProperty propEndTime = cubeObjectFactory.createXProperty();
    propEndTime.setName(MetastoreUtil.getStoragetableEndTimesKey());
    propEndTime.setValue("now +5 days");
    props.getProperty().add(propStartTime);
    props.getProperty().add(propEndTime);
    xs1.setCollectionDelimiter(",");
    xs1.setEscapeChar("\\");
    xs1.setFieldDelimiter("\t");
    xs1.setLineDelimiter("\n");
    xs1.setMapKeyDelimiter("\r");
    xs1.setTableLocation(new Path(new File("target").getAbsolutePath(), name).toString());
    xs1.setExternal(true);
    xs1.setPartCols(new XColumns());
    xs1.setTableParameters(props);
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

  private XDimensionTable createDimTable(String dimTableName, MediaType mediaType) throws Exception {
    XDimension dimension = createDimension("testdim");
    APIResult result = target().path("metastore").path("dimensions")
      .queryParam("sessionid", lensSessionId).request(
        mediaType).post(Entity.entity(new GenericEntity<JAXBElement<XDimension>>(cubeObjectFactory
        .createXDimension(dimension)) {}, mediaType), APIResult.class);
    assertSuccess(result);
    XDimensionTable dt = createDimTable("testdim", dimTableName);
    dt.getStorageTables().getStorageTable().add(createStorageTblElement("test", dimTableName, "HOURLY"));
    result = target()
      .path("metastore")
      .path("dimtables").queryParam("sessionid", lensSessionId)
      .request(mediaType)
      .post(Entity.entity(
        new GenericEntity<JAXBElement<XDimensionTable>>(cubeObjectFactory.createXDimensionTable(dt)) {},
        mediaType), APIResult.class);
    assertSuccess(result);
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
    XExprSpec es = new XExprSpec();
    es.setExpr("substr(col1, 3)");
    xe1.getExprSpec().add(es);
    dimension.getExpressions().getExpression().add(xe1);

    XProperty xp1 = cubeObjectFactory.createXProperty();
    xp1.setName("dimension.foo");
    xp1.setValue("dim.bar");
    dimension.getProperties().getProperty().add(xp1);
    return dimension;
  }

  private void createdChainedDimensions(MediaType mediaType) throws Exception {
    XDimension dimension = createDimension("testdim");
    XDimension dimension2 = createDimension("testdim2");

    XDimAttribute xd3 = cubeObjectFactory.createXDimAttribute();
    xd3.setName("col3");
    xd3.setType("STRING");
    xd3.setDescription("inline column");
    xd3.setDisplayString("Column3");
    xd3.getValues().add("Val1");
    xd3.getValues().add("Val2");
    xd3.getValues().add("Val3");

    XDimAttribute xd4 = cubeObjectFactory.createXDimAttribute();
    xd4.setName("col4");
    xd4.setDescription("hierarchical column");
    xd4.setDisplayString("Column4");
    XDimAttributes hierarchy = new XDimAttributes();
    XDimAttribute hd1 = cubeObjectFactory.createXDimAttribute();
    hd1.setName("col4-h1");
    hd1.setType("STRING");
    hd1.setDescription("inline column");
    hd1.setDisplayString("Column4-h1");
    hd1.getValues().add("Val1-h1");
    hd1.getValues().add("Val2-h1");
    hd1.getValues().add("Val3-h1");
    hierarchy.getDimAttribute().add(hd1);
    XDimAttribute hd2 = cubeObjectFactory.createXDimAttribute();
    hd2.setName("col4-h2");
    hd2.setType("STRING");
    hd2.setDescription("base column");
    hd2.setDisplayString("Column4-h2");
    hierarchy.getDimAttribute().add(hd2);
    XDimAttribute hd3 = cubeObjectFactory.createXDimAttribute();
    hd3.setName("col4-h3");
    hd3.setType("STRING");
    hd3.setDescription("ref column");
    hd3.setDisplayString("Column4-h3");
    XChainColumn xcc = new XChainColumn();
    xcc.setChainName("chain1");
    xcc.setRefCol("col2");
    hd3.getChainRefColumn().add(xcc);
    hd3.setNumDistinctValues(1000L);
    hierarchy.getDimAttribute().add(hd3);
    xd4.setHierarchy(hierarchy);

    XDimAttribute xd5 = cubeObjectFactory.createXDimAttribute();
    xd5.setName("col5");
    xd5.setType("INT");
    xd5.setDescription("ref column");
    xd5.setDisplayString("Column5");
    xd5.getChainRefColumn().add(xcc);
    xd5.getValues().add("1");
    xd5.getValues().add("2");
    xd5.getValues().add("3");

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
    dimension.getAttributes().getDimAttribute().add(xd3);
    dimension.getAttributes().getDimAttribute().add(xd4);
    dimension.getAttributes().getDimAttribute().add(xd5);

    final WebTarget target = target().path("metastore").path("dimensions");

    // create
    APIResult result = target.queryParam("sessionid", lensSessionId).request(
      mediaType).post(Entity.entity(new GenericEntity<JAXBElement<XDimension>>(cubeObjectFactory
      .createXDimension(dimension)){}, mediaType), APIResult.class);
    assertNotNull(result);
    assertSuccess(result);

    // create
    result = target.queryParam("sessionid", lensSessionId).request(
      mediaType).post(Entity.entity(new GenericEntity<JAXBElement<XDimension>>(cubeObjectFactory
      .createXDimension(dimension2)){}, mediaType), APIResult.class);
    assertNotNull(result);
    assertSuccess(result);
  }

  @Test(dataProvider = "mediaTypeData")
  public void testDimension(MediaType mediaType) throws Exception {
    final String DB = dbPFX + "test_dimension" + mediaType.getSubtype();
    String prevDb = getCurrentDatabase(mediaType);
    createDatabase(DB, mediaType);
    setCurrentDatabase(DB, mediaType);
    try {
      createdChainedDimensions(mediaType);

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
      assertEquals(testDim.getAttributes().getDimAttribute().size(), 5);
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
      assertNotNull(dim.getAttributeByName("col3"));
      BaseDimAttribute col3 = (BaseDimAttribute) dim.getAttributeByName("col3");
      assertEquals(col3.getDescription(), "inline column");
      assertEquals(col3.getDisplayString(), "Column3");
      assertEquals(col3.getType(), "string");
      assertEquals(col3.getValues().get().get(0), "Val1");
      assertEquals(col3.getValues().get().get(1), "Val2");
      assertEquals(col3.getValues().get().get(2), "Val3");
      assertEquals(col3.getNumOfDistinctValues().get(), (Long) 3L);
      assertNotNull(dim.getAttributeByName("col4"));
      HierarchicalDimAttribute col4 = (HierarchicalDimAttribute) dim.getAttributeByName("col4");
      assertEquals(col4.getDescription(), "hierarchical column");
      BaseDimAttribute col4h1 = (BaseDimAttribute) col4.getHierarchy().get(0);
      assertEquals(col4h1.getName(), "col4-h1");
      assertEquals(col4h1.getType(), "string");
      assertEquals(col4h1.getDescription(), "inline column");
      assertEquals(col4h1.getDisplayString(), "Column4-h1");
      assertEquals(col4h1.getValues().get().get(0), "Val1-h1");
      assertEquals(col4h1.getValues().get().get(1), "Val2-h1");
      assertEquals(col4h1.getValues().get().get(2), "Val3-h1");
      assertEquals(col4h1.getNumOfDistinctValues().get(), (Long) 3L);
      BaseDimAttribute col4h2 = (BaseDimAttribute) col4.getHierarchy().get(1);
      assertEquals(col4h2.getName(), "col4-h2");
      assertEquals(col4h2.getType(), "string");
      assertEquals(col4h2.getDescription(), "base column");
      assertEquals(col4h2.getDisplayString(), "Column4-h2");
      ReferencedDimAttribute col4h3 = (ReferencedDimAttribute) col4.getHierarchy().get(2);
      assertEquals(col4h3.getName(), "col4-h3");
      assertEquals(col4h3.getDescription(), "ref column");
      assertEquals(col4h3.getDisplayString(), "Column4-h3");
      assertEquals(col4h3.getType(), "string");
      assertEquals(col4h3.getChainRefColumns().get(0).getChainName(), "chain1");
      assertEquals(col4h3.getChainRefColumns().get(0).getRefColumn(), "col2");
      assertEquals(col4h3.getNumOfDistinctValues().get(), (Long) 1000L);
      assertNotNull(dim.getAttributeByName("col5"));
      ReferencedDimAttribute col5 = (ReferencedDimAttribute) dim.getAttributeByName("col5");
      assertEquals(col5.getDescription(), "ref column");
      assertEquals(col5.getDisplayString(), "Column5");
      assertEquals(col5.getType(), "int");
      assertEquals(col5.getChainRefColumns().get(0).getChainName(), "chain1");
      assertEquals(col5.getChainRefColumns().get(0).getRefColumn(), "col2");
      assertEquals(col5.getValues().get().get(0), "1");
      assertEquals(col5.getValues().get().get(1), "2");
      assertEquals(col5.getValues().get().get(2), "3");
      assertEquals(col5.getNumOfDistinctValues().get(), (Long) 3L);

      // alter dimension
      XProperty prop = cubeObjectFactory.createXProperty();
      prop.setName("dim.prop2.name");
      prop.setValue("dim.prop2.value");
      testDim.getProperties().getProperty().add(prop);

      testDim.getAttributes().getDimAttribute().remove(1);
      XDimAttribute xd1 = cubeObjectFactory.createXDimAttribute();
      xd1.setName("col3Added");
      xd1.setType("STRING");
      testDim.getAttributes().getDimAttribute().add(xd1);

      APIResult result = target.path("testdim")
        .queryParam("sessionid", lensSessionId)
        .request(mediaType).put(Entity.entity(new GenericEntity<JAXBElement<XDimension>>(cubeObjectFactory
          .createXDimension(testDim)){}, mediaType), APIResult.class);
      assertSuccess(result);

      XDimension altered = target.path("testdim").queryParam("sessionid", lensSessionId).request(mediaType).get(
        XDimension.class);
      assertEquals(altered.getName(), "testdim");
      assertTrue(altered.getProperties().getProperty().size() >= 2);
      assertTrue(JAXBUtils.mapFromXProperties(altered.getProperties()).containsKey("dim.prop2.name"));
      assertEquals(JAXBUtils.mapFromXProperties(altered.getProperties()).get("dim.prop2.name"), "dim.prop2.value");
      assertTrue(JAXBUtils.mapFromXProperties(altered.getProperties()).containsKey("dimension.foo"));
      assertEquals(JAXBUtils.mapFromXProperties(altered.getProperties()).get("dimension.foo"), "dim.bar");
      assertEquals(testDim.getAttributes().getDimAttribute().size(), 5);

      dim = JAXBUtils.dimensionFromXDimension(altered);
      assertNotNull(dim.getAttributeByName("col3Added"));
      assertTrue(dim.getAttributeByName("col2") == null || dim.getAttributeByName("col1") == null
        || dim.getAttributeByName("col3") == null || dim.getAttributeByName("col4") == null
        || dim.getAttributeByName("col5") == null);

      // drop the dimension
      result = target.path("testdim")
        .queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
      assertSuccess(result);

      // Now get should give 404
      try {
        JAXBElement<XDimension> got =
          target.path("testdim").queryParam("sessionid", lensSessionId).request(
            mediaType).get(new GenericType<JAXBElement<XDimension>>() {});
        fail("Should have thrown 404, but got" + got.getValue().getName());
      } catch (NotFoundException ex) {
        log.error("Resource not found.", ex);
      }

      try {
        result = target.path("testdim")
          .queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
        fail("Should have thrown 404, but got" + result.getStatus());
      } catch (NotFoundException ex) {
        log.error("Resource not found.", ex);
      }
    } finally {
      dropDatabase(DB, mediaType);
      setCurrentDatabase(prevDb, mediaType);
    }
  }

  @Test(dataProvider = "mediaTypeData")
  public void testCreateAndDropDimensionTable(MediaType mediaType) throws Exception {
    final String table = "test_create_dim";
    final String DB = dbPFX + "test_dim_db" + mediaType.getSubtype();
    String prevDb = getCurrentDatabase(mediaType);
    createDatabase(DB, mediaType);
    setCurrentDatabase(DB, mediaType);
    createStorage("test", mediaType);

    try {
      createDimTable(table, mediaType);

      // Drop the table now
      APIResult result =
        target().path("metastore/dimtables").path(table)
          .queryParam("cascade", "true")
          .queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
      assertSuccess(result);

      // Drop again, should get 404 now
      try {
        target().path("metastore/dimtables").path(table)
          .queryParam("cascade", "true")
          .queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
        fail("Should have got 404");
      } catch (NotFoundException e404) {
        log.info("correct");
      }

    } finally {
      setCurrentDatabase(prevDb, mediaType);
      dropDatabase(DB, mediaType);
    }
  }

  @Test(dataProvider = "mediaTypeData")
  public void testGetAndUpdateDimensionTable(MediaType mediaType) throws Exception {
    final String table = "test_get_dim";
    final String DB = dbPFX + "test_get_dim_db" + mediaType.getSubtype();
    String prevDb = getCurrentDatabase(mediaType);
    createDatabase(DB, mediaType);
    setCurrentDatabase(DB, mediaType);
    createStorage("test", mediaType);

    try {
      XDimensionTable dt1 = createDimTable(table, mediaType);

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
        .put(Entity.entity(new GenericEntity<JAXBElement<XDimensionTable>>(cubeObjectFactory
            .createXDimensionTable(dt2)){}, mediaType),
          APIResult.class);
      assertSuccess(result);

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

      // Update storage tables
      dt3.getStorageTables().getStorageTable().get(0).getTableDesc().setFieldDelimiter(":");
      dt3.getStorageTables().getStorageTable().get(0).getTableDesc().setInputFormat(
        SequenceFileInputFormat.class.getCanonicalName());
      // add one more storage table
      createStorage("testAlterDimStorage", mediaType);
      XStorageTableElement newStorage = createStorageTblElement("testAlterDimStorage", dt3.getTableName(),
        (String[]) null);
      newStorage.getTableDesc().setFieldDelimiter(":");
      dt3.getStorageTables().getStorageTable().add(newStorage);
      // Update the table
      result = target().path("metastore/dimtables")
        .path(table)
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .put(Entity.entity(new GenericEntity<JAXBElement<XDimensionTable>>(cubeObjectFactory
            .createXDimensionTable(dt3)){}, mediaType),
          APIResult.class);
      assertSuccess(result);

      // Get the updated table
      JAXBElement<XDimensionTable> dtElement4 = target().path("metastore/dimtables").path(table)
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XDimensionTable>>() {});
      XDimensionTable dt4 = dtElement4.getValue();
      assertEquals(dt4.getStorageTables().getStorageTable().size(), 2);

      WebTarget nativeTarget = target().path("metastore").path("nativetables");
      // get all native tables
      StringList nativetables = nativeTarget.queryParam("sessionid", lensSessionId).request(mediaType).get(
        StringList.class);
      assertTrue(nativetables.getElements().contains("test_" + table));
      assertTrue(nativetables.getElements().contains("testalterdimstorage_" + table));

      // get native table and validate altered property
      XNativeTable newdNativeTable = nativeTarget.path("testalterdimstorage_" + table)
        .queryParam("sessionid", lensSessionId)
        .request(mediaType).get(new GenericType<JAXBElement<XNativeTable>>() {}).getValue();
      assertEquals(newdNativeTable.getStorageDescriptor().getFieldDelimiter(), ":");
      XNativeTable alteredNativeTable = nativeTarget.path("test_" + table).queryParam("sessionid", lensSessionId)
        .request(mediaType).get(XNativeTable.class);
      assertEquals(alteredNativeTable.getStorageDescriptor().getInputFormat(),
        SequenceFileInputFormat.class.getCanonicalName());
      // Drop table
      result =
        target().path("metastore/dimtables").path(table)
          .queryParam("cascade", "true")
          .queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
      assertSuccess(result);
    } finally {
      setCurrentDatabase(prevDb, mediaType);
      dropDatabase(DB, mediaType);
    }
  }

  @Test(dataProvider = "mediaTypeData")
  public void testGetDimensionStorages(MediaType mediaType) throws Exception {
    final String table = "test_get_storage";
    final String DB = dbPFX + "test_get_dim_storage_db";
    String prevDb = getCurrentDatabase(mediaType);
    createDatabase(DB, mediaType);
    setCurrentDatabase(DB, mediaType);
    createStorage("test", mediaType);

    try {
      createDimTable(table, mediaType);
      StringList storages = target().path("metastore").path("dimtables")
        .path(table).path("storages")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(StringList.class);
      assertEquals(storages.getElements().size(), 1);
      assertTrue(storages.getElements().contains("test"));
    } finally {
      setCurrentDatabase(prevDb, mediaType);
      dropDatabase(DB, mediaType);
    }
  }

  @Test(dataProvider = "mediaTypeData")
  public void testAddAndDropDimensionStorages(MediaType mediaType) throws Exception {
    final String table = "test_add_drop_storage";
    final String DB = dbPFX + "test_add_drop_dim_storage_db" + mediaType.getSubtype();
    createDatabase(DB, mediaType);
    String prevDb = getCurrentDatabase(mediaType);
    setCurrentDatabase(DB, mediaType);
    createStorage("test", mediaType);
    createStorage("test2", mediaType);
    createStorage("test3", mediaType);
    try {
      createDimTable(table, mediaType);

      XStorageTableElement sTbl = createStorageTblElement("test2", table, "DAILY");
      APIResult result = target().path("metastore/dimtables").path(table).path("/storages")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(new GenericEntity<JAXBElement<XStorageTableElement>>(cubeObjectFactory
          .createXStorageTableElement(sTbl)) {
        }, mediaType), APIResult.class);
      assertSuccess(result);

      StringList storages = target().path("metastore").path("dimtables")
        .path(table).path("storages")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(StringList.class);
      assertEquals(storages.getElements().size(), 2);
      assertTrue(storages.getElements().contains("test"), "Got " + storages.getElements().toString());
      assertTrue(storages.getElements().contains("test2"), "Got " + storages.getElements().toString());

      // Check get table also contains the storage
      JAXBElement<XDimensionTable> dt = target().path("metastore/dimtables").path(table)
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XDimensionTable>>() {});
      XDimensionTable dimTable = dt.getValue();
      CubeDimensionTable cdim = JAXBUtils.cubeDimTableFromDimTable(dimTable);
      assertTrue(cdim.getStorages().contains("test"));
      assertTrue(cdim.getStorages().contains("test2"));
      assertEquals(cdim.getSnapshotDumpPeriods().get("test2"), DAILY);
      assertEquals(cdim.getSnapshotDumpPeriods().get("test"), HOURLY);

      result = target().path("metastore/dimtables/").path(table).path("storages").path("test")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .delete(APIResult.class);
      assertSuccess(result);

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
      assertEquals(cdim.getSnapshotDumpPeriods().get("test2"), DAILY);

      // add another storage without dump period
      sTbl = createStorageTblElement("test3", table, (String[]) null);
      result = target().path("metastore/dimtables").path(table).path("/storages")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(new GenericEntity<JAXBElement<XStorageTableElement>>(cubeObjectFactory
          .createXStorageTableElement(sTbl)){}, mediaType), APIResult.class);
      assertSuccess(result);

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
      setCurrentDatabase(prevDb, mediaType);
      dropDatabase(DB, mediaType);
    }
  }

  @Test(dataProvider = "mediaTypeData")
  public void testAddDropAllDimStorages(MediaType mediaType) throws Exception {
    final String table = "testAddDropAllDimStorages";
    final String DB = dbPFX + "testAddDropAllDimStorages_db" + mediaType.getSubtype();
    String prevDb = getCurrentDatabase(mediaType);
    createDatabase(DB, mediaType);
    setCurrentDatabase(DB, mediaType);
    createStorage("test", mediaType);
    createStorage("test2", mediaType);

    try {
      createDimTable(table, mediaType);
      XStorageTableElement sTbl = createStorageTblElement("test2", table, "DAILY");
      APIResult result = target().path("metastore/dimtables").path(table).path("/storages")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(new GenericEntity<JAXBElement<XStorageTableElement>>(cubeObjectFactory
          .createXStorageTableElement(sTbl)){}, mediaType), APIResult.class);
      assertSuccess(result);

      result = target().path("metastore/dimtables/").path(table).path("storages")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .delete(APIResult.class);
      assertSuccess(result);

      JAXBElement<XDimensionTable> dt = target().path("metastore/dimtables").path(table)
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XDimensionTable>>() {});
      XDimensionTable dimTable = dt.getValue();
      CubeDimensionTable cdim = JAXBUtils.cubeDimTableFromDimTable(dimTable);
      assertTrue(cdim.getStorages().isEmpty());
      assertTrue(cdim.getSnapshotDumpPeriods().isEmpty());
    } finally {
      setCurrentDatabase(prevDb, mediaType);
      dropDatabase(DB, mediaType);
    }
  }

  private XCubeSegmentation createCubeSegmentation(String segName) {
    return createCubeSegmentation(segName, "testCube");
  }

  private XCubeSegmentation createCubeSegmentation(String segName, String cubeName) {
    XCubeSegmentation seg = cubeObjectFactory.createXCubeSegmentation();

    //Create Xproperties
    XProperties props1 = cubeObjectFactory.createXProperties();
    XProperty prop1 = new XProperty();
    prop1.setName("prop_key1");
    prop1.setValue("prop_val1");
    props1.getProperty().add(prop1);

    XProperties props2 = cubeObjectFactory.createXProperties();
    XProperty prop2 = new XProperty();
    prop2.setName("prop_key2");
    prop2.setValue("prop_val2");
    props2.getProperty().add(prop2);

    // Create XcubeSegments
    XCubeSegments cubes =  new XCubeSegments();
    XCubeSegment c1 = cubeObjectFactory.createXCubeSegment();
    c1.setCubeName("cube1");
    c1.setSegmentParameters(props1);

    XCubeSegment c2 = cubeObjectFactory.createXCubeSegment();
    c2.setCubeName("cube2");
    c2.setSegmentParameters(props2);

    cubes.getCubeSegment().add(c1);
    cubes.getCubeSegment().add(c2);

    seg.setProperties(new XProperties());
    seg.setName(segName);
    seg.setWeight(10.0);
    seg.setCubeName(cubeName);
    seg.setCubeSegements(cubes);
    Map<String, String> properties = LensUtil.getHashMap("foo", "bar");
    seg.getProperties().getProperty().addAll(JAXBUtils.xPropertiesFromMap(properties));

    return seg;
  }

  @Test(dataProvider = "mediaTypeData")
  public void testCreateAndAlterCubeSegmentation(MediaType mediaType) throws Exception {
    final String segname = "testCreateCubeSegmentation";
    final String DB = dbPFX + "testCreateCubeSegmentation_DB" + mediaType.getSubtype();
    String prevDb = getCurrentDatabase(mediaType);
    createDatabase(DB, mediaType);
    setCurrentDatabase(DB, mediaType);

    try {
      XCubeSegmentation seg = createCubeSegmentation(segname);

      APIResult result = target()
              .path("metastore")
              .path("cubesegmentations").queryParam("sessionid", lensSessionId)
              .request(mediaType)
              .post(Entity.entity(
                              new GenericEntity<JAXBElement<XCubeSegmentation>>(
                                      cubeObjectFactory.createXCubeSegmentation(seg)){}, mediaType),
                      APIResult.class);
      assertSuccess(result);

      // Get all cube segmentations, this should contain the cube segmentation created earlier
      StringList segNames = target().path("metastore/cubesegmentations")
              .queryParam("sessionid", lensSessionId).request(mediaType).get(StringList.class);
      assertTrue(segNames.getElements().contains(segname.toLowerCase()));

      // Get the created cubesegmentation
      JAXBElement<XCubeSegmentation> gotCubeSegmentation = target().path("metastore/cubesegmentations")
              .path(segname)
              .queryParam("sessionid", lensSessionId).request(mediaType)
              .get(new GenericType<JAXBElement<XCubeSegmentation>>() {});
      XCubeSegmentation gotSeg = gotCubeSegmentation.getValue();
      assertTrue(gotSeg.getName().equalsIgnoreCase(segname));
      assertEquals(gotSeg.getWeight(), 10.0);
      CubeSegmentation cs = JAXBUtils.cubeSegmentationFromXCubeSegmentation(seg);

      // Check for cube segemnts
      boolean foundCube1 = false;
      for (CubeSegment cube : cs.getCubeSegments()) {
        if (cube.getName().equalsIgnoreCase("cube1")
            && cube.getProperties().get("prop_key1").equals("prop_val1")) {
          foundCube1 = true;
          break;
        }
      }
      assertTrue(foundCube1);
      assertEquals(cs.getProperties().get("foo"), "bar");

      // update cube segmentation
      XCubeSegmentation update = JAXBUtils.xsegmentationFromCubeSegmentation(cs);
      XCubeSegments cubes =  new XCubeSegments();
      XCubeSegment c1 = cubeObjectFactory.createXCubeSegment();
      c1.setCubeName("cube11");
      XCubeSegment c2 = cubeObjectFactory.createXCubeSegment();
      c2.setCubeName("cube22");
      cubes.getCubeSegment().add(c1);
      cubes.getCubeSegment().add(c2);

      update.setWeight(20.0);
      update.setCubeSegements(cubes);

      result = target().path("metastore").path("cubesegmentations").path(segname)
              .queryParam("sessionid", lensSessionId).request(mediaType)
              .put(Entity.entity(new GenericEntity<JAXBElement<XCubeSegmentation>>(
                cubeObjectFactory.createXCubeSegmentation(update)){}, mediaType),
              APIResult.class);
      assertSuccess(result);

      // Get the updated table
      JAXBElement<XCubeSegmentation>  gotUpdatedCubeSeg = target().path("metastore/cubesegmentations").path(segname)
              .queryParam("sessionid", lensSessionId).request(mediaType)
              .get(new GenericType<JAXBElement<XCubeSegmentation>>() {});
      XCubeSegmentation gotUpSeg = gotUpdatedCubeSeg.getValue();
      CubeSegmentation usg = JAXBUtils.cubeSegmentationFromXCubeSegmentation(gotUpSeg);

      assertEquals(usg.getCubeSegments().size(), 2);
      for (CubeSegment segmnt : usg.getCubeSegments()) {
        assertTrue(segmnt.getName().equals("cube11") || segmnt.getName().equals("cube22"));
      }

      // Finally, drop the cube segmentation
      result = target().path("metastore").path("cubesegmentations").path(segname)
              .queryParam("sessionid", lensSessionId).request(mediaType)
              .delete(APIResult.class);

      assertSuccess(result);

      // Drop again, this time it should give a 404
      try {
        target().path("metastore").path("cubesegmentations").path(segname)
                .queryParam("cascade", "true")
                .queryParam("sessionid", lensSessionId).request(mediaType)
                .delete(APIResult.class);
        fail("Expected 404");
      } catch (NotFoundException nfe) {
        // PASS
      }
    } finally {
      setCurrentDatabase(prevDb, mediaType);
      dropDatabase(DB, mediaType);
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


    Map<String, String> properties = LensUtil.getHashMap("foo", "bar");
    f.getProperties().getProperty().addAll(JAXBUtils.xPropertiesFromMap(properties));
    return f;
  }

  @Test(dataProvider = "mediaTypeData")
  public void testCreateFactTable(MediaType mediaType) throws Exception {
    final String table = "testCreateFactTable";
    final String DB = dbPFX + "testCreateFactTable_DB" + mediaType.getSubtype();
    String prevDb = getCurrentDatabase(mediaType);
    createDatabase(DB, mediaType);
    setCurrentDatabase(DB, mediaType);
    createStorage("S1", mediaType);
    createStorage("S2", mediaType);
    try {

      XFactTable f = createFactTable(table);
      f.getStorageTables().getStorageTable().add(createStorageTblElement("S1", table, "HOURLY"));
      f.getStorageTables().getStorageTable().add(createStorageTblElement("S2", table, "DAILY"));
      APIResult result = target()
        .path("metastore")
        .path("facts").queryParam("sessionid", lensSessionId)
        .request(mediaType)
        .post(Entity.entity(
          new GenericEntity<JAXBElement<XFactTable>>(cubeObjectFactory.createXFactTable(f)){}, mediaType),
          APIResult.class);
      assertSuccess(result);

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
      assertTrue(cf.getUpdatePeriods().get("S1").contains(HOURLY));
      assertTrue(cf.getUpdatePeriods().get("S2").contains(DAILY));

      // Finally, drop the fact table
      result = target().path("metastore").path("facts").path(table)
        .queryParam("cascade", "true")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .delete(APIResult.class);

      assertSuccess(result);

      // Drop again, this time it should give a 404
      try {
        target().path("metastore").path("facts").path(table)
          .queryParam("cascade", "true")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .delete(APIResult.class);
        fail("Expected 404");
      } catch (NotFoundException nfe) {
        // PASS
      }
    } finally {
      setCurrentDatabase(prevDb, mediaType);
      dropDatabase(DB, mediaType);
    }
  }

  @Test(dataProvider = "mediaTypeData")
  public void testUpdateFactTable(MediaType mediaType) throws Exception {
    final String table = "testUpdateFactTable";
    final String DB = dbPFX + "testUpdateFactTable_DB" + mediaType.getSubtype();
    String prevDb = getCurrentDatabase(mediaType);
    createDatabase(DB, mediaType);
    setCurrentDatabase(DB, mediaType);
    createStorage("S1", mediaType);
    createStorage("S2", mediaType);
    createStorage("S3", mediaType);
    try {

      XFactTable f = createFactTable(table);
      f.getStorageTables().getStorageTable().add(createStorageTblElement("S1", table, "HOURLY"));
      f.getStorageTables().getStorageTable().add(createStorageTblElement("S2", table, "DAILY"));
      APIResult result = target()
        .path("metastore")
        .path("facts").queryParam("sessionid", lensSessionId)
        .request(mediaType)
        .post(Entity.entity(
            new GenericEntity<JAXBElement<XFactTable>>(cubeObjectFactory.createXFactTable(f)){}, mediaType),
          APIResult.class);
      assertSuccess(result);

      // Get the created table
      JAXBElement<XFactTable> gotFactElement = target().path("metastore/facts").path(table)
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XFactTable>>() {});
      XFactTable gotFact = gotFactElement.getValue();
      assertTrue(gotFact.getName().equalsIgnoreCase(table));
      assertEquals(gotFact.getWeight(), 10.0);
      CubeFactTable cf = JAXBUtils.cubeFactFromFactTable(gotFact);

      // Do some changes to test update
      cf.alterWeight(20.0);
      cf.alterColumn(new FieldSchema("c2", "int", "changed to int"));

      XFactTable update = JAXBUtils.factTableFromCubeFactTable(cf);
      XStorageTableElement s1Tbl = createStorageTblElement("S1", table, "HOURLY");
      s1Tbl.getTableDesc().setFieldDelimiter("#");
      update.getStorageTables().getStorageTable().add(s1Tbl);
      update.getStorageTables().getStorageTable().add(createStorageTblElement("S2", table, "MONTHLY"));
      update.getStorageTables().getStorageTable().add(createStorageTblElement("S3", table, "DAILY"));

      // Update
      result = target().path("metastore").path("facts").path(table)
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .put(Entity.entity(new GenericEntity<JAXBElement<XFactTable>>(cubeObjectFactory.createXFactTable(update)){},
            mediaType),
          APIResult.class);
      assertSuccess(result);

      // Get the updated table
      gotFactElement = target().path("metastore/facts").path(table)
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XFactTable>>() {});
      gotFact = gotFactElement.getValue();
      CubeFactTable ucf = JAXBUtils.cubeFactFromFactTable(gotFact);

      assertEquals(ucf.weight(), 20.0);
      assertTrue(ucf.getUpdatePeriods().get("S2").contains(MONTHLY));
      assertTrue(ucf.getUpdatePeriods().get("S3").contains(DAILY));

      boolean foundC2 = false;
      for (FieldSchema fs : cf.getColumns()) {
        if (fs.getName().equalsIgnoreCase("c2") && fs.getType().equalsIgnoreCase("int")) {
          foundC2 = true;
          break;
        }
      }
      assertTrue(foundC2);

      WebTarget nativeTarget = target().path("metastore").path("nativetables");
      // get all native tables
      StringList nativetables = nativeTarget.queryParam("sessionid", lensSessionId).request(mediaType).get(
        StringList.class);
      assertTrue(nativetables.getElements().contains("s1_" + table.toLowerCase()));
      assertTrue(nativetables.getElements().contains("s2_" + table.toLowerCase()));
      assertTrue(nativetables.getElements().contains("s3_" + table.toLowerCase()));

      // get native table and validate altered property
      XNativeTable alteredNativeTable = nativeTarget.path("s1_" + table).queryParam("sessionid", lensSessionId)
        .request(mediaType).get(new GenericType<JAXBElement<XNativeTable>>() {}).getValue();
      assertEquals(alteredNativeTable.getStorageDescriptor().getFieldDelimiter(), "#");

      // Finally, drop the fact table
      result = target().path("metastore").path("facts").path(table)
        .queryParam("cascade", "true")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .delete(APIResult.class);

      assertSuccess(result);

      // Drop again, this time it should give a 404
      try {
        target().path("metastore").path("facts").path(table)
          .queryParam("cascade", "true")
          .queryParam("sessionid", lensSessionId).request(mediaType)
          .delete(APIResult.class);
        fail("Expected 404");
      } catch (NotFoundException nfe) {
        // PASS
      }
    } finally {
      setCurrentDatabase(prevDb, mediaType);
      dropDatabase(DB, mediaType);
    }
  }

  @Test(dataProvider = "mediaTypeData")
  public void testFactStorages(MediaType mediaType) throws Exception {
    final String table = "testFactStorages";
    final String DB = dbPFX + "testFactStorages_DB" + mediaType.getSubtype();
    String prevDb = getCurrentDatabase(mediaType);
    createDatabase(DB, mediaType);
    setCurrentDatabase(DB, mediaType);
    createStorage("S1", mediaType);
    createStorage("S2", mediaType);
    createStorage("S3", mediaType);

    try {
      XFactTable f = createFactTable(table);
      f.getStorageTables().getStorageTable().add(createStorageTblElement("S1", table, "HOURLY"));
      f.getStorageTables().getStorageTable().add(createStorageTblElement("S2", table, "DAILY"));
      APIResult result = target()
        .path("metastore")
        .path("facts").queryParam("sessionid", lensSessionId)
        .request(mediaType)
        .post(Entity.entity(
            new GenericEntity<JAXBElement<XFactTable>>(cubeObjectFactory.createXFactTable(f)){}, mediaType),
          APIResult.class);
      assertSuccess(result);

      // Test get storages
      StringList storageList = target().path("metastore/facts").path(table).path("storages")
        .queryParam("sessionid", lensSessionId).request(mediaType).get(StringList.class);
      assertEquals(storageList.getElements().size(), 2);
      assertTrue(storageList.getElements().contains("S1"));
      assertTrue(storageList.getElements().contains("S2"));

      XStorageTableElement sTbl = createStorageTblElement("S3", table, "HOURLY", "DAILY", "MONTHLY");
      result = target().path("metastore/facts").path(table).path("storages")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(new GenericEntity<JAXBElement<XStorageTableElement>>(cubeObjectFactory
          .createXStorageTableElement(sTbl)){}, mediaType), APIResult.class);
      assertSuccess(result);

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
        .get(new GenericType<JAXBElement<XFactTable>>() {
        });
      XFactTable gotFact = gotFactElement.getValue();
      CubeFactTable ucf = JAXBUtils.cubeFactFromFactTable(gotFact);

      assertTrue(ucf.getUpdatePeriods().get("S3").contains(MONTHLY));
      assertTrue(ucf.getUpdatePeriods().get("S3").contains(DAILY));
      assertTrue(ucf.getUpdatePeriods().get("S3").contains(HOURLY));

      // Drop new storage
      result = target().path("metastore/facts").path(table).path("storages").path("S3")
        .queryParam("sessionid", lensSessionId).request(mediaType).delete(APIResult.class);
      assertSuccess(result);

      // Now S3 should not be available
      storageList = target().path("metastore/facts").path(table).path("storages")
        .queryParam("sessionid", lensSessionId).request(mediaType).get(StringList.class);
      assertEquals(storageList.getElements().size(), 2);
      assertFalse(storageList.getElements().contains("S3"));
    } finally {
      setCurrentDatabase(prevDb, mediaType);
      dropDatabase(DB, mediaType);
    }
  }

  private XPartition createPartition(String cubeTableName, Date partDate) {
    return createPartition(cubeTableName, partDate, "dt");
  }

  private XTimePartSpecElement createTimePartSpecElement(Date partDate, String timeDimension) {
    XTimePartSpecElement timePart = cubeObjectFactory.createXTimePartSpecElement();
    timePart.setKey(timeDimension);
    timePart.setValue(JAXBUtils.getXMLGregorianCalendar(HOURLY.truncate(partDate)));
    return timePart;
  }

  private XPartition createPartition(String cubeTableName, Date partDate, final String timeDimension) {

    return createPartition(cubeTableName, Lists.newArrayList(createTimePartSpecElement(partDate, timeDimension)));
  }

  private XPartition createPartition(String cubeTableName, final List<XTimePartSpecElement> timePartSpecs) {

    XPartition xp = cubeObjectFactory.createXPartition();
    xp.setLocation(new Path(new File("target").getAbsolutePath(), "part/test_part").toString());
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

  @Test(dataProvider = "mediaTypeData")
  public void testLatestDateWithInputTimeDimAbsentFromAtleastOneFactPartition(MediaType mediaType) throws Exception {

    final String dbName = dbPFX + getUniqueDbName();
    String prevDb = getCurrentDatabase(mediaType);

    try {

      // Begin: Setup
      createDatabase(dbName, mediaType);
      setCurrentDatabase(dbName, mediaType);

      String[] storages = {"S1"};
      for (String storage : storages) {
        createStorage(storage, mediaType);
      }

      // Create a cube with name testCube
      final String cubeName = "testCube";
      final XCube cube = createTestCube(cubeName);
      APIResult result = target().path("metastore").path("cubes").queryParam("sessionid", lensSessionId)
        .request(mediaType).post(Entity.entity(new GenericEntity<JAXBElement<XCube>>(
          cubeObjectFactory.createXCube(cube)){}, mediaType), APIResult.class);
      assertSuccess(result);

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

      createTestFactAndStorageTable(cubeName, storages, fact1TableName, fact1TimePartColNames, mediaType);
      createTestFactAndStorageTable(cubeName, storages, fact2TableName, fact2TimePartColNames, mediaType);

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
        .post(Entity.entity(new GenericEntity<JAXBElement<XPartition>>(cubeObjectFactory.createXPartition(xp)){},
            mediaType),
          APIResult.class);
      assertSuccess(partAddResult);

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
      setCurrentDatabase(prevDb, mediaType);
      dropDatabase(dbName, mediaType);
    }
  }
  @SuppressWarnings("deprecation")
  @Test(dataProvider = "mediaTypeData")
  public void testSkipFactStoragePartitions(MediaType mediaType) throws Exception {

    final String table = "testSkipFactStoragePartitions";
    final String DB = dbPFX + "testSkipFactStoragePartitions_DB" + mediaType.getSubtype();
    String prevDb = getCurrentDatabase(mediaType);
    createDatabase(DB, mediaType);
    setCurrentDatabase(DB, mediaType);
    createStorage("S1", mediaType);
    createStorage("S2", mediaType);

    try {
      final Date partDate = new Date();
      final XCube cube = createTestCube("testCube");
      XFactTable f = createFactTable(table);
      f.getStorageTables().getStorageTable().add(createStorageTblElement("S1", table, "HOURLY"));
      f.getStorageTables().getStorageTable().add(createStorageTblElement("S2", table, "DAILY"));
      f.getStorageTables().getStorageTable().add(createStorageTblElement("S2", table, "HOURLY"));

      APIResult result = target()
              .path("metastore")
              .path("facts").queryParam("sessionid", lensSessionId)
              .request(mediaType)
              .post(Entity.entity(
                   new GenericEntity<JAXBElement<XFactTable>>(cubeObjectFactory.createXFactTable(f)){}, mediaType),
                      APIResult.class);
      assertSuccess(result);

      APIResult partAddResult;
      // skip partitons if it starts before storage start date
      // Add two partitions one before storage start time and other one after start time
      // Add partition status will return partial
      XPartitionList partList = new XPartitionList();
      partList.getPartition().add(createPartition(table, DateUtils.addHours(partDate, 1)));
      partList.getPartition().add(createPartition(table, DateUtils.addHours(partDate, -300)));
      partAddResult = target().path("metastore/facts/").path(table).path("storages/S2/partitions")
              .queryParam("sessionid", lensSessionId).request(mediaType)
              .post(Entity.entity(new GenericEntity<JAXBElement<XPartitionList>>(
                      cubeObjectFactory.createXPartitionList(partList)) {
              }, mediaType), APIResult.class);
      assertEquals(partAddResult.getStatus(), Status.PARTIAL);
    } finally {
      setCurrentDatabase(prevDb, mediaType);
      dropDatabase(DB, mediaType);
    }
  }


  @SuppressWarnings("deprecation")
  @Test(dataProvider = "mediaTypeData")
  public void testFactStoragePartitions(MediaType mediaType) throws Exception {
    final String table = "testFactStoragePartitions";
    final String DB = dbPFX + "testFactStoragePartitions_DB" + mediaType.getSubtype();
    String prevDb = getCurrentDatabase(mediaType);
    createDatabase(DB, mediaType);
    setCurrentDatabase(DB, mediaType);
    createStorage("S1", mediaType);
    createStorage("S2", mediaType);

    try {

      final XCube cube = createTestCube("testCube");
      target().path("metastore").path("cubes").queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(new GenericEntity<JAXBElement<XCube>>(cubeObjectFactory.createXCube(cube)) {
          }, mediaType),
          APIResult.class);

      XFactTable f = createFactTable(table);
      f.getStorageTables().getStorageTable().add(createStorageTblElement("S1", table, "HOURLY"));
      f.getStorageTables().getStorageTable().add(createStorageTblElement("S2", table, "DAILY"));
      f.getStorageTables().getStorageTable().add(createStorageTblElement("S2", table, "HOURLY"));
      APIResult result = target()
        .path("metastore")
        .path("facts").queryParam("sessionid", lensSessionId)
        .request(mediaType)
        .post(Entity.entity(
            new GenericEntity<JAXBElement<XFactTable>>(cubeObjectFactory.createXFactTable(f)){}, mediaType),
          APIResult.class);
      assertSuccess(result);

      APIResult partAddResult;
      // Add null partition
      Response resp = target().path("metastore/facts/").path(table).path("storages/S2/partition")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(null);
      Assert.assertEquals(resp.getStatus(), 400);

      // Add wrong partition
      final Date partDate = new Date();
      XPartition xp2 = createPartition(table, partDate);
      xp2.getTimePartitionSpec().getPartSpecElement()
        .add(createTimePartSpecElement(partDate, "non_existant_time_part"));
      partAddResult = target().path("metastore/facts/").path(table).path("storages/S2/partition")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(new GenericEntity<JAXBElement<XPartition>>(cubeObjectFactory.createXPartition(xp2)){},
          mediaType), APIResult.class);
      assertEquals(partAddResult.getStatus(), Status.FAILED);
      assertEquals(partAddResult.getMessage(), "No timeline found for fact=testFactStoragePartitions, storage=S2, "
        + "update period=HOURLY, partition column=non_existant_time_part.");
      // Add a partition
      XPartition xp = createPartition(table, partDate);
      partAddResult = target().path("metastore/facts/").path(table).path("storages/S2/partition")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(new GenericEntity<JAXBElement<XPartition>>(cubeObjectFactory.createXPartition(xp)){},
          mediaType), APIResult.class);
      assertSuccess(partAddResult);

      // add same should fail
      partAddResult = target().path("metastore/facts/").path(table).path("storages/S2/partition")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(new GenericEntity<JAXBElement<XPartition>>(cubeObjectFactory.createXPartition(xp)){},
          mediaType), APIResult.class);
      assertEquals(partAddResult.getStatus(), Status.FAILED);

      xp.setLocation(xp.getLocation() + "/a/b/c");
      APIResult partUpdateResult = target().path("metastore/facts/").path(table).path("storages/S2/partition")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .put(Entity.entity(new GenericEntity<JAXBElement<XPartition>>(cubeObjectFactory.createXPartition(xp)){},
          mediaType), APIResult.class);
      assertSuccess(partUpdateResult);

      JAXBElement<XPartitionList> partitionsElement = target().path("metastore/facts").path(table)
        .path("storages/S2/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XPartitionList>>() {
        });

      XPartitionList partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getPartition().size(), 1);
      XPartition readPartition = partitions.getPartition().get(0);
      assertEquals(readPartition.getLocation(), xp.getLocation());
      assertEquals(readPartition.getTimePartitionSpec(), xp.getTimePartitionSpec());
      assertEquals(readPartition.getNonTimePartitionSpec(), xp.getNonTimePartitionSpec());
      assertNotNull(readPartition.getFullPartitionSpec());
      XTimePartSpecElement timePartSpec = readPartition.getTimePartitionSpec().getPartSpecElement().iterator().next();
      XPartSpecElement fullPartSpec = readPartition.getFullPartitionSpec().getPartSpecElement().iterator().next();
      assertEquals(timePartSpec.getKey(), fullPartSpec.getKey());
      assertEquals(UpdatePeriod.valueOf(xp.getUpdatePeriod().name()).format(JAXBUtils.getDateFromXML(
        timePartSpec.getValue())), fullPartSpec.getValue());
      DateTime date =
        target().path("metastore/cubes").path("testCube").path("latestdate").queryParam("timeDimension", "dt")
          .queryParam("sessionid", lensSessionId).request(mediaType).get(DateTime.class);

      partDate.setMinutes(0);
      partDate.setSeconds(0);
      partDate.setTime(partDate.getTime() - partDate.getTime() % 1000);
      assertEquals(date.getDate(), partDate);
      // add two partitions, one of them already added. result should be partial
      XPartitionList parts = new XPartitionList();
      parts.getPartition().add(xp);
      parts.getPartition().add(createPartition(table, DateUtils.addHours(partDate, 1)));
      partAddResult = target().path("metastore/facts/").path(table).path("storages/S2/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(new GenericEntity<JAXBElement<XPartitionList>>(
          cubeObjectFactory.createXPartitionList(parts)){}, mediaType), APIResult.class);
      assertEquals(partAddResult.getStatus(), Status.PARTIAL);

      // Drop the partitions
      APIResult dropResult = target().path("metastore/facts").path(table).path("storages/S2/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .delete(APIResult.class);

      assertSuccess(dropResult);

      // Verify partition was dropped
      partitionsElement = target().path("metastore/facts").path(table).path("storages/S2/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XPartitionList>>() {
        });

      partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getPartition().size(), 0);
      // add null in batch
      resp = target().path("metastore/facts/").path(table).path("storages/S2/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(null);
      Assert.assertEquals(resp.getStatus(), 400);

      // Try adding in batch, but to a wrong endpoint
      resp = target().path("metastore/facts/").path(table).path("storages/S2/partition")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(new GenericEntity<JAXBElement<XPartitionList>>(cubeObjectFactory
          .createXPartitionList(toXPartitionList(xp))) {
        }, mediaType));
      assertXMLError(resp, mediaType);


      // Try adding in batch, but provide just an XPartition
      resp = target().path("metastore/facts/").path(table).path("storages/S2/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(new GenericEntity<JAXBElement<XPartition>>(cubeObjectFactory.createXPartition(xp)){},
          mediaType));
      if (mediaType.equals(MediaType.APPLICATION_XML_TYPE)) {
        assertXMLError(resp, mediaType);
      } else {
        // for json input, XPartitionList is getting created
        assertEquals(resp.getStatus(), 200);
      }

      // Try adding in batch with one partition being wrong wrt partition column.
      partAddResult = target().path("metastore/facts/").path(table).path("storages/S2/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(new GenericEntity<JAXBElement<XPartitionList>>(cubeObjectFactory
          .createXPartitionList(toXPartitionList(xp2))){}, mediaType),
          APIResult.class);
      assertEquals(partAddResult.getStatus(), Status.FAILED);
      assertEquals(partAddResult.getMessage(), "No timeline found for fact=testFactStoragePartitions, storage=S2, "
        + "update period=HOURLY, partition column=non_existant_time_part.");
      // Add in batch
      partAddResult = target().path("metastore/facts/").path(table).path("storages/S2/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(new GenericEntity<JAXBElement<XPartitionList>>(cubeObjectFactory
            .createXPartitionList(toXPartitionList(xp))) {
          }, mediaType),
          APIResult.class);
      assertSuccess(partAddResult);

      // Verify partition was added
      partitionsElement = target().path("metastore/facts").path(table).path("storages/S2/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XPartitionList>>() {});

      partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getPartition().size(), 1);

      // Drop again by values
      String[] val = new String[]{HOURLY.format(partDate)};
      dropResult = target().path("metastore/facts").path(table).path("storages/S2/partition")
        .queryParam("values", StringUtils.join(val, ","))
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .delete(APIResult.class);
      assertSuccess(dropResult);

      // Verify partition was dropped
      partitionsElement = target().path("metastore/facts").path(table).path("storages/S2/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XPartitionList>>() {});

      partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getPartition().size(), 0);
    } finally {
      setCurrentDatabase(prevDb, mediaType);
      dropDatabase(DB, mediaType);
    }
  }

  @Test(dataProvider = "mediaTypeData")
  public void testDimStoragePartitions(MediaType mediaType) throws Exception {
    final String table = "testDimStoragePartitions";
    final String DB = dbPFX + "testDimStoragePartitions_DB" + mediaType.getSubtype();
    String prevDb = getCurrentDatabase(mediaType);
    createDatabase(DB, mediaType);
    setCurrentDatabase(DB, mediaType);
    createStorage("S1", mediaType);
    createStorage("S2", mediaType);
    createStorage("test", mediaType);

    try {
      createDimTable(table, mediaType);
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
        .post(Entity.entity(new GenericEntity<JAXBElement<XPartition>>(cubeObjectFactory.createXPartition(xp)){},
          mediaType), APIResult.class);
      assertSuccess(partAddResult);

      // create call for same
      partAddResult = target().path("metastore/dimtables/").path(table).path("storages/test/partition")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(new GenericEntity<JAXBElement<XPartition>>(cubeObjectFactory.createXPartition(xp)){},
          mediaType), APIResult.class);
      assertEquals(partAddResult.getStatus(), Status.FAILED);


      xp.setLocation(xp.getLocation() + "/a/b/c");
      APIResult partUpdateResult = target().path("metastore/dimtables/").path(table).path("storages/test/partition")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .put(Entity.entity(new GenericEntity<JAXBElement<XPartition>>(cubeObjectFactory.createXPartition(xp)){},
          mediaType), APIResult.class);
      assertSuccess(partUpdateResult);

      JAXBElement<XPartitionList> partitionsElement = target().path("metastore/dimtables").path(table)
        .path("storages/test/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XPartitionList>>() {});

      XPartitionList partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getPartition().size(), 2);

      assertEquals(partitions.getPartition().get(0).getLocation(), xp.getLocation());
      assertEquals(partitions.getPartition().get(1).getLocation(), xp.getLocation());

      // one is latest partition.
      assertTrue(partitions.getPartition().get(0).getTimePartitionSpec() == null
        || partitions.getPartition().get(1).getTimePartitionSpec() == null);
      XPartition postedPartition, latestPartition;
      if (partitions.getPartition().get(0).getTimePartitionSpec() == null) {
        postedPartition = partitions.getPartition().get(1);
        latestPartition = partitions.getPartition().get(0);
      } else {
        postedPartition = partitions.getPartition().get(0);
        latestPartition = partitions.getPartition().get(1);
      }

      assertEquals(postedPartition.getTimePartitionSpec(), xp.getTimePartitionSpec());
      assertEquals(postedPartition.getNonTimePartitionSpec(), xp.getNonTimePartitionSpec());
      assertNotNull(postedPartition.getFullPartitionSpec());

      XTimePartSpecElement timePartSpec = postedPartition.getTimePartitionSpec().getPartSpecElement().iterator().next();
      XPartSpecElement fullPartSpec = postedPartition.getFullPartitionSpec().getPartSpecElement().iterator().next();
      assertEquals(timePartSpec.getKey(), fullPartSpec.getKey());
      assertEquals(UpdatePeriod.valueOf(xp.getUpdatePeriod().name()).format(JAXBUtils.getDateFromXML(
        timePartSpec.getValue())), fullPartSpec.getValue());

      assertNull(latestPartition.getTimePartitionSpec());
      assertNull(latestPartition.getNonTimePartitionSpec());
      assertEquals(latestPartition.getFullPartitionSpec().getPartSpecElement().get(0).getValue(),
        "latest");

      // create should give partial now:
      XPartition xp2 = createPartition(table, DateUtils.addHours(partDate, 1));
      XPartitionList parts = new XPartitionList();
      parts.getPartition().add(xp);
      parts.getPartition().add(xp2);
      partAddResult = target().path("metastore/dimtables/").path(table).path("storages/test/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(new GenericEntity<JAXBElement<XPartitionList>>(
          cubeObjectFactory.createXPartitionList(parts)){}, mediaType), APIResult.class);
      assertEquals(partAddResult.getStatus(), Status.PARTIAL);

      // Drop the partitions
      APIResult dropResult = target().path("metastore/dimtables").path(table).path("storages/test/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .delete(APIResult.class);

      assertSuccess(dropResult);

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

      // Try adding in batch, but to a wrong endpoint
      resp = target().path("metastore/dimtables/").path(table).path("storages/test/partition")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(
          new GenericEntity<JAXBElement<XPartitionList>>(cubeObjectFactory.createXPartitionList(toXPartitionList(xp)))
          {}, mediaType));
      assertXMLError(resp, mediaType);

      // Try adding in batch, but provide just an XPartition
      resp = target().path("metastore/dimtables/").path(table).path("storages/test/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(new GenericEntity<JAXBElement<XPartition>>(cubeObjectFactory.createXPartition(xp)){},
          mediaType));
      if (mediaType.equals(MediaType.APPLICATION_XML_TYPE)) {
        assertXMLError(resp, mediaType);
      } else {
        // for json input, XPartitionList is getting created
        assertEquals(resp.getStatus(), 200);
      }
      // Add in batch
      partAddResult = target().path("metastore/dimtables/").path(table).path("storages/test/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(
          new GenericEntity<JAXBElement<XPartitionList>>(cubeObjectFactory.createXPartitionList(toXPartitionList(xp)))
          {},
          mediaType),
          APIResult.class);
      assertSuccess(partAddResult);

      // Verify partition was added
      partitionsElement = target().path("metastore/dimtables").path(table).path("storages/test/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XPartitionList>>() {});

      partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getPartition().size(), 2);

      // Drop again by values
      String[] val = new String[]{HOURLY.format(partDate)};
      dropResult = target().path("metastore/dimtables").path(table).path("storages/test/partition")
        .queryParam("values", StringUtils.join(val, ","))
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .delete(APIResult.class);
      assertSuccess(dropResult);

      // Verify partition was dropped
      partitionsElement = target().path("metastore/dimtables").path(table).path("storages/test/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XPartitionList>>() {});
      partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getPartition().size(), 0);

      // add again, this time we'll drop by filter
      partAddResult = target().path("metastore/dimtables/").path(table).path("storages/test/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(new GenericEntity<JAXBElement<XPartitionList>>(
            cubeObjectFactory.createXPartitionList(toXPartitionList(xp))){}, mediaType),
          APIResult.class);
      assertSuccess(partAddResult);

      // drop by filter
      dropResult = target().path("metastore/dimtables").path(table).path("storages/test/partitions")
        .queryParam("filter", "dt='" + HOURLY.format(partDate) + "'")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .delete(APIResult.class);
      assertSuccess(dropResult);

      // Verify partition was dropped
      partitionsElement = target().path("metastore/dimtables").path(table).path("storages/test/partitions")
        .queryParam("sessionid", lensSessionId).request(mediaType)
        .get(new GenericType<JAXBElement<XPartitionList>>() {});
      partitions = partitionsElement.getValue();
      assertNotNull(partitions);
      assertEquals(partitions.getPartition().size(), 0);
    } finally {
      setCurrentDatabase(prevDb, mediaType);
      dropDatabase(DB, mediaType);
    }
  }

  private void assertXMLError(Response resp, MediaType mt) {
    assertEquals(resp.getStatus(), 400);
    if (mt.equals(MediaType.APPLICATION_XML_TYPE)) {
      LensAPIResult entity = resp.readEntity(LensAPIResult.class);
      assertTrue(entity.isErrorResult());
      assertEquals(entity.getLensErrorTO().getCode(), LensCommonErrorCode.INVALID_XML_ERROR.getValue());
      assertTrue(entity.getLensErrorTO().getMessage().contains("unexpected element"));
    }
  }

  private XPartitionList toXPartitionList(final XPartition... xps) {
    XPartitionList ret = new XPartitionList();
    Collections.addAll(ret.getPartition(), xps);
    return ret;
  }

  @Test(dataProvider = "mediaTypeData")
  public void testNativeTables(MediaType mediaType) throws Exception {
    final String DB = dbPFX + "test_native_tables" + mediaType.getSubtype();
    String prevDb = getCurrentDatabase(mediaType);
    createDatabase(DB, mediaType);
    setCurrentDatabase(DB, mediaType);

    try {
      // create hive table
      String tableName = "test_simple_table";
      SessionState.get().setCurrentDatabase(DB);
      LensServerTestUtil.createHiveTable(tableName, new HashMap<String, String>());

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

      // test for lens.session.metastore.exclude.cubetables.from.nativetables session config
      String cubeTableName = "test_cube_table";
      Map<String, String> params = new HashMap<String, String>();
      params.put(MetastoreConstants.TABLE_TYPE_KEY, CubeTableType.CUBE.name());
      LensServerTestUtil.createHiveTable(cubeTableName, params);

      // Test for excluding cube tables
      nativetables = target.queryParam("sessionid", lensSessionId).queryParam("dbName", DB)
        .queryParam("dbOption", "current").request(mediaType).get(StringList.class);
      assertEquals(nativetables.getElements().size(), 1);
      assertEquals(nativetables.getElements().get(0), tableName);

      // Test for not excluding cube tables
      Map<String, String> sessionConf = new HashMap<String, String>();
      sessionConf.put(LensConfConstants.EXCLUDE_CUBE_TABLES, "false");
      LensSessionHandle lensSessionId2 =
        metastoreService.openSession("foo", "bar", sessionConf);
      nativetables = target.queryParam("sessionid", lensSessionId2).queryParam("dbName", DB)
        .queryParam("dbOption", "current").request(mediaType).get(StringList.class);
      assertEquals(nativetables.getElements().size(), 2);
      assertTrue(nativetables.getElements().contains(tableName));
      assertTrue(nativetables.getElements().contains(cubeTableName));

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
          lensSessionId).request(mediaType).post(Entity.entity(new GenericEntity<JAXBElement<XCube>>(element){},
          mediaType), APIResult.class);
      assertSuccess(result);

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
      dropDatabase(DB, mediaType);
      setCurrentDatabase(prevDb, mediaType);
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

  @Test(dataProvider = "mediaTypeData")
  public void testFlattenedView(MediaType mediaType) throws Exception {
    final String DB = dbPFX + "test_flattened_view" + mediaType.getSubtype();
    String prevDb = getCurrentDatabase(mediaType);
    createDatabase(DB, mediaType);
    setCurrentDatabase(DB, mediaType);

    try {
      // Create the tables
      XCube flatTestCube = createTestCube("flatTestCube");
      // Create cube
      final WebTarget cubeTarget = target().path("metastore").path("cubes");
      APIResult result =
        cubeTarget.queryParam("sessionid", lensSessionId).request(mediaType)
          .post(Entity.entity(new GenericEntity<JAXBElement<XCube>>(cubeObjectFactory.createXCube(flatTestCube)){},
            mediaType), APIResult.class);
      assertNotNull(result);
      assertSuccess(result);

      // create chained dimensions - testdim and testdim2
      createdChainedDimensions(mediaType);

      // Now test flattened view
      final WebTarget flatCubeTarget = target().path("metastore").path("flattened").path("flattestcube");
      XFlattenedColumns flattenedColumns;
      JAXBElement<XFlattenedColumns> actualElement = flatCubeTarget.queryParam("sessionid", lensSessionId).request()
        .get(new GenericType<JAXBElement<XFlattenedColumns>>() {});
      flattenedColumns = actualElement.getValue();
      assertNotNull(flattenedColumns);

      List<XFlattenedColumn> columns = flattenedColumns.getFlattenedColumn();
      assertNotNull(columns);
      assertTrue(!columns.isEmpty());

      Set<String> tables = new HashSet<>();
      Set<String> colSet = new HashSet<>();
      populateActualTablesAndCols(columns, tables, colSet);

      assertEquals(tables, Sets.newHashSet("flattestcube", "testdim", "testdim2"));
      assertEquals(colSet, Sets.newHashSet(
        "flattestcube.msr1",
        "flattestcube.msr2",
        "flattestcube.dim1",
        "flattestcube.dim2",
        "flattestcube.testdim2col2",
        "flattestcube.dim4",
        "flattestcube.expr1",
        "flattestcube.expr2",
        "chain1-testdim.col2",
        "chain1-testdim.col1",
        "chain1-testdim.col3",
        "chain1-testdim.col4",
        "chain1-testdim.col5",
        "chain1-testdim.dimexpr",
        "dim2chain-testdim2.col2",
        "dim2chain-testdim2.col1",
        "dim2chain-testdim2.dimexpr"
      ));

      // Now test flattened view for dimension
      final WebTarget flatDimTarget = target().path("metastore").path("flattened").path("testdim");
      actualElement = flatDimTarget.queryParam("sessionid", lensSessionId).request()
        .get(new GenericType<JAXBElement<XFlattenedColumns>>() {});
      flattenedColumns = actualElement.getValue();
      assertNotNull(flattenedColumns);

      columns = flattenedColumns.getFlattenedColumn();
      assertNotNull(columns);
      assertTrue(!columns.isEmpty());

      tables = new HashSet<>();
      colSet = new HashSet<>();
      populateActualTablesAndCols(columns, tables, colSet);

      assertEquals(tables, Sets.newHashSet("testdim", "testdim2"));
      assertEquals(colSet, Sets.newHashSet(
        "testdim.col2",
        "testdim.col1",
        "testdim.col3",
        "testdim.col4",
        "testdim.col5",
        "testdim.dimexpr",
        "chain1-testdim2.col2",
        "chain1-testdim2.col1",
        "chain1-testdim2.dimexpr"
      ));

    } finally {
      dropDatabase(DB, mediaType);
      setCurrentDatabase(prevDb, mediaType);
    }
  }

  private void createTestFactAndStorageTable(final String cubeName, final String[] storages, final String tableName,
    final String[] timePartColNames, MediaType mediaType) {

    // Create a fact table object linked to cubeName
    XFactTable f = createFactTable(tableName, cubeName);
    // Create a storage tables
    for(String storage: storages) {
      f.getStorageTables().getStorageTable()
        .add(createStorageTblElement(storage, tableName, timePartColNames, "HOURLY"));
    }

    // Call API to create a fact table and storage table
    APIResult result = target()
      .path("metastore")
      .path("facts").queryParam("sessionid", lensSessionId)
      .request(mediaType)
      .post(Entity.entity(
          new GenericEntity<JAXBElement<XFactTable>>(cubeObjectFactory.createXFactTable(f)) {
          }, mediaType),
        APIResult.class);
    assertSuccess(result);
  }

  private String getUniqueDbName() {
    // hyphens replaced with underscore to create a valid db name
    return java.util.UUID.randomUUID().toString().replaceAll("-", "_");
  }
}

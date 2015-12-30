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

package org.apache.lens.cube.metadata;

import static org.apache.lens.cube.metadata.DateFactory.*;
import static org.apache.lens.cube.metadata.MetastoreUtil.*;
import static org.apache.lens.cube.metadata.UpdatePeriod.*;
import static org.apache.lens.server.api.util.LensUtil.getHashMap;

import static org.testng.Assert.*;

import java.util.*;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.ExprColumn.ExprSpec;
import org.apache.lens.cube.metadata.ReferencedDimAtrribute.ChainRefCol;
import org.apache.lens.cube.metadata.timeline.EndsAndHolesPartitionTimeline;
import org.apache.lens.cube.metadata.timeline.PartitionTimeline;
import org.apache.lens.cube.metadata.timeline.StoreAllPartitionTimeline;
import org.apache.lens.cube.metadata.timeline.TestPartitionTimelines;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.util.LensUtil;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.*;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class TestCubeMetastoreClient {

  private static CubeMetastoreClient client;

  // cube members
  private static Cube cube;
  private static Cube cubeWithProps;
  private static DerivedCube derivedCube;
  private static DerivedCube derivedCubeWithProps;
  private static Set<String> measures;
  private static Set<String> dimensions;
  private static Set<CubeMeasure> cubeMeasures;
  private static Set<CubeDimAttribute> cubeDimensions;
  private static Set<UpdatePeriod> hourlyAndDaily = Sets.newHashSet(HOURLY, DAILY);
  private static final String CUBE_NAME = "testMetastoreCube";
  private static final String CUBE_NAME_WITH_PROPS = "testMetastoreCubeWithProps";
  private static final String DERIVED_CUBE_NAME = "derivedTestMetastoreCube";
  private static final String DERIVED_CUBE_NAME_WITH_PROPS = "derivedTestMetastoreCubeWithProps";
  private static final Map<String, String> CUBE_PROPERTIES = new HashMap<>();
  private static HiveConf conf = new HiveConf(TestCubeMetastoreClient.class);
  private static FieldSchema dtPart = new FieldSchema(getDatePartitionKey(), serdeConstants.STRING_TYPE_NAME,
    "date partition");
  private static ArrayList<String> datePartKeySingleton = Lists.newArrayList(getDatePartitionKey());
  private static ArrayList<FieldSchema> datePartSingleton = Lists.newArrayList(getDatePartition());
  private static Map<String, String> emptyHashMap = ImmutableMap.copyOf(LensUtil.<String, String>getHashMap());
  private static String c1 = "C1";
  private static String c2 = "C2";
  private static String c3 = "C3";
  private static String c4 = "C4";
  private static Dimension zipDim, cityDim, stateDim, countryDim;
  private static Set<CubeDimAttribute> zipAttrs = new HashSet<>();
  private static Set<CubeDimAttribute> cityAttrs = new HashSet<>();
  private static Set<CubeDimAttribute> stateAttrs = new HashSet<>();
  private static Set<CubeDimAttribute> countryAttrs = new HashSet<>();
  private static Set<ExprColumn> cubeExpressions = new HashSet<>();
  private static Set<JoinChain> joinChains = new HashSet<>();
  private static Set<ExprColumn> dimExpressions = new HashSet<>();


  /**
   * Get the date partition as field schema
   *
   * @return FieldSchema
   */
  public static FieldSchema getDatePartition() {
    return dtPart;
  }

  /**
   * Get the date partition key
   *
   * @return String
   */
  public static String getDatePartitionKey() {
    return StorageConstants.DATE_PARTITION_KEY;
  }

  private static HashMap<String, Date> getTimePartitionByOffsets(Object... args) {
    for (int i = 1; i < args.length; i += 2) {
      if (args[i] instanceof Integer) {
        args[i] = getDateWithOffset(HOURLY, (Integer) args[i]);
      }
    }
    return getHashMap(args);
  }

  @BeforeClass
  public static void setup() throws HiveException, AlreadyExistsException, LensException {
    SessionState.start(conf);
    Database database = new Database();
    database.setName(TestCubeMetastoreClient.class.getSimpleName());
    Hive.get(conf).createDatabase(database);
    SessionState.get().setCurrentDatabase(TestCubeMetastoreClient.class.getSimpleName());
    client = CubeMetastoreClient.getInstance(conf);
    defineCube(CUBE_NAME, CUBE_NAME_WITH_PROPS, DERIVED_CUBE_NAME, DERIVED_CUBE_NAME_WITH_PROPS);
    defineUberDims();
  }

  @AfterClass
  public static void teardown() throws Exception {
    // Drop the cube
    client.dropCube(CUBE_NAME);
    client = CubeMetastoreClient.getInstance(conf);
    assertFalse(client.tableExists(CUBE_NAME));

    Hive.get().dropDatabase(TestCubeMetastoreClient.class.getSimpleName(), true, true, true);
    CubeMetastoreClient.close();
  }

  private static void defineCube(String cubeName, String cubeNameWithProps, String derivedCubeName,
    String derivedCubeNameWithProps) throws LensException {
    cubeMeasures = new HashSet<>();
    cubeMeasures.add(new ColumnMeasure(
      new FieldSchema("msr1", "int", "first measure"), null, null, null, null, null, null, null, 0.0, 9999.0));
    cubeMeasures.add(new ColumnMeasure(
      new FieldSchema("msr2", "float", "second measure"), "Measure2", null, "SUM", "RS"));
    cubeMeasures.add(new ColumnMeasure(
      new FieldSchema("msr3", "double", "third measure"), "Measure3", null, "MAX", null));
    cubeMeasures.add(new ColumnMeasure(
      new FieldSchema("msr4", "bigint", "fourth measure"), "Measure4", null, "COUNT", null));
    cubeMeasures.add(new ColumnMeasure(
      new FieldSchema("msrstarttime", "int", "measure with start time"),
      "Measure With Starttime", null, null, null, NOW, null, null, 0.0, 999999.0));
    cubeMeasures.add(new ColumnMeasure(
      new FieldSchema("msrendtime", "float", "measure with end time"),
      "Measure With Endtime", null, "SUM", "RS", NOW, NOW, null));
    cubeMeasures.add(new ColumnMeasure(
      new FieldSchema("msrcost", "double", "measure with cost"), "Measure With cost",
      null, "MAX", null, NOW, NOW, 100.0));
    cubeMeasures.add(new ColumnMeasure(
      new FieldSchema("msrcost2", "bigint", "measure with cost"),
      "Measure With cost2", null, "MAX", null, null, null, 100.0, 0.0, 999999999999999999999999999.0));
    Set<CubeMeasure> dummyMeasure = Sets.newHashSet();
    for (int i = 0; i < 5000; i++) {
      dummyMeasure.add(new ColumnMeasure(new FieldSchema("dummy_msr" + i, "bigint", "dummy measure " + i),
        "", null, "SUM", null, null, null, 100.0, 0.0, 999999999999999999999999999.0));
    }
    cubeDimensions = new HashSet<>();
    List<CubeDimAttribute> locationHierarchy = new ArrayList<>();
    locationHierarchy.add(new ReferencedDimAtrribute(new FieldSchema("zipcode", "int", "zip"), "Zip refer",
      new TableReference("zipdim", "zipcode")));
    locationHierarchy.add(new ReferencedDimAtrribute(new FieldSchema("cityid", "int", "city"), "City refer",
      new TableReference("citydim", "id")));
    locationHierarchy.add(new ReferencedDimAtrribute(new FieldSchema("stateid", "int", "state"), "State refer",
      new TableReference("statedim", "id")));
    locationHierarchy.add(new ReferencedDimAtrribute(new FieldSchema("countryid", "int", "country"), "Country refer",
      new TableReference("countrydim", "id")));
    List<String> regions = Arrays.asList("APAC", "EMEA", "USA");
    locationHierarchy.add(new BaseDimAttribute(new FieldSchema("regionname", "string", "region"), "regionname", null,
      null, null, null, regions));
    cubeDimensions.add(new HierarchicalDimAttribute("location", "location hierarchy", locationHierarchy));
    cubeDimensions.add(new BaseDimAttribute(new FieldSchema("dim1", "string", "basedim")));
    cubeDimensions.add(new ReferencedDimAtrribute(new FieldSchema("dim2", "id", "ref dim"), "Dim2 refer",
      new TableReference("testdim2", "id")));
    Set<CubeDimAttribute> dummyDimAttributes = Sets.newHashSet();
    for (int i = 0; i < 5000; i++) {
      dummyDimAttributes.add(new BaseDimAttribute(new FieldSchema("dummy_dim" + i, "string", "dummy dim " + i),
        "dummy_dim" + i, null, null, null, null, regions));
    }

    ExprSpec expr1 = new ExprSpec();
    expr1.setExpr("avg(msr1 + msr2)");
    ExprSpec expr2 = new ExprSpec();
    expr2.setExpr("avg(msr2 + msr1)");
    ExprSpec expr3 = new ExprSpec();
    expr3.setExpr("avg(msr1 + msr2 - msr1 + msr1)");
    cubeExpressions.add(new ExprColumn(new FieldSchema("msr5", "double", "fifth measure"), "Avg msr5",
      expr1, expr2, expr3));
    expr1 = new ExprSpec();
    expr1.setExpr("avg(msr1 + msr2)");
    cubeExpressions.add(new ExprColumn(new FieldSchema("msr5start", "double", "expr measure with start and end times"),
      "AVG of SUM", expr1));
    expr1 = new ExprSpec();
    expr1.setExpr("dim1 != 'x' AND dim2 != 10 ");
    expr2 = new ExprSpec();
    expr2.setExpr("dim1 | dim2 AND dim2 = 'XYZ'");
    cubeExpressions.add(new ExprColumn(new FieldSchema("booleancut", "boolean", "a boolean expression"), "Boolean Cut",
      expr1, expr2));
    expr1 = new ExprSpec();
    expr1.setExpr("substr(dim1, 3)");
    expr2 = new ExprSpec();
    expr2.setExpr("substr(dim2, 3)");
    cubeExpressions.add(new ExprColumn(new FieldSchema("substrexpr", "string", "a subt string expression"),
      "SUBSTR EXPR", expr1, expr2));

    List<CubeDimAttribute> locationHierarchyWithStartTime = new ArrayList<>();
    locationHierarchyWithStartTime.add(new ReferencedDimAtrribute(new FieldSchema("zipcode2", "int", "zip"),
      "Zip refer2", new TableReference("zipdim", "zipcode"), NOW, NOW,
      100.0, true, 1000L));
    locationHierarchyWithStartTime.add(new ReferencedDimAtrribute(new FieldSchema("cityid2", "int", "city"),
      "City refer2", new TableReference("citydim", "id"), NOW, null, null));
    locationHierarchyWithStartTime.add(new ReferencedDimAtrribute(new FieldSchema("stateid2", "int", "state"),
      "state refer2", new TableReference("statedim", "id"), NOW, null, 100.0));
    locationHierarchyWithStartTime.add(new ReferencedDimAtrribute(new FieldSchema("countryid2", "int", "country"),
      "Country refer2", new TableReference("countrydim", "id"), null, null, null));
    locationHierarchyWithStartTime.add(new BaseDimAttribute(new FieldSchema("regionname2", "string", "region"),
      "regionname2", null, null, null, null, regions));

    cubeDimensions
      .add(new HierarchicalDimAttribute("location2", "localtion hierarchy2", locationHierarchyWithStartTime));
    cubeDimensions.add(new BaseDimAttribute(new FieldSchema("dim1startTime", "string", "basedim"),
      "Dim With starttime", NOW, null, 100.0));
    cubeDimensions.add(new ReferencedDimAtrribute(new FieldSchema("dim2start", "string", "ref dim"),
      "Dim2 with starttime", new TableReference("testdim2", "id"),
      NOW, NOW, 100.0));

    List<TableReference> multiRefs = new ArrayList<>();
    multiRefs.add(new TableReference("testdim2", "id"));
    multiRefs.add(new TableReference("testdim3", "id"));
    multiRefs.add(new TableReference("testdim4", "id"));

    cubeDimensions.add(new ReferencedDimAtrribute(new FieldSchema("dim3", "string", "multi ref dim"), "Dim3 refer",
      multiRefs));
    cubeDimensions.add(new ReferencedDimAtrribute(new FieldSchema("dim3start", "string", "multi ref dim"),
      "Dim3 with starttime", multiRefs, NOW, null, 100.0));

    cubeDimensions.add(new BaseDimAttribute(new FieldSchema("region", "string", "region dim"), "region", null, null,
      null, null, regions));
    cubeDimensions.add(new BaseDimAttribute(new FieldSchema("regionstart", "string", "region dim"),
      "Region with starttime", NOW, null, 100.0, null, regions));
    JoinChain zipCity = new JoinChain("cityFromZip", "Zip City", "zip city desc");
    List<TableReference> chain = new ArrayList<>();
    chain.add(new TableReference(cubeName, "zipcode"));
    chain.add(new TableReference("zipdim", "zipcode"));
    chain.add(new TableReference("zipdim", "cityid"));
    chain.add(new TableReference("citydim", "id"));
    zipCity.addPath(chain);
    List<TableReference> chain2 = new ArrayList<>();
    chain2.add(new TableReference(cubeName, "zipcode2"));
    chain2.add(new TableReference("zipdim", "zipcode"));
    chain2.add(new TableReference("zipdim", "cityid"));
    chain2.add(new TableReference("citydim", "id"));
    zipCity.addPath(chain2);
    joinChains.add(zipCity);
    JoinChain cityChain = new JoinChain("city", "Cube City", "cube city desc");
    chain = new ArrayList<>();
    chain.add(new TableReference(cubeName, "cityid"));
    chain.add(new TableReference("citydim", "id"));
    cityChain.addPath(chain);
    joinChains.add(cityChain);
    cubeDimensions.add(new ReferencedDimAtrribute(new FieldSchema("zipcityname", "string", "zip city name"),
      "Zip city name", "cityFromZip", "name", null, null, null));
    cubeMeasures.addAll(dummyMeasure);
    cubeDimensions.addAll(dummyDimAttributes);
    cube = new Cube(cubeName, cubeMeasures, cubeDimensions, cubeExpressions, joinChains, emptyHashMap, 0.0);
    measures = Sets.newHashSet("msr1", "msr2", "msr3");
    for(CubeMeasure measure: dummyMeasure) {
      measures.add(measure.getName());
    }
    dimensions = Sets.newHashSet("dim1", "dim2", "dim3");
    for(CubeDimAttribute dimAttribute: dummyDimAttributes) {
      dimensions.add(dimAttribute.getName());
    }
    derivedCube = new DerivedCube(derivedCubeName, measures, dimensions, cube);

    CUBE_PROPERTIES.put(MetastoreUtil.getCubeTimedDimensionListKey(cubeNameWithProps), "dt,mydate");
    CUBE_PROPERTIES.put(MetastoreConstants.CUBE_ALL_FIELDS_QUERIABLE, "false");
    CUBE_PROPERTIES.put("cube.custom.prop", "myval");
    cubeWithProps = new Cube(cubeNameWithProps, cubeMeasures, cubeDimensions, CUBE_PROPERTIES);
    derivedCubeWithProps =
      new DerivedCube(derivedCubeNameWithProps, measures, dimensions, CUBE_PROPERTIES, 0L, cubeWithProps);
  }

  private static void defineUberDims() throws LensException {
    // Define zip dimension
    zipAttrs.add(new BaseDimAttribute(new FieldSchema("zipcode", "int", "code")));
    zipAttrs.add(new BaseDimAttribute(new FieldSchema("f1", "string", "field1")));
    zipAttrs.add(new BaseDimAttribute(new FieldSchema("f2", "string", "field1")));
    List<TableReference> stateRefs = Lists.newArrayList(new TableReference("statedim", "id"),
      new TableReference("stateWeatherDim", "id"));
    zipAttrs.add(new ReferencedDimAtrribute(new FieldSchema("stateid", "int", "state id"), "State refer", stateRefs));
    zipAttrs.add(new ReferencedDimAtrribute(new FieldSchema("cityid", "int", "city id"), "City refer",
      new TableReference("citydim", "id")));
    zipAttrs.add(new ReferencedDimAtrribute(new FieldSchema("countryid", "int", "country id"), "Country refer",
      new TableReference("countrydim", "id")));
    zipDim = new Dimension("zipdim", zipAttrs);

    // Define city table
    cityAttrs.add(new BaseDimAttribute(new FieldSchema("id", "int", "code")));
    cityAttrs.add(new BaseDimAttribute(new FieldSchema("name", "string", "city name")));
    cityAttrs.add(new ReferencedDimAtrribute(new FieldSchema("stateid", "int", "state id"), "State refer",
      new TableReference("statedim", "id")));
    dimExpressions.add(new ExprColumn(new FieldSchema("stateAndCountry", "String", "state and country together"),
      "State and Country", new ExprSpec("concat(statedim.name, \":\", countrydim.name)", null, null),
      new ExprSpec("state_and_country", null, null)));
    dimExpressions.add(new ExprColumn(new FieldSchema("CityAddress", "string", "city with state and city and zip"),
      "City Address", "concat(citydim.name, \":\", statedim.name, \":\", countrydim.name, \":\", zipcode.code)"));
    Map<String, String> dimProps = getHashMap(getDimTimedDimensionKey("citydim"), getDatePartitionKey());
    cityDim = new Dimension("citydim", cityAttrs, dimExpressions, dimProps, 0L);

    // Define state table
    stateAttrs.add(new BaseDimAttribute(new FieldSchema("id", "int", "state id"), "State ID", null, null, null));
    stateAttrs.add(new BaseDimAttribute(new FieldSchema("name", "string", "state name")));
    stateAttrs.add(new BaseDimAttribute(new FieldSchema("capital", "string", "state capital")));
    stateAttrs.add(new ReferencedDimAtrribute(new FieldSchema("countryid", "int", "country id"), "Country refer",
      new TableReference("countrydim", "id")));
    stateDim = new Dimension("statedim", stateAttrs);

    countryAttrs.add(new BaseDimAttribute(new FieldSchema("id", "int", "country id")));
    countryAttrs.add(new BaseDimAttribute(new FieldSchema("name", "string", "country name")));
    countryAttrs.add(new BaseDimAttribute(new FieldSchema("capital", "string", "country capital")));
    countryAttrs.add(new BaseDimAttribute(new FieldSchema("region", "string", "region name")));
    countryDim = new Dimension("countrydim", stateAttrs);

  }

  @Test(priority = 1)
  public void testStorage() throws Exception {
    Storage hdfsStorage = new HDFSStorage(c1);
    client.createStorage(hdfsStorage);
    assertEquals(client.getAllStorages().size(), 1);

    Storage hdfsStorage2 = new HDFSStorage(c2);
    client.createStorage(hdfsStorage2);
    assertEquals(client.getAllStorages().size(), 2);

    Storage hdfsStorage3 = new HDFSStorage(c3);
    client.createStorage(hdfsStorage3);
    assertEquals(client.getAllStorages().size(), 3);

    Storage hdfsStorage4 = new HDFSStorage(c4);
    client.createStorage(hdfsStorage4);
    assertEquals(client.getAllStorages().size(), 4);

    assertEquals(hdfsStorage, client.getStorage(c1));
    assertEquals(hdfsStorage2, client.getStorage(c2));
    assertEquals(hdfsStorage3, client.getStorage(c3));
    assertEquals(hdfsStorage4, client.getStorage(c4));
  }

  @Test(priority = 1)
  public void testDimension() throws Exception {
    client.createDimension(zipDim);
    client.createDimension(cityDim);
    client.createDimension(stateDim);
    client.createDimension(countryDim);

    assertEquals(client.getAllDimensions().size(), 4);
    assertTrue(client.tableExists(cityDim.getName()));
    assertTrue(client.tableExists(stateDim.getName()));
    assertTrue(client.tableExists(countryDim.getName()));

    validateDim(zipDim, zipAttrs, "zipcode", "stateid");
    validateDim(cityDim, cityAttrs, "id", "stateid");
    validateDim(stateDim, stateAttrs, "id", "countryid");
    validateDim(countryDim, countryAttrs, "id", null);

    // validate expression in citydim
    Dimension city = client.getDimension(cityDim.getName());
    assertEquals(dimExpressions.size(), city.getExpressions().size());
    assertEquals(dimExpressions.size(), city.getExpressionNames().size());
    assertNotNull(city.getExpressionByName("stateAndCountry"));
    assertNotNull(city.getExpressionByName("cityaddress"));
    assertEquals(city.getExpressionByName("cityaddress").getDescription(), "city with state and city and zip");
    assertEquals(city.getExpressionByName("cityaddress").getDisplayString(), "City Address");

    ExprColumn stateCountryExpr = new ExprColumn(new FieldSchema("stateAndCountry", "String",
      "state and country together with hiphen as separator"), "State and Country",
      "concat(statedim.name, \"-\", countrydim.name)");
    ExprSpec expr1 = new ExprSpec();
    expr1.setExpr("concat(countrydim.name, \"-\", countrydim.name)");
    stateCountryExpr.addExpression(expr1);

    // Assert expression validation
    try {
      expr1 = new ExprSpec();
      expr1.setExpr("contact(countrydim.name");
      stateCountryExpr.addExpression(expr1);
      fail("Expected add expression to fail because of syntax error");
    } catch (LensException exc) {
      // Pass
    }
    city.alterExpression(stateCountryExpr);


    city.removeExpression("cityAddress");
    city = client.getDimension(cityDim.getName());
    assertEquals(1, city.getExpressions().size());

    ExprColumn stateAndCountryActual = city.getExpressionByName("stateAndCountry");
    assertNotNull(stateAndCountryActual.getExpressions());
    assertEquals(2, stateAndCountryActual.getExpressions().size());
    assertTrue(stateAndCountryActual.getExpressions().contains("concat(statedim.name, \"-\", countrydim.name)"));
    assertTrue(stateAndCountryActual.getExpressions()
      .contains("concat(countrydim.name, \"-\", countrydim.name)"));

    assertNotNull(city.getExpressionByName("stateAndCountry"));
    assertEquals(city.getExpressionByName("stateAndCountry").getExpr(),
      "concat(statedim.name, \"-\", countrydim.name)");

    stateAndCountryActual.removeExpression("concat(countrydim.name, \"-\", countrydim.name)");
    city.alterExpression(stateAndCountryActual);
    client.alterDimension(city.getName(), city);
    Dimension cityAltered = client.getDimension(city.getName());
    assertEquals(1, cityAltered.getExpressionByName("stateAndCountry").getExpressions().size());


    List<TableReference> chain = new ArrayList<>();
    chain.add(new TableReference("zipdim", "cityid"));
    chain.add(new TableReference("citydim", "id"));
    chain.add(new TableReference("citydim", "stateid"));
    chain.add(new TableReference("statedim", "id"));
    JoinChain zipState = new JoinChain("stateFromZip", "Zip State", "zip State desc");
    zipState.addPath(chain);
    joinChains.add(zipState);

    // alter dimension
    Table tbl = client.getHiveTable(zipDim.getName());
    Dimension toAlter = new Dimension(tbl);
    toAlter.alterAttribute(new BaseDimAttribute(new FieldSchema("newZipDim", "int", "new dim added"), null, null, null,
      null, 1000L));
    toAlter.alterAttribute(new ReferencedDimAtrribute(new FieldSchema("newRefDim", "int", "new ref-dim added"),
      "New city ref", new TableReference("citydim", "id")));
    toAlter.alterAttribute(new BaseDimAttribute(new FieldSchema("f2", "varchar", "modified field")));
    List<TableReference> stateRefs = Lists.newArrayList(new TableReference("statedim", "id"));
    toAlter.alterAttribute(new ReferencedDimAtrribute(new FieldSchema("stateid", "int", "state id"), "State refer",
      stateRefs));
    toAlter.removeAttribute("f1");
    toAlter.getProperties().put("alter.prop", "altered");
    toAlter.alterExpression(new ExprColumn(new FieldSchema("formattedcode", "string", "formatted zipcode"),
      "Formatted zipcode", "format_number(code, \"#,###,###\")"));
    toAlter.alterJoinChain(zipState);

    client.alterDimension(zipDim.getName(), toAlter);
    Dimension altered = client.getDimension(zipDim.getName());


    assertEquals(toAlter, altered);
    assertNotNull(altered.getAttributeByName("newZipDim"));
    assertNotNull(altered.getAttributeByName("newRefDim"));
    assertNotNull(altered.getAttributeByName("f2"));
    assertNotNull(altered.getAttributeByName("stateid"));
    assertNull(altered.getAttributeByName("f1"));
    assertEquals(1, altered.getExpressions().size());
    assertNotNull(altered.getExpressionByName("formattedcode"));
    assertEquals(altered.getExpressionByName("formattedcode").getExpr(), "format_number(code, \"#,###,###\")");

    CubeDimAttribute newzipdim = altered.getAttributeByName("newZipDim");
    assertTrue(newzipdim instanceof BaseDimAttribute);
    assertEquals(((BaseDimAttribute) newzipdim).getType(), "int");
    assertEquals((((BaseDimAttribute) newzipdim).getNumOfDistinctValues().get()), Long.valueOf(1000));

    CubeDimAttribute newrefdim = altered.getAttributeByName("newRefDim");
    assertTrue(newrefdim instanceof ReferencedDimAtrribute);
    assertEquals(((ReferencedDimAtrribute) newrefdim).getReferences().size(), 1);
    assertEquals(((ReferencedDimAtrribute) newrefdim).getReferences().get(0).getDestTable(), cityDim.getName());
    assertEquals(((ReferencedDimAtrribute) newrefdim).getReferences().get(0).getDestColumn(), "id");

    CubeDimAttribute f2 = altered.getAttributeByName("f2");
    assertTrue(f2 instanceof BaseDimAttribute);
    assertEquals(((BaseDimAttribute) f2).getType(), "varchar");

    CubeDimAttribute stateid = altered.getAttributeByName("stateid");
    assertTrue(stateid instanceof ReferencedDimAtrribute);
    assertEquals(((ReferencedDimAtrribute) stateid).getReferences().size(), 1);
    assertEquals(((ReferencedDimAtrribute) stateid).getReferences().get(0).getDestTable(), stateDim.getName());
    assertEquals(((ReferencedDimAtrribute) stateid).getReferences().get(0).getDestColumn(), "id");

    assertEquals(altered.getProperties().get("alter.prop"), "altered");

    assertEquals(altered.getChainByName("stateFromZip"), zipState);

    assertEquals(altered.getJoinChains().size(), 1);
    JoinChain zipchain = altered.getChainByName("stateFromZip");
    assertEquals(zipchain.getDisplayString(), "Zip State");
    assertEquals(zipchain.getDescription(), "zip State desc");
    assertEquals(zipchain.getPaths().size(), 1);
    assertEquals(zipchain.getPaths().get(0).getReferences().size(), 4);
    assertEquals(zipchain.getPaths().get(0).getReferences().get(0).toString(), "zipdim.cityid");
    assertEquals(zipchain.getPaths().get(0).getReferences().get(1).toString(), "citydim.id");
    assertEquals(zipchain.getPaths().get(0).getReferences().get(2).toString(), "citydim.stateid");
    assertEquals(zipchain.getPaths().get(0).getReferences().get(3).toString(), "statedim.id");
  }

  private void validateDim(Dimension udim, Set<CubeDimAttribute> attrs, String basedim, String referdim)
    throws HiveException {
    assertTrue(client.tableExists(udim.getName()));
    Table dimTbl = client.getHiveTable(udim.getName());
    assertTrue(client.isDimension(dimTbl));
    Dimension dim = new Dimension(dimTbl);
    assertTrue(udim.equals(dim));
    assertTrue(udim.equals(client.getDimension(udim.getName())));
    assertEquals(dim.getAttributes().size(), attrs.size());
    assertNotNull(dim.getAttributeByName(basedim));
    assertTrue(dim.getAttributeByName(basedim) instanceof BaseDimAttribute);
    if (referdim != null) {
      assertNotNull(dim.getAttributeByName(referdim));
      assertTrue(dim.getAttributeByName(referdim) instanceof ReferencedDimAtrribute);
    }
    assertEquals(udim.getAttributeNames().size() + udim.getExpressionNames().size(), dim.getAllFieldNames().size());
  }

  @Test(priority = 1)
  public void testCube() throws Exception {
    client.createCube(CUBE_NAME, cubeMeasures, cubeDimensions, cubeExpressions, joinChains, emptyHashMap);
    assertTrue(client.tableExists(CUBE_NAME));
    Table cubeTbl = client.getHiveTable(CUBE_NAME);
    assertTrue(client.isCube(cubeTbl));
    Cube cube2 = new Cube(cubeTbl);
    assertTrue(cube.equals(cube2));
    assertFalse(cube2.isDerivedCube());
    assertTrue(cube2.getTimedDimensions().isEmpty());
    assertEquals(cubeMeasures.size(), cube2.getMeasureNames().size());
    // +8 is for hierarchical dimension
    assertEquals(cubeDimensions.size() + 8, cube2.getDimAttributeNames().size());
    assertEquals(cubeMeasures.size(), cube2.getMeasures().size());
    assertEquals(cubeExpressions.size(), cube2.getExpressions().size());
    assertEquals(cubeExpressions.size(), cube2.getExpressionNames().size());
    assertEquals(cubeDimensions.size(), cube2.getDimAttributes().size());
    assertEquals(cubeDimensions.size() + 8 + cubeMeasures.size() + cubeExpressions.size(), cube2
      .getAllFieldNames().size());
    assertNotNull(cube2.getMeasureByName("msr4"));
    assertEquals(cube2.getMeasureByName("msr4").getDescription(), "fourth measure");
    assertEquals(cube2.getMeasureByName("msr4").getDisplayString(), "Measure4");
    assertNotNull(cube2.getDimAttributeByName("location"));
    assertEquals(cube2.getDimAttributeByName("location").getDescription(), "location hierarchy");
    assertNotNull(cube2.getDimAttributeByName("dim1"));
    assertEquals(cube2.getDimAttributeByName("dim1").getDescription(), "basedim");
    assertNull(cube2.getDimAttributeByName("dim1").getDisplayString());
    assertNotNull(cube2.getDimAttributeByName("dim2"));
    assertEquals(cube2.getDimAttributeByName("dim2").getDescription(), "ref dim");
    assertEquals(cube2.getDimAttributeByName("dim2").getDisplayString(), "Dim2 refer");
    assertNotNull(cube2.getExpressionByName("msr5"));
    assertEquals(cube2.getExpressionByName("msr5").getDescription(), "fifth measure");
    assertEquals(cube2.getExpressionByName("msr5").getDisplayString(), "Avg msr5");
    assertNotNull(cube2.getExpressionByName("booleancut"));
    assertEquals(cube2.getExpressionByName("booleancut").getDescription(), "a boolean expression");
    assertEquals(cube2.getExpressionByName("booleancut").getDisplayString(), "Boolean Cut");
    assertEquals(cube2.getExpressionByName("booleancut").getExpressions().size(), 2);
    // Validate expression can contain delimiter character
    List<String> booleanCutExprs = new ArrayList<>(cube2.getExpressionByName("booleancut").getExpressions());
    assertTrue(booleanCutExprs.contains("dim1 | dim2 AND dim2 = 'XYZ'"));
    assertTrue(cube2.allFieldsQueriable());

    assertTrue(cube2.getJoinChainNames().contains("cityfromzip"));
    assertTrue(cube2.getJoinChainNames().contains("city"));
    assertFalse(cube2.getJoinChains().isEmpty());
    assertEquals(cube2.getJoinChains().size(), 2);
    JoinChain zipchain = cube2.getChainByName("cityfromzip");
    assertEquals(zipchain.getDisplayString(), "Zip City");
    assertEquals(zipchain.getDescription(), "zip city desc");
    assertEquals(zipchain.getPaths().size(), 2);
    assertEquals(zipchain.getPaths().get(0).getReferences().size(), 4);
    assertEquals(zipchain.getPaths().get(0).getReferences().get(0).toString(), "testmetastorecube.zipcode");
    assertEquals(zipchain.getPaths().get(0).getReferences().get(1).toString(), "zipdim.zipcode");
    assertEquals(zipchain.getPaths().get(0).getReferences().get(2).toString(), "zipdim.cityid");
    assertEquals(zipchain.getPaths().get(0).getReferences().get(3).toString(), "citydim.id");
    assertEquals(zipchain.getPaths().get(1).getReferences().size(), 4);
    assertEquals(zipchain.getPaths().get(1).getReferences().get(0).toString(), "testmetastorecube.zipcode2");
    assertEquals(zipchain.getPaths().get(1).getReferences().get(1).toString(), "zipdim.zipcode");
    assertEquals(zipchain.getPaths().get(1).getReferences().get(2).toString(), "zipdim.cityid");
    assertEquals(zipchain.getPaths().get(1).getReferences().get(3).toString(), "citydim.id");
    JoinChain citychain = cube2.getChainByName("city");
    assertEquals(citychain.getDisplayString(), "Cube City");
    assertEquals(citychain.getDescription(), "cube city desc");
    assertEquals(citychain.getPaths().size(), 1);
    assertEquals(citychain.getPaths().get(0).getReferences().size(), 2);
    assertEquals(citychain.getPaths().get(0).getReferences().get(0).toString(), "testmetastorecube.cityid");
    assertEquals(citychain.getPaths().get(0).getReferences().get(1).toString(), "citydim.id");
    assertNotNull(cube2.getDimAttributeByName("zipcityname"));
    ChainRefCol zipCityChain = ((ReferencedDimAtrribute) cube2.getDimAttributeByName("zipcityname"))
      .getChainRefColumns().get(0);
    assertEquals(zipCityChain.getChainName(), "cityfromzip");
    assertEquals(zipCityChain.getRefColumn(), "name");

    client.createDerivedCube(CUBE_NAME, DERIVED_CUBE_NAME, measures, dimensions, emptyHashMap, 0L);
    assertTrue(client.tableExists(DERIVED_CUBE_NAME));
    Table derivedTbl = client.getHiveTable(DERIVED_CUBE_NAME);
    assertTrue(client.isCube(derivedTbl));
    DerivedCube dcube2 = new DerivedCube(derivedTbl, cube);
    assertTrue(derivedCube.equals(dcube2));
    assertTrue(dcube2.isDerivedCube());
    assertTrue(dcube2.getTimedDimensions().isEmpty());
    assertEquals(measures.size(), dcube2.getMeasureNames().size());
    assertEquals(dimensions.size(), dcube2.getDimAttributeNames().size());
    assertEquals(measures.size(), dcube2.getMeasures().size());
    assertEquals(dimensions.size(), dcube2.getDimAttributes().size());
    assertNotNull(dcube2.getMeasureByName("msr3"));
    assertNull(dcube2.getMeasureByName("msr4"));
    assertNull(dcube2.getDimAttributeByName("location"));
    assertNotNull(dcube2.getDimAttributeByName("dim1"));
    assertTrue(dcube2.allFieldsQueriable());

    client.createCube(CUBE_NAME_WITH_PROPS, cubeMeasures, cubeDimensions, CUBE_PROPERTIES);
    assertTrue(client.tableExists(CUBE_NAME_WITH_PROPS));
    cubeTbl = client.getHiveTable(CUBE_NAME_WITH_PROPS);
    assertTrue(client.isCube(cubeTbl));
    cube2 = new Cube(cubeTbl);
    assertTrue(cubeWithProps.equals(cube2));
    assertFalse(cube2.isDerivedCube());
    assertFalse(cubeWithProps.getTimedDimensions().isEmpty());
    assertTrue(cubeWithProps.getTimedDimensions().contains("dt"));
    assertTrue(cubeWithProps.getTimedDimensions().contains("mydate"));
    assertEquals(cubeMeasures.size(), cube2.getMeasureNames().size());
    assertEquals(cubeDimensions.size() + 8, cube2.getDimAttributeNames().size());
    assertEquals(cubeMeasures.size(), cube2.getMeasures().size());
    assertEquals(cubeDimensions.size(), cube2.getDimAttributes().size());
    assertNotNull(cube2.getMeasureByName("msr4"));
    assertNotNull(cube2.getDimAttributeByName("location"));
    assertFalse(cube2.allFieldsQueriable());

    client.createDerivedCube(CUBE_NAME_WITH_PROPS, DERIVED_CUBE_NAME_WITH_PROPS, measures, dimensions,
      CUBE_PROPERTIES, 0L);
    assertTrue(client.tableExists(DERIVED_CUBE_NAME_WITH_PROPS));
    derivedTbl = client.getHiveTable(DERIVED_CUBE_NAME_WITH_PROPS);
    assertTrue(client.isCube(derivedTbl));
    dcube2 = new DerivedCube(derivedTbl, cubeWithProps);
    assertTrue(derivedCubeWithProps.equals(dcube2));
    assertTrue(dcube2.isDerivedCube());
    assertNotNull(derivedCubeWithProps.getProperties().get("cube.custom.prop"));
    assertEquals(derivedCubeWithProps.getProperties().get("cube.custom.prop"), "myval");
    assertNull(dcube2.getMeasureByName("msr4"));
    assertNotNull(dcube2.getMeasureByName("msr3"));
    assertNull(dcube2.getDimAttributeByName("location"));
    assertNotNull(dcube2.getDimAttributeByName("dim1"));
    assertTrue(dcube2.allFieldsQueriable());
  }

  @Test(priority = 1)
  public void testAlterCube() throws Exception {
    String cubeName = "alter_test_cube";
    client.createCube(cubeName, cubeMeasures, cubeDimensions);
    // Test alter cube
    Table cubeTbl = client.getHiveTable(cubeName);
    Cube toAlter = new Cube(cubeTbl);
    toAlter.alterMeasure(new ColumnMeasure(new FieldSchema("testAddMsr1", "int", "testAddMeasure")));
    toAlter.alterMeasure(new ColumnMeasure(new FieldSchema("msr3", "float", "third altered measure"),
      "Measure3Altered", null, "MAX", "alterunit"));
    toAlter.removeMeasure("msr4");
    toAlter.alterDimension(new BaseDimAttribute(new FieldSchema("testAddDim1", "string", "dim to add")));
    toAlter.alterDimension(new BaseDimAttribute(new FieldSchema("dim1", "int", "basedim altered")));
    toAlter.removeDimension("location2");
    toAlter.addTimedDimension("zt");
    toAlter.removeTimedDimension("dt");

    JoinChain cityChain = new JoinChain("city", "Cube City", "cube city desc modified");
    List<TableReference> chain = new ArrayList<>();
    chain.add(new TableReference(cubeName, "cityid"));
    chain.add(new TableReference("citydim", "id"));
    cityChain.addPath(chain);
    toAlter.alterJoinChain(cityChain);
    toAlter.removeJoinChain("cityFromZip");

    assertNotNull(toAlter.getMeasureByName("testAddMsr1"));
    assertNotNull(toAlter.getMeasureByName("msr3"));
    assertEquals(toAlter.getMeasureByName("msr3").getDisplayString(), "Measure3Altered");
    assertEquals(toAlter.getMeasureByName("msr3").getDescription(), "third altered measure");
    assertNull(toAlter.getMeasureByName("msr4"));
    assertNotNull(toAlter.getDimAttributeByName("testAddDim1"));
    assertEquals(toAlter.getDimAttributeByName("testAddDim1").getDescription(), "dim to add");
    assertNotNull(toAlter.getDimAttributeByName("dim1"));
    assertEquals(toAlter.getDimAttributeByName("dim1").getDescription(), "basedim altered");
    assertNull(toAlter.getDimAttributeByName("location2"));

    client.alterCube(cubeName, toAlter);

    Table alteredHiveTbl = Hive.get(conf).getTable(cubeName);

    Cube altered = new Cube(alteredHiveTbl);

    assertEquals(toAlter, altered);
    assertNotNull(altered.getMeasureByName("testAddMsr1"));
    CubeMeasure addedMsr = altered.getMeasureByName("testAddMsr1");
    assertEquals(addedMsr.getType(), "int");
    assertNotNull(altered.getDimAttributeByName("testAddDim1"));
    BaseDimAttribute addedDim = (BaseDimAttribute) altered.getDimAttributeByName("testAddDim1");
    assertEquals(addedDim.getType(), "string");
    assertEquals(addedDim.getDescription(), "dim to add");
    assertTrue(altered.getTimedDimensions().contains("zt"));
    assertEquals(altered.getMeasureByName("msr3").getDisplayString(), "Measure3Altered");
    assertEquals(altered.getMeasureByName("msr3").getDescription(), "third altered measure");
    assertNotNull(altered.getDimAttributeByName("dim1"));
    assertEquals(altered.getDimAttributeByName("dim1").getDescription(), "basedim altered");
    assertNull(altered.getDimAttributeByName("location2"));
    assertNull(altered.getChainByName("cityFromZip"));
    assertEquals(altered.getChainByName("city").getDescription(), "cube city desc modified");

    toAlter.alterMeasure(new ColumnMeasure(new FieldSchema("testAddMsr1", "double", "testAddMeasure")));
    client.alterCube(cubeName, toAlter);
    altered = new Cube(Hive.get(conf).getTable(cubeName));
    addedMsr = altered.getMeasureByName("testaddmsr1");
    assertNotNull(addedMsr);
    assertEquals(addedMsr.getType(), "double");
    assertTrue(client.getAllFacts(altered).isEmpty());
  }

  @Test(priority = 2)
  public void testAlterDerivedCube() throws Exception {
    String name = "alter_derived_cube";
    client.createDerivedCube(CUBE_NAME, name, measures, dimensions, emptyHashMap, 0L);
    // Test alter cube
    Table cubeTbl = client.getHiveTable(name);
    DerivedCube toAlter = new DerivedCube(cubeTbl, (Cube) client.getCube(CUBE_NAME));
    toAlter.addMeasure("msr4");
    toAlter.removeMeasure("msr3");
    toAlter.addDimension("dim1StartTime");
    toAlter.removeDimension("dim1");

    assertNotNull(toAlter.getMeasureByName("msr4"));
    assertNotNull(toAlter.getMeasureByName("msr2"));
    assertNull(toAlter.getMeasureByName("msr3"));
    assertNotNull(toAlter.getDimAttributeByName("dim1StartTime"));
    assertNotNull(toAlter.getDimAttributeByName("dim2"));
    assertNull(toAlter.getDimAttributeByName("dim1"));

    client.alterCube(name, toAlter);

    DerivedCube altered = (DerivedCube) client.getCube(name);

    assertEquals(toAlter, altered);
    assertNotNull(altered.getMeasureByName("msr4"));
    CubeMeasure addedMsr = altered.getMeasureByName("msr4");
    assertEquals(addedMsr.getType(), "bigint");
    assertNotNull(altered.getDimAttributeByName("dim1StartTime"));
    BaseDimAttribute addedDim = (BaseDimAttribute) altered.getDimAttributeByName("dim1StartTime");
    assertEquals(addedDim.getType(), "string");
    assertNotNull(addedDim.getStartTime());

    client.dropCube(name);
    assertFalse(client.tableExists(name));
  }

  @Test(priority = 2)
  public void testCubeFact() throws Exception {
    String factName = "testMetastoreFact";
    List<FieldSchema> factColumns = new ArrayList<>(cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add one dimension of the cube
    factColumns.add(new FieldSchema("zipcode", "int", "zip"));

    StorageTableDesc s1 = new StorageTableDesc(TextInputFormat.class, HiveIgnoreKeyTextOutputFormat.class,
      datePartSingleton, datePartKeySingleton);
    Map<String, Set<UpdatePeriod>> updatePeriods = getHashMap(c1, Sets.newHashSet(HOURLY, DAILY));
    Map<String, StorageTableDesc> storageTables = getHashMap(c1, s1);

    CubeFactTable cubeFact = new CubeFactTable(CUBE_NAME, factName, factColumns, updatePeriods);

    // create cube fact
    client.createCubeFactTable(CUBE_NAME, factName, factColumns, updatePeriods, 0L, null, storageTables);
    assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    assertTrue(client.isFactTable(cubeTbl));
    assertTrue(client.isFactTableForCube(cubeTbl, CUBE_NAME));
    assertEquals(client.getAllFacts(client.getCube(CUBE_NAME)).get(0).getName(), factName.toLowerCase());
    assertEquals(client.getAllFacts(client.getCube(DERIVED_CUBE_NAME)).get(0).getName(), factName.toLowerCase());
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    assertTrue(cubeFact.equals(cubeFact2));

    // Assert for storage tables
    for (String entry : storageTables.keySet()) {
      String storageTableName = getFactOrDimtableStorageTableName(factName, entry);
      assertTrue(client.tableExists(storageTableName));
    }

    Map<String, Date> timeParts = getTimePartitionByOffsets(getDatePartitionKey(), 0, "non_existing_part_col", 0);
    // test error on adding invalid partition
    // test partition
    StoragePartitionDesc partSpec = new StoragePartitionDesc(cubeFact.getName(), timeParts, null, HOURLY);
    try {
      client.addPartition(partSpec, c1);
      fail("Add should fail since non_existing_part_col is non-existing");
    } catch (LensException e) {
      assertEquals(e.getErrorCode(), LensCubeErrorCode.TIMELINE_ABSENT.getLensErrorInfo().getErrorCode());
    }
    timeParts.remove("non_existing_part_col");
    partSpec = new StoragePartitionDesc(cubeFact.getName(), timeParts, null, HOURLY);
    client.addPartition(partSpec, c1);
    assertTrue(client.factPartitionExists(cubeFact.getName(), c1, HOURLY, timeParts, emptyHashMap));
    assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, getDatePartitionKey()));

    // Partition with different schema
    FieldSchema newcol = new FieldSchema("newcol", "int", "new col for part");
    cubeFact.alterColumn(newcol);
    client.alterCubeFactTable(cubeFact.getName(), cubeFact, storageTables);
    String storageTableName = getFactOrDimtableStorageTableName(factName, c1);
    assertEquals(client.getAllParts(storageTableName).size(), 1);
    List<Partition> parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    assertEquals(parts.size(), 0);

    Map<String, Date> timeParts2 = getTimePartitionByOffsets(getDatePartitionKey(), 1);
    StoragePartitionDesc partSpec2 = new StoragePartitionDesc(cubeFact.getName(), timeParts2, null, HOURLY);
    partSpec2.setInputFormat(SequenceFileInputFormat.class.getCanonicalName());
    partSpec2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    client.addPartition(partSpec2, c1);
    assertEquals(client.getAllParts(storageTableName).size(), 2);
    assertTrue(client.factPartitionExists(cubeFact.getName(), c1, HOURLY, timeParts, emptyHashMap));
    assertTrue(client.factPartitionExists(cubeFact.getName(), c1, HOURLY, timeParts2, emptyHashMap));
    assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, getDatePartitionKey()));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    assertEquals(parts.size(), 0);

    client.dropPartition(cubeFact.getName(), c1, timeParts2, null, HOURLY);
    assertEquals(client.getAllParts(storageTableName).size(), 1);
    assertTrue(client.factPartitionExists(cubeFact.getName(), c1, HOURLY, timeParts, emptyHashMap));
    assertFalse(client.factPartitionExists(cubeFact.getName(), c1, HOURLY, timeParts2, emptyHashMap));
    assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, getDatePartitionKey()));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    assertEquals(parts.size(), 0);

    client.dropPartition(cubeFact.getName(), c1, timeParts, null, HOURLY);
    assertEquals(client.getAllParts(storageTableName).size(), 0);
    assertFalse(client.factPartitionExists(cubeFact.getName(), c1, HOURLY, timeParts, emptyHashMap));
    assertFalse(client.factPartitionExists(cubeFact.getName(), c1, HOURLY, timeParts2, emptyHashMap));
    assertFalse(client.latestPartitionExists(cubeFact.getName(), c1, getDatePartitionKey()));
  }

  @Test(priority = 2)
  public void testAlterCubeFact() throws Exception {
    String factName = "test_alter_fact";
    List<FieldSchema> factColumns = new ArrayList<>(cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add one dimension of the cube
    factColumns.add(new FieldSchema("zipcode", "int", "zip"));

    StorageTableDesc s1 = new StorageTableDesc(TextInputFormat.class, HiveIgnoreKeyTextOutputFormat.class,
      datePartSingleton, datePartKeySingleton);
    Map<String, Set<UpdatePeriod>> updatePeriods = getHashMap(c1, hourlyAndDaily, c2, hourlyAndDaily);
    Map<String, StorageTableDesc> storageTables = getHashMap(c1, s1, c2, s1);

    // create cube fact
    client.createCubeFactTable(CUBE_NAME, factName, factColumns, updatePeriods, 0L, null, storageTables);

    CubeFactTable factTable = new CubeFactTable(Hive.get(conf).getTable(factName));
    factTable.alterColumn(new FieldSchema("testFactColAdd", "int", "test add column"));
    factTable.alterColumn(new FieldSchema("msr3", "int", "test alter column"));
    factTable.alterWeight(100L);
    Map<String, String> newProp = getHashMap("new.prop", "val");
    factTable.addProperties(newProp);
    factTable.addUpdatePeriod(c1, MONTHLY);
    factTable.removeUpdatePeriod(c1, HOURLY);
    Set<UpdatePeriod> alterupdates = Sets.newHashSet(HOURLY, DAILY, MONTHLY);
    factTable.alterStorage(c2, alterupdates);

    client.alterCubeFactTable(factName, factTable, storageTables);

    Table factHiveTable = Hive.get(conf).getTable(factName);
    CubeFactTable altered = new CubeFactTable(factHiveTable);

    assertTrue(altered.weight() == 100L);
    assertTrue(altered.getProperties().get("new.prop").equals("val"));
    assertTrue(altered.getUpdatePeriods().get(c1).contains(MONTHLY));
    assertFalse(altered.getUpdatePeriods().get(c1).contains(HOURLY));
    assertTrue(altered.getUpdatePeriods().get(c2).contains(MONTHLY));
    assertTrue(altered.getUpdatePeriods().get(c2).contains(DAILY));
    assertTrue(altered.getUpdatePeriods().get(c2).contains(HOURLY));
    assertTrue(altered.getCubeName().equalsIgnoreCase(CUBE_NAME.toLowerCase()));
    boolean contains = false;
    for (FieldSchema column : altered.getColumns()) {
      if (column.getName().equals("testfactcoladd") && column.getType().equals("int")) {
        contains = true;
        break;
      }
    }
    assertTrue(contains);

    // alter storage table desc
    String c1TableName = getFactOrDimtableStorageTableName(factName, c1);
    Table c1Table = client.getTable(c1TableName);
    assertEquals(c1Table.getInputFormatClass().getCanonicalName(), TextInputFormat.class.getCanonicalName());
    s1 = new StorageTableDesc(SequenceFileInputFormat.class, HiveIgnoreKeyTextOutputFormat.class,
      datePartSingleton, datePartKeySingleton);
    s1.setFieldDelim(":");
    storageTables.put(c1, s1);
    storageTables.put(c4, s1);
    factTable.addStorage(c4, hourlyAndDaily);
    client.alterCubeFactTable(factName, factTable, storageTables);
    CubeFactTable altered2 = client.getCubeFact(factName);
    assertTrue(client.tableExists(c1TableName));
    Table alteredC1Table = client.getTable(c1TableName);
    assertEquals(alteredC1Table.getInputFormatClass(), SequenceFileInputFormat.class);
    assertEquals(alteredC1Table.getSerdeParam(serdeConstants.FIELD_DELIM), ":");

    boolean storageTableColsAltered = false;
    for (FieldSchema column : alteredC1Table.getAllCols()) {
      if (column.getName().equals("testfactcoladd") && column.getType().equals("int")) {
        storageTableColsAltered = true;
        break;
      }
    }
    assertTrue(storageTableColsAltered);

    assertTrue(altered2.getStorages().contains("C4"));
    assertTrue(altered2.getUpdatePeriods().get("C4").equals(hourlyAndDaily));
    String c4TableName = getFactOrDimtableStorageTableName(factName, c4);
    assertTrue(client.tableExists(c4TableName));

    // add storage
    client.addStorage(altered2, c3, hourlyAndDaily, s1);
    CubeFactTable altered3 = client.getCubeFact(factName);
    assertTrue(altered3.getStorages().contains("C3"));
    assertTrue(altered3.getUpdatePeriods().get("C3").equals(hourlyAndDaily));
    String storageTableName = getFactOrDimtableStorageTableName(factName, c3);
    assertTrue(client.tableExists(storageTableName));
    client.dropStorageFromFact(factName, c2);
    storageTableName = getFactOrDimtableStorageTableName(factName, c2);
    assertFalse(client.tableExists(storageTableName));
    List<CubeFactTable> cubeFacts = client.getAllFacts(client.getCube(CUBE_NAME));
    List<String> cubeFactNames = new ArrayList<>();
    for (CubeFactTable cfact : cubeFacts) {
      cubeFactNames.add(cfact.getName());
    }
    assertTrue(cubeFactNames.contains(factName.toLowerCase()));
    cubeFacts = client.getAllFacts(client.getCube(DERIVED_CUBE_NAME));
    cubeFactNames = new ArrayList<>();
    for (CubeFactTable cfact : cubeFacts) {
      cubeFactNames.add(cfact.getName());
    }
    assertTrue(cubeFactNames.contains(factName.toLowerCase()));
    client.dropFact(factName, true);
    assertFalse(client.tableExists(getFactOrDimtableStorageTableName(factName, c1)));
    assertFalse(client.tableExists(getFactOrDimtableStorageTableName(factName, c3)));
    assertFalse(client.tableExists(factName));
    cubeFacts = client.getAllFacts(cube);
    cubeFactNames = new ArrayList<>();
    for (CubeFactTable cfact : cubeFacts) {
      cubeFactNames.add(cfact.getName());
    }
    assertFalse(cubeFactNames.contains(factName.toLowerCase()));
  }

  @Test(priority = 2)
  public void testCubeFactWithTwoTimedParts() throws Exception {
    String factName = "testMetastoreFactTimedParts";
    List<FieldSchema> factColumns = new ArrayList<>(cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add one dimension of the cube
    factColumns.add(new FieldSchema("zipcode", "int", "zip"));

    Set<UpdatePeriod> updates = Sets.newHashSet(HOURLY, DAILY);
    FieldSchema testDtPart = new FieldSchema("mydate", "string", "date part");
    StorageTableDesc s1 = new StorageTableDesc(TextInputFormat.class, HiveIgnoreKeyTextOutputFormat.class,
      Lists.newArrayList(getDatePartition(), testDtPart),
      Lists.newArrayList(getDatePartitionKey(), testDtPart.getName()));
    Map<String, Set<UpdatePeriod>> updatePeriods = getHashMap(c1, updates);
    Map<String, StorageTableDesc> storageTables = getHashMap(c1, s1);

    CubeFactTable cubeFact = new CubeFactTable(CUBE_NAME_WITH_PROPS, factName, factColumns, updatePeriods);

    // create cube fact
    client.createCubeFactTable(CUBE_NAME_WITH_PROPS, factName, factColumns, updatePeriods, 0L, null, storageTables);

    assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    assertTrue(client.isFactTable(cubeTbl));
    assertTrue(client.isFactTableForCube(cubeTbl, CUBE_NAME_WITH_PROPS));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    assertTrue(cubeFact.equals(cubeFact2));

    // Assert for storage tables
    for (String entry : storageTables.keySet()) {
      String storageTableName = getFactOrDimtableStorageTableName(factName, entry);
      assertTrue(client.tableExists(storageTableName));
    }

    //test partition
    Map<String, Date> timeParts = getTimePartitionByOffsets(getDatePartitionKey(), 0, testDtPart.getName(), -1);
    StoragePartitionDesc partSpec = new StoragePartitionDesc(cubeFact.getName(), timeParts, null, HOURLY);
    client.addPartition(partSpec, c1);
    assertTrue(client.factPartitionExists(cubeFact.getName(), c1, HOURLY, timeParts, emptyHashMap));
    assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, testDtPart.getName()));
    assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, getDatePartitionKey()));
    String storageTableName = getFactOrDimtableStorageTableName(cubeFact.getName(), c1);
    List<Partition> parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    assertEquals(parts.size(), 0);

    assertEquals(client.getAllParts(storageTableName).size(), 1);
    parts = client.getPartitionsByFilter(storageTableName, testDtPart.getName() + "='latest'");
    assertEquals(parts.size(), 0);

    client.dropPartition(cubeFact.getName(), c1, timeParts, null, HOURLY);
    assertEquals(client.getAllParts(storageTableName).size(), 0);
    assertFalse(client.factPartitionExists(cubeFact.getName(), c1, HOURLY, timeParts, emptyHashMap));
    assertFalse(client.latestPartitionExists(cubeFact.getName(), c1, getDatePartitionKey()));
    assertFalse(client.latestPartitionExists(cubeFact.getName(), c1, testDtPart.getName()));
  }

  @Test(priority = 2)
  public void testCubeFactWithThreeTimedParts() throws Exception {
    String factName = "testMetastoreFact3TimedParts";
    List<FieldSchema> factColumns = new ArrayList<>(cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add one dimension of the cube
    factColumns.add(new FieldSchema("zipcode", "int", "zip"));
    FieldSchema itPart = new FieldSchema("it", "string", "date part");
    FieldSchema etPart = new FieldSchema("et", "string", "date part");
    StorageTableDesc s1 = new StorageTableDesc(TextInputFormat.class, HiveIgnoreKeyTextOutputFormat.class,
      Lists.newArrayList(getDatePartition(), itPart, etPart),
      Lists.newArrayList(getDatePartitionKey(), itPart.getName(), etPart.getName()));

    Map<String, Set<UpdatePeriod>> updatePeriods = getHashMap(c1, hourlyAndDaily, c2, hourlyAndDaily);
    Map<String, StorageTableDesc> storageTables = getHashMap(c1, s1, c2, s1);

    CubeFactTable cubeFact = new CubeFactTable(CUBE_NAME_WITH_PROPS, factName, factColumns, updatePeriods);

    // create cube fact
    client.createCubeFactTable(CUBE_NAME_WITH_PROPS, factName, factColumns, updatePeriods, 0L, null, storageTables);

    assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    assertTrue(client.isFactTable(cubeTbl));
    assertTrue(client.isFactTableForCube(cubeTbl, CUBE_NAME_WITH_PROPS));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    assertTrue(cubeFact.equals(cubeFact2));

    String[] storages = new String[]{c1, c2};
    String[] partColNames = new String[]{getDatePartitionKey(), itPart.getName(), etPart.getName()};

    // Assert for storage tables
    for (String entry : storageTables.keySet()) {
      String storageTableName = getFactOrDimtableStorageTableName(factName, entry);
      assertTrue(client.tableExists(storageTableName));
    }

    String c1TableName = getFactOrDimtableStorageTableName(cubeFact.getName(), c1);
    String c2TableName = getFactOrDimtableStorageTableName(cubeFact.getName(), c2);

    client.getHiveTable(c1TableName);
    Table c2Table = client.getHiveTable(c2TableName);
    c2Table.getParameters().put(getPartitionTimelineStorageClassKey(HOURLY,
      getDatePartitionKey()), StoreAllPartitionTimeline.class.getCanonicalName());
    c2Table.getParameters().put(getPartitionTimelineStorageClassKey(HOURLY,
      itPart.getName()), StoreAllPartitionTimeline.class.getCanonicalName());
    c2Table.getParameters().put(getPartitionTimelineStorageClassKey(HOURLY,
      etPart.getName()), StoreAllPartitionTimeline.class.getCanonicalName());
    client.pushHiveTable(c2Table);

    // same before insertion.
    assertSameTimelines(factName, storages, HOURLY, partColNames);
    EndsAndHolesPartitionTimeline timelineDt = ((EndsAndHolesPartitionTimeline) client.partitionTimelineCache.get(
      factName, c1, HOURLY, getDatePartitionKey()));
    EndsAndHolesPartitionTimeline timelineIt = ((EndsAndHolesPartitionTimeline) client.partitionTimelineCache.get(
      factName, c1, HOURLY, itPart.getName()));
    EndsAndHolesPartitionTimeline timelineEt = ((EndsAndHolesPartitionTimeline) client.partitionTimelineCache.get(
      factName, c1, HOURLY, etPart.getName()));
    StoreAllPartitionTimeline timelineDtC2 = ((StoreAllPartitionTimeline) client.partitionTimelineCache.get(
      factName, c2, HOURLY, getDatePartitionKey()));
    StoreAllPartitionTimeline timelineItC2 = ((StoreAllPartitionTimeline) client.partitionTimelineCache.get(
      factName, c2, HOURLY, itPart.getName()));
    StoreAllPartitionTimeline timelineEtC2 = ((StoreAllPartitionTimeline) client.partitionTimelineCache.get(
      factName, c2, HOURLY, etPart.getName()));

    Map<String, Date> timeParts1 = getTimePartitionByOffsets(getDatePartitionKey(), 0, itPart.getName(), 0,
      etPart.getName(), 0);
    StoragePartitionDesc partSpec1 = new StoragePartitionDesc(cubeFact.getName(), timeParts1, null, HOURLY);

    Map<String, Date> timeParts2 = getTimePartitionByOffsets(getDatePartitionKey(), 0, etPart.getName(), 1);
    Map<String, String> nonTimeSpec = getHashMap(itPart.getName(), "default");
    final StoragePartitionDesc partSpec2 = new StoragePartitionDesc(cubeFact.getName(), timeParts2, nonTimeSpec,
      HOURLY);

    Map<String, Date> timeParts3 = getTimePartitionByOffsets(getDatePartitionKey(), 0, etPart.getName(), 0);
    final StoragePartitionDesc partSpec3 = new StoragePartitionDesc(cubeFact.getName(), timeParts3, nonTimeSpec,
      HOURLY);

    client.addPartitions(Arrays.asList(partSpec1, partSpec2, partSpec3), c1);
    client.addPartitions(Arrays.asList(partSpec1, partSpec2, partSpec3), c2);
    PartitionTimeline timeline1Temp = client.partitionTimelineCache.get(factName, c1, HOURLY, getDatePartitionKey());
    PartitionTimeline timeline2Temp = client.partitionTimelineCache.get(factName, c2, HOURLY, getDatePartitionKey());

    assertEquals(timeline1Temp.getClass(), EndsAndHolesPartitionTimeline.class);
    assertEquals(timeline2Temp.getClass(), StoreAllPartitionTimeline.class);

    assertEquals(client.getAllParts(c1TableName).size(), 3);
    assertEquals(client.getAllParts(c2TableName).size(), 3);

    assertSameTimelines(factName, storages, HOURLY, partColNames);

    assertTimeline(timelineDt, timelineDtC2, HOURLY, 0, 0);
    assertTimeline(timelineEt, timelineEtC2, HOURLY, 0, 1);
    assertTimeline(timelineIt, timelineItC2, HOURLY, 0, 0);

    assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, getDatePartitionKey()));
    assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, itPart.getName()));
    assertTrue(client.latestPartitionExists(cubeFact.getName(), c2, etPart.getName()));

    assertNoPartitionNamedLatest(c1TableName, partColNames);
    assertNoPartitionNamedLatest(c2TableName, partColNames);

    Map<String, Date> timeParts4 = getTimePartitionByOffsets(getDatePartitionKey(), 0, itPart.getName(), 1,
      etPart.getName(), -1);
    final StoragePartitionDesc partSpec4 = new StoragePartitionDesc(cubeFact.getName(), timeParts4, null, HOURLY);


    Map<String, Date> timeParts5 = getTimePartitionByOffsets(getDatePartitionKey(), 1, itPart.getName(), -1,
      etPart.getName(), -2);
    final StoragePartitionDesc partSpec5 = new StoragePartitionDesc(cubeFact.getName(), timeParts5, null, HOURLY);

    client.addPartitions(Arrays.asList(partSpec4, partSpec5), c1);
    client.addPartitions(Arrays.asList(partSpec4, partSpec5), c2);

    assertEquals(client.getAllParts(c1TableName).size(), 5);
    assertEquals(client.getAllParts(c2TableName).size(), 5);

    assertSameTimelines(factName, storages, HOURLY, partColNames);

    assertTimeline(timelineDt, timelineDtC2, HOURLY, 0, 1);
    assertTimeline(timelineEt, timelineEtC2, HOURLY, -2, 1);
    assertTimeline(timelineIt, timelineItC2, HOURLY, -1, 1);

    assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, getDatePartitionKey()));
    assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, itPart.getName()));
    assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, etPart.getName()));

    Map<String, Date> timeParts6 = getTimePartitionByOffsets(getDatePartitionKey(), -2, itPart.getName(), -1,
      etPart.getName(), -2);
    final StoragePartitionDesc partSpec6 = new StoragePartitionDesc(cubeFact.getName(), timeParts6, null, HOURLY);

    client.addPartition(partSpec6, c1);
    client.addPartition(partSpec6, c2);

    assertEquals(client.getAllParts(c1TableName).size(), 6);
    assertEquals(client.getAllParts(c2TableName).size(), 6);

    assertSameTimelines(factName, storages, HOURLY, partColNames);

    assertTimeline(timelineDt, timelineDtC2, HOURLY, -2, 1, -1);
    assertTimeline(timelineEt, timelineEtC2, HOURLY, -2, 1);
    assertTimeline(timelineIt, timelineItC2, HOURLY, -1, 1);

    Map<String, Date> timeParts7 = getTimePartitionByOffsets(getDatePartitionKey(), -5, itPart.getName(), -5,
      etPart.getName(), -5);
    final StoragePartitionDesc partSpec7 = new StoragePartitionDesc(cubeFact.getName(), timeParts7, null, HOURLY);

    client.addPartition(partSpec7, c1);
    client.addPartition(partSpec7, c2);

    List<Partition> c1Parts = client.getAllParts(c1TableName);
    List<Partition> c2Parts = client.getAllParts(c2TableName);

    assertEquals(c1Parts.size(), 7);
    assertEquals(c2Parts.size(), 7);

    // Test update partition
    Random random = new Random();
    for (Partition partition : c1Parts) {
      partition.setLocation("blah");
      partition.setBucketCount(random.nextInt());
      client.updatePartition(factName, c1, partition);
    }
    assertSamePartitions(client.getAllParts(c1TableName), c1Parts);
    for (Partition partition : c2Parts) {
      partition.setLocation("blah");
      partition.setBucketCount(random.nextInt());
    }
    client.updatePartitions(factName, c2, c2Parts);
    assertSamePartitions(client.getAllParts(c2TableName), c2Parts);

    assertSameTimelines(factName, storages, HOURLY, partColNames);

    assertTimeline(timelineDt, timelineDtC2, HOURLY, -5, 1, -4, -3, -1);
    assertTimeline(timelineEt, timelineEtC2, HOURLY, -5, 1, -4, -3);
    assertTimeline(timelineIt, timelineItC2, HOURLY, -5, 1, -4, -3, -2);

    assertNoPartitionNamedLatest(c1TableName, partColNames);
    assertSameTimelines(factName, storages, HOURLY, partColNames);
    assertEquals(Hive.get(client.getConf()).getTable(c1TableName).getParameters().get(
      getPartitionTimelineCachePresenceKey()), "true");
    assertEquals(Hive.get(client.getConf()).getTable(c2TableName).getParameters().get(
      getPartitionTimelineCachePresenceKey()), "true");

    // alter tables and see timeline still exists
    client.alterCubeFactTable(factName, cubeFact, storageTables);
    assertSameTimelines(factName, storages, HOURLY, partColNames);
    assertEquals(Hive.get(client.getConf()).getTable(c1TableName).getParameters().get(
      getPartitionTimelineCachePresenceKey()), "true");
    assertEquals(Hive.get(client.getConf()).getTable(c2TableName).getParameters().get(
      getPartitionTimelineCachePresenceKey()), "true");


    client.dropPartition(cubeFact.getName(), c1, timeParts5, null, HOURLY);
    client.dropPartition(cubeFact.getName(), c2, timeParts5, null, HOURLY);
    assertEquals(client.getAllParts(c1TableName).size(), 6);
    assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, getDatePartitionKey()));
    assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, itPart.getName()));
    assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, etPart.getName()));

    assertNoPartitionNamedLatest(c1TableName, partColNames);
    assertSameTimelines(factName, storages, HOURLY, partColNames);

    assertTimeline(timelineDt, timelineDtC2, HOURLY, -5, 0, -4, -3, -1);
    assertTimeline(timelineEt, timelineEtC2, HOURLY, -5, 1, -4, -3);
    assertTimeline(timelineIt, timelineItC2, HOURLY, -5, 1, -4, -3, -2);


    client.dropPartition(cubeFact.getName(), c1, timeParts7, null, HOURLY);
    client.dropPartition(cubeFact.getName(), c2, timeParts7, null, HOURLY);
    assertEquals(client.getAllParts(c1TableName).size(), 5);
    assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, getDatePartitionKey()));
    assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, itPart.getName()));
    assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, etPart.getName()));

    assertNoPartitionNamedLatest(c1TableName, partColNames);
    assertSameTimelines(factName, storages, HOURLY, partColNames);
    assertTimeline(timelineDt, timelineDtC2, HOURLY, -2, 0, -1);
    assertTimeline(timelineEt, timelineEtC2, HOURLY, -2, 1);
    assertTimeline(timelineIt, timelineItC2, HOURLY, -1, 1);


    client.dropPartition(cubeFact.getName(), c1, timeParts2, nonTimeSpec, HOURLY);
    client.dropPartition(cubeFact.getName(), c2, timeParts2, nonTimeSpec, HOURLY);
    assertEquals(client.getAllParts(c1TableName).size(), 4);
    assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, getDatePartitionKey()));
    assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, itPart.getName()));
    assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, etPart.getName()));

    assertNoPartitionNamedLatest(c1TableName, partColNames);
    assertSameTimelines(factName, storages, HOURLY, partColNames);
    assertTimeline(timelineDt, timelineDtC2, HOURLY, -2, 0, -1);
    assertTimeline(timelineEt, timelineEtC2, HOURLY, -2, 0);
    assertTimeline(timelineIt, timelineItC2, HOURLY, -1, 1);

    client.dropPartition(cubeFact.getName(), c1, timeParts4, null, HOURLY);
    client.dropPartition(cubeFact.getName(), c2, timeParts4, null, HOURLY);
    assertEquals(client.getAllParts(c1TableName).size(), 3);

    assertNoPartitionNamedLatest(c1TableName, partColNames);
    assertSameTimelines(factName, storages, HOURLY, partColNames);
    assertTimeline(timelineDt, timelineDtC2, HOURLY, -2, 0, -1);
    assertTimeline(timelineEt, timelineEtC2, HOURLY, -2, 0, -1);
    assertTimeline(timelineIt, timelineItC2, HOURLY, -1, 0);
    client.dropPartition(cubeFact.getName(), c1, timeParts3, nonTimeSpec, HOURLY);
    client.dropPartition(cubeFact.getName(), c2, timeParts3, nonTimeSpec, HOURLY);
    assertSameTimelines(factName, storages, HOURLY, partColNames);
    assertTimeline(timelineDt, timelineDtC2, HOURLY, -2, 0, -1);
    assertTimeline(timelineEt, timelineEtC2, HOURLY, -2, 0, -1);
    assertTimeline(timelineIt, timelineItC2, HOURLY, -1, 0);

    client.dropPartition(cubeFact.getName(), c1, timeParts6, null, HOURLY);
    client.dropPartition(cubeFact.getName(), c2, timeParts6, null, HOURLY);
    assertSameTimelines(factName, storages, HOURLY, partColNames);
    assertTimeline(timelineDt, timelineDtC2, HOURLY, 0, 0);
    assertTimeline(timelineEt, timelineEtC2, HOURLY, 0, 0);
    assertTimeline(timelineIt, timelineItC2, HOURLY, 0, 0);
    client.dropPartition(cubeFact.getName(), c1, timeParts1, null, HOURLY);
    client.dropPartition(cubeFact.getName(), c2, timeParts1, null, HOURLY);
    assertSameTimelines(factName, storages, HOURLY, partColNames);
    assertTrue(timelineDt.isEmpty());
    assertTrue(timelineEt.isEmpty());
    assertTrue(timelineIt.isEmpty());

  }

  private void assertSamePartitions(List<Partition> parts1, List<Partition> parts2) {
    assertEquals(parts1.size(), parts2.size());
    for (int i = 0; i < parts1.size(); i++) {
      assertEquals(parts1.get(i).toString(), parts2.get(i).toString(), "toString differs at element " + i);
      assertEquals(parts1.get(i).getPath(), parts2.get(i).getPath(), "path differs at element " + i);
      assertEquals(parts1.get(i).getLocation(), parts2.get(i).getLocation(), "location differs at element " + i);
      assertEquals(parts1.get(i).getBucketCount(), parts2.get(i).getBucketCount(),
        "bucket count differs at element " + i);
      assertEquals(parts1.get(i).getCompleteName(), parts2.get(i).getCompleteName(),
        "complete name differs at element " + i);
    }
  }

  private void assertTimeline(EndsAndHolesPartitionTimeline endsAndHolesPartitionTimeline,
    StoreAllPartitionTimeline storeAllPartitionTimeline, UpdatePeriod updatePeriod,
    int firstOffset, int latestOffset, int... holeOffsets) throws LensException {
    Date[] holeDates = new Date[holeOffsets.length];
    for(int i = 0; i < holeOffsets.length; i++) {
      holeDates[i] = getDateWithOffset(HOURLY, holeOffsets[i]);
    }
    assertTimeline(endsAndHolesPartitionTimeline, storeAllPartitionTimeline, updatePeriod,
      getDateWithOffset(HOURLY, firstOffset), getDateWithOffset(HOURLY, latestOffset), holeDates);
  }
  private void assertTimeline(EndsAndHolesPartitionTimeline endsAndHolesPartitionTimeline,
    StoreAllPartitionTimeline storeAllPartitionTimeline, UpdatePeriod updatePeriod,
    Date first, Date latest, Date... holes) throws LensException {
    TimePartition firstPart = TimePartition.of(updatePeriod, first);
    TimePartition latestPart = TimePartition.of(updatePeriod, latest);
    assertEquals(endsAndHolesPartitionTimeline.getFirst(), firstPart);
    assertEquals(endsAndHolesPartitionTimeline.getLatest(), TimePartition.of(updatePeriod, latest));
    assertEquals(endsAndHolesPartitionTimeline.getHoles().size(), holes.length);
    for (Date date : holes) {
      assertTrue(endsAndHolesPartitionTimeline.getHoles().contains(TimePartition.of(updatePeriod, date)));
    }

    TreeSet<TimePartition> partitions = new TreeSet<>();
    for (Date dt : TimeRange.iterable(firstPart.getDate(), latestPart.next().getDate(), updatePeriod, 1)) {
      partitions.add(TimePartition.of(updatePeriod, dt));
    }
    for (Date holeDate : holes) {
      partitions.remove(TimePartition.of(updatePeriod, holeDate));
    }
    assertEquals(storeAllPartitionTimeline.getAllPartitions(), partitions);
  }

  private void assertSameTimelines(String factName, String[] storages, UpdatePeriod updatePeriod, String[] partCols)
    throws HiveException, LensException {
    for (String partCol : partCols) {
      ArrayList<PartitionTimeline> timelines = Lists.newArrayList();
      for (String storage : storages) {
        timelines.add(client.partitionTimelineCache.get(factName, storage, updatePeriod, partCol));
      }
      TestPartitionTimelines.assertSameTimelines(timelines);
    }
  }

  private void assertNoPartitionNamedLatest(String storageTableName, String... partCols) throws HiveException {
    for (String p : partCols) {
      assertEquals(client.getPartitionsByFilter(storageTableName, p + "='latest'").size(), 0);
    }
  }

  private TimePartition[] getLatestValues(String storageTableName, UpdatePeriod updatePeriod, String[] partCols,
    Map<String, String> nonTimeParts)
    throws LensException, HiveException {
    TimePartition[] values = new TimePartition[partCols.length];
    for (int i = 0; i < partCols.length; i++) {
      List<Partition> parts = client.getPartitionsByFilter(storageTableName,
        StorageConstants.getLatestPartFilter(partCols[i], nonTimeParts));
      for (Partition part : parts) {
        boolean ignore = false;
        if (nonTimeParts != null) {
          for (Map.Entry<String, String> entry : part.getSpec().entrySet()) {
            if (!nonTimeParts.containsKey(entry.getKey())) {
              try {
                updatePeriod.parse(entry.getValue());
              } catch (java.text.ParseException e) {
                ignore = true;
                break;
              }
            }
          }
        }
        if (ignore) {
          break;
        }
        TimePartition tp = TimePartition.of(updatePeriod, part.getParameters().get(
          getLatestPartTimestampKey(partCols[i])));
        if (values[i] == null || values[i].before(tp)) {
          values[i] = tp;
        }
      }
    }
    return values;
  }

  private TimePartition[] toPartitionArray(UpdatePeriod updatePeriod, int... offsets) throws LensException {
    TimePartition[] values = new TimePartition[offsets.length];
    for (int i = 0; i < offsets.length; i++) {
      values[i] = TimePartition.of(updatePeriod, getDateWithOffset(updatePeriod, offsets[i]));
    }
    return values;
  }

  @Test(priority = 2)
  public void testCubeFactWithWeight() throws Exception {
    String factName = "testFactWithWeight";
    List<FieldSchema> factColumns = new ArrayList<>(cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add one dimension of the cube
    factColumns.add(new FieldSchema("zipcode", "int", "zip"));
    Set<UpdatePeriod> updates = Sets.newHashSet(HOURLY, DAILY);
    StorageTableDesc s1 = new StorageTableDesc(TextInputFormat.class, HiveIgnoreKeyTextOutputFormat.class,
      datePartSingleton, datePartKeySingleton);
    Map<String, Set<UpdatePeriod>> updatePeriods = getHashMap(c1, updates);
    Map<String, StorageTableDesc> storageTables = getHashMap(c1, s1);

    CubeFactTable cubeFact = new CubeFactTable(CUBE_NAME, factName, factColumns, updatePeriods, 100L);

    // create cube fact
    client.createCubeFactTable(CUBE_NAME, factName, factColumns, updatePeriods, 100L, null, storageTables);

    assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    assertTrue(client.isFactTable(cubeTbl));
    assertTrue(client.isFactTableForCube(cubeTbl, CUBE_NAME));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    assertTrue(cubeFact.equals(cubeFact2));

    // Assert for storage tables
    for (String entry : storageTables.keySet()) {
      String storageTableName = getFactOrDimtableStorageTableName(factName, entry);
      assertTrue(client.tableExists(storageTableName));
    }

    // test partition
    Map<String, Date> timeParts = getTimePartitionByOffsets(getDatePartitionKey(), 0);
    StoragePartitionDesc partSpec = new StoragePartitionDesc(cubeFact.getName(), timeParts, null, HOURLY);
    client.addPartition(partSpec, c1);
    assertTrue(client.factPartitionExists(cubeFact.getName(), c1, HOURLY, timeParts, emptyHashMap));
    assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, getDatePartitionKey()));
    String storageTableName = getFactOrDimtableStorageTableName(cubeFact.getName(), c1);
    List<Partition> parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    assertEquals(parts.size(), 0);
    assertEquals(client.getAllParts(storageTableName).size(), 1);

    client.dropPartition(cubeFact.getName(), c1, timeParts, null, HOURLY);
    assertFalse(client.factPartitionExists(cubeFact.getName(), c1, HOURLY, timeParts, emptyHashMap));
    assertFalse(client.latestPartitionExists(cubeFact.getName(), c1, getDatePartitionKey()));
    assertEquals(client.getAllParts(storageTableName).size(), 0);
  }

  @Test(priority = 2)
  public void testCubeFactWithParts() throws Exception {
    List<FieldSchema> factColumns = new ArrayList<>(cubeMeasures.size());
    String factNameWithPart = "testFactPart";

    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add some dimensions of the cube
    factColumns.add(new FieldSchema("dim1", "string", "dim1"));
    factColumns.add(new FieldSchema("dim2", "string", "dim2"));

    List<FieldSchema> factPartColumns = Lists.newArrayList(new FieldSchema("region", "string", "region part"));

    Set<UpdatePeriod> updates = Sets.newHashSet(HOURLY, DAILY);
    ArrayList<FieldSchema> partCols = Lists.newArrayList(getDatePartition(), factPartColumns.get(0));
    StorageTableDesc s1 = new StorageTableDesc(TextInputFormat.class, HiveIgnoreKeyTextOutputFormat.class, partCols,
      datePartKeySingleton);
    Map<String, Set<UpdatePeriod>> updatePeriods = getHashMap(c1, updates);
    Map<String, StorageTableDesc> storageTables = getHashMap(c1, s1);

    CubeFactTable cubeFactWithParts = new CubeFactTable(CUBE_NAME, factNameWithPart, factColumns, updatePeriods);

    // create cube fact
    client.createCubeFactTable(CUBE_NAME, factNameWithPart, factColumns, updatePeriods, 0L, null, storageTables);

    assertTrue(client.tableExists(factNameWithPart));
    Table cubeTbl = client.getHiveTable(factNameWithPart);
    assertTrue(client.isFactTable(cubeTbl));
    assertTrue(client.isFactTableForCube(cubeTbl, CUBE_NAME));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    assertTrue(cubeFactWithParts.equals(cubeFact2));

    // Assert for storage tables
    for (String entry : storageTables.keySet()) {
      String storageTableName = getFactOrDimtableStorageTableName(factNameWithPart, entry);
      assertTrue(client.tableExists(storageTableName));
    }

    Map<String, String> partSpec = getHashMap(factPartColumns.get(0).getName(), "APAC");
    Map<String, Date> timeParts = getTimePartitionByOffsets(getDatePartitionKey(), 0);
    // test partition
    StoragePartitionDesc sPartSpec =
      new StoragePartitionDesc(cubeFactWithParts.getName(), timeParts, partSpec, HOURLY);
    client.addPartition(sPartSpec, c1);
    assertTrue(client.factPartitionExists(cubeFactWithParts.getName(), c1, HOURLY, timeParts, partSpec));
    assertTrue(client.latestPartitionExists(cubeFactWithParts.getName(), c1, getDatePartitionKey()));
    assertFalse(client.latestPartitionExists(cubeFactWithParts.getName(), c1, factPartColumns.get(0).getName()));
    String storageTableName = getFactOrDimtableStorageTableName(cubeFactWithParts.getName(), c1);
    assertEquals(client.getAllParts(storageTableName).size(), 1);
    List<Partition> parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    assertEquals(parts.size(), 0);

    client.dropPartition(cubeFactWithParts.getName(), c1, timeParts, partSpec, HOURLY);
    assertFalse(client.factPartitionExists(cubeFactWithParts.getName(), c1, HOURLY, timeParts, partSpec));
    assertFalse(client.latestPartitionExists(cubeFactWithParts.getName(), c1, getDatePartitionKey()));
    assertEquals(client.getAllParts(storageTableName).size(), 0);
  }

  @Test(priority = 2)
  public void testCubeFactWithPartsAndTimedParts() throws Exception {
    List<FieldSchema> factColumns = new ArrayList<>(cubeMeasures.size());
    String factNameWithPart = "testFactPartAndTimedParts";

    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add some dimensions of the cube
    factColumns.add(new FieldSchema("dim1", "string", "dim1"));
    factColumns.add(new FieldSchema("dim2", "string", "dim2"));

    List<FieldSchema> factPartColumns = new ArrayList<>();
    factPartColumns.add(new FieldSchema("region", "string", "region part"));

    Set<UpdatePeriod> updates = Sets.newHashSet(HOURLY, DAILY);
    FieldSchema testDtPart = new FieldSchema("mydate", "string", "date part");
    ArrayList<FieldSchema> partCols = Lists.newArrayList(getDatePartition(), testDtPart, factPartColumns.get(0));
    List<String> timePartCols = Lists.newArrayList(getDatePartitionKey(), testDtPart.getName());
    StorageTableDesc s1 = new StorageTableDesc(TextInputFormat.class, HiveIgnoreKeyTextOutputFormat.class, partCols,
      timePartCols);
    Map<String, Set<UpdatePeriod>> updatePeriods = getHashMap(c1, updates);
    Map<String, StorageTableDesc> storageTables = getHashMap(c1, s1);

    CubeFactTable cubeFactWithParts = new CubeFactTable(CUBE_NAME, factNameWithPart, factColumns, updatePeriods);

    // create cube fact
    client.createCubeFactTable(CUBE_NAME, factNameWithPart, factColumns, updatePeriods, 0L, null, storageTables);

    assertTrue(client.tableExists(factNameWithPart));
    Table cubeTbl = client.getHiveTable(factNameWithPart);
    assertTrue(client.isFactTable(cubeTbl));
    assertTrue(client.isFactTableForCube(cubeTbl, CUBE_NAME));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    assertTrue(cubeFactWithParts.equals(cubeFact2));

    // Assert for storage tables
    for (String entry : storageTables.keySet()) {
      String storageTableName = getFactOrDimtableStorageTableName(factNameWithPart, entry);
      assertTrue(client.tableExists(storageTableName));
    }

    Map<String, String> partSpec = getHashMap(factPartColumns.get(0).getName(), "APAC");
    Map<String, Date> timeParts = getTimePartitionByOffsets(getDatePartitionKey(), 0, testDtPart.getName(), -1);
    // test partition
    StoragePartitionDesc sPartSpec =
      new StoragePartitionDesc(cubeFactWithParts.getName(), timeParts, partSpec, HOURLY);
    client.addPartition(sPartSpec, c1);
    assertTrue(client.factPartitionExists(cubeFactWithParts.getName(), c1, HOURLY, timeParts, partSpec));
    assertTrue(client.latestPartitionExists(cubeFactWithParts.getName(), c1, getDatePartitionKey()));
    assertTrue(client.latestPartitionExists(cubeFactWithParts.getName(), c1, testDtPart.getName()));
    assertFalse(client.latestPartitionExists(cubeFactWithParts.getName(), c1, factPartColumns.get(0).getName()));
    String storageTableName = getFactOrDimtableStorageTableName(cubeFactWithParts.getName(), c1);
    assertEquals(client.getAllParts(storageTableName).size(), 1);

    assertNoPartitionNamedLatest(storageTableName, "dt", testDtPart.getName());

    client.dropPartition(cubeFactWithParts.getName(), c1, timeParts, partSpec, HOURLY);
    assertFalse(client.factPartitionExists(cubeFactWithParts.getName(), c1, HOURLY, timeParts, partSpec));
    assertFalse(client.latestPartitionExists(cubeFactWithParts.getName(), c1, testDtPart.getName()));
    assertFalse(client.latestPartitionExists(cubeFactWithParts.getName(), c1, getDatePartitionKey()));
    assertEquals(client.getAllParts(storageTableName).size(), 0);
  }

  @Test(priority = 2)
  public void testCubeFactWithTwoStorages() throws Exception {
    List<FieldSchema> factColumns = new ArrayList<>(cubeMeasures.size());
    String factName = "testFactTwoStorages";

    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add some dimensions of the cube
    factColumns.add(new FieldSchema("dim1", "string", "dim1"));
    factColumns.add(new FieldSchema("dim2", "string", "dim2"));

    List<FieldSchema> factPartColumns = new ArrayList<>();
    factPartColumns.add(new FieldSchema("region", "string", "region part"));

    Set<UpdatePeriod> updates = Sets.newHashSet(HOURLY, DAILY);
    ArrayList<FieldSchema> partCols = Lists.newArrayList(getDatePartition(), factPartColumns.get(0));
    StorageTableDesc s1 = new StorageTableDesc(SequenceFileInputFormat.class, HiveIgnoreKeyTextOutputFormat.class,
      partCols, datePartKeySingleton);
    StorageTableDesc s2 = new StorageTableDesc(TextInputFormat.class, HiveIgnoreKeyTextOutputFormat.class,
      datePartSingleton, datePartKeySingleton);
    Map<String, Set<UpdatePeriod>> updatePeriods = getHashMap(c1, updates, c2, updates);
    Map<String, StorageTableDesc> storageTables = getHashMap(c1, s1, c2, s2);

    CubeFactTable cubeFactWithTwoStorages = new CubeFactTable(CUBE_NAME, factName, factColumns, updatePeriods);

    client.createCubeFactTable(CUBE_NAME, factName, factColumns, updatePeriods, 0L, null, storageTables);

    assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    assertTrue(client.isFactTable(cubeTbl));
    assertTrue(client.isFactTableForCube(cubeTbl, CUBE_NAME));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    assertTrue(cubeFactWithTwoStorages.equals(cubeFact2));

    // Assert for storage tables
    for (String entry : storageTables.keySet()) {
      String storageTableName = getFactOrDimtableStorageTableName(factName, entry);
      assertTrue(client.tableExists(storageTableName));
    }

    Map<String, String> partSpec = getHashMap(factPartColumns.get(0).getName(), "APAC");
    Map<String, Date> timeParts = getTimePartitionByOffsets(getDatePartitionKey(), 0);
    // test partition
    StoragePartitionDesc sPartSpec =
      new StoragePartitionDesc(cubeFactWithTwoStorages.getName(), timeParts, partSpec, HOURLY);
    client.addPartition(sPartSpec, c1);
    assertTrue(client.factPartitionExists(cubeFactWithTwoStorages.getName(), c1, HOURLY, timeParts, partSpec));
    assertTrue(client.latestPartitionExists(cubeFactWithTwoStorages.getName(), c1, getDatePartitionKey()));
    String storageTableName = getFactOrDimtableStorageTableName(cubeFactWithTwoStorages.getName(), c1);
    assertEquals(client.getAllParts(storageTableName).size(), 1);

    assertNoPartitionNamedLatest(storageTableName, "dt");

    StoragePartitionDesc sPartSpec2 =
      new StoragePartitionDesc(cubeFactWithTwoStorages.getName(), timeParts, null, HOURLY);
    client.addPartition(sPartSpec2, c2);
    assertTrue(client.factPartitionExists(cubeFactWithTwoStorages.getName(), c2, HOURLY, timeParts, emptyHashMap));
    assertTrue(client.latestPartitionExists(cubeFactWithTwoStorages.getName(), c2, getDatePartitionKey()));
    String storageTableName2 = getFactOrDimtableStorageTableName(cubeFactWithTwoStorages.getName(), c2);
    assertEquals(client.getAllParts(storageTableName2).size(), 1);

    assertNoPartitionNamedLatest(storageTableName2, "dt");

    client.dropPartition(cubeFactWithTwoStorages.getName(), c1, timeParts, partSpec, HOURLY);
    assertFalse(client.factPartitionExists(cubeFactWithTwoStorages.getName(), c1, HOURLY, timeParts, partSpec));
    assertFalse(client.latestPartitionExists(cubeFactWithTwoStorages.getName(), c1, getDatePartitionKey()));
    assertEquals(client.getAllParts(storageTableName).size(), 0);

    client.dropPartition(cubeFactWithTwoStorages.getName(), c2, timeParts, null, HOURLY);
    assertFalse(client.factPartitionExists(cubeFactWithTwoStorages.getName(), c2, HOURLY, timeParts, emptyHashMap));
    assertFalse(client.latestPartitionExists(cubeFactWithTwoStorages.getName(), c2, getDatePartitionKey()));
    assertEquals(client.getAllParts(storageTableName2).size(), 0);
  }

  @Test(priority = 2)
  public void testCubeDimWithWeight() throws Exception {
    String dimName = "statetable";

    List<FieldSchema> dimColumns = new ArrayList<>();
    dimColumns.add(new FieldSchema("id", "int", "state id"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("capital", "string", "field2"));
    dimColumns.add(new FieldSchema("countryid", "int", "country id"));


    StorageTableDesc s1 = new StorageTableDesc(TextInputFormat.class, HiveIgnoreKeyTextOutputFormat.class,
      datePartSingleton, datePartKeySingleton);

    Map<String, UpdatePeriod> dumpPeriods = getHashMap(c1, HOURLY);
    Map<String, StorageTableDesc> storageTables = getHashMap(c1, s1);

    CubeDimensionTable cubeDim = new CubeDimensionTable(stateDim.getName(), dimName, dimColumns, 100L, dumpPeriods);
    client.createCubeDimensionTable(stateDim.getName(), dimName, dimColumns, 100L, dumpPeriods, null, storageTables);
    assertTrue(client.tableExists(dimName));
    Table cubeTbl = client.getHiveTable(dimName);
    assertTrue(client.isDimensionTable(cubeTbl));
    CubeDimensionTable cubeDim2 = new CubeDimensionTable(cubeTbl);
    assertTrue(cubeDim.equals(cubeDim2));

    List<CubeDimensionTable> stateTbls = client.getAllDimensionTables(stateDim);
    boolean found = false;
    for (CubeDimensionTable dim : stateTbls) {
      if (dim.getName().equalsIgnoreCase(dimName)) {
        found = true;
        break;
      }
    }
    assertTrue(found);

    // Assert for storage tables
    for (String storage : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getStorageTableName(dimName, Storage.getPrefix(storage));
      assertTrue(client.tableExists(storageTableName));
    }

    // test partition
    Map<String, Date> timeParts = getTimePartitionByOffsets(getDatePartitionKey(), 0);
    StoragePartitionDesc sPartSpec = new StoragePartitionDesc(cubeDim.getName(), timeParts, null, HOURLY);
    client.addPartition(sPartSpec, c1);
    assertTrue(client.dimPartitionExists(cubeDim.getName(), c1, timeParts));
    assertTrue(client.latestPartitionExists(cubeDim.getName(), c1, getDatePartitionKey()));
    String storageTableName = getFactOrDimtableStorageTableName(dimName, c1);
    assertEquals(client.getAllParts(storageTableName).size(), 2);
    List<Partition> parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    assertEquals(1, parts.size());
    assertEquals(TextInputFormat.class.getCanonicalName(), parts.get(0).getInputFormatClass().getCanonicalName());
    assertEquals(parts.get(0).getParameters().get(getLatestPartTimestampKey("dt")), getDateStringWithOffset(HOURLY, 0));

    client.dropPartition(cubeDim.getName(), c1, timeParts, null, HOURLY);
    assertFalse(client.dimPartitionExists(cubeDim.getName(), c1, timeParts));
    assertFalse(client.latestPartitionExists(cubeDim.getName(), c1, getDatePartitionKey()));
    assertEquals(client.getAllParts(storageTableName).size(), 0);
  }

  @Test(priority = 2)
  public void testCubeDim() throws Exception {
    String dimName = "ziptableMeta";

    List<FieldSchema> dimColumns;
    dimColumns = new ArrayList<>();
    dimColumns.add(new FieldSchema("zipcode", "int", "code"));
    dimColumns.add(new FieldSchema("f1", "string", "field1"));
    dimColumns.add(new FieldSchema("f2", "string", "field2"));
    dimColumns.add(new FieldSchema("stateid", "int", "state id"));
    dimColumns.add(new FieldSchema("statei2", "int", "state id"));

    StorageTableDesc s1 = new StorageTableDesc(TextInputFormat.class, HiveIgnoreKeyTextOutputFormat.class,
      datePartSingleton, datePartKeySingleton);

    Map<String, UpdatePeriod> dumpPeriods = getHashMap(c1, HOURLY);
    Map<String, StorageTableDesc> storageTables = getHashMap(c1, s1);

    CubeDimensionTable cubeDim = new CubeDimensionTable(zipDim.getName(), dimName, dimColumns, 0L, dumpPeriods);
    client.createCubeDimensionTable(zipDim.getName(), dimName, dimColumns, 0L, dumpPeriods, null, storageTables);

    assertTrue(client.tableExists(dimName));

    Table cubeTbl = client.getHiveTable(dimName);
    assertTrue(client.isDimensionTable(cubeTbl));

    List<CubeDimensionTable> tbls = client.getAllDimensionTables(zipDim);
    boolean found = false;
    for (CubeDimensionTable dim : tbls) {
      if (dim.getName().equalsIgnoreCase(dimName)) {
        found = true;
        break;
      }
    }
    assertTrue(found);

    CubeDimensionTable cubeDim2 = new CubeDimensionTable(cubeTbl);
    assertTrue(cubeDim.equals(cubeDim2));

    // Assert for storage tables
    for (String storage : storageTables.keySet()) {
      String storageTableName = getFactOrDimtableStorageTableName(dimName, storage);
      assertTrue(client.tableExists(storageTableName));
    }

    FieldSchema newcol = new FieldSchema("newcol", "int", "new col for part");

    // test partition
    String storageTableName = getFactOrDimtableStorageTableName(dimName, c1);
    assertFalse(client.dimTableLatestPartitionExists(storageTableName));

    Map<String, Date> timePartsNow = getHashMap(getDatePartitionKey(), NOW);
    StoragePartitionDesc sPartSpec0 = new StoragePartitionDesc(cubeDim.getName(), timePartsNow, null, HOURLY);

    client.addPartition(sPartSpec0, c1);
    assertTrue(client.dimPartitionExists(cubeDim.getName(), c1, timePartsNow));
    assertTrue(client.latestPartitionExists(cubeDim.getName(), c1, getDatePartitionKey()));
    assertEquals(client.getAllParts(storageTableName).size(), 2);

    List<Partition> parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    assertEquals(parts.size(), 1);
    Partition latestPart = parts.get(0);
    assertEquals(latestPart.getInputFormatClass(), TextInputFormat.class);
    assertFalse(latestPart.getCols().contains(newcol));
    assertEquals(latestPart.getParameters().get(getLatestPartTimestampKey("dt")), getDateStringWithOffset(HOURLY, 0));

    // Partition with different schema
    cubeDim.alterColumn(newcol);
    client.alterCubeDimensionTable(cubeDim.getName(), cubeDim, storageTables);

    Map<String, Date> timeParts1 = getTimePartitionByOffsets(getDatePartitionKey(), 1);
    StoragePartitionDesc sPartSpec1 = new StoragePartitionDesc(cubeDim.getName(), timeParts1, null, HOURLY);
    sPartSpec1.setInputFormat(SequenceFileInputFormat.class.getCanonicalName());
    sPartSpec1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    client.addPartition(sPartSpec1, c1);
    // assert on all partitions
    assertEquals(client.getAllParts(storageTableName).size(), 3);
    // non-latest partitions
    assertTrue(client.dimPartitionExists(cubeDim.getName(), c1, timePartsNow));
    assertTrue(client.dimPartitionExists(cubeDim.getName(), c1, timeParts1));
    // assert on latest partition
    assertTrue(client.latestPartitionExists(cubeDim.getName(), c1, getDatePartitionKey()));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    assertEquals(parts.size(), 1);
    latestPart = parts.get(0);
    assertEquals(latestPart.getInputFormatClass(), SequenceFileInputFormat.class);
    assertTrue(latestPart.getCols().contains(newcol));
    assertEquals(latestPart.getParameters().get(getLatestPartTimestampKey("dt")), getDateStringWithOffset(HOURLY, 1));

    // add one more partition
    Map<String, Date> timeParts2 = getTimePartitionByOffsets(getDatePartitionKey(), 2);
    StoragePartitionDesc sPartSpec2 = new StoragePartitionDesc(cubeDim.getName(), timeParts2, null, HOURLY);
    client.addPartition(sPartSpec2, c1);
    // assert on all partitions
    assertEquals(client.getAllParts(storageTableName).size(), 4);
    // non-latest partitions
    assertTrue(client.dimPartitionExists(cubeDim.getName(), c1, timePartsNow));
    assertTrue(client.dimPartitionExists(cubeDim.getName(), c1, timeParts1));
    assertTrue(client.dimPartitionExists(cubeDim.getName(), c1, timeParts2));
    // assert on latest partition
    assertTrue(client.latestPartitionExists(cubeDim.getName(), c1, getDatePartitionKey()));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    assertEquals(parts.size(), 1);
    latestPart = parts.get(0);
    assertEquals(latestPart.getInputFormatClass(), TextInputFormat.class);
    assertTrue(latestPart.getCols().contains(newcol));
    assertEquals(latestPart.getParameters().get(getLatestPartTimestampKey("dt")), getDateStringWithOffset(HOURLY, 2));

    // drop the last added partition
    client.dropPartition(cubeDim.getName(), c1, timeParts2, null, HOURLY);
    assertTrue(client.dimPartitionExists(cubeDim.getName(), c1, timeParts1));
    assertTrue(client.dimPartitionExists(cubeDim.getName(), c1, timePartsNow));
    assertTrue(client.latestPartitionExists(cubeDim.getName(), c1, getDatePartitionKey()));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    assertEquals(parts.size(), 1);
    latestPart = parts.get(0);
    assertEquals(latestPart.getInputFormatClass(), SequenceFileInputFormat.class);
    assertEquals(latestPart.getParameters().get(getLatestPartTimestampKey("dt")), getDateStringWithOffset(HOURLY, 1));
    assertEquals(client.getAllParts(storageTableName).size(), 3);

    // drop the first partition, leaving the middle.
    client.dropPartition(cubeDim.getName(), c1, timePartsNow, null, HOURLY);
    assertTrue(client.dimPartitionExists(cubeDim.getName(), c1, timeParts1));
    assertFalse(client.dimPartitionExists(cubeDim.getName(), c1, timePartsNow));
    assertTrue(client.latestPartitionExists(cubeDim.getName(), c1, getDatePartitionKey()));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    assertEquals(parts.size(), 1);
    latestPart = parts.get(0);
    assertEquals(latestPart.getInputFormatClass(), SequenceFileInputFormat.class);
    assertEquals(latestPart.getParameters().get(getLatestPartTimestampKey("dt")), getDateStringWithOffset(HOURLY, 1));
    assertEquals(client.getAllParts(storageTableName).size(), 2);

    client.dropPartition(cubeDim.getName(), c1, timeParts1, null, HOURLY);
    assertFalse(client.dimPartitionExists(cubeDim.getName(), c1, timeParts1));
    assertFalse(client.dimPartitionExists(cubeDim.getName(), c1, timePartsNow));
    assertFalse(client.latestPartitionExists(cubeDim.getName(), c1, getDatePartitionKey()));
    assertEquals(client.getAllParts(storageTableName).size(), 0);
    assertFalse(client.dimTableLatestPartitionExists(storageTableName));

    client.addPartition(sPartSpec1, c1);
    assertTrue(client.dimTableLatestPartitionExists(storageTableName));
    client.dropStorageFromDim(cubeDim.getName(), c1);
    assertFalse(client.dimTableLatestPartitionExists(storageTableName));
  }

  @Test(priority = 2)
  public void testCubeDimWithNonTimeParts() throws Exception {
    String dimName = "countrytable_partitioned";

    List<FieldSchema> dimColumns = new ArrayList<>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("capital", "string", "field2"));

    Set<String> storageNames = new HashSet<>();

    ArrayList<FieldSchema> partCols = Lists.newArrayList(new FieldSchema("region", "string", "region name"),
      getDatePartition());
    StorageTableDesc s1 = new StorageTableDesc(TextInputFormat.class, HiveIgnoreKeyTextOutputFormat.class, partCols,
      datePartKeySingleton);
    storageNames.add(c3);

    Map<String, StorageTableDesc> storageTables = getHashMap(c3, s1);

    client.createCubeDimensionTable(countryDim.getName(), dimName, dimColumns, 0L, storageNames, null, storageTables);
    // test partition
    String storageTableName = getFactOrDimtableStorageTableName(dimName, c3);
    assertFalse(client.dimTableLatestPartitionExists(storageTableName));
    Map<String, Date> expectedLatestValues = Maps.newHashMap();
    Map<String, Date> timeParts = new HashMap<>();
    Map<String, String> nonTimeParts = new HashMap<>();

    timeParts.put(getDatePartitionKey(), NOW);
    nonTimeParts.put("region", "asia");
    StoragePartitionDesc sPartSpec = new StoragePartitionDesc(dimName, timeParts, nonTimeParts, HOURLY);
    client.addPartition(sPartSpec, c3);
    expectedLatestValues.put("asia", NOW);
    assertLatestForRegions(storageTableName, expectedLatestValues);

    timeParts.put(getDatePartitionKey(), getDateWithOffset(HOURLY, -1));
    nonTimeParts.put("region", "africa");
    sPartSpec = new StoragePartitionDesc(dimName, timeParts, nonTimeParts, HOURLY);
    client.addPartition(sPartSpec, c3);
    expectedLatestValues.put("asia", NOW);
    expectedLatestValues.put("africa", getDateWithOffset(HOURLY, -1));
    assertLatestForRegions(storageTableName, expectedLatestValues);

    timeParts.put(getDatePartitionKey(), getDateWithOffset(HOURLY, 1));
    nonTimeParts.put("region", "africa");
    sPartSpec = new StoragePartitionDesc(dimName, timeParts, nonTimeParts, HOURLY);
    client.addPartition(sPartSpec, c3);
    expectedLatestValues.put("asia", NOW);
    expectedLatestValues.put("africa", getDateWithOffset(HOURLY, 1));
    assertLatestForRegions(storageTableName, expectedLatestValues);

    timeParts.put(getDatePartitionKey(), getDateWithOffset(HOURLY, 3));
    nonTimeParts.put("region", "asia");
    sPartSpec = new StoragePartitionDesc(dimName, timeParts, nonTimeParts, HOURLY);
    client.addPartition(sPartSpec, c3);
    expectedLatestValues.put("asia", getDateWithOffset(HOURLY, 3));
    expectedLatestValues.put("africa", getDateWithOffset(HOURLY, 1));
    assertLatestForRegions(storageTableName, expectedLatestValues);

    client.dropPartition(dimName, c3, timeParts, nonTimeParts, HOURLY);
    expectedLatestValues.put("asia", NOW);
    expectedLatestValues.put("africa", getDateWithOffset(HOURLY, 1));
    assertLatestForRegions(storageTableName, expectedLatestValues);

    timeParts.put(getDatePartitionKey(), NOW);
    client.dropPartition(dimName, c3, timeParts, nonTimeParts, HOURLY);
    expectedLatestValues.remove("asia");
    assertLatestForRegions(storageTableName, expectedLatestValues);

    nonTimeParts.put("region", "africa");
    timeParts.put(getDatePartitionKey(), getDateWithOffset(HOURLY, -1));
    assertLatestForRegions(storageTableName, expectedLatestValues);

    timeParts.put(getDatePartitionKey(), getDateWithOffset(HOURLY, 3));
    nonTimeParts.remove("africa");
    assertLatestForRegions(storageTableName, expectedLatestValues);
  }

  private void assertLatestForRegions(String storageTableName, Map<String, Date> expectedLatestValues)
    throws HiveException, LensException {
    List<Partition> parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    assertEquals(parts.size(), expectedLatestValues.size());
    for (Partition part : parts) {
      assertEquals(MetastoreUtil.getLatestTimeStampFromPartition(part, getDatePartitionKey()),
        TimePartition.of(HOURLY, expectedLatestValues.get(part.getSpec().get("region"))).getDate());
    }
  }


  @Test(priority = 2)
  public void testCubeDimWithThreeTimedParts() throws Exception {
    String dimName = "ziptableMetaWithThreeTimedParts";

    List<FieldSchema> dimColumns = new ArrayList<>();
    dimColumns.add(new FieldSchema("zipcode", "int", "code"));
    dimColumns.add(new FieldSchema("f1", "string", "field1"));
    dimColumns.add(new FieldSchema("f2", "string", "field2"));
    dimColumns.add(new FieldSchema("stateid", "int", "state id"));
    dimColumns.add(new FieldSchema("statei2", "int", "state id"));

    FieldSchema itPart = new FieldSchema("it", "string", "date part");
    FieldSchema etPart = new FieldSchema("et", "string", "date part");
    ArrayList<FieldSchema> partCols = Lists.newArrayList(getDatePartition(), itPart, etPart);
    List<String> timePartCols = Lists.newArrayList(getDatePartitionKey(), itPart.getName(), etPart.getName());
    StorageTableDesc s1 = new StorageTableDesc(TextInputFormat.class, HiveIgnoreKeyTextOutputFormat.class, partCols,
      timePartCols);
    String[] partColNames = new String[]{getDatePartitionKey(), itPart.getName(), etPart.getName()};
    Map<String, UpdatePeriod> dumpPeriods = getHashMap(c1, HOURLY);
    Map<String, StorageTableDesc> storageTables = getHashMap(c1, s1);

    CubeDimensionTable cubeDim = new CubeDimensionTable(zipDim.getName(), dimName, dimColumns, 0L, dumpPeriods);
    client.createCubeDimensionTable(zipDim.getName(), dimName, dimColumns, 0L, dumpPeriods, null, storageTables);

    assertTrue(client.tableExists(dimName));

    Table cubeTbl = client.getHiveTable(dimName);
    assertTrue(client.isDimensionTable(cubeTbl));

    List<CubeDimensionTable> tbls = client.getAllDimensionTables(zipDim);
    boolean found = false;
    for (CubeDimensionTable dim : tbls) {
      if (dim.getName().equalsIgnoreCase(dimName)) {
        found = true;
        break;
      }
    }
    assertTrue(found);

    CubeDimensionTable cubeDim2 = new CubeDimensionTable(cubeTbl);
    assertTrue(cubeDim.equals(cubeDim2));

    // Assert for storage tables
    for (String storage : storageTables.keySet()) {
      String storageTableName = getFactOrDimtableStorageTableName(dimName, storage);
      assertTrue(client.tableExists(storageTableName));
    }

    Map<String, Date> timeParts1 = getTimePartitionByOffsets(getDatePartitionKey(), 0, itPart.getName(), 0,
      etPart.getName(), 0);
    StoragePartitionDesc partSpec1 = new StoragePartitionDesc(cubeDim.getName(), timeParts1, null, HOURLY);

    Map<String, Date> timeParts2 = getTimePartitionByOffsets(getDatePartitionKey(), 0, etPart.getName(), 1);
    Map<String, String> nonTimeSpec = getHashMap(itPart.getName(), "default");
    final StoragePartitionDesc partSpec2 = new StoragePartitionDesc(cubeDim.getName(), timeParts2, nonTimeSpec, HOURLY);

    Map<String, Date> timeParts3 = getTimePartitionByOffsets(getDatePartitionKey(), 0, etPart.getName(), 0);
    final StoragePartitionDesc partSpec3 = new StoragePartitionDesc(cubeDim.getName(), timeParts3, nonTimeSpec, HOURLY);

    client.addPartitions(Arrays.asList(partSpec1, partSpec2, partSpec3), c1);
    String c1TableName = getFactOrDimtableStorageTableName(cubeDim.getName(), c1);
    assertEquals(client.getAllParts(c1TableName).size(), 8);

    assertEquals(getLatestValues(c1TableName, HOURLY, partColNames, null), toPartitionArray(HOURLY, 0, 0, 1));

    Map<String, Date> timeParts4 = getTimePartitionByOffsets(getDatePartitionKey(), 0, itPart.getName(), 1,
      etPart.getName(), -1);
    final StoragePartitionDesc partSpec4 = new StoragePartitionDesc(cubeDim.getName(), timeParts4, null, HOURLY);

    Map<String, Date> timeParts5 = getTimePartitionByOffsets(getDatePartitionKey(), 1, itPart.getName(), -1,
      etPart.getName(), -2);
    final StoragePartitionDesc partSpec5 = new StoragePartitionDesc(cubeDim.getName(), timeParts5, null, HOURLY);

    client.addPartitions(Arrays.asList(partSpec4, partSpec5), c1);

    assertEquals(client.getAllParts(c1TableName).size(), 10);
    assertEquals(getLatestValues(c1TableName, HOURLY, partColNames, null), toPartitionArray(HOURLY, 1, 1, 1));
    Map<String, Date> timeParts6 = getTimePartitionByOffsets(getDatePartitionKey(), -2, itPart.getName(), -1,
      etPart.getName(), -2);
    final StoragePartitionDesc partSpec6 = new StoragePartitionDesc(cubeDim.getName(), timeParts6, null, HOURLY);

    client.addPartition(partSpec6, c1);

    assertEquals(client.getAllParts(c1TableName).size(), 11);

    Map<String, Date> timeParts7 = getTimePartitionByOffsets(getDatePartitionKey(), -5, itPart.getName(), -5,
      etPart.getName(), -5);
    final StoragePartitionDesc partSpec7 = new StoragePartitionDesc(cubeDim.getName(), timeParts7, null, HOURLY);

    client.addPartition(partSpec7, c1);
    assertEquals(client.getAllParts(c1TableName).size(), 12);
    assertEquals(getLatestValues(c1TableName, HOURLY, partColNames, null), toPartitionArray(HOURLY, 1, 1, 1));

    client.dropPartition(cubeDim.getName(), c1, timeParts5, null, HOURLY);
    assertEquals(client.getAllParts(c1TableName).size(), 11);
    assertEquals(getLatestValues(c1TableName, HOURLY, partColNames, null), toPartitionArray(HOURLY, 0, 1, 1));

    client.dropPartition(cubeDim.getName(), c1, timeParts7, null, HOURLY);
    assertEquals(client.getAllParts(c1TableName).size(), 10);
    assertEquals(getLatestValues(c1TableName, HOURLY, partColNames, null), toPartitionArray(HOURLY, 0, 1, 1));

    client.dropPartition(cubeDim.getName(), c1, timeParts2, nonTimeSpec, HOURLY);
    assertEquals(client.getAllParts(c1TableName).size(), 9);
    assertEquals(getLatestValues(c1TableName, HOURLY, partColNames, null), toPartitionArray(HOURLY, 0, 1, 0));

    client.dropPartition(cubeDim.getName(), c1, timeParts4, null, HOURLY);
    assertEquals(client.getAllParts(c1TableName).size(), 8);
    assertEquals(getLatestValues(c1TableName, HOURLY, partColNames, null), toPartitionArray(HOURLY, 0, 0, 0));

    client.dropPartition(cubeDim.getName(), c1, timeParts3, nonTimeSpec, HOURLY);
    assertEquals(client.getAllParts(c1TableName).size(), 5);
    client.dropPartition(cubeDim.getName(), c1, timeParts6, null, HOURLY);
    assertEquals(client.getAllParts(c1TableName).size(), 4);
    client.dropPartition(cubeDim.getName(), c1, timeParts1, null, HOURLY);
    assertEquals(client.getAllParts(c1TableName).size(), 0);
    assertNoPartitionNamedLatest(c1TableName, partColNames);
  }

  @Test(priority = 2)
  public void testAlterDim() throws Exception {
    String dimTblName = "test_alter_dim";

    List<FieldSchema> dimColumns = new ArrayList<>();
    dimColumns.add(new FieldSchema("zipcode", "int", "code"));

    StorageTableDesc s1 = new StorageTableDesc(TextInputFormat.class, HiveIgnoreKeyTextOutputFormat.class,
      datePartSingleton, datePartKeySingleton);
    Map<String, UpdatePeriod> dumpPeriods = getHashMap(c1, HOURLY);
    Map<String, StorageTableDesc> storageTables = getHashMap(c1, s1);

    client.createCubeDimensionTable(zipDim.getName(), dimTblName, dimColumns, 100L, dumpPeriods, null, storageTables);

    CubeDimensionTable dimTable = client.getDimensionTable(dimTblName);
    dimTable.alterColumn(new FieldSchema("testAddDim", "string", "test add column"));

    List<CubeDimensionTable> tbls = client.getAllDimensionTables(zipDim);
    boolean found = false;
    for (CubeDimensionTable dim : tbls) {
      if (dim.getName().equalsIgnoreCase(dimTblName)) {
        found = true;
        break;
      }
    }
    assertTrue(found);

    client.alterCubeDimensionTable(dimTblName, dimTable, storageTables);

    Table alteredHiveTable = Hive.get(conf).getTable(dimTblName);
    CubeDimensionTable altered = new CubeDimensionTable(alteredHiveTable);
    List<FieldSchema> columns = altered.getColumns();
    boolean contains = false;
    for (FieldSchema column : columns) {
      if (column.getName().equals("testadddim") && column.getType().equals("string")) {
        contains = true;
        break;
      }
    }
    assertTrue(contains);

    // Test alter column
    dimTable.alterColumn(new FieldSchema("testAddDim", "int", "change type"));
    client.alterCubeDimensionTable(dimTblName, dimTable, storageTables);

    altered = new CubeDimensionTable(Hive.get(conf).getTable(dimTblName));
    boolean typeChanged = false;
    for (FieldSchema column : altered.getColumns()) {
      if (column.getName().equals("testadddim") && column.getType().equals("int")) {
        typeChanged = true;
        break;
      }
    }
    assertTrue(typeChanged);

    // alter storage table desc
    String c1TableName = getFactOrDimtableStorageTableName(dimTblName, c1);
    Table c1Table = client.getTable(c1TableName);
    assertEquals(c1Table.getInputFormatClass(), TextInputFormat.class);
    s1 = new StorageTableDesc(SequenceFileInputFormat.class, HiveIgnoreKeyTextOutputFormat.class,
      datePartSingleton, datePartKeySingleton);
    storageTables.put(c1, s1);
    storageTables.put(c4, s1);
    dimTable.alterSnapshotDumpPeriod(c4, null);
    client.alterCubeDimensionTable(dimTblName, dimTable, storageTables);
    CubeDimensionTable altered2 = client.getDimensionTable(dimTblName);
    assertTrue(client.tableExists(c1TableName));
    Table alteredC1Table = client.getTable(c1TableName);
    assertEquals(alteredC1Table.getInputFormatClass(), SequenceFileInputFormat.class);
    boolean storageTblColAltered = false;
    for (FieldSchema column : alteredC1Table.getAllCols()) {
      if (column.getName().equals("testadddim") && column.getType().equals("int")) {
        storageTblColAltered = true;
        break;
      }
    }
    assertTrue(storageTblColAltered);
    String c4TableName = getFactOrDimtableStorageTableName(dimTblName, c4);
    assertTrue(client.tableExists(c4TableName));
    Table c4Table = client.getTable(c4TableName);
    assertEquals(c4Table.getInputFormatClass(), SequenceFileInputFormat.class);
    assertTrue(altered2.getStorages().contains("C4"));
    assertFalse(altered2.hasStorageSnapshots("C4"));

    StorageTableDesc s2 = new StorageTableDesc(TextInputFormat.class, HiveIgnoreKeyTextOutputFormat.class, null, null);
    client.addStorage(dimTable, c2, null, s2);
    client.addStorage(dimTable, c3, DAILY, s1);
    assertTrue(client.tableExists(getFactOrDimtableStorageTableName(dimTblName, c2)));
    assertTrue(client.tableExists(getFactOrDimtableStorageTableName(dimTblName, c3)));
    CubeDimensionTable altered3 = client.getDimensionTable(dimTblName);
    assertFalse(altered3.hasStorageSnapshots("C2"));
    assertTrue(altered3.hasStorageSnapshots("C3"));
    client.dropStorageFromDim(dimTblName, "C1");
    assertFalse(client.tableExists(getFactOrDimtableStorageTableName(dimTblName, c1)));
    client.dropDimensionTable(dimTblName, true);
    assertFalse(client.tableExists(getFactOrDimtableStorageTableName(dimTblName, c2)));
    assertFalse(client.tableExists(getFactOrDimtableStorageTableName(dimTblName, c3)));
    assertFalse(client.tableExists(dimTblName));
    // alter storage tables
  }

  @Test(priority = 2)
  public void testCubeDimWithoutDumps() throws Exception {
    String dimName = "countrytableMeta";

    List<FieldSchema> dimColumns = new ArrayList<>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("capital", "string", "field2"));
    dimColumns.add(new FieldSchema("region", "string", "region name"));

    Set<String> storageNames = new HashSet<>();

    StorageTableDesc s1 = new StorageTableDesc(TextInputFormat.class, HiveIgnoreKeyTextOutputFormat.class, null, null);
    storageNames.add(c1);
    Map<String, StorageTableDesc> storageTables = getHashMap(c1, s1);
    CubeDimensionTable cubeDim = new CubeDimensionTable(countryDim.getName(), dimName, dimColumns, 0L, storageNames);
    client.createCubeDimensionTable(countryDim.getName(), dimName, dimColumns, 0L, storageNames, null, storageTables);

    assertTrue(client.tableExists(dimName));
    Table cubeTbl = client.getHiveTable(dimName);
    assertTrue(client.isDimensionTable(cubeTbl));

    List<CubeDimensionTable> tbls = client.getAllDimensionTables(countryDim);
    boolean found = false;
    for (CubeDimensionTable dim : tbls) {
      if (dim.getName().equalsIgnoreCase(dimName)) {
        found = true;
        break;
      }
    }
    assertTrue(found);

    CubeDimensionTable cubeDim2 = new CubeDimensionTable(cubeTbl);
    assertTrue(cubeDim.equals(cubeDim2));

    // Assert for storage tables
    for (String storageName : storageTables.keySet()) {
      String storageTableName = getFactOrDimtableStorageTableName(dimName, storageName);
      assertTrue(client.tableExists(storageTableName));
      assertTrue(!client.getHiveTable(storageTableName).isPartitioned());
    }
  }

  @Test(priority = 2)
  public void testCubeDimWithTwoStorages() throws Exception {
    String dimName = "citytableMeta";

    List<FieldSchema> dimColumns = new ArrayList<>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("stateid", "int", "state id"));

    StorageTableDesc s1 = new StorageTableDesc(TextInputFormat.class, HiveIgnoreKeyTextOutputFormat.class,
      datePartSingleton, datePartKeySingleton);
    StorageTableDesc s2 = new StorageTableDesc(TextInputFormat.class, HiveIgnoreKeyTextOutputFormat.class, null, null);

    Map<String, UpdatePeriod> dumpPeriods = getHashMap(c1, HOURLY, c2, null);
    Map<String, StorageTableDesc> storageTables = getHashMap(c1, s1, c2, s2);

    CubeDimensionTable cubeDim = new CubeDimensionTable(cityDim.getName(), dimName, dimColumns, 0L, dumpPeriods);
    client.createCubeDimensionTable(cityDim.getName(), dimName, dimColumns, 0L, dumpPeriods, null, storageTables);

    assertTrue(client.tableExists(dimName));
    Table cubeTbl = client.getHiveTable(dimName);
    assertTrue(client.isDimensionTable(cubeTbl));

    List<CubeDimensionTable> tbls = client.getAllDimensionTables(cityDim);
    boolean found = false;
    for (CubeDimensionTable dim : tbls) {
      if (dim.getName().equalsIgnoreCase(dimName)) {
        found = true;
        break;
      }
    }
    assertTrue(found);

    CubeDimensionTable cubeDim2 = new CubeDimensionTable(cubeTbl);
    assertTrue(cubeDim.equals(cubeDim2));

    // Assert for storage tables
    String storageTableName1 = getFactOrDimtableStorageTableName(dimName, c1);
    assertTrue(client.tableExists(storageTableName1));
    String storageTableName2 = getFactOrDimtableStorageTableName(dimName, c2);
    assertTrue(client.tableExists(storageTableName2));
    assertTrue(!client.getHiveTable(storageTableName2).isPartitioned());
  }

  @Test(priority = 3)
  public void testCaching() throws HiveException, LensException {
    client = CubeMetastoreClient.getInstance(conf);
    CubeMetastoreClient client2 = CubeMetastoreClient.getInstance(new HiveConf(TestCubeMetastoreClient.class));
    assertEquals(5, client.getAllCubes().size());
    assertEquals(5, client2.getAllCubes().size());

    defineCube("testcache1", "testcache2", "derived1", "derived2");
    client.createCube("testcache1", cubeMeasures, cubeDimensions);
    client.createCube("testcache2", cubeMeasures, cubeDimensions, CUBE_PROPERTIES);
    client.createDerivedCube("testcache1", "derived1", measures, dimensions, emptyHashMap, 0L);
    client.createDerivedCube("testcache2", "derived2", measures, dimensions, CUBE_PROPERTIES, 0L);
    assertNotNull(client.getCube("testcache1"));
    assertNotNull(client2.getCube("testcache1"));
    assertEquals(9, client.getAllCubes().size());
    assertEquals(9, client2.getAllCubes().size());

    client2 = CubeMetastoreClient.getInstance(conf);
    assertEquals(9, client.getAllCubes().size());
    assertEquals(9, client2.getAllCubes().size());

    conf.setBoolean(MetastoreConstants.METASTORE_ENABLE_CACHING, false);
    client = CubeMetastoreClient.getInstance(conf);
    client2 = CubeMetastoreClient.getInstance(conf);
    assertEquals(9, client.getAllCubes().size());
    assertEquals(9, client2.getAllCubes().size());
    defineCube("testcache3", "testcache4", "dervied3", "derived4");
    client.createCube("testcache3", cubeMeasures, cubeDimensions);
    client.createCube("testcache4", cubeMeasures, cubeDimensions, CUBE_PROPERTIES);
    client.createDerivedCube("testcache3", "derived3", measures, dimensions, emptyHashMap, 0L);
    client.createDerivedCube("testcache4", "derived4", measures, dimensions, CUBE_PROPERTIES, 0L);
    assertNotNull(client.getCube("testcache3"));
    assertNotNull(client2.getCube("testcache3"));
    assertEquals(13, client.getAllCubes().size());
    assertEquals(13, client2.getAllCubes().size());
    conf.setBoolean(MetastoreConstants.METASTORE_ENABLE_CACHING, true);
    client = CubeMetastoreClient.getInstance(conf);
  }
}

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

import java.util.*;

import org.apache.lens.api.LensException;
import org.apache.lens.cube.metadata.timeline.EndsAndHolesPartitionTimeline;
import org.apache.lens.cube.metadata.timeline.PartitionTimeline;
import org.apache.lens.cube.metadata.timeline.StoreAllPartitionTimeline;
import org.apache.lens.cube.parse.TimeRange;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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
  private static final String CUBE_NAME = "testMetastoreCube";
  private static final String CUBE_NAME_WITH_PROPS = "testMetastoreCubeWithProps";
  private static final String DERIVED_CUBE_NAME = "derivedTestMetastoreCube";
  private static final String DERIVED_CUBE_NAME_WITH_PROPS = "derivedTestMetastoreCubeWithProps";
  private static final Map<String, String> CUBE_PROPERTIES = new HashMap<String, String>();
  private static Date now;
  private static Date nowPlus1;
  private static Date nowMinus1;
  private static Date nowMinus2;
  private static Date nowMinus3;
  private static Date nowMinus4;
  private static Date nowMinus5;
  private static HiveConf conf = new HiveConf(TestCubeMetastoreClient.class);
  private static FieldSchema dtPart = new FieldSchema(getDatePartitionKey(), serdeConstants.STRING_TYPE_NAME,
    "date partition");
  private static String c1 = "C1";
  private static String c2 = "C2";
  private static String c3 = "C3";
  private static Dimension zipDim, cityDim, stateDim, countryDim;
  private static Set<CubeDimAttribute> zipAttrs = new HashSet<CubeDimAttribute>();
  private static Set<CubeDimAttribute> cityAttrs = new HashSet<CubeDimAttribute>();
  private static Set<CubeDimAttribute> stateAttrs = new HashSet<CubeDimAttribute>();
  private static Set<CubeDimAttribute> countryAttrs = new HashSet<CubeDimAttribute>();
  private static Set<ExprColumn> cubeExpressions = new HashSet<ExprColumn>();
  private static Set<JoinChain> joinChains = new HashSet<JoinChain>();
  private static Set<ExprColumn> dimExpressions = new HashSet<ExprColumn>();

  /**
   * Get the date partition as field schema
   *
   * @return FieldSchema
   */
  public static FieldSchema getDatePartition() {
    return TestCubeMetastoreClient.dtPart;
  }

  /**
   * Get the date partition key
   *
   * @return String
   */
  public static String getDatePartitionKey() {
    return StorageConstants.DATE_PARTITION_KEY;
  }

  @BeforeClass
  public static void setup() throws HiveException, AlreadyExistsException, ParseException {
    SessionState.start(conf);
    now = new Date();
    Calendar cal = Calendar.getInstance();
    cal.setTime(now);
    cal.add(Calendar.HOUR_OF_DAY, 1);
    nowPlus1 = cal.getTime();
    cal.setTime(now);
    cal.add(Calendar.HOUR, -1);
    nowMinus1 = cal.getTime();
    cal.add(Calendar.HOUR, -1);
    nowMinus2 = cal.getTime();
    cal.add(Calendar.HOUR, -1);
    nowMinus3 = cal.getTime();
    cal.add(Calendar.HOUR, -1);
    nowMinus4 = cal.getTime();
    cal.add(Calendar.HOUR, -1);
    nowMinus5 = cal.getTime();

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
    Assert.assertFalse(client.tableExists(CUBE_NAME));

    Hive.get().dropDatabase(TestCubeMetastoreClient.class.getSimpleName(), true, true, true);
    CubeMetastoreClient.close();
  }

  private static void defineCube(String cubeName, String cubeNameWithProps, String derivedCubeName,
    String derivedCubeNameWithProps) throws ParseException {
    cubeMeasures = new HashSet<CubeMeasure>();
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
      "Measure With Starttime", null, null, null, now, null, null, 0.0, 999999.0));
    cubeMeasures.add(new ColumnMeasure(
      new FieldSchema("msrendtime", "float", "measure with end time"),
      "Measure With Endtime", null, "SUM", "RS", now, now, null));
    cubeMeasures.add(new ColumnMeasure(
      new FieldSchema("msrcost", "double", "measure with cost"), "Measure With cost",
      null, "MAX", null, now, now, 100.0));
    cubeMeasures.add(new ColumnMeasure(
      new FieldSchema("msrcost2", "bigint", "measure with cost"),
      "Measure With cost2", null, "MAX", null, null, null, 100.0, 0.0, 999999999999999999999999999.0));

    cubeDimensions = new HashSet<CubeDimAttribute>();
    List<CubeDimAttribute> locationHierarchy = new ArrayList<CubeDimAttribute>();
    locationHierarchy.add(new ReferencedDimAtrribute(new FieldSchema("zipcode", "int", "zip"), "Zip refer",
      new TableReference("zipdim", "zipcode")));
    locationHierarchy.add(new ReferencedDimAtrribute(new FieldSchema("cityid", "int", "city"), "City refer",
      new TableReference("citydim", "id")));
    locationHierarchy.add(new ReferencedDimAtrribute(new FieldSchema("stateid", "int", "state"), "State refer",
      new TableReference("statedim", "id")));
    locationHierarchy.add(new ReferencedDimAtrribute(new FieldSchema("countryid", "int", "country"), "Country refer",
      new TableReference("countrydim", "id")));
    List<String> regions = Arrays.asList("APAC", "EMEA", "USA");
    locationHierarchy.add(new InlineDimAttribute(new FieldSchema("regionname", "string", "region"), regions));
    cubeDimensions.add(new HierarchicalDimAttribute("location", "location hierarchy", locationHierarchy));
    cubeDimensions.add(new BaseDimAttribute(new FieldSchema("dim1", "string", "basedim")));
    cubeDimensions.add(new ReferencedDimAtrribute(new FieldSchema("dim2", "id", "ref dim"), "Dim2 refer",
      new TableReference("testdim2", "id")));

    cubeExpressions.add(new ExprColumn(new FieldSchema("msr5", "double", "fifth measure"), "Avg msr5",
      "avg(msr1 + msr2)", "avg(msr2 + msr1)", "avg(msr1 + msr2 - msr1 + msr1)"));
    cubeExpressions.add(new ExprColumn(new FieldSchema("msr5start", "double", "expr measure with start and end times"),
      "AVG of SUM", "avg(msr1 + msr2)"));
    cubeExpressions.add(new ExprColumn(new FieldSchema("booleancut", "boolean", "a boolean expression"), "Boolean Cut",
      "dim1 != 'x' AND dim2 != 10 ", "dim1 | dim2 AND dim2 = 'XYZ'"));
    cubeExpressions.add(new ExprColumn(new FieldSchema("substrexpr", "string", "a subt string expression"),
      "SUBSTR EXPR", "substr(dim1, 3)", "substr(dim2, 3)"));

    List<CubeDimAttribute> locationHierarchyWithStartTime = new ArrayList<CubeDimAttribute>();
    locationHierarchyWithStartTime.add(new ReferencedDimAtrribute(new FieldSchema("zipcode2", "int", "zip"),
      "Zip refer2", new TableReference("zipdim", "zipcode"), now, now, 100.0));
    locationHierarchyWithStartTime.add(new ReferencedDimAtrribute(new FieldSchema("cityid2", "int", "city"),
      "City refer2", new TableReference("citydim", "id"), now, null, null));
    locationHierarchyWithStartTime.add(new ReferencedDimAtrribute(new FieldSchema("stateid2", "int", "state"),
      "state refer2", new TableReference("statedim", "id"), now, null, 100.0));
    locationHierarchyWithStartTime.add(new ReferencedDimAtrribute(new FieldSchema("countryid2", "int", "country"),
      "Country refer2", new TableReference("countrydim", "id"), null, null, null));
    locationHierarchyWithStartTime.add(new InlineDimAttribute(new FieldSchema("regionname2", "string", "region"),
      regions));

    cubeDimensions
      .add(new HierarchicalDimAttribute("location2", "localtion hierarchy2", locationHierarchyWithStartTime));
    cubeDimensions.add(new BaseDimAttribute(new FieldSchema("dim1startTime", "string", "basedim"),
      "Dim With starttime", now, null, 100.0));
    cubeDimensions.add(new ReferencedDimAtrribute(new FieldSchema("dim2start", "string", "ref dim"),
      "Dim2 with starttime", new TableReference("testdim2", "id"), now, now, 100.0));

    List<TableReference> multiRefs = new ArrayList<TableReference>();
    multiRefs.add(new TableReference("testdim2", "id"));
    multiRefs.add(new TableReference("testdim3", "id"));
    multiRefs.add(new TableReference("testdim4", "id"));

    cubeDimensions.add(new ReferencedDimAtrribute(new FieldSchema("dim3", "string", "multi ref dim"), "Dim3 refer",
      multiRefs));
    cubeDimensions.add(new ReferencedDimAtrribute(new FieldSchema("dim3start", "string", "multi ref dim"),
      "Dim3 with starttime", multiRefs, now, null, 100.0));

    cubeDimensions.add(new InlineDimAttribute(new FieldSchema("region", "string", "region dim"), regions));
    cubeDimensions.add(new InlineDimAttribute(new FieldSchema("regionstart", "string", "region dim"),
      "Region with starttime", now, null, 100.0, regions));
    JoinChain zipCity = new JoinChain("cityFromZip", "Zip City", "zip city desc");
    List<TableReference> chain = new ArrayList<TableReference>();
    chain.add(new TableReference(cubeName, "zipcode"));
    chain.add(new TableReference("zipdim", "zipcode"));
    chain.add(new TableReference("zipdim", "cityid"));
    chain.add(new TableReference("citydim", "id"));
    zipCity.addPath(chain);
    List<TableReference> chain2 = new ArrayList<TableReference>();
    chain2.add(new TableReference(cubeName, "zipcode2"));
    chain2.add(new TableReference("zipdim", "zipcode"));
    chain2.add(new TableReference("zipdim", "cityid"));
    chain2.add(new TableReference("citydim", "id"));
    zipCity.addPath(chain2);
    joinChains.add(zipCity);
    JoinChain cityChain = new JoinChain("city", "Cube City", "cube city desc");
    chain = new ArrayList<TableReference>();
    chain.add(new TableReference(cubeName, "cityid"));
    chain.add(new TableReference("citydim", "id"));
    cityChain.addPath(chain);
    joinChains.add(cityChain);
    cubeDimensions.add(new ReferencedDimAtrribute(new FieldSchema("zipcityname", "string", "zip city name"),
      "Zip city name", "cityFromZip", "name", null, null, null));
    cube = new Cube(cubeName, cubeMeasures, cubeDimensions, cubeExpressions, joinChains,
      new HashMap<String, String>(), 0.0);
    measures = new HashSet<String>();
    measures.add("msr1");
    measures.add("msr2");
    measures.add("msr3");
    dimensions = new HashSet<String>();
    dimensions.add("dim1");
    dimensions.add("dim2");
    dimensions.add("dim3");
    derivedCube = new DerivedCube(derivedCubeName, measures, dimensions, cube);

    CUBE_PROPERTIES.put(MetastoreUtil.getCubeTimedDimensionListKey(cubeNameWithProps), "dt,mydate");
    CUBE_PROPERTIES.put(MetastoreConstants.CUBE_ALL_FIELDS_QUERIABLE, "false");
    CUBE_PROPERTIES.put("cube.custom.prop", "myval");
    cubeWithProps = new Cube(cubeNameWithProps, cubeMeasures, cubeDimensions, CUBE_PROPERTIES);
    derivedCubeWithProps =
      new DerivedCube(derivedCubeNameWithProps, measures, dimensions, CUBE_PROPERTIES, 0L, cubeWithProps);
  }

  private static void defineUberDims() throws ParseException {
    // Define zip dimension
    zipAttrs.add(new BaseDimAttribute(new FieldSchema("zipcode", "int", "code")));
    zipAttrs.add(new BaseDimAttribute(new FieldSchema("f1", "string", "field1")));
    zipAttrs.add(new BaseDimAttribute(new FieldSchema("f2", "string", "field1")));
    List<TableReference> stateRefs = new ArrayList<TableReference>();
    stateRefs.add(new TableReference("statedim", "id"));
    stateRefs.add(new TableReference("stateWeatherDim", "id"));
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
      "State and Country", "concat(statedim.name, \":\", countrydim.name)", "state_and_country"));
    dimExpressions.add(new ExprColumn(new FieldSchema("CityAddress", "string", "city with state and city and zip"),
      "City Address", "concat(citydim.name, \":\", statedim.name, \":\", countrydim.name, \":\", zipcode.code)"));
    Map<String, String> dimProps = new HashMap<String, String>();
    dimProps.put(MetastoreUtil.getDimTimedDimensionKey("citydim"), TestCubeMetastoreClient.getDatePartitionKey());
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
    Assert.assertEquals(1, client.getAllStorages().size());

    Storage hdfsStorage2 = new HDFSStorage(c2);
    client.createStorage(hdfsStorage2);
    Assert.assertEquals(2, client.getAllStorages().size());

    Storage hdfsStorage3 = new HDFSStorage(c3);
    client.createStorage(hdfsStorage3);
    Assert.assertEquals(3, client.getAllStorages().size());

    Assert.assertEquals(hdfsStorage, client.getStorage(c1));
    Assert.assertEquals(hdfsStorage2, client.getStorage(c2));
    Assert.assertEquals(hdfsStorage3, client.getStorage(c3));
  }

  @Test(priority = 1)
  public void testDimension() throws Exception {
    client.createDimension(zipDim);
    client.createDimension(cityDim);
    client.createDimension(stateDim);
    client.createDimension(countryDim);

    Assert.assertEquals(client.getAllDimensions().size(), 4);
    Assert.assertTrue(client.tableExists(cityDim.getName()));
    Assert.assertTrue(client.tableExists(stateDim.getName()));
    Assert.assertTrue(client.tableExists(countryDim.getName()));

    validateDim(zipDim, zipAttrs, "zipcode", "stateid");
    validateDim(cityDim, cityAttrs, "id", "stateid");
    validateDim(stateDim, stateAttrs, "id", "countryid");
    validateDim(countryDim, countryAttrs, "id", null);

    // validate expression in citydim
    Dimension city = client.getDimension(cityDim.getName());
    Assert.assertEquals(dimExpressions.size(), city.getExpressions().size());
    Assert.assertEquals(dimExpressions.size(), city.getExpressionNames().size());
    Assert.assertNotNull(city.getExpressionByName("stateAndCountry"));
    Assert.assertNotNull(city.getExpressionByName("cityaddress"));
    Assert.assertEquals(city.getExpressionByName("cityaddress").getDescription(), "city with state and city and zip");
    Assert.assertEquals(city.getExpressionByName("cityaddress").getDisplayString(), "City Address");

    ExprColumn stateCountryExpr = new ExprColumn(new FieldSchema("stateAndCountry", "String",
      "state and country together with hiphen as separator"), "State and Country",
      "concat(statedim.name, \"-\", countrydim.name)");
    stateCountryExpr.addExpression("concat(countrydim.name, \"-\", countrydim.name)");

    // Assert expression validation
    try {
      stateCountryExpr.addExpression("contact(countrydim.name");
      Assert.fail("Expected add expression to fail because of syntax error");
    } catch (ParseException exc) {
      // Pass
    }
    city.alterExpression(stateCountryExpr);


    city.removeExpression("cityAddress");
    city = client.getDimension(cityDim.getName());
    Assert.assertEquals(1, city.getExpressions().size());

    ExprColumn stateAndCountryActual = city.getExpressionByName("stateAndCountry");
    Assert.assertNotNull(stateAndCountryActual.getExpressions());
    Assert.assertEquals(2, stateAndCountryActual.getExpressions().size());
    Assert.assertTrue(stateAndCountryActual.getExpressions().contains("concat(statedim.name, \"-\", countrydim.name)"));
    Assert.assertTrue(stateAndCountryActual.getExpressions()
      .contains("concat(countrydim.name, \"-\", countrydim.name)"));

    Assert.assertNotNull(city.getExpressionByName("stateAndCountry"));
    Assert.assertEquals(city.getExpressionByName("stateAndCountry").getExpr(),
      "concat(statedim.name, \"-\", countrydim.name)");

    stateAndCountryActual.removeExpression("concat(countrydim.name, \"-\", countrydim.name)");
    city.alterExpression(stateAndCountryActual);
    client.alterDimension(city.getName(), city);
    Dimension cityAltered = client.getDimension(city.getName());
    Assert.assertEquals(1, cityAltered.getExpressionByName("stateAndCountry").getExpressions().size());


    List<TableReference> chain = new ArrayList<TableReference>();
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
    toAlter.alterAttribute(new BaseDimAttribute(new FieldSchema("newZipDim", "int", "new dim added")));
    toAlter.alterAttribute(new ReferencedDimAtrribute(new FieldSchema("newRefDim", "int", "new ref-dim added"),
      "New city ref", new TableReference("citydim", "id")));
    toAlter.alterAttribute(new BaseDimAttribute(new FieldSchema("f2", "varchar", "modified field")));
    List<TableReference> stateRefs = new ArrayList<TableReference>();
    stateRefs.add(new TableReference("statedim", "id"));
    toAlter.alterAttribute(new ReferencedDimAtrribute(new FieldSchema("stateid", "int", "state id"), "State refer",
      stateRefs));
    toAlter.removeAttribute("f1");
    toAlter.getProperties().put("alter.prop", "altered");
    toAlter.alterExpression(new ExprColumn(new FieldSchema("formattedcode", "string", "formatted zipcode"),
      "Formatted zipcode", "format_number(code, \"#,###,###\")"));
    toAlter.alterJoinChain(zipState);

    client.alterDimension(zipDim.getName(), toAlter);
    Dimension altered = client.getDimension(zipDim.getName());


    Assert.assertEquals(toAlter, altered);
    Assert.assertNotNull(altered.getAttributeByName("newZipDim"));
    Assert.assertNotNull(altered.getAttributeByName("newRefDim"));
    Assert.assertNotNull(altered.getAttributeByName("f2"));
    Assert.assertNotNull(altered.getAttributeByName("stateid"));
    Assert.assertNull(altered.getAttributeByName("f1"));
    Assert.assertEquals(1, altered.getExpressions().size());
    Assert.assertNotNull(altered.getExpressionByName("formattedcode"));
    Assert.assertEquals(altered.getExpressionByName("formattedcode").getExpr(), "format_number(code, \"#,###,###\")");

    CubeDimAttribute newzipdim = altered.getAttributeByName("newZipDim");
    Assert.assertTrue(newzipdim instanceof BaseDimAttribute);
    Assert.assertEquals(((BaseDimAttribute) newzipdim).getType(), "int");

    CubeDimAttribute newrefdim = altered.getAttributeByName("newRefDim");
    Assert.assertTrue(newrefdim instanceof ReferencedDimAtrribute);
    Assert.assertEquals(((ReferencedDimAtrribute) newrefdim).getReferences().size(), 1);
    Assert.assertEquals(((ReferencedDimAtrribute) newrefdim).getReferences().get(0).getDestTable(), cityDim.getName());
    Assert.assertEquals(((ReferencedDimAtrribute) newrefdim).getReferences().get(0).getDestColumn(), "id");

    CubeDimAttribute f2 = altered.getAttributeByName("f2");
    Assert.assertTrue(f2 instanceof BaseDimAttribute);
    Assert.assertEquals(((BaseDimAttribute) f2).getType(), "varchar");

    CubeDimAttribute stateid = altered.getAttributeByName("stateid");
    Assert.assertTrue(stateid instanceof ReferencedDimAtrribute);
    Assert.assertEquals(((ReferencedDimAtrribute) stateid).getReferences().size(), 1);
    Assert.assertEquals(((ReferencedDimAtrribute) stateid).getReferences().get(0).getDestTable(), stateDim.getName());
    Assert.assertEquals(((ReferencedDimAtrribute) stateid).getReferences().get(0).getDestColumn(), "id");

    Assert.assertEquals(altered.getProperties().get("alter.prop"), "altered");

    Assert.assertEquals(altered.getChainByName("stateFromZip"), zipState);

    Assert.assertEquals(altered.getJoinChains().size(), 1);
    JoinChain zipchain = altered.getChainByName("stateFromZip");
    Assert.assertEquals(zipchain.getDisplayString(), "Zip State");
    Assert.assertEquals(zipchain.getDescription(), "zip State desc");
    Assert.assertEquals(zipchain.getPaths().size(), 1);
    Assert.assertEquals(zipchain.getPaths().get(0).getReferences().size(), 4);
    Assert.assertEquals(zipchain.getPaths().get(0).getReferences().get(0).toString(), "zipdim.cityid");
    Assert.assertEquals(zipchain.getPaths().get(0).getReferences().get(1).toString(), "citydim.id");
    Assert.assertEquals(zipchain.getPaths().get(0).getReferences().get(2).toString(), "citydim.stateid");
    Assert.assertEquals(zipchain.getPaths().get(0).getReferences().get(3).toString(), "statedim.id");
  }

  private void validateDim(Dimension udim, Set<CubeDimAttribute> attrs, String basedim, String referdim)
    throws HiveException {
    Assert.assertTrue(client.tableExists(udim.getName()));
    Table dimTbl = client.getHiveTable(udim.getName());
    Assert.assertTrue(client.isDimension(dimTbl));
    Dimension dim = new Dimension(dimTbl);
    Assert.assertTrue(udim.equals(dim));
    Assert.assertTrue(udim.equals(client.getDimension(udim.getName())));
    Assert.assertEquals(dim.getAttributes().size(), attrs.size());
    Assert.assertNotNull(dim.getAttributeByName(basedim));
    Assert.assertTrue(dim.getAttributeByName(basedim) instanceof BaseDimAttribute);
    if (referdim != null) {
      Assert.assertNotNull(dim.getAttributeByName(referdim));
      Assert.assertTrue(dim.getAttributeByName(referdim) instanceof ReferencedDimAtrribute);
    }
    Assert.assertEquals(udim.getAttributeNames().size() + udim.getExpressionNames().size(), dim.getAllFieldNames()
      .size());
  }

  @Test(priority = 1)
  public void testCube() throws Exception {
    client.createCube(CUBE_NAME, cubeMeasures, cubeDimensions, cubeExpressions, joinChains,
      new HashMap<String, String>());
    Assert.assertTrue(client.tableExists(CUBE_NAME));
    Table cubeTbl = client.getHiveTable(CUBE_NAME);
    Assert.assertTrue(client.isCube(cubeTbl));
    Cube cube2 = new Cube(cubeTbl);
    Assert.assertTrue(cube.equals(cube2));
    Assert.assertFalse(cube2.isDerivedCube());
    Assert.assertTrue(cube2.getTimedDimensions().isEmpty());
    Assert.assertEquals(cubeMeasures.size(), cube2.getMeasureNames().size());
    // +8 is for hierarchical dimension
    Assert.assertEquals(cubeDimensions.size() + 8, cube2.getDimAttributeNames().size());
    Assert.assertEquals(cubeMeasures.size(), cube2.getMeasures().size());
    Assert.assertEquals(cubeExpressions.size(), cube2.getExpressions().size());
    Assert.assertEquals(cubeExpressions.size(), cube2.getExpressionNames().size());
    Assert.assertEquals(cubeDimensions.size(), cube2.getDimAttributes().size());
    Assert.assertEquals(cubeDimensions.size() + 8 + cubeMeasures.size() + cubeExpressions.size(), cube2
      .getAllFieldNames().size());
    Assert.assertNotNull(cube2.getMeasureByName("msr4"));
    Assert.assertEquals(cube2.getMeasureByName("msr4").getDescription(), "fourth measure");
    Assert.assertEquals(cube2.getMeasureByName("msr4").getDisplayString(), "Measure4");
    Assert.assertNotNull(cube2.getDimAttributeByName("location"));
    Assert.assertEquals(cube2.getDimAttributeByName("location").getDescription(), "location hierarchy");
    Assert.assertNotNull(cube2.getDimAttributeByName("dim1"));
    Assert.assertEquals(cube2.getDimAttributeByName("dim1").getDescription(), "basedim");
    Assert.assertNull(cube2.getDimAttributeByName("dim1").getDisplayString());
    Assert.assertNotNull(cube2.getDimAttributeByName("dim2"));
    Assert.assertEquals(cube2.getDimAttributeByName("dim2").getDescription(), "ref dim");
    Assert.assertEquals(cube2.getDimAttributeByName("dim2").getDisplayString(), "Dim2 refer");
    Assert.assertNotNull(cube2.getExpressionByName("msr5"));
    Assert.assertEquals(cube2.getExpressionByName("msr5").getDescription(), "fifth measure");
    Assert.assertEquals(cube2.getExpressionByName("msr5").getDisplayString(), "Avg msr5");
    Assert.assertNotNull(cube2.getExpressionByName("booleancut"));
    Assert.assertEquals(cube2.getExpressionByName("booleancut").getDescription(), "a boolean expression");
    Assert.assertEquals(cube2.getExpressionByName("booleancut").getDisplayString(), "Boolean Cut");
    Assert.assertEquals(cube2.getExpressionByName("booleancut").getExpressions().size(), 2);
    // Validate expression can contain delimiter character
    List<String> booleanCutExprs = new ArrayList<String>(cube2.getExpressionByName("booleancut").getExpressions());
    Assert.assertTrue(booleanCutExprs.contains("dim1 | dim2 AND dim2 = 'XYZ'"));
    Assert.assertTrue(cube2.allFieldsQueriable());

    Assert.assertTrue(cube2.getJoinChainNames().contains("cityfromzip"));
    Assert.assertTrue(cube2.getJoinChainNames().contains("city"));
    Assert.assertFalse(cube2.getJoinChains().isEmpty());
    Assert.assertEquals(cube2.getJoinChains().size(), 2);
    JoinChain zipchain = cube2.getChainByName("cityfromzip");
    Assert.assertEquals(zipchain.getDisplayString(), "Zip City");
    Assert.assertEquals(zipchain.getDescription(), "zip city desc");
    Assert.assertEquals(zipchain.getPaths().size(), 2);
    Assert.assertEquals(zipchain.getPaths().get(0).getReferences().size(), 4);
    Assert.assertEquals(zipchain.getPaths().get(0).getReferences().get(0).toString(), "testmetastorecube.zipcode");
    Assert.assertEquals(zipchain.getPaths().get(0).getReferences().get(1).toString(), "zipdim.zipcode");
    Assert.assertEquals(zipchain.getPaths().get(0).getReferences().get(2).toString(), "zipdim.cityid");
    Assert.assertEquals(zipchain.getPaths().get(0).getReferences().get(3).toString(), "citydim.id");
    Assert.assertEquals(zipchain.getPaths().get(1).getReferences().size(), 4);
    Assert.assertEquals(zipchain.getPaths().get(1).getReferences().get(0).toString(), "testmetastorecube.zipcode2");
    Assert.assertEquals(zipchain.getPaths().get(1).getReferences().get(1).toString(), "zipdim.zipcode");
    Assert.assertEquals(zipchain.getPaths().get(1).getReferences().get(2).toString(), "zipdim.cityid");
    Assert.assertEquals(zipchain.getPaths().get(1).getReferences().get(3).toString(), "citydim.id");
    JoinChain citychain = cube2.getChainByName("city");
    Assert.assertEquals(citychain.getDisplayString(), "Cube City");
    Assert.assertEquals(citychain.getDescription(), "cube city desc");
    Assert.assertEquals(citychain.getPaths().size(), 1);
    Assert.assertEquals(citychain.getPaths().get(0).getReferences().size(), 2);
    Assert.assertEquals(citychain.getPaths().get(0).getReferences().get(0).toString(), "testmetastorecube.cityid");
    Assert.assertEquals(citychain.getPaths().get(0).getReferences().get(1).toString(), "citydim.id");
    Assert.assertNotNull(cube2.getDimAttributeByName("zipcityname"));
    Assert.assertEquals(((ReferencedDimAtrribute) cube2.getDimAttributeByName("zipcityname")).getChainName(),
      "cityfromzip");
    Assert.assertEquals(((ReferencedDimAtrribute) cube2.getDimAttributeByName("zipcityname")).getRefColumn(),
      "name");

    client.createDerivedCube(CUBE_NAME, DERIVED_CUBE_NAME, measures, dimensions, new HashMap<String, String>(), 0L);
    Assert.assertTrue(client.tableExists(DERIVED_CUBE_NAME));
    Table derivedTbl = client.getHiveTable(DERIVED_CUBE_NAME);
    Assert.assertTrue(client.isCube(derivedTbl));
    DerivedCube dcube2 = new DerivedCube(derivedTbl, cube);
    Assert.assertTrue(derivedCube.equals(dcube2));
    Assert.assertTrue(dcube2.isDerivedCube());
    Assert.assertTrue(dcube2.getTimedDimensions().isEmpty());
    Assert.assertEquals(measures.size(), dcube2.getMeasureNames().size());
    Assert.assertEquals(dimensions.size(), dcube2.getDimAttributeNames().size());
    Assert.assertEquals(measures.size(), dcube2.getMeasures().size());
    Assert.assertEquals(dimensions.size(), dcube2.getDimAttributes().size());
    Assert.assertNotNull(dcube2.getMeasureByName("msr3"));
    Assert.assertNull(dcube2.getMeasureByName("msr4"));
    Assert.assertNull(dcube2.getDimAttributeByName("location"));
    Assert.assertNotNull(dcube2.getDimAttributeByName("dim1"));
    Assert.assertTrue(dcube2.allFieldsQueriable());

    client.createCube(CUBE_NAME_WITH_PROPS, cubeMeasures, cubeDimensions, CUBE_PROPERTIES);
    Assert.assertTrue(client.tableExists(CUBE_NAME_WITH_PROPS));
    cubeTbl = client.getHiveTable(CUBE_NAME_WITH_PROPS);
    Assert.assertTrue(client.isCube(cubeTbl));
    cube2 = new Cube(cubeTbl);
    Assert.assertTrue(cubeWithProps.equals(cube2));
    Assert.assertFalse(cube2.isDerivedCube());
    Assert.assertFalse(cubeWithProps.getTimedDimensions().isEmpty());
    Assert.assertTrue(cubeWithProps.getTimedDimensions().contains("dt"));
    Assert.assertTrue(cubeWithProps.getTimedDimensions().contains("mydate"));
    Assert.assertEquals(cubeMeasures.size(), cube2.getMeasureNames().size());
    Assert.assertEquals(cubeDimensions.size() + 8, cube2.getDimAttributeNames().size());
    Assert.assertEquals(cubeMeasures.size(), cube2.getMeasures().size());
    Assert.assertEquals(cubeDimensions.size(), cube2.getDimAttributes().size());
    Assert.assertNotNull(cube2.getMeasureByName("msr4"));
    Assert.assertNotNull(cube2.getDimAttributeByName("location"));
    Assert.assertFalse(cube2.allFieldsQueriable());

    client.createDerivedCube(CUBE_NAME_WITH_PROPS, DERIVED_CUBE_NAME_WITH_PROPS, measures, dimensions,
      CUBE_PROPERTIES, 0L);
    Assert.assertTrue(client.tableExists(DERIVED_CUBE_NAME_WITH_PROPS));
    derivedTbl = client.getHiveTable(DERIVED_CUBE_NAME_WITH_PROPS);
    Assert.assertTrue(client.isCube(derivedTbl));
    dcube2 = new DerivedCube(derivedTbl, cubeWithProps);
    Assert.assertTrue(derivedCubeWithProps.equals(dcube2));
    Assert.assertTrue(dcube2.isDerivedCube());
    Assert.assertNotNull(derivedCubeWithProps.getProperties().get("cube.custom.prop"));
    Assert.assertEquals(derivedCubeWithProps.getProperties().get("cube.custom.prop"), "myval");
    Assert.assertNull(dcube2.getMeasureByName("msr4"));
    Assert.assertNotNull(dcube2.getMeasureByName("msr3"));
    Assert.assertNull(dcube2.getDimAttributeByName("location"));
    Assert.assertNotNull(dcube2.getDimAttributeByName("dim1"));
    Assert.assertTrue(dcube2.allFieldsQueriable());
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
    List<TableReference> chain = new ArrayList<TableReference>();
    chain.add(new TableReference(cubeName, "cityid"));
    chain.add(new TableReference("citydim", "id"));
    cityChain.addPath(chain);
    toAlter.alterJoinChain(cityChain);
    toAlter.removeJoinChain("cityFromZip");

    Assert.assertNotNull(toAlter.getMeasureByName("testAddMsr1"));
    Assert.assertNotNull(toAlter.getMeasureByName("msr3"));
    Assert.assertEquals(toAlter.getMeasureByName("msr3").getDisplayString(), "Measure3Altered");
    Assert.assertEquals(toAlter.getMeasureByName("msr3").getDescription(), "third altered measure");
    Assert.assertNull(toAlter.getMeasureByName("msr4"));
    Assert.assertNotNull(toAlter.getDimAttributeByName("testAddDim1"));
    Assert.assertEquals(toAlter.getDimAttributeByName("testAddDim1").getDescription(), "dim to add");
    Assert.assertNotNull(toAlter.getDimAttributeByName("dim1"));
    Assert.assertEquals(toAlter.getDimAttributeByName("dim1").getDescription(), "basedim altered");
    Assert.assertNull(toAlter.getDimAttributeByName("location2"));

    client.alterCube(cubeName, toAlter);

    Table alteredHiveTbl = Hive.get(conf).getTable(cubeName);

    Cube altered = new Cube(alteredHiveTbl);

    Assert.assertEquals(toAlter, altered);
    Assert.assertNotNull(altered.getMeasureByName("testAddMsr1"));
    CubeMeasure addedMsr = altered.getMeasureByName("testAddMsr1");
    Assert.assertEquals(addedMsr.getType(), "int");
    Assert.assertNotNull(altered.getDimAttributeByName("testAddDim1"));
    BaseDimAttribute addedDim = (BaseDimAttribute) altered.getDimAttributeByName("testAddDim1");
    Assert.assertEquals(addedDim.getType(), "string");
    Assert.assertEquals(addedDim.getDescription(), "dim to add");
    Assert.assertTrue(altered.getTimedDimensions().contains("zt"));
    Assert.assertEquals(altered.getMeasureByName("msr3").getDisplayString(), "Measure3Altered");
    Assert.assertEquals(altered.getMeasureByName("msr3").getDescription(), "third altered measure");
    Assert.assertNotNull(altered.getDimAttributeByName("dim1"));
    Assert.assertEquals(altered.getDimAttributeByName("dim1").getDescription(), "basedim altered");
    Assert.assertNull(altered.getDimAttributeByName("location2"));
    Assert.assertNull(altered.getChainByName("cityFromZip"));
    Assert.assertEquals(altered.getChainByName("city").getDescription(), "cube city desc modified");

    toAlter.alterMeasure(new ColumnMeasure(new FieldSchema("testAddMsr1", "double", "testAddMeasure")));
    client.alterCube(cubeName, toAlter);
    altered = new Cube(Hive.get(conf).getTable(cubeName));
    addedMsr = altered.getMeasureByName("testaddmsr1");
    Assert.assertNotNull(addedMsr);
    Assert.assertEquals(addedMsr.getType(), "double");
    Assert.assertTrue(client.getAllFactTables(altered).isEmpty());
  }

  @Test(priority = 2)
  public void testAlterDerivedCube() throws Exception {
    String name = "alter_derived_cube";
    client.createDerivedCube(CUBE_NAME, name, measures, dimensions, new HashMap<String, String>(), 0L);
    // Test alter cube
    Table cubeTbl = client.getHiveTable(name);
    DerivedCube toAlter = new DerivedCube(cubeTbl, (Cube) client.getCube(CUBE_NAME));
    toAlter.addMeasure("msr4");
    toAlter.removeMeasure("msr3");
    toAlter.addDimension("dim1StartTime");
    toAlter.removeDimension("dim1");

    Assert.assertNotNull(toAlter.getMeasureByName("msr4"));
    Assert.assertNotNull(toAlter.getMeasureByName("msr2"));
    Assert.assertNull(toAlter.getMeasureByName("msr3"));
    Assert.assertNotNull(toAlter.getDimAttributeByName("dim1StartTime"));
    Assert.assertNotNull(toAlter.getDimAttributeByName("dim2"));
    Assert.assertNull(toAlter.getDimAttributeByName("dim1"));

    client.alterCube(name, toAlter);

    DerivedCube altered = (DerivedCube) client.getCube(name);

    Assert.assertEquals(toAlter, altered);
    Assert.assertNotNull(altered.getMeasureByName("msr4"));
    CubeMeasure addedMsr = altered.getMeasureByName("msr4");
    Assert.assertEquals(addedMsr.getType(), "bigint");
    Assert.assertNotNull(altered.getDimAttributeByName("dim1StartTime"));
    BaseDimAttribute addedDim = (BaseDimAttribute) altered.getDimAttributeByName("dim1StartTime");
    Assert.assertEquals(addedDim.getType(), "string");
    Assert.assertNotNull(addedDim.getStartTime());

    client.dropCube(name);
    Assert.assertFalse(client.tableExists(name));
  }

  @Test(priority = 2)
  public void testCubeFact() throws Exception {
    String factName = "testMetastoreFact";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add one dimension of the cube
    factColumns.add(new FieldSchema("zipcode", "int", "zip"));

    Map<String, Set<UpdatePeriod>> updatePeriods = new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates = new HashSet<UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    timePartCols.add(getDatePartitionKey());
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    updatePeriods.put(c1, updates);
    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    CubeFactTable cubeFact = new CubeFactTable(CUBE_NAME, factName, factColumns, updatePeriods);

    // create cube fact
    client.createCubeFactTable(CUBE_NAME, factName, factColumns, updatePeriods, 0L, null, storageTables);
    Assert.assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, CUBE_NAME));
    Assert.assertEquals(client.getAllFactTables(client.getCube(CUBE_NAME)).get(0).getName(), factName.toLowerCase());
    Assert.assertEquals(client.getAllFactTables(client.getCube(DERIVED_CUBE_NAME)).get(0).getName(),
      factName.toLowerCase());
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFact.equals(cubeFact2));

    // Assert for storage tables
    for (String entry : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getFactStorageTableName(factName, entry);
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    // test partition
    StoragePartitionDesc partSpec = new StoragePartitionDesc(cubeFact.getName(), timeParts, null, UpdatePeriod.HOURLY);
    client.addPartition(partSpec, c1);
    Assert.assertTrue(client.factPartitionExists(cubeFact.getName(), c1, UpdatePeriod.HOURLY, timeParts,
      new HashMap<String, String>()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
      TestCubeMetastoreClient.getDatePartitionKey()));

    // Partition with different schema
    FieldSchema newcol = new FieldSchema("newcol", "int", "new col for part");
    cubeFact.alterColumn(newcol);
    client.alterCubeFactTable(cubeFact.getName(), cubeFact);
    String storageTableName = MetastoreUtil.getFactStorageTableName(factName, c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 1);
    List<Partition> parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(parts.size(), 0);

    Map<String, Date> timeParts2 = new HashMap<String, Date>();
    timeParts2.put(TestCubeMetastoreClient.getDatePartitionKey(), nowPlus1);
    StoragePartitionDesc partSpec2 =
      new StoragePartitionDesc(cubeFact.getName(), timeParts2, null, UpdatePeriod.HOURLY);
    partSpec2.setInputFormat(SequenceFileInputFormat.class.getCanonicalName());
    partSpec2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    client.addPartition(partSpec2, c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 2);
    Assert.assertTrue(client.factPartitionExists(cubeFact.getName(), c1, UpdatePeriod.HOURLY, timeParts,
      new HashMap<String, String>()));
    Assert.assertTrue(client.factPartitionExists(cubeFact.getName(), c1, UpdatePeriod.HOURLY, timeParts2,
      new HashMap<String, String>()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
      TestCubeMetastoreClient.getDatePartitionKey()));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(parts.size(), 0);

    client.dropPartition(cubeFact.getName(), c1, timeParts2, null, UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 1);
    Assert.assertTrue(client.factPartitionExists(cubeFact.getName(), c1, UpdatePeriod.HOURLY, timeParts,
      new HashMap<String, String>()));
    Assert.assertFalse(client.factPartitionExists(cubeFact.getName(), c1, UpdatePeriod.HOURLY, timeParts2,
      new HashMap<String, String>()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
      TestCubeMetastoreClient.getDatePartitionKey()));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(parts.size(), 0);

    client.dropPartition(cubeFact.getName(), c1, timeParts, null, UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 0);
    Assert.assertFalse(client.factPartitionExists(cubeFact.getName(), c1, UpdatePeriod.HOURLY, timeParts,
      new HashMap<String, String>()));
    Assert.assertFalse(client.factPartitionExists(cubeFact.getName(), c1, UpdatePeriod.HOURLY, timeParts2,
      new HashMap<String, String>()));
    Assert.assertFalse(client.latestPartitionExists(cubeFact.getName(), c1,
      TestCubeMetastoreClient.getDatePartitionKey()));
  }

  @Test(priority = 2)
  public void testAlterCubeFact() throws Exception {
    String factName = "test_alter_fact";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add one dimension of the cube
    factColumns.add(new FieldSchema("zipcode", "int", "zip"));

    Map<String, Set<UpdatePeriod>> updatePeriods = new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);

    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    timePartCols.add(getDatePartitionKey());
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    updatePeriods.put(c1, updates);

    updatePeriods.put(c2, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s1);

    // create cube fact
    client.createCubeFactTable(CUBE_NAME, factName, factColumns, updatePeriods, 0L, null, storageTables);

    CubeFactTable factTable = new CubeFactTable(Hive.get(conf).getTable(factName));
    factTable.alterColumn(new FieldSchema("testFactColAdd", "int", "test add column"));
    factTable.alterColumn(new FieldSchema("msr3", "int", "test alter column"));
    factTable.alterWeight(100L);
    Map<String, String> newProp = new HashMap<String, String>();
    newProp.put("new.prop", "val");
    factTable.addProperties(newProp);
    factTable.addUpdatePeriod(c1, UpdatePeriod.MONTHLY);
    factTable.removeUpdatePeriod(c1, UpdatePeriod.HOURLY);
    Set<UpdatePeriod> alterupdates = new HashSet<UpdatePeriod>();
    alterupdates.add(UpdatePeriod.HOURLY);
    alterupdates.add(UpdatePeriod.DAILY);
    alterupdates.add(UpdatePeriod.MONTHLY);
    factTable.alterStorage(c2, alterupdates);

    client.alterCubeFactTable(factName, factTable);

    Table factHiveTable = Hive.get(conf).getTable(factName);
    CubeFactTable altered = new CubeFactTable(factHiveTable);

    Assert.assertTrue(altered.weight() == 100L);
    Assert.assertTrue(altered.getProperties().get("new.prop").equals("val"));
    Assert.assertTrue(altered.getUpdatePeriods().get(c1).contains(UpdatePeriod.MONTHLY));
    Assert.assertFalse(altered.getUpdatePeriods().get(c1).contains(UpdatePeriod.HOURLY));
    Assert.assertTrue(altered.getUpdatePeriods().get(c2).contains(UpdatePeriod.MONTHLY));
    Assert.assertTrue(altered.getUpdatePeriods().get(c2).contains(UpdatePeriod.DAILY));
    Assert.assertTrue(altered.getUpdatePeriods().get(c2).contains(UpdatePeriod.HOURLY));
    Assert.assertTrue(altered.getCubeName().equalsIgnoreCase(CUBE_NAME.toLowerCase()));
    boolean contains = false;
    for (FieldSchema column : altered.getColumns()) {
      if (column.getName().equals("testfactcoladd") && column.getType().equals("int")) {
        contains = true;
        break;
      }
    }
    Assert.assertTrue(contains);

    client.addStorage(altered, c3, updates, s1);
    Assert.assertTrue(altered.getStorages().contains("C3"));
    Assert.assertTrue(altered.getUpdatePeriods().get("C3").equals(updates));
    String storageTableName = MetastoreUtil.getFactStorageTableName(factName, c3);
    Assert.assertTrue(client.tableExists(storageTableName));
    client.dropStorageFromFact(factName, c2);
    storageTableName = MetastoreUtil.getFactStorageTableName(factName, c2);
    Assert.assertFalse(client.tableExists(storageTableName));
    List<CubeFactTable> cubeFacts = client.getAllFactTables(client.getCube(CUBE_NAME));
    List<String> cubeFactNames = new ArrayList<String>();
    for (CubeFactTable cfact : cubeFacts) {
      cubeFactNames.add(cfact.getName());
    }
    Assert.assertTrue(cubeFactNames.contains(factName.toLowerCase()));
    cubeFacts = client.getAllFactTables(client.getCube(DERIVED_CUBE_NAME));
    cubeFactNames = new ArrayList<String>();
    for (CubeFactTable cfact : cubeFacts) {
      cubeFactNames.add(cfact.getName());
    }
    Assert.assertTrue(cubeFactNames.contains(factName.toLowerCase()));
    client.dropFact(factName, true);
    Assert.assertFalse(client.tableExists(MetastoreUtil.getFactStorageTableName(factName, c1)));
    Assert.assertFalse(client.tableExists(MetastoreUtil.getFactStorageTableName(factName, c3)));
    Assert.assertFalse(client.tableExists(factName));
    cubeFacts = client.getAllFactTables(cube);
    cubeFactNames = new ArrayList<String>();
    for (CubeFactTable cfact : cubeFacts) {
      cubeFactNames.add(cfact.getName());
    }
    Assert.assertFalse(cubeFactNames.contains(factName.toLowerCase()));
  }

  @Test(priority = 2)
  public void testCubeFactWithTwoTimedParts() throws Exception {
    String factName = "testMetastoreFactTimedParts";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add one dimension of the cube
    factColumns.add(new FieldSchema("zipcode", "int", "zip"));

    Map<String, Set<UpdatePeriod>> updatePeriods = new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    FieldSchema testDtPart = new FieldSchema("mydate", "string", "date part");
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    partCols.add(testDtPart);
    timePartCols.add(getDatePartitionKey());
    timePartCols.add(testDtPart.getName());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    updatePeriods.put(c1, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    CubeFactTable cubeFact = new CubeFactTable(CUBE_NAME_WITH_PROPS, factName, factColumns, updatePeriods);

    // create cube fact
    client.createCubeFactTable(CUBE_NAME_WITH_PROPS, factName, factColumns, updatePeriods, 0L, null, storageTables);

    Assert.assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, CUBE_NAME_WITH_PROPS));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFact.equals(cubeFact2));

    // Assert for storage tables
    for (String entry : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getFactStorageTableName(factName, entry);
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    // test partition
    Calendar cal = new GregorianCalendar();
    cal.setTime(now);
    cal.add(Calendar.HOUR, -1);
    Date testDt = cal.getTime();
    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    timeParts.put(testDtPart.getName(), testDt);
    StoragePartitionDesc partSpec = new StoragePartitionDesc(cubeFact.getName(), timeParts, null, UpdatePeriod.HOURLY);
    client.addPartition(partSpec, c1);
    Assert.assertTrue(client.factPartitionExists(cubeFact.getName(), c1, UpdatePeriod.HOURLY, timeParts,
      new HashMap<String, String>()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, testDtPart.getName()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
      TestCubeMetastoreClient.getDatePartitionKey()));
    String storageTableName = MetastoreUtil.getFactStorageTableName(cubeFact.getName(), c1);
    List<Partition> parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(parts.size(), 0);

    Assert.assertEquals(client.getAllParts(storageTableName).size(), 1);
    parts = client.getPartitionsByFilter(storageTableName, testDtPart.getName() + "='latest'");
    Assert.assertEquals(parts.size(), 0);

    client.dropPartition(cubeFact.getName(), c1, timeParts, null, UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 0);
    Assert.assertFalse(client.factPartitionExists(cubeFact.getName(), c1, UpdatePeriod.HOURLY, timeParts,
      new HashMap<String, String>()));
    Assert.assertFalse(client.latestPartitionExists(cubeFact.getName(), c1,
      TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertFalse(client.latestPartitionExists(cubeFact.getName(), c1, testDtPart.getName()));
  }

  @Test(priority = 2)
  public void testCubeFactWithThreeTimedParts() throws Exception {
    String factName = "testMetastoreFact3TimedParts";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add one dimension of the cube
    factColumns.add(new FieldSchema("zipcode", "int", "zip"));

    Map<String, Set<UpdatePeriod>> updatePeriods = new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    FieldSchema itPart = new FieldSchema("it", "string", "date part");
    FieldSchema etPart = new FieldSchema("et", "string", "date part");
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    partCols.add(itPart);
    partCols.add(etPart);
    timePartCols.add(getDatePartitionKey());
    timePartCols.add(itPart.getName());
    timePartCols.add(etPart.getName());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    updatePeriods.put(c1, updates);
    updatePeriods.put(c2, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s1);

    CubeFactTable cubeFact = new CubeFactTable(CUBE_NAME_WITH_PROPS, factName, factColumns, updatePeriods);

    // create cube fact
    client.createCubeFactTable(CUBE_NAME_WITH_PROPS, factName, factColumns, updatePeriods, 0L, null, storageTables);

    Assert.assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, CUBE_NAME_WITH_PROPS));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFact.equals(cubeFact2));

    String[] storages = new String[]{c1, c2};
    String[] partColNames = new String[]{getDatePartitionKey(), itPart.getName(), etPart.getName()};

    // Assert for storage tables
    for (String entry : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getFactStorageTableName(factName, entry);
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    String c1TableName = MetastoreUtil.getFactStorageTableName(cubeFact.getName(), c1);
    String c2TableName = MetastoreUtil.getFactStorageTableName(cubeFact.getName(), c2);

    Table c1Table = client.getHiveTable(c1TableName);
    Table c2Table = client.getHiveTable(c2TableName);
    c2Table.getParameters().put(MetastoreUtil.getPartitionTimelineStorageClassKey(UpdatePeriod.HOURLY,
      getDatePartitionKey()), StoreAllPartitionTimeline.class.getCanonicalName());
    c2Table.getParameters().put(MetastoreUtil.getPartitionTimelineStorageClassKey(UpdatePeriod.HOURLY,
      itPart.getName()), StoreAllPartitionTimeline.class.getCanonicalName());
    c2Table.getParameters().put(MetastoreUtil.getPartitionTimelineStorageClassKey(UpdatePeriod.HOURLY,
      etPart.getName()), StoreAllPartitionTimeline.class.getCanonicalName());
    client.pushHiveTable(c2Table);

    // same before insertion.
    assertSameTimelines(factName, storages, UpdatePeriod.HOURLY, partColNames);
    EndsAndHolesPartitionTimeline timelineDt = ((EndsAndHolesPartitionTimeline) client.partitionTimelineCache.get(
      factName, c1, UpdatePeriod.HOURLY, getDatePartitionKey()));
    EndsAndHolesPartitionTimeline timelineIt = ((EndsAndHolesPartitionTimeline) client.partitionTimelineCache.get(
      factName, c1, UpdatePeriod.HOURLY, itPart.getName()));
    EndsAndHolesPartitionTimeline timelineEt = ((EndsAndHolesPartitionTimeline) client.partitionTimelineCache.get(
      factName, c1, UpdatePeriod.HOURLY, etPart.getName()));
    StoreAllPartitionTimeline timelineDtC2 = ((StoreAllPartitionTimeline) client.partitionTimelineCache.get(
      factName, c2, UpdatePeriod.HOURLY, getDatePartitionKey()));
    StoreAllPartitionTimeline timelineItC2 = ((StoreAllPartitionTimeline) client.partitionTimelineCache.get(
      factName, c2, UpdatePeriod.HOURLY, itPart.getName()));
    StoreAllPartitionTimeline timelineEtC2 = ((StoreAllPartitionTimeline) client.partitionTimelineCache.get(
      factName, c2, UpdatePeriod.HOURLY, etPart.getName()));

    Map<String, Date> timeParts1 = new HashMap<String, Date>();
    timeParts1.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    timeParts1.put(itPart.getName(), now);
    timeParts1.put(etPart.getName(), now);
    StoragePartitionDesc partSpec1 = new StoragePartitionDesc(cubeFact.getName(), timeParts1, null,
      UpdatePeriod.HOURLY);

    Map<String, Date> timeParts2 = new HashMap<String, Date>();
    timeParts2.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    timeParts2.put(etPart.getName(), nowPlus1);
    Map<String, String> nonTimeSpec = new HashMap<String, String>();
    nonTimeSpec.put(itPart.getName(), "default");
    final StoragePartitionDesc partSpec2 = new StoragePartitionDesc(cubeFact.getName(), timeParts2, nonTimeSpec,
      UpdatePeriod.HOURLY);

    Map<String, Date> timeParts3 = new HashMap<String, Date>();
    timeParts3.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    timeParts3.put(etPart.getName(), now);
    final StoragePartitionDesc partSpec3 = new StoragePartitionDesc(cubeFact.getName(), timeParts3, nonTimeSpec,
      UpdatePeriod.HOURLY);

    client.addPartitions(Arrays.asList(partSpec1, partSpec2, partSpec3), c1);
    client.addPartitions(Arrays.asList(partSpec1, partSpec2, partSpec3), c2);
    PartitionTimeline timeline1Temp = client.partitionTimelineCache.get(factName, c1, UpdatePeriod.HOURLY,
      getDatePartitionKey());
    PartitionTimeline timeline2Temp = client.partitionTimelineCache.get(factName, c2, UpdatePeriod.HOURLY,
      getDatePartitionKey());

    Assert.assertEquals(timeline1Temp.getClass(), EndsAndHolesPartitionTimeline.class);
    Assert.assertEquals(timeline2Temp.getClass(), StoreAllPartitionTimeline.class);


    Assert.assertEquals(client.getAllParts(c1TableName).size(), 3);
    Assert.assertEquals(client.getAllParts(c2TableName).size(), 3);

    assertSameTimelines(factName, storages, UpdatePeriod.HOURLY, partColNames);

    assertTimeline(timelineDt, timelineDtC2, UpdatePeriod.HOURLY, now, now);
    assertTimeline(timelineEt, timelineEtC2, UpdatePeriod.HOURLY, now, nowPlus1);
    assertTimeline(timelineIt, timelineItC2, UpdatePeriod.HOURLY, now, now);

    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
      TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, itPart.getName()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c2, etPart.getName()));

    assertNoPartitionNamedLatest(c1TableName, partColNames);
    assertNoPartitionNamedLatest(c2TableName, partColNames);

    Map<String, Date> timeParts4 = new HashMap<String, Date>();
    timeParts4.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    timeParts4.put(itPart.getName(), nowPlus1);
    timeParts4.put(etPart.getName(), nowMinus1);
    final StoragePartitionDesc partSpec4 = new StoragePartitionDesc(cubeFact.getName(), timeParts4, null,
      UpdatePeriod.HOURLY);


    Map<String, Date> timeParts5 = new HashMap<String, Date>();
    timeParts5.put(TestCubeMetastoreClient.getDatePartitionKey(), nowPlus1);
    timeParts5.put(itPart.getName(), nowMinus1);
    timeParts5.put(etPart.getName(), nowMinus2);
    final StoragePartitionDesc partSpec5 = new StoragePartitionDesc(cubeFact.getName(), timeParts5, null,
      UpdatePeriod.HOURLY);

    client.addPartitions(Arrays.asList(partSpec4, partSpec5), c1);
    client.addPartitions(Arrays.asList(partSpec4, partSpec5), c2);

    Assert.assertEquals(client.getAllParts(c1TableName).size(), 5);
    Assert.assertEquals(client.getAllParts(c2TableName).size(), 5);

    assertSameTimelines(factName, storages, UpdatePeriod.HOURLY, partColNames);

    assertTimeline(timelineDt, timelineDtC2, UpdatePeriod.HOURLY, now, nowPlus1);
    assertTimeline(timelineEt, timelineEtC2, UpdatePeriod.HOURLY, nowMinus2, nowPlus1);
    assertTimeline(timelineIt, timelineItC2, UpdatePeriod.HOURLY, nowMinus1, nowPlus1);

    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
      TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, itPart.getName()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, etPart.getName()));

    Map<String, Date> timeParts6 = new HashMap<String, Date>();
    timeParts6.put(TestCubeMetastoreClient.getDatePartitionKey(), nowMinus2);
    timeParts6.put(itPart.getName(), nowMinus1);
    timeParts6.put(etPart.getName(), nowMinus2);
    final StoragePartitionDesc partSpec6 = new StoragePartitionDesc(cubeFact.getName(), timeParts6, null,
      UpdatePeriod.HOURLY);

    client.addPartition(partSpec6, c1);
    client.addPartition(partSpec6, c2);

    Assert.assertEquals(client.getAllParts(c1TableName).size(), 6);
    Assert.assertEquals(client.getAllParts(c2TableName).size(), 6);

    assertSameTimelines(factName, storages, UpdatePeriod.HOURLY, partColNames);

    assertTimeline(timelineDt, timelineDtC2, UpdatePeriod.HOURLY, nowMinus2, nowPlus1, nowMinus1);
    assertTimeline(timelineEt, timelineEtC2, UpdatePeriod.HOURLY, nowMinus2, nowPlus1);
    assertTimeline(timelineIt, timelineItC2, UpdatePeriod.HOURLY, nowMinus1, nowPlus1);

    Map<String, Date> timeParts7 = new HashMap<String, Date>();
    timeParts7.put(TestCubeMetastoreClient.getDatePartitionKey(), nowMinus5);
    timeParts7.put(itPart.getName(), nowMinus5);
    timeParts7.put(etPart.getName(), nowMinus5);
    final StoragePartitionDesc partSpec7 = new StoragePartitionDesc(cubeFact.getName(), timeParts7, null,
      UpdatePeriod.HOURLY);

    client.addPartition(partSpec7, c1);
    client.addPartition(partSpec7, c2);

    Assert.assertEquals(client.getAllParts(c1TableName).size(), 7);
    Assert.assertEquals(client.getAllParts(c2TableName).size(), 7);

    assertSameTimelines(factName, storages, UpdatePeriod.HOURLY, partColNames);

    assertTimeline(timelineDt, timelineDtC2, UpdatePeriod.HOURLY, nowMinus5, nowPlus1, nowMinus4, nowMinus3, nowMinus1);
    assertTimeline(timelineEt, timelineEtC2, UpdatePeriod.HOURLY, nowMinus5, nowPlus1, nowMinus4, nowMinus3);
    assertTimeline(timelineIt, timelineItC2, UpdatePeriod.HOURLY, nowMinus5, nowPlus1, nowMinus4, nowMinus3, nowMinus2);


    assertNoPartitionNamedLatest(c1TableName, partColNames);
    assertSameTimelines(factName, storages, UpdatePeriod.HOURLY, partColNames);

    client.dropPartition(cubeFact.getName(), c1, timeParts5, null, UpdatePeriod.HOURLY);
    client.dropPartition(cubeFact.getName(), c2, timeParts5, null, UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(c1TableName).size(), 6);
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
      TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, itPart.getName()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, etPart.getName()));

    assertNoPartitionNamedLatest(c1TableName, partColNames);
    assertSameTimelines(factName, storages, UpdatePeriod.HOURLY, partColNames);

    assertTimeline(timelineDt, timelineDtC2, UpdatePeriod.HOURLY, nowMinus5, now, nowMinus4, nowMinus3, nowMinus1);
    assertTimeline(timelineEt, timelineEtC2, UpdatePeriod.HOURLY, nowMinus5, nowPlus1, nowMinus4, nowMinus3);
    assertTimeline(timelineIt, timelineItC2, UpdatePeriod.HOURLY, nowMinus5, nowPlus1, nowMinus4, nowMinus3, nowMinus2);


    client.dropPartition(cubeFact.getName(), c1, timeParts7, null, UpdatePeriod.HOURLY);
    client.dropPartition(cubeFact.getName(), c2, timeParts7, null, UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(c1TableName).size(), 5);
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
      TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, itPart.getName()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, etPart.getName()));

    assertNoPartitionNamedLatest(c1TableName, partColNames);
    assertSameTimelines(factName, storages, UpdatePeriod.HOURLY, partColNames);
    assertTimeline(timelineDt, timelineDtC2, UpdatePeriod.HOURLY, nowMinus2, now, nowMinus1);
    assertTimeline(timelineEt, timelineEtC2, UpdatePeriod.HOURLY, nowMinus2, nowPlus1);
    assertTimeline(timelineIt, timelineItC2, UpdatePeriod.HOURLY, nowMinus1, nowPlus1);


    client.dropPartition(cubeFact.getName(), c1, timeParts2, nonTimeSpec, UpdatePeriod.HOURLY);
    client.dropPartition(cubeFact.getName(), c2, timeParts2, nonTimeSpec, UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(c1TableName).size(), 4);
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
      TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, itPart.getName()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, etPart.getName()));

    assertNoPartitionNamedLatest(c1TableName, partColNames);
    assertSameTimelines(factName, storages, UpdatePeriod.HOURLY, partColNames);
    assertTimeline(timelineDt, timelineDtC2, UpdatePeriod.HOURLY, nowMinus2, now, nowMinus1);
    assertTimeline(timelineEt, timelineEtC2, UpdatePeriod.HOURLY, nowMinus2, now);
    assertTimeline(timelineIt, timelineItC2, UpdatePeriod.HOURLY, nowMinus1, nowPlus1);

    client.dropPartition(cubeFact.getName(), c1, timeParts4, null, UpdatePeriod.HOURLY);
    client.dropPartition(cubeFact.getName(), c2, timeParts4, null, UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(c1TableName).size(), 3);

    assertNoPartitionNamedLatest(c1TableName, partColNames);
    assertSameTimelines(factName, storages, UpdatePeriod.HOURLY, partColNames);
    assertTimeline(timelineDt, timelineDtC2, UpdatePeriod.HOURLY, nowMinus2, now, nowMinus1);
    assertTimeline(timelineEt, timelineEtC2, UpdatePeriod.HOURLY, nowMinus2, now, nowMinus1);
    assertTimeline(timelineIt, timelineItC2, UpdatePeriod.HOURLY, nowMinus1, now);
    client.dropPartition(cubeFact.getName(), c1, timeParts3, nonTimeSpec, UpdatePeriod.HOURLY);
    client.dropPartition(cubeFact.getName(), c2, timeParts3, nonTimeSpec, UpdatePeriod.HOURLY);
    assertSameTimelines(factName, storages, UpdatePeriod.HOURLY, partColNames);
    assertTimeline(timelineDt, timelineDtC2, UpdatePeriod.HOURLY, nowMinus2, now, nowMinus1);
    assertTimeline(timelineEt, timelineEtC2, UpdatePeriod.HOURLY, nowMinus2, now, nowMinus1);
    assertTimeline(timelineIt, timelineItC2, UpdatePeriod.HOURLY, nowMinus1, now);

    client.dropPartition(cubeFact.getName(), c1, timeParts6, null, UpdatePeriod.HOURLY);
    client.dropPartition(cubeFact.getName(), c2, timeParts6, null, UpdatePeriod.HOURLY);
    assertSameTimelines(factName, storages, UpdatePeriod.HOURLY, partColNames);
    assertTimeline(timelineDt, timelineDtC2, UpdatePeriod.HOURLY, now, now);
    assertTimeline(timelineEt, timelineEtC2, UpdatePeriod.HOURLY, now, now);
    assertTimeline(timelineIt, timelineItC2, UpdatePeriod.HOURLY, now, now);
    client.dropPartition(cubeFact.getName(), c1, timeParts1, null, UpdatePeriod.HOURLY);
    client.dropPartition(cubeFact.getName(), c2, timeParts1, null, UpdatePeriod.HOURLY);
    assertSameTimelines(factName, storages, UpdatePeriod.HOURLY, partColNames);
    Assert.assertTrue(timelineDt.isEmpty());
    Assert.assertTrue(timelineEt.isEmpty());
    Assert.assertTrue(timelineIt.isEmpty());

  }

  private void assertTimeline(EndsAndHolesPartitionTimeline endsAndHolesPartitionTimeline,
    StoreAllPartitionTimeline storeAllPartitionTimeline, UpdatePeriod updatePeriod,
    Date first, Date latest, Date... holes) throws LensException {
    TimePartition firstPart = TimePartition.of(updatePeriod, first);
    TimePartition latestPart = TimePartition.of(updatePeriod, latest);
    Assert.assertEquals(endsAndHolesPartitionTimeline.getFirst(), firstPart);
    Assert.assertEquals(endsAndHolesPartitionTimeline.getLatest(), TimePartition.of(updatePeriod, latest));
    Assert.assertEquals(endsAndHolesPartitionTimeline.getHoles().size(), holes.length);
    for (Date date : holes) {
      Assert.assertTrue(endsAndHolesPartitionTimeline.getHoles().contains(TimePartition.of(updatePeriod, date)));
    }

    TreeSet<TimePartition> partitions = new TreeSet<TimePartition>();
    for (Date dt : TimeRange.iterable(firstPart.getDate(), latestPart.next().getDate(), updatePeriod, 1)) {
      partitions.add(TimePartition.of(updatePeriod, dt));
    }
    for (Date holeDate : holes) {
      partitions.remove(TimePartition.of(updatePeriod, holeDate));
    }
    Assert.assertEquals(storeAllPartitionTimeline.getAllPartitions(), partitions);
  }

  private void assertSameTimelines(String factName, String[] storages, UpdatePeriod updatePeriod, String[] partCols)
    throws HiveException, LensException {
    for (String partCol : partCols) {
      PartitionTimeline[] timelines = new PartitionTimeline[storages.length];
      for (int i = 0; i < storages.length; i++) {
        timelines[i] = client.partitionTimelineCache.get(factName, storages[i], updatePeriod, partCol);
      }
      for (int i = 0; i < timelines.length; i++) {
        for (int j = i + 1; j < timelines.length; j++) {
          assertSameTimelines(timelines[i], timelines[j]);
        }
      }
    }
  }

  private void assertSameTimelines(PartitionTimeline timeline1, PartitionTimeline timeline2) {
    Iterator<TimePartition> iter1 = timeline1.iterator();
    Iterator<TimePartition> iter2 = timeline2.iterator();
    while (iter1.hasNext()) {
      Assert.assertTrue(iter2.hasNext());
      Assert.assertEquals(iter1.next(), iter2.next());
    }
    Assert.assertFalse(iter2.hasNext());
  }

  private StoragePartitionDesc getStoragePartSpec(String cubeFactName, UpdatePeriod updatePeriod,
    String[] partCols, int[] partOffsets) {
    Map<String, Date> timeParts = new HashMap<String, Date>();
    Calendar cal = Calendar.getInstance();
    for (int i = 0; i < partCols.length; i++) {
      cal.setTime(now);
      cal.add(updatePeriod.calendarField(), partOffsets[i]);
      timeParts.put(partCols[i], cal.getTime());
    }
    return new StoragePartitionDesc(cubeFactName, timeParts, null, updatePeriod);
  }

  private void assertNoPartitionNamedLatest(String storageTableName, String... partCols) throws HiveException {
    for (String p : partCols) {
      Assert.assertEquals(client.getPartitionsByFilter(storageTableName, p + "='latest'").size(), 0);
    }
  }

  private TimePartition[] getLatestValues(String storageTableName, UpdatePeriod updatePeriod, String... partCols)
    throws LensException, HiveException {
    TimePartition[] values = new TimePartition[partCols.length];
    for (int i = 0; i < partCols.length; i++) {
      List<Partition> part = client.getPartitionsByFilter(storageTableName, partCols[i] + "='latest'");
      Assert.assertEquals(part.size(), 1);
      values[i] = TimePartition.of(updatePeriod, part.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey(partCols[i])));
    }
    return values;
  }

  private TimePartition[] toPartitionArray(UpdatePeriod updatePeriod, Date... dates) throws LensException {
    TimePartition[] values = new TimePartition[dates.length];
    for (int i = 0; i < dates.length; i++) {
      values[i] = TimePartition.of(updatePeriod, dates[i]);
    }
    return values;
  }

  @Test(priority = 2)
  public void testCubeFactWithWeight() throws Exception {
    String factName = "testFactWithWeight";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add one dimension of the cube
    factColumns.add(new FieldSchema("zipcode", "int", "zip"));

    Map<String, Set<UpdatePeriod>> updatePeriods = new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    timePartCols.add(getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    updatePeriods.put(c1, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    CubeFactTable cubeFact = new CubeFactTable(CUBE_NAME, factName, factColumns, updatePeriods, 100L);

    // create cube fact
    client.createCubeFactTable(CUBE_NAME, factName, factColumns, updatePeriods, 100L, null, storageTables);

    Assert.assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, CUBE_NAME));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFact.equals(cubeFact2));

    // Assert for storage tables
    for (String entry : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getFactStorageTableName(factName, entry);
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    // test partition
    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    StoragePartitionDesc partSpec = new StoragePartitionDesc(cubeFact.getName(), timeParts, null, UpdatePeriod.HOURLY);
    client.addPartition(partSpec, c1);
    Assert.assertTrue(client.factPartitionExists(cubeFact.getName(), c1, UpdatePeriod.HOURLY, timeParts,
      new HashMap<String, String>()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
      TestCubeMetastoreClient.getDatePartitionKey()));
    String storageTableName = MetastoreUtil.getFactStorageTableName(cubeFact.getName(), c1);
    List<Partition> parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(parts.size(), 0);
//    Assert
//      .assertEquals(TextInputFormat.class.getCanonicalName(), parts.get(0).getInputFormatClass().getCanonicalName());
//    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey("dt")),
//      UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 1);

    client.dropPartition(cubeFact.getName(), c1, timeParts, null, UpdatePeriod.HOURLY);
    Assert.assertFalse(client.factPartitionExists(cubeFact.getName(), c1, UpdatePeriod.HOURLY, timeParts,
      new HashMap<String, String>()));
    Assert.assertFalse(client.latestPartitionExists(cubeFact.getName(), c1,
      TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 0);
  }

  @Test(priority = 2)
  public void testCubeFactWithParts() throws Exception {
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(cubeMeasures.size());
    String factNameWithPart = "testFactPart";

    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add some dimensions of the cube
    factColumns.add(new FieldSchema("dim1", "string", "dim1"));
    factColumns.add(new FieldSchema("dim2", "string", "dim2"));

    List<FieldSchema> factPartColumns = new ArrayList<FieldSchema>();
    factPartColumns.add(new FieldSchema("region", "string", "region part"));

    Map<String, Set<UpdatePeriod>> updatePeriods = new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    partCols.add(factPartColumns.get(0));
    timePartCols.add(getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    updatePeriods.put(c1, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    List<String> cubeNames = new ArrayList<String>();
    cubeNames.add(CUBE_NAME);

    CubeFactTable cubeFactWithParts = new CubeFactTable(CUBE_NAME, factNameWithPart, factColumns, updatePeriods);

    // create cube fact
    client.createCubeFactTable(CUBE_NAME, factNameWithPart, factColumns, updatePeriods, 0L, null, storageTables);

    Assert.assertTrue(client.tableExists(factNameWithPart));
    Table cubeTbl = client.getHiveTable(factNameWithPart);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, CUBE_NAME));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFactWithParts.equals(cubeFact2));

    // Assert for storage tables
    for (String entry : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getFactStorageTableName(factNameWithPart, entry);
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put(factPartColumns.get(0).getName(), "APAC");
    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    // test partition
    StoragePartitionDesc sPartSpec =
      new StoragePartitionDesc(cubeFactWithParts.getName(), timeParts, partSpec, UpdatePeriod.HOURLY);
    client.addPartition(sPartSpec, c1);
    Assert.assertTrue(client.factPartitionExists(cubeFactWithParts.getName(), c1, UpdatePeriod.HOURLY, timeParts,
      partSpec));
    Assert.assertTrue(client.latestPartitionExists(cubeFactWithParts.getName(), c1,
      TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertFalse(client.latestPartitionExists(cubeFactWithParts.getName(), c1, factPartColumns.get(0).getName()));
    String storageTableName = MetastoreUtil.getFactStorageTableName(cubeFactWithParts.getName(), c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 1);
    List<Partition> parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(parts.size(), 0);
//    Assert
//      .assertEquals(TextInputFormat.class.getCanonicalName(), parts.get(0).getInputFormatClass().getCanonicalName());
//    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey("dt")),
//      UpdatePeriod.HOURLY.format().format(now));

    client.dropPartition(cubeFactWithParts.getName(), c1, timeParts, partSpec, UpdatePeriod.HOURLY);
    Assert.assertFalse(client.factPartitionExists(cubeFactWithParts.getName(), c1, UpdatePeriod.HOURLY, timeParts,
      partSpec));
    Assert.assertFalse(client.latestPartitionExists(cubeFactWithParts.getName(), c1,
      TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 0);
  }

  @Test(priority = 2)
  public void testCubeFactWithPartsAndTimedParts() throws Exception {
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(cubeMeasures.size());
    String factNameWithPart = "testFactPartAndTimedParts";

    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add some dimensions of the cube
    factColumns.add(new FieldSchema("dim1", "string", "dim1"));
    factColumns.add(new FieldSchema("dim2", "string", "dim2"));

    List<FieldSchema> factPartColumns = new ArrayList<FieldSchema>();
    factPartColumns.add(new FieldSchema("region", "string", "region part"));

    Map<String, Set<UpdatePeriod>> updatePeriods = new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    FieldSchema testDtPart = new FieldSchema("mydate", "string", "date part");
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    partCols.add(testDtPart);
    partCols.add(factPartColumns.get(0));
    timePartCols.add(getDatePartitionKey());
    timePartCols.add(testDtPart.getName());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    updatePeriods.put(c1, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    CubeFactTable cubeFactWithParts = new CubeFactTable(CUBE_NAME, factNameWithPart, factColumns, updatePeriods);

    // create cube fact
    client.createCubeFactTable(CUBE_NAME, factNameWithPart, factColumns, updatePeriods, 0L, null, storageTables);

    Assert.assertTrue(client.tableExists(factNameWithPart));
    Table cubeTbl = client.getHiveTable(factNameWithPart);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, CUBE_NAME));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFactWithParts.equals(cubeFact2));

    // Assert for storage tables
    for (String entry : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getFactStorageTableName(factNameWithPart, entry);
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    Calendar cal = new GregorianCalendar();
    cal.setTime(now);
    cal.add(Calendar.HOUR, -1);
    Date testDt = cal.getTime();
    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put(factPartColumns.get(0).getName(), "APAC");
    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    timeParts.put(testDtPart.getName(), testDt);
    // test partition
    StoragePartitionDesc sPartSpec =
      new StoragePartitionDesc(cubeFactWithParts.getName(), timeParts, partSpec, UpdatePeriod.HOURLY);
    client.addPartition(sPartSpec, c1);
    Assert.assertTrue(client.factPartitionExists(cubeFactWithParts.getName(), c1, UpdatePeriod.HOURLY, timeParts,
      partSpec));
    Assert.assertTrue(client.latestPartitionExists(cubeFactWithParts.getName(), c1,
      TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertTrue(client.latestPartitionExists(cubeFactWithParts.getName(), c1, testDtPart.getName()));
    Assert.assertFalse(client.latestPartitionExists(cubeFactWithParts.getName(), c1, factPartColumns.get(0).getName()));
    String storageTableName = MetastoreUtil.getFactStorageTableName(cubeFactWithParts.getName(), c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 1);

    assertNoPartitionNamedLatest(storageTableName, "dt", testDtPart.getName());

    client.dropPartition(cubeFactWithParts.getName(), c1, timeParts, partSpec, UpdatePeriod.HOURLY);
    Assert.assertFalse(client.factPartitionExists(cubeFactWithParts.getName(), c1, UpdatePeriod.HOURLY, timeParts,
      partSpec));
    Assert.assertFalse(client.latestPartitionExists(cubeFactWithParts.getName(), c1, testDtPart.getName()));
    Assert.assertFalse(client.latestPartitionExists(cubeFactWithParts.getName(), c1,
      TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 0);
  }

  @Test(priority = 2)
  public void testCubeFactWithTwoStorages() throws Exception {
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(cubeMeasures.size());
    String factName = "testFactTwoStorages";

    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add some dimensions of the cube
    factColumns.add(new FieldSchema("dim1", "string", "dim1"));
    factColumns.add(new FieldSchema("dim2", "string", "dim2"));

    List<FieldSchema> factPartColumns = new ArrayList<FieldSchema>();
    factPartColumns.add(new FieldSchema("region", "string", "region part"));

    Map<String, Set<UpdatePeriod>> updatePeriods = new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    partCols.add(factPartColumns.get(0));
    timePartCols.add(getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(SequenceFileInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    updatePeriods.put(c1, updates);
    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    ArrayList<FieldSchema> partCols2 = new ArrayList<FieldSchema>();
    partCols2.add(getDatePartition());
    s2.setPartCols(partCols2);
    s2.setTimePartCols(timePartCols);
    updatePeriods.put(c2, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s2);

    CubeFactTable cubeFactWithTwoStorages = new CubeFactTable(CUBE_NAME, factName, factColumns, updatePeriods);

    client.createCubeFactTable(CUBE_NAME, factName, factColumns, updatePeriods, 0L, null, storageTables);

    Assert.assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, CUBE_NAME));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFactWithTwoStorages.equals(cubeFact2));

    // Assert for storage tables
    for (String entry : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getFactStorageTableName(factName, entry);
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put(factPartColumns.get(0).getName(), "APAC");
    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    // test partition
    StoragePartitionDesc sPartSpec =
      new StoragePartitionDesc(cubeFactWithTwoStorages.getName(), timeParts, partSpec, UpdatePeriod.HOURLY);
    client.addPartition(sPartSpec, c1);
    Assert.assertTrue(client.factPartitionExists(cubeFactWithTwoStorages.getName(), c1, UpdatePeriod.HOURLY, timeParts,
      partSpec));
    Assert.assertTrue(client.latestPartitionExists(cubeFactWithTwoStorages.getName(), c1,
      TestCubeMetastoreClient.getDatePartitionKey()));
    String storageTableName = MetastoreUtil.getFactStorageTableName(cubeFactWithTwoStorages.getName(), c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 1);

    assertNoPartitionNamedLatest(storageTableName, "dt");

    StoragePartitionDesc sPartSpec2 =
      new StoragePartitionDesc(cubeFactWithTwoStorages.getName(), timeParts, null, UpdatePeriod.HOURLY);
    client.addPartition(sPartSpec2, c2);
    Assert.assertTrue(client.factPartitionExists(cubeFactWithTwoStorages.getName(), c2, UpdatePeriod.HOURLY, timeParts,
      new HashMap<String, String>()));
    Assert.assertTrue(client.latestPartitionExists(cubeFactWithTwoStorages.getName(), c2,
      TestCubeMetastoreClient.getDatePartitionKey()));
    String storageTableName2 = MetastoreUtil.getFactStorageTableName(cubeFactWithTwoStorages.getName(), c2);
    Assert.assertEquals(client.getAllParts(storageTableName2).size(), 1);

    assertNoPartitionNamedLatest(storageTableName2, "dt");

    client.dropPartition(cubeFactWithTwoStorages.getName(), c1, timeParts, partSpec, UpdatePeriod.HOURLY);
    Assert.assertFalse(client.factPartitionExists(cubeFactWithTwoStorages.getName(), c1, UpdatePeriod.HOURLY,
      timeParts, partSpec));
    Assert.assertFalse(client.latestPartitionExists(cubeFactWithTwoStorages.getName(), c1,
      TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 0);

    client.dropPartition(cubeFactWithTwoStorages.getName(), c2, timeParts, null, UpdatePeriod.HOURLY);
    Assert.assertFalse(client.factPartitionExists(cubeFactWithTwoStorages.getName(), c2, UpdatePeriod.HOURLY,
      timeParts, new HashMap<String, String>()));
    Assert.assertFalse(client.latestPartitionExists(cubeFactWithTwoStorages.getName(), c2,
      TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertEquals(client.getAllParts(storageTableName2).size(), 0);
  }

  @Test(priority = 2)
  public void testCubeDimWithWeight() throws Exception {
    String dimName = "statetable";

    List<FieldSchema> dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "state id"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("capital", "string", "field2"));
    dimColumns.add(new FieldSchema("countryid", "int", "country id"));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    timePartCols.add(getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, UpdatePeriod.HOURLY);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    CubeDimensionTable cubeDim = new CubeDimensionTable(stateDim.getName(), dimName, dimColumns, 100L, dumpPeriods);
    client.createCubeDimensionTable(stateDim.getName(), dimName, dimColumns, 100L, dumpPeriods, null, storageTables);
    Assert.assertTrue(client.tableExists(dimName));
    Table cubeTbl = client.getHiveTable(dimName);
    Assert.assertTrue(client.isDimensionTable(cubeTbl));
    CubeDimensionTable cubeDim2 = new CubeDimensionTable(cubeTbl);
    Assert.assertTrue(cubeDim.equals(cubeDim2));

    List<CubeDimensionTable> stateTbls = client.getAllDimensionTables(stateDim);
    boolean found = false;
    for (CubeDimensionTable dim : stateTbls) {
      if (dim.getName().equalsIgnoreCase(dimName)) {
        found = true;
        break;
      }
    }
    Assert.assertTrue(found);

    // Assert for storage tables
    for (String storage : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getStorageTableName(dimName, Storage.getPrefix(storage));
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    // test partition
    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    StoragePartitionDesc sPartSpec = new StoragePartitionDesc(cubeDim.getName(), timeParts, null, UpdatePeriod.HOURLY);
    client.addPartition(sPartSpec, c1);
    Assert.assertTrue(client.dimPartitionExists(cubeDim.getName(), c1, timeParts));
    Assert
      .assertTrue(client.latestPartitionExists(cubeDim.getName(), c1, TestCubeMetastoreClient.getDatePartitionKey()));
    String storageTableName = MetastoreUtil.getDimStorageTableName(dimName, c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 2);
    List<Partition> parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert
      .assertEquals(TextInputFormat.class.getCanonicalName(), parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey("dt")),
      UpdatePeriod.HOURLY.format().format(now));

    client.dropPartition(cubeDim.getName(), c1, timeParts, null, UpdatePeriod.HOURLY);
    Assert.assertFalse(client.dimPartitionExists(cubeDim.getName(), c1, timeParts));
    Assert.assertFalse(client.latestPartitionExists(cubeDim.getName(), c1,
      TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 0);
  }

  @Test(priority = 2)
  public void testCubeDim() throws Exception {
    String dimName = "ziptableMeta";

    List<FieldSchema> dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("zipcode", "int", "code"));
    dimColumns.add(new FieldSchema("f1", "string", "field1"));
    dimColumns.add(new FieldSchema("f2", "string", "field2"));
    dimColumns.add(new FieldSchema("stateid", "int", "state id"));
    dimColumns.add(new FieldSchema("statei2", "int", "state id"));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    timePartCols.add(getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, UpdatePeriod.HOURLY);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    CubeDimensionTable cubeDim = new CubeDimensionTable(zipDim.getName(), dimName, dimColumns, 0L, dumpPeriods);
    client.createCubeDimensionTable(zipDim.getName(), dimName, dimColumns, 0L, dumpPeriods, null, storageTables);

    Assert.assertTrue(client.tableExists(dimName));

    Table cubeTbl = client.getHiveTable(dimName);
    Assert.assertTrue(client.isDimensionTable(cubeTbl));

    List<CubeDimensionTable> tbls = client.getAllDimensionTables(zipDim);
    boolean found = false;
    for (CubeDimensionTable dim : tbls) {
      if (dim.getName().equalsIgnoreCase(dimName)) {
        found = true;
        break;
      }
    }
    Assert.assertTrue(found);

    CubeDimensionTable cubeDim2 = new CubeDimensionTable(cubeTbl);
    Assert.assertTrue(cubeDim.equals(cubeDim2));

    // Assert for storage tables
    for (String storage : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getDimStorageTableName(dimName, storage);
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    FieldSchema newcol = new FieldSchema("newcol", "int", "new col for part");

    // test partition
    String storageTableName = MetastoreUtil.getDimStorageTableName(dimName, c1);
    Assert.assertFalse(client.dimTableLatestPartitionExists(storageTableName));
    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    StoragePartitionDesc sPartSpec = new StoragePartitionDesc(cubeDim.getName(), timeParts, null, UpdatePeriod.HOURLY);
    client.addPartition(sPartSpec, c1);
    Assert.assertTrue(client.dimPartitionExists(cubeDim.getName(), c1, timeParts));
    Assert
      .assertTrue(client.latestPartitionExists(cubeDim.getName(), c1, TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 2);
    Assert.assertTrue(client.dimTableLatestPartitionExists(storageTableName));
    List<Partition> parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert
      .assertEquals(TextInputFormat.class.getCanonicalName(), parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertFalse(parts.get(0).getCols().contains(newcol));
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey("dt")),
      UpdatePeriod.HOURLY.format().format(now));

    // Partition with different schema
    cubeDim.alterColumn(newcol);
    client.alterCubeDimensionTable(cubeDim.getName(), cubeDim);

    Map<String, Date> timeParts2 = new HashMap<String, Date>();
    timeParts2.put(TestCubeMetastoreClient.getDatePartitionKey(), nowPlus1);
    StoragePartitionDesc sPartSpec2 =
      new StoragePartitionDesc(cubeDim.getName(), timeParts2, null, UpdatePeriod.HOURLY);
    sPartSpec2.setInputFormat(SequenceFileInputFormat.class.getCanonicalName());
    sPartSpec2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    client.addPartition(sPartSpec2, c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 3);
    Assert.assertTrue(client.dimPartitionExists(cubeDim.getName(), c1, timeParts));
    Assert.assertTrue(client.dimPartitionExists(cubeDim.getName(), c1, timeParts2));
    Assert
      .assertTrue(client.latestPartitionExists(cubeDim.getName(), c1, TestCubeMetastoreClient.getDatePartitionKey()));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(SequenceFileInputFormat.class.getCanonicalName(), parts.get(0).getInputFormatClass()
      .getCanonicalName());
    Assert.assertTrue(parts.get(0).getCols().contains(newcol));
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey("dt")),
      UpdatePeriod.HOURLY.format().format(nowPlus1));

    client.dropPartition(cubeDim.getName(), c1, timeParts2, null, UpdatePeriod.HOURLY);
    Assert.assertTrue(client.dimPartitionExists(cubeDim.getName(), c1, timeParts));
    Assert.assertFalse(client.dimPartitionExists(cubeDim.getName(), c1, timeParts2));
    Assert
      .assertTrue(client.latestPartitionExists(cubeDim.getName(), c1, TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertTrue(client.dimTableLatestPartitionExists(storageTableName));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert
      .assertEquals(TextInputFormat.class.getCanonicalName(), parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey("dt")),
      UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 2);

    client.dropPartition(cubeDim.getName(), c1, timeParts, null, UpdatePeriod.HOURLY);
    Assert.assertFalse(client.dimPartitionExists(cubeDim.getName(), c1, timeParts));
    Assert.assertFalse(client.dimPartitionExists(cubeDim.getName(), c1, timeParts2));
    Assert.assertFalse(client.latestPartitionExists(cubeDim.getName(), c1,
      TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 0);
    Assert.assertFalse(client.dimTableLatestPartitionExists(storageTableName));

    client.addPartition(sPartSpec2, c1);
    Assert.assertTrue(client.dimTableLatestPartitionExists(storageTableName));
    client.dropStorageFromDim(cubeDim.getName(), c1);
    Assert.assertFalse(client.dimTableLatestPartitionExists(storageTableName));
  }

  @Test(priority = 2)
  public void testCubeDimWithThreeTimedParts() throws Exception {
    String dimName = "ziptableMetaWithThreeTimedParts";

    List<FieldSchema> dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("zipcode", "int", "code"));
    dimColumns.add(new FieldSchema("f1", "string", "field1"));
    dimColumns.add(new FieldSchema("f2", "string", "field2"));
    dimColumns.add(new FieldSchema("stateid", "int", "state id"));
    dimColumns.add(new FieldSchema("statei2", "int", "state id"));

    FieldSchema itPart = new FieldSchema("it", "string", "date part");
    FieldSchema etPart = new FieldSchema("et", "string", "date part");
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    partCols.add(itPart);
    partCols.add(etPart);
    timePartCols.add(getDatePartitionKey());
    timePartCols.add(itPart.getName());
    timePartCols.add(etPart.getName());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    String[] partColNames = new String[]{getDatePartitionKey(), itPart.getName(), etPart.getName()};
    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    dumpPeriods.put(c1, UpdatePeriod.HOURLY);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    CubeDimensionTable cubeDim = new CubeDimensionTable(zipDim.getName(), dimName, dimColumns, 0L, dumpPeriods);
    client.createCubeDimensionTable(zipDim.getName(), dimName, dimColumns, 0L, dumpPeriods, null, storageTables);

    Assert.assertTrue(client.tableExists(dimName));

    Table cubeTbl = client.getHiveTable(dimName);
    Assert.assertTrue(client.isDimensionTable(cubeTbl));

    List<CubeDimensionTable> tbls = client.getAllDimensionTables(zipDim);
    boolean found = false;
    for (CubeDimensionTable dim : tbls) {
      if (dim.getName().equalsIgnoreCase(dimName)) {
        found = true;
        break;
      }
    }
    Assert.assertTrue(found);

    CubeDimensionTable cubeDim2 = new CubeDimensionTable(cubeTbl);
    Assert.assertTrue(cubeDim.equals(cubeDim2));

    // Assert for storage tables
    for (String storage : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getDimStorageTableName(dimName, storage);
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    Map<String, Date> timeParts1 = new HashMap<String, Date>();
    timeParts1.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    timeParts1.put(itPart.getName(), now);
    timeParts1.put(etPart.getName(), now);
    StoragePartitionDesc partSpec1 = new StoragePartitionDesc(cubeDim.getName(), timeParts1, null,
      UpdatePeriod.HOURLY);

    Map<String, Date> timeParts2 = new HashMap<String, Date>();
    timeParts2.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    timeParts2.put(etPart.getName(), nowPlus1);
    Map<String, String> nonTimeSpec = new HashMap<String, String>();
    nonTimeSpec.put(itPart.getName(), "default");
    final StoragePartitionDesc partSpec2 = new StoragePartitionDesc(cubeDim.getName(), timeParts2, nonTimeSpec,
      UpdatePeriod.HOURLY);

    Map<String, Date> timeParts3 = new HashMap<String, Date>();
    timeParts3.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    timeParts3.put(etPart.getName(), now);
    final StoragePartitionDesc partSpec3 = new StoragePartitionDesc(cubeDim.getName(), timeParts3, nonTimeSpec,
      UpdatePeriod.HOURLY);

    client.addPartitions(Arrays.asList(partSpec1, partSpec2, partSpec3), c1);
    String c1TableName = MetastoreUtil.getDimStorageTableName(cubeDim.getName(), c1);
    Assert.assertEquals(client.getAllParts(c1TableName).size(), 6);

    Assert.assertEquals(getLatestValues(c1TableName, UpdatePeriod.HOURLY, partColNames),
      toPartitionArray(UpdatePeriod.HOURLY, now, now, nowPlus1));

    Map<String, Date> timeParts4 = new HashMap<String, Date>();
    timeParts4.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    timeParts4.put(itPart.getName(), nowPlus1);
    timeParts4.put(etPart.getName(), nowMinus1);
    final StoragePartitionDesc partSpec4 = new StoragePartitionDesc(cubeDim.getName(), timeParts4, null,
      UpdatePeriod.HOURLY);


    Map<String, Date> timeParts5 = new HashMap<String, Date>();
    timeParts5.put(TestCubeMetastoreClient.getDatePartitionKey(), nowPlus1);
    timeParts5.put(itPart.getName(), nowMinus1);
    timeParts5.put(etPart.getName(), nowMinus2);
    final StoragePartitionDesc partSpec5 = new StoragePartitionDesc(cubeDim.getName(), timeParts5, null,
      UpdatePeriod.HOURLY);

    client.addPartitions(Arrays.asList(partSpec4, partSpec5), c1);

    Assert.assertEquals(client.getAllParts(c1TableName).size(), 8);
    Assert.assertEquals(getLatestValues(c1TableName, UpdatePeriod.HOURLY, partColNames),
      toPartitionArray(UpdatePeriod.HOURLY, nowPlus1, nowPlus1, nowPlus1));
    Map<String, Date> timeParts6 = new HashMap<String, Date>();
    timeParts6.put(TestCubeMetastoreClient.getDatePartitionKey(), nowMinus2);
    timeParts6.put(itPart.getName(), nowMinus1);
    timeParts6.put(etPart.getName(), nowMinus2);
    final StoragePartitionDesc partSpec6 = new StoragePartitionDesc(cubeDim.getName(), timeParts6, null,
      UpdatePeriod.HOURLY);

    client.addPartition(partSpec6, c1);

    Assert.assertEquals(client.getAllParts(c1TableName).size(), 9);


    Map<String, Date> timeParts7 = new HashMap<String, Date>();
    timeParts7.put(TestCubeMetastoreClient.getDatePartitionKey(), nowMinus5);
    timeParts7.put(itPart.getName(), nowMinus5);
    timeParts7.put(etPart.getName(), nowMinus5);
    final StoragePartitionDesc partSpec7 = new StoragePartitionDesc(cubeDim.getName(), timeParts7, null,
      UpdatePeriod.HOURLY);

    client.addPartition(partSpec7, c1);
    Assert.assertEquals(client.getAllParts(c1TableName).size(), 10);
    Assert.assertEquals(getLatestValues(c1TableName, UpdatePeriod.HOURLY, partColNames),
      toPartitionArray(UpdatePeriod.HOURLY, nowPlus1, nowPlus1, nowPlus1));

    client.dropPartition(cubeDim.getName(), c1, timeParts5, null, UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(c1TableName).size(), 9);
    Assert.assertEquals(getLatestValues(c1TableName, UpdatePeriod.HOURLY, partColNames),
      toPartitionArray(UpdatePeriod.HOURLY, now, nowPlus1, nowPlus1));

    client.dropPartition(cubeDim.getName(), c1, timeParts7, null, UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(c1TableName).size(), 8);
    Assert.assertEquals(getLatestValues(c1TableName, UpdatePeriod.HOURLY, partColNames),
      toPartitionArray(UpdatePeriod.HOURLY, now, nowPlus1, nowPlus1));

    client.dropPartition(cubeDim.getName(), c1, timeParts2, nonTimeSpec, UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(c1TableName).size(), 7);
    Assert.assertEquals(getLatestValues(c1TableName, UpdatePeriod.HOURLY, partColNames),
      toPartitionArray(UpdatePeriod.HOURLY, now, nowPlus1, now));

    client.dropPartition(cubeDim.getName(), c1, timeParts4, null, UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(c1TableName).size(), 6);
    Assert.assertEquals(getLatestValues(c1TableName, UpdatePeriod.HOURLY, partColNames),
      toPartitionArray(UpdatePeriod.HOURLY, now, now, now));

    client.dropPartition(cubeDim.getName(), c1, timeParts3, nonTimeSpec, UpdatePeriod.HOURLY);
    client.dropPartition(cubeDim.getName(), c1, timeParts6, null, UpdatePeriod.HOURLY);
    client.dropPartition(cubeDim.getName(), c1, timeParts1, null, UpdatePeriod.HOURLY);

    Assert.assertEquals(client.getAllParts(c1TableName).size(), 0);
    assertNoPartitionNamedLatest(c1TableName, partColNames);
  }

  @Test(priority = 2)
  public void testAlterDim() throws Exception {
    String dimName = "test_alter_dim";

    List<FieldSchema> dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("zipcode", "int", "code"));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    timePartCols.add(getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, UpdatePeriod.HOURLY);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    client.createCubeDimensionTable(zipDim.getName(), dimName, dimColumns, 100L, dumpPeriods, null, storageTables);

    CubeDimensionTable dimTable = client.getDimensionTable(dimName);
    dimTable.alterColumn(new FieldSchema("testAddDim", "string", "test add column"));

    List<CubeDimensionTable> tbls = client.getAllDimensionTables(zipDim);
    boolean found = false;
    for (CubeDimensionTable dim : tbls) {
      if (dim.getName().equalsIgnoreCase(dimName)) {
        found = true;
        break;
      }
    }
    Assert.assertTrue(found);

    client.alterCubeDimensionTable(dimName, dimTable);

    Table alteredHiveTable = Hive.get(conf).getTable(dimName);
    CubeDimensionTable altered = new CubeDimensionTable(alteredHiveTable);
    List<FieldSchema> columns = altered.getColumns();
    boolean contains = false;
    for (FieldSchema column : columns) {
      if (column.getName().equals("testadddim") && column.getType().equals("string")) {
        contains = true;
        break;
      }
    }
    Assert.assertTrue(contains);

    // Test alter column
    dimTable.alterColumn(new FieldSchema("testAddDim", "int", "change type"));
    client.alterCubeDimensionTable(dimName, dimTable);

    altered = new CubeDimensionTable(Hive.get(conf).getTable(dimName));
    boolean typeChanged = false;
    for (FieldSchema column : altered.getColumns()) {
      if (column.getName().equals("testadddim") && column.getType().equals("int")) {
        typeChanged = true;
        break;
      }
    }
    Assert.assertTrue(typeChanged);
    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    client.addStorage(dimTable, c2, null, s2);
    client.addStorage(dimTable, c3, UpdatePeriod.DAILY, s1);
    Assert.assertTrue(client.tableExists(MetastoreUtil.getDimStorageTableName(dimName, c2)));
    Assert.assertTrue(client.tableExists(MetastoreUtil.getDimStorageTableName(dimName, c3)));
    Assert.assertFalse(dimTable.hasStorageSnapshots("C2"));
    Assert.assertTrue(dimTable.hasStorageSnapshots("C3"));
    client.dropStorageFromDim(dimName, "C1");
    Assert.assertFalse(client.tableExists(MetastoreUtil.getDimStorageTableName(dimName, c1)));
    client.dropDimensionTable(dimName, true);
    Assert.assertFalse(client.tableExists(MetastoreUtil.getDimStorageTableName(dimName, c2)));
    Assert.assertFalse(client.tableExists(MetastoreUtil.getDimStorageTableName(dimName, c3)));
    Assert.assertFalse(client.tableExists(dimName));
  }

  @Test(priority = 2)
  public void testCubeDimWithoutDumps() throws Exception {
    String dimName = "countrytableMeta";

    List<FieldSchema> dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("capital", "string", "field2"));
    dimColumns.add(new FieldSchema("region", "string", "region name"));

    Set<String> storageNames = new HashSet<String>();

    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    storageNames.add(c1);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    CubeDimensionTable cubeDim = new CubeDimensionTable(countryDim.getName(), dimName, dimColumns, 0L, storageNames);

    client.createCubeDimensionTable(countryDim.getName(), dimName, dimColumns, 0L, storageNames, null, storageTables);

    Assert.assertTrue(client.tableExists(dimName));
    Table cubeTbl = client.getHiveTable(dimName);
    Assert.assertTrue(client.isDimensionTable(cubeTbl));

    List<CubeDimensionTable> tbls = client.getAllDimensionTables(countryDim);
    boolean found = false;
    for (CubeDimensionTable dim : tbls) {
      if (dim.getName().equalsIgnoreCase(dimName)) {
        found = true;
        break;
      }
    }
    Assert.assertTrue(found);

    CubeDimensionTable cubeDim2 = new CubeDimensionTable(cubeTbl);
    Assert.assertTrue(cubeDim.equals(cubeDim2));

    // Assert for storage tables
    for (String storageName : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getDimStorageTableName(dimName, storageName);
      Assert.assertTrue(client.tableExists(storageTableName));
      Assert.assertTrue(!client.getHiveTable(storageTableName).isPartitioned());
    }
  }

  @Test(priority = 2)
  public void testCubeDimWithTwoStorages() throws Exception {
    String dimName = "citytableMeta";

    List<FieldSchema> dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("stateid", "int", "state id"));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    timePartCols.add(getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, UpdatePeriod.HOURLY);

    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    dumpPeriods.put(c2, null);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s2);

    CubeDimensionTable cubeDim = new CubeDimensionTable(cityDim.getName(), dimName, dimColumns, 0L, dumpPeriods);
    client.createCubeDimensionTable(cityDim.getName(), dimName, dimColumns, 0L, dumpPeriods, null, storageTables);

    Assert.assertTrue(client.tableExists(dimName));
    Table cubeTbl = client.getHiveTable(dimName);
    Assert.assertTrue(client.isDimensionTable(cubeTbl));

    List<CubeDimensionTable> tbls = client.getAllDimensionTables(cityDim);
    boolean found = false;
    for (CubeDimensionTable dim : tbls) {
      if (dim.getName().equalsIgnoreCase(dimName)) {
        found = true;
        break;
      }
    }
    Assert.assertTrue(found);

    CubeDimensionTable cubeDim2 = new CubeDimensionTable(cubeTbl);
    Assert.assertTrue(cubeDim.equals(cubeDim2));

    // Assert for storage tables
    String storageTableName1 = MetastoreUtil.getDimStorageTableName(dimName, c1);
    Assert.assertTrue(client.tableExists(storageTableName1));
    String storageTableName2 = MetastoreUtil.getDimStorageTableName(dimName, c2);
    Assert.assertTrue(client.tableExists(storageTableName2));
    Assert.assertTrue(!client.getHiveTable(storageTableName2).isPartitioned());
  }

  @Test(priority = 3)
  public void testCaching() throws HiveException, ParseException {
    client = CubeMetastoreClient.getInstance(conf);
    CubeMetastoreClient client2 = CubeMetastoreClient.getInstance(new HiveConf(TestCubeMetastoreClient.class));
    Assert.assertEquals(5, client.getAllCubes().size());
    Assert.assertEquals(5, client2.getAllCubes().size());

    defineCube("testcache1", "testcache2", "derived1", "derived2");
    client.createCube("testcache1", cubeMeasures, cubeDimensions);
    client.createCube("testcache2", cubeMeasures, cubeDimensions, CUBE_PROPERTIES);
    client.createDerivedCube("testcache1", "derived1", measures, dimensions, new HashMap<String, String>(), 0L);
    client.createDerivedCube("testcache2", "derived2", measures, dimensions, CUBE_PROPERTIES, 0L);
    Assert.assertNotNull(client.getCube("testcache1"));
    Assert.assertNotNull(client2.getCube("testcache1"));
    Assert.assertEquals(9, client.getAllCubes().size());
    Assert.assertEquals(9, client2.getAllCubes().size());

    client2 = CubeMetastoreClient.getInstance(conf);
    Assert.assertEquals(9, client.getAllCubes().size());
    Assert.assertEquals(9, client2.getAllCubes().size());

    conf.setBoolean(MetastoreConstants.METASTORE_ENABLE_CACHING, false);
    client = CubeMetastoreClient.getInstance(conf);
    client2 = CubeMetastoreClient.getInstance(conf);
    Assert.assertEquals(9, client.getAllCubes().size());
    Assert.assertEquals(9, client2.getAllCubes().size());
    defineCube("testcache3", "testcache4", "dervied3", "derived4");
    client.createCube("testcache3", cubeMeasures, cubeDimensions);
    client.createCube("testcache4", cubeMeasures, cubeDimensions, CUBE_PROPERTIES);
    client.createDerivedCube("testcache3", "derived3", measures, dimensions, new HashMap<String, String>(), 0L);
    client.createDerivedCube("testcache4", "derived4", measures, dimensions, CUBE_PROPERTIES, 0L);
    Assert.assertNotNull(client.getCube("testcache3"));
    Assert.assertNotNull(client2.getCube("testcache3"));
    Assert.assertEquals(13, client.getAllCubes().size());
    Assert.assertEquals(13, client2.getAllCubes().size());
    conf.setBoolean(MetastoreConstants.METASTORE_ENABLE_CACHING, true);
    client = CubeMetastoreClient.getInstance(conf);
  }
}

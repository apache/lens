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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  private static final String cubeName = "testMetastoreCube";
  private static final String cubeNameWithProps = "testMetastoreCubeWithProps";
  private static final String derivedCubeName = "derivedTestMetastoreCube";
  private static final String derivedCubeNameWithProps = "derivedTestMetastoreCubeWithProps";
  private static final Map<String, String> cubeProperties = new HashMap<String, String>();
  private static Date now;
  private static Date nowPlus1;
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
    client = CubeMetastoreClient.getInstance(conf);
    now = new Date();
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.HOUR_OF_DAY, 1);
    nowPlus1 = cal.getTime();
    Database database = new Database();
    database.setName(TestCubeMetastoreClient.class.getSimpleName());
    Hive.get(conf).createDatabase(database);
    client.setCurrentDatabase(TestCubeMetastoreClient.class.getSimpleName());
    defineCube(cubeName, cubeNameWithProps, derivedCubeName, derivedCubeNameWithProps);
    defineUberDims();
  }

  @AfterClass
  public static void teardown() throws Exception {
    // Drop the cube
    client.dropCube(cubeName);
    client = CubeMetastoreClient.getInstance(conf);
    Assert.assertFalse(client.tableExists(cubeName));

    Hive.get().dropDatabase(TestCubeMetastoreClient.class.getSimpleName(), true, true, true);
    CubeMetastoreClient.close();
  }

  private static void defineCube(String cubeName, String cubeNameWithProps, String derivedCubeName,
      String derivedCubeNameWithProps) throws ParseException {
    cubeMeasures = new HashSet<CubeMeasure>();
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msr1", "int", "first measure")));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msr2", "float", "second measure"), "Measure2", null, "SUM",
        "RS"));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msr3", "double", "third measure"), "Measure3", null, "MAX",
        null));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msr4", "bigint", "fourth measure"), "Measure4", null, "COUNT",
        null));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msrstarttime", "int", "measure with start time"),
        "Measure With Starttime", null, null, null, now, null, null));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msrendtime", "float", "measure with end time"),
        "Measure With Endtime", null, "SUM", "RS", now, now, null));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msrcost", "double", "measure with cost"), "Measure With cost",
        null, "MAX", null, now, now, 100.0));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msrcost2", "bigint", "measure with cost"),
        "Measure With cost2", null, "MAX", null, null, null, 100.0));

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
        "avg(msr1 + msr2)"));
    cubeExpressions.add(new ExprColumn(new FieldSchema("msr5start", "double", "expr measure with start and end times"),
        "AVG of SUM", "avg(msr1 + msr2)"));
    cubeExpressions.add(new ExprColumn(new FieldSchema("booleancut", "boolean", "a boolean expression"), "Boolean Cut",
        "dim1 != 'x' AND dim2 != 10 "));
    cubeExpressions.add(new ExprColumn(new FieldSchema("substrexpr", "string", "a subt string expression"),
        "SUBSTR EXPR", "substr(dim1, 3)"));

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
    cube = new Cube(cubeName, cubeMeasures, cubeDimensions, cubeExpressions, new HashMap<String, String>(), 0.0);
    measures = new HashSet<String>();
    measures.add("msr1");
    measures.add("msr2");
    measures.add("msr3");
    dimensions = new HashSet<String>();
    dimensions.add("dim1");
    dimensions.add("dim2");
    dimensions.add("dim3");
    derivedCube = new DerivedCube(derivedCubeName, measures, dimensions, cube);

    cubeProperties.put(MetastoreUtil.getCubeTimedDimensionListKey(cubeNameWithProps), "dt,mydate");
    cubeProperties.put(MetastoreConstants.CUBE_ALL_FIELDS_QUERIABLE, "false");
    cubeProperties.put("cube.custom.prop", "myval");
    cubeWithProps = new Cube(cubeNameWithProps, cubeMeasures, cubeDimensions, cubeProperties);
    derivedCubeWithProps =
        new DerivedCube(derivedCubeNameWithProps, measures, dimensions, cubeProperties, 0L, cubeWithProps);
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
        "State and Country", "concat(statedim.name, \":\", countrydim.name)"));
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
    city.alterExpression(new ExprColumn(new FieldSchema("stateAndCountry", "String",
        "state and country together with hiphen as separator"), "State and Country",
        "concat(statedim.name, \"-\", countrydim.name)"));
    city.removeExpression("cityAddress");
    city = client.getDimension(cityDim.getName());
    Assert.assertEquals(1, city.getExpressions().size());
    Assert.assertNotNull(city.getExpressionByName("stateAndCountry"));
    Assert.assertEquals(city.getExpressionByName("stateAndCountry").getExpr(),
        "concat(statedim.name, \"-\", countrydim.name)");

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
    Assert.assertEquals(client.getCurrentDatabase(), this.getClass().getSimpleName());
    client.createCube(cubeName, cubeMeasures, cubeDimensions, cubeExpressions, new HashMap<String, String>());
    Assert.assertTrue(client.tableExists(cubeName));
    Table cubeTbl = client.getHiveTable(cubeName);
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
    Assert.assertTrue(cube2.allFieldsQueriable());

    client.createDerivedCube(cubeName, derivedCubeName, measures, dimensions, new HashMap<String, String>(), 0L);
    Assert.assertTrue(client.tableExists(derivedCubeName));
    Table derivedTbl = client.getHiveTable(derivedCubeName);
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

    client.createCube(cubeNameWithProps, cubeMeasures, cubeDimensions, cubeProperties);
    Assert.assertTrue(client.tableExists(cubeNameWithProps));
    cubeTbl = client.getHiveTable(cubeNameWithProps);
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

    client.createDerivedCube(cubeNameWithProps, derivedCubeNameWithProps, measures, dimensions, cubeProperties, 0L);
    Assert.assertTrue(client.tableExists(derivedCubeNameWithProps));
    derivedTbl = client.getHiveTable(derivedCubeNameWithProps);
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
    client.createDerivedCube(cubeName, name, measures, dimensions, new HashMap<String, String>(), 0L);
    // Test alter cube
    Table cubeTbl = client.getHiveTable(name);
    DerivedCube toAlter = new DerivedCube(cubeTbl, (Cube) client.getCube(cubeName));
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

    CubeFactTable cubeFact = new CubeFactTable(cubeName, factName, factColumns, updatePeriods);

    // create cube fact
    client.createCubeFactTable(cubeName, factName, factColumns, updatePeriods, 0L, null, storageTables);
    Assert.assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, cubeName));
    Assert.assertEquals(client.getAllFactTables(client.getCube(cubeName)).get(0).getName(), factName.toLowerCase());
    Assert.assertEquals(client.getAllFactTables(client.getCube(derivedCubeName)).get(0).getName(),
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
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 2);
    List<Partition> parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert
        .assertEquals(TextInputFormat.class.getCanonicalName(), parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertFalse(parts.get(0).getCols().contains(newcol));
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));

    Map<String, Date> timeParts2 = new HashMap<String, Date>();
    timeParts2.put(TestCubeMetastoreClient.getDatePartitionKey(), nowPlus1);
    StoragePartitionDesc partSpec2 =
        new StoragePartitionDesc(cubeFact.getName(), timeParts2, null, UpdatePeriod.HOURLY);
    partSpec2.setInputFormat(SequenceFileInputFormat.class.getCanonicalName());
    partSpec2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    client.addPartition(partSpec2, c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 3);
    Assert.assertTrue(client.factPartitionExists(cubeFact.getName(), c1, UpdatePeriod.HOURLY, timeParts,
        new HashMap<String, String>()));
    Assert.assertTrue(client.factPartitionExists(cubeFact.getName(), c1, UpdatePeriod.HOURLY, timeParts2,
        new HashMap<String, String>()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(SequenceFileInputFormat.class.getCanonicalName(), parts.get(0).getInputFormatClass()
        .getCanonicalName());
    Assert.assertTrue(parts.get(0).getCols().contains(newcol));
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(nowPlus1));

    client.dropPartition(cubeFact.getName(), c1, timeParts2, null, UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 2);
    Assert.assertTrue(client.factPartitionExists(cubeFact.getName(), c1, UpdatePeriod.HOURLY, timeParts,
        new HashMap<String, String>()));
    Assert.assertFalse(client.factPartitionExists(cubeFact.getName(), c1, UpdatePeriod.HOURLY, timeParts2,
        new HashMap<String, String>()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert
        .assertEquals(TextInputFormat.class.getCanonicalName(), parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertFalse(parts.get(0).getCols().contains(newcol));
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));

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
    client.createCubeFactTable(cubeName, factName, factColumns, updatePeriods, 0L, null, storageTables);

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
    Assert.assertTrue(altered.getCubeName().equalsIgnoreCase(cubeName.toLowerCase()));
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
    List<CubeFactTable> cubeFacts = client.getAllFactTables(client.getCube(cubeName));
    List<String> cubeFactNames = new ArrayList<String>();
    for (CubeFactTable cfact : cubeFacts) {
      cubeFactNames.add(cfact.getName());
    }
    Assert.assertTrue(cubeFactNames.contains(factName.toLowerCase()));
    cubeFacts = client.getAllFactTables(client.getCube(derivedCubeName));
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

    CubeFactTable cubeFact = new CubeFactTable(cubeNameWithProps, factName, factColumns, updatePeriods);

    // create cube fact
    client.createCubeFactTable(cubeNameWithProps, factName, factColumns, updatePeriods, 0L, null, storageTables);

    Assert.assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, cubeNameWithProps));
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
    Assert.assertEquals(1, parts.size());
    Assert
        .assertEquals(TextInputFormat.class.getCanonicalName(), parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 3);
    parts = client.getPartitionsByFilter(storageTableName, testDtPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert
        .assertEquals(TextInputFormat.class.getCanonicalName(), parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(
        parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey(testDtPart.getName())),
        UpdatePeriod.HOURLY.format().format(testDt));

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

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    CubeFactTable cubeFact = new CubeFactTable(cubeNameWithProps, factName, factColumns, updatePeriods);

    // create cube fact
    client.createCubeFactTable(cubeNameWithProps, factName, factColumns, updatePeriods, 0L, null, storageTables);

    Assert.assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, cubeNameWithProps));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFact.equals(cubeFact2));

    // Assert for storage tables
    for (String entry : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getFactStorageTableName(factName, entry);
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    String storageTableName = MetastoreUtil.getFactStorageTableName(cubeFact.getName(), c1);

    // test partition
    Calendar cal = new GregorianCalendar();
    cal.setTime(now);
    cal.add(Calendar.HOUR, -1);
    Date nowMinus1 = cal.getTime();
    cal.add(Calendar.HOUR, -1);
    Date nowMinus2 = cal.getTime();

    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    timeParts.put(itPart.getName(), now);
    timeParts.put(etPart.getName(), now);
    StoragePartitionDesc partSpec = new StoragePartitionDesc(cubeFact.getName(), timeParts, null, UpdatePeriod.HOURLY);
    client.addPartition(partSpec, c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 4);

    Map<String, Date> timeParts2 = new HashMap<String, Date>();
    timeParts2.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    timeParts2.put(etPart.getName(), nowPlus1);
    Map<String, String> nonTimeSpec = new HashMap<String, String>();
    nonTimeSpec.put(itPart.getName(), "default");
    partSpec = new StoragePartitionDesc(cubeFact.getName(), timeParts2, nonTimeSpec, UpdatePeriod.HOURLY);
    client.addPartition(partSpec, c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 5);

    Map<String, Date> timeParts3 = new HashMap<String, Date>();
    timeParts3.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    timeParts3.put(etPart.getName(), now);
    partSpec = new StoragePartitionDesc(cubeFact.getName(), timeParts3, nonTimeSpec, UpdatePeriod.HOURLY);
    client.addPartition(partSpec, c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 6);

    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, itPart.getName()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, etPart.getName()));
    List<Partition> parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert
        .assertEquals(TextInputFormat.class.getCanonicalName(), parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(1), "default");
    Assert.assertEquals(parts.get(0).getValues().get(2), UpdatePeriod.HOURLY.format().format(now));
    parts = client.getPartitionsByFilter(storageTableName, itPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert
        .assertEquals(TextInputFormat.class.getCanonicalName(), parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey(itPart.getName())),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(0), UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(2), UpdatePeriod.HOURLY.format().format(now));
    parts = client.getPartitionsByFilter(storageTableName, etPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert
        .assertEquals(TextInputFormat.class.getCanonicalName(), parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey(etPart.getName())),
        UpdatePeriod.HOURLY.format().format(nowPlus1));
    Assert.assertEquals(parts.get(0).getValues().get(0), UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(1), "default");

    Map<String, Date> timeParts4 = new HashMap<String, Date>();
    timeParts4.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    timeParts4.put(itPart.getName(), nowPlus1);
    timeParts4.put(etPart.getName(), nowMinus1);
    partSpec = new StoragePartitionDesc(cubeFact.getName(), timeParts4, null, UpdatePeriod.HOURLY);
    client.addPartition(partSpec, c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 7);

    Map<String, Date> timeParts5 = new HashMap<String, Date>();
    timeParts5.put(TestCubeMetastoreClient.getDatePartitionKey(), nowPlus1);
    timeParts5.put(itPart.getName(), nowMinus1);
    timeParts5.put(etPart.getName(), nowMinus2);
    partSpec = new StoragePartitionDesc(cubeFact.getName(), timeParts5, null, UpdatePeriod.HOURLY);
    client.addPartition(partSpec, c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 8);

    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, itPart.getName()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, etPart.getName()));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(nowPlus1));
    Assert.assertEquals(parts.get(0).getValues().get(1), UpdatePeriod.HOURLY.format().format(nowMinus1));
    Assert.assertEquals(parts.get(0).getValues().get(2), UpdatePeriod.HOURLY.format().format(nowMinus2));

    parts = client.getPartitionsByFilter(storageTableName, itPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey(itPart.getName())),
        UpdatePeriod.HOURLY.format().format(nowPlus1));
    Assert.assertEquals(parts.get(0).getValues().get(0), UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(2), UpdatePeriod.HOURLY.format().format(nowMinus1));
    parts = client.getPartitionsByFilter(storageTableName, etPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey(etPart.getName())),
        UpdatePeriod.HOURLY.format().format(nowPlus1));
    Assert.assertEquals(parts.get(0).getValues().get(0), UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(1), "default");

    client.dropPartition(cubeFact.getName(), c1, timeParts, null, UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 7);
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, itPart.getName()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, etPart.getName()));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(nowPlus1));
    Assert.assertEquals(parts.get(0).getValues().get(1), UpdatePeriod.HOURLY.format().format(nowMinus1));
    Assert.assertEquals(parts.get(0).getValues().get(2), UpdatePeriod.HOURLY.format().format(nowMinus2));

    parts = client.getPartitionsByFilter(storageTableName, itPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey(itPart.getName())),
        UpdatePeriod.HOURLY.format().format(nowPlus1));
    Assert.assertEquals(parts.get(0).getValues().get(0), UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(2), UpdatePeriod.HOURLY.format().format(nowMinus1));
    parts = client.getPartitionsByFilter(storageTableName, etPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey(etPart.getName())),
        UpdatePeriod.HOURLY.format().format(nowPlus1));
    Assert.assertEquals(parts.get(0).getValues().get(0), UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(1), "default");

    client.dropPartition(cubeFact.getName(), c1, timeParts2, nonTimeSpec, UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 6);
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, itPart.getName()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, etPart.getName()));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(nowPlus1));
    Assert.assertEquals(parts.get(0).getValues().get(1), UpdatePeriod.HOURLY.format().format(nowMinus1));
    Assert.assertEquals(parts.get(0).getValues().get(2), UpdatePeriod.HOURLY.format().format(nowMinus2));

    parts = client.getPartitionsByFilter(storageTableName, itPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey(itPart.getName())),
        UpdatePeriod.HOURLY.format().format(nowPlus1));
    Assert.assertEquals(parts.get(0).getValues().get(0), UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(2), UpdatePeriod.HOURLY.format().format(nowMinus1));

    parts = client.getPartitionsByFilter(storageTableName, etPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey(etPart.getName())),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(0), UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(1), "default");

    client.dropPartition(cubeFact.getName(), c1, timeParts4, null, UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 5);
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, itPart.getName()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, etPart.getName()));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(nowPlus1));
    Assert.assertEquals(parts.get(0).getValues().get(1), UpdatePeriod.HOURLY.format().format(nowMinus1));
    Assert.assertEquals(parts.get(0).getValues().get(2), UpdatePeriod.HOURLY.format().format(nowMinus2));

    parts = client.getPartitionsByFilter(storageTableName, itPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey(itPart.getName())),
        UpdatePeriod.HOURLY.format().format(nowMinus1));
    Assert.assertEquals(parts.get(0).getValues().get(0), UpdatePeriod.HOURLY.format().format(nowPlus1));
    Assert.assertEquals(parts.get(0).getValues().get(2), UpdatePeriod.HOURLY.format().format(nowMinus2));

    parts = client.getPartitionsByFilter(storageTableName, etPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey(etPart.getName())),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(0), UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(1), "default");

    client.dropPartition(cubeFact.getName(), c1, timeParts5, null, UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 3);
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertFalse(client.latestPartitionExists(cubeFact.getName(), c1, itPart.getName()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1, etPart.getName()));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(1), "default");
    Assert.assertEquals(parts.get(0).getValues().get(2), UpdatePeriod.HOURLY.format().format(now));

    parts = client.getPartitionsByFilter(storageTableName, itPart.getName() + "='latest'");
    Assert.assertEquals(0, parts.size());

    parts = client.getPartitionsByFilter(storageTableName, etPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey(etPart.getName())),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(0), UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(1), "default");

    client.dropPartition(cubeFact.getName(), c1, timeParts3, nonTimeSpec, UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 0);
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

    CubeFactTable cubeFact = new CubeFactTable(cubeName, factName, factColumns, updatePeriods, 100L);

    // create cube fact
    client.createCubeFactTable(cubeName, factName, factColumns, updatePeriods, 100L, null, storageTables);

    Assert.assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, cubeName));
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
    Assert.assertEquals(1, parts.size());
    Assert
        .assertEquals(TextInputFormat.class.getCanonicalName(), parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 2);

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
    cubeNames.add(cubeName);

    CubeFactTable cubeFactWithParts = new CubeFactTable(cubeName, factNameWithPart, factColumns, updatePeriods);

    // create cube fact
    client.createCubeFactTable(cubeName, factNameWithPart, factColumns, updatePeriods, 0L, null, storageTables);

    Assert.assertTrue(client.tableExists(factNameWithPart));
    Table cubeTbl = client.getHiveTable(factNameWithPart);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, cubeName));
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
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 2);
    List<Partition> parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert
        .assertEquals(TextInputFormat.class.getCanonicalName(), parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));

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

    CubeFactTable cubeFactWithParts = new CubeFactTable(cubeName, factNameWithPart, factColumns, updatePeriods);

    // create cube fact
    client.createCubeFactTable(cubeName, factNameWithPart, factColumns, updatePeriods, 0L, null, storageTables);

    Assert.assertTrue(client.tableExists(factNameWithPart));
    Table cubeTbl = client.getHiveTable(factNameWithPart);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, cubeName));
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
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 3);
    List<Partition> parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert
        .assertEquals(TextInputFormat.class.getCanonicalName(), parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));
    parts = client.getPartitionsByFilter(storageTableName, testDtPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert
        .assertEquals(TextInputFormat.class.getCanonicalName(), parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(
        parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey(testDtPart.getName())),
        UpdatePeriod.HOURLY.format().format(testDt));

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

    CubeFactTable cubeFactWithTwoStorages = new CubeFactTable(cubeName, factName, factColumns, updatePeriods);

    client.createCubeFactTable(cubeName, factName, factColumns, updatePeriods, 0L, null, storageTables);

    Assert.assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, cubeName));
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
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 2);
    List<Partition> parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(SequenceFileInputFormat.class.getCanonicalName(), parts.get(0).getInputFormatClass()
        .getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));

    StoragePartitionDesc sPartSpec2 =
        new StoragePartitionDesc(cubeFactWithTwoStorages.getName(), timeParts, null, UpdatePeriod.HOURLY);
    client.addPartition(sPartSpec2, c2);
    Assert.assertTrue(client.factPartitionExists(cubeFactWithTwoStorages.getName(), c2, UpdatePeriod.HOURLY, timeParts,
        new HashMap<String, String>()));
    Assert.assertTrue(client.latestPartitionExists(cubeFactWithTwoStorages.getName(), c2,
        TestCubeMetastoreClient.getDatePartitionKey()));
    String storageTableName2 = MetastoreUtil.getFactStorageTableName(cubeFactWithTwoStorages.getName(), c2);
    Assert.assertEquals(client.getAllParts(storageTableName2).size(), 2);
    parts = client.getPartitionsByFilter(storageTableName2, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert
        .assertEquals(TextInputFormat.class.getCanonicalName(), parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));

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
    client.createCube("testcache2", cubeMeasures, cubeDimensions, cubeProperties);
    client.createDerivedCube("testcache1", "derived1", measures, dimensions, new HashMap<String, String>(), 0L);
    client.createDerivedCube("testcache2", "derived2", measures, dimensions, cubeProperties, 0L);
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
    client.createCube("testcache4", cubeMeasures, cubeDimensions, cubeProperties);
    client.createDerivedCube("testcache3", "derived3", measures, dimensions, new HashMap<String, String>(), 0L);
    client.createDerivedCube("testcache4", "derived4", measures, dimensions, cubeProperties, 0L);
    Assert.assertNotNull(client.getCube("testcache3"));
    Assert.assertNotNull(client2.getCube("testcache3"));
    Assert.assertEquals(13, client.getAllCubes().size());
    Assert.assertEquals(13, client2.getAllCubes().size());
    conf.setBoolean(MetastoreConstants.METASTORE_ENABLE_CACHING, true);
    client = CubeMetastoreClient.getInstance(conf);
  }
}

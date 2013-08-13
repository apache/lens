package com.inmobi.yoda.cube.ddl;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.cube.metadata.Cube;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.MetastoreUtil;
import org.apache.hadoop.hive.ql.cube.metadata.Storage;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestDDL {

  HiveConf conf = new HiveConf(this.getClass());

  DimensionDDL dimDDL;

  @BeforeTest
  public void setup()
      throws AlreadyExistsException, HiveException, IOException {
    Hive client = Hive.get(conf);
    Database database = new Database();
    database.setName(TestDDL.class.getSimpleName());
    client.createDatabase(database);
    client.setCurrentDatabase(TestDDL.class.getSimpleName());    
    dimDDL = new DimensionDDL(conf);
  }

  @AfterTest
  public void tearDown() throws HiveException, NoSuchObjectException {
    Hive client = Hive.get(conf);
    client.dropDatabase(TestDDL.class.getSimpleName(), true, true,
        true);
  }

  @Test
  public void testAllDimensions() throws HiveException, IOException {
    dimDDL.createAllDimensions();
    CubeMetastoreClient cc =  CubeMetastoreClient.getInstance(conf);
    List<String> dimTables = new ArrayList<String>();
    for (CubeDimensionTable dim : cc.getAllDimensionTables()) {
      dimTables.add(dim.getName());
      Assert.assertTrue(dim.hasStorageSnapshots(CubeDDL.YODA_STORAGE));
    }
    System.out.println("Dimension tables :" + dimTables);
    Assert.assertEquals(dimTables.size(), 101);
    // assert for some random dimension table names
    Assert.assertTrue(dimTables.contains("wap_ad_m3"));
    Assert.assertTrue(dimTables.contains("wap_site"));
    Assert.assertTrue(dimTables.contains("handset_device_metadata_m3"));
    Assert.assertTrue(dimTables.contains("site_tags"));
    Assert.assertTrue(dimTables.contains("campaign"));
    // network_object is not a dimension table
    Assert.assertFalse(dimTables.contains("network_object"));
  }

  @Test
  public void testAllCubes() throws HiveException, IOException {
    CubeDDL cubeDDL = new CubeDDL(dimDDL, conf);
    cubeDDL.createAllCubes();
    CubeMetastoreClient cc =  CubeMetastoreClient.getInstance(conf);
    List<String> cubes = cc.getAllCubeNames();
    // assert for some random cube table names
    Assert.assertTrue(cubes.contains("cube_request"));
    Assert.assertTrue(cubes.contains("cube_campaign"));
    Assert.assertTrue(cubes.contains("cube_user"));
    Assert.assertTrue(cubes.contains("cube_userappdistribution"));
    // campaign is not a cube table name
    Assert.assertFalse(cubes.contains("campaign"));
    List<String> cubesWithPISStorage = Arrays.asList("cube_request", "cube_click",
        "cube_impression", "cube_rrcube");

    Assert.assertEquals(cc.getAllCubeNames().size(), 14);
    for (Cube cube : cc.getAllCubes()) {
      Assert.assertFalse(cube.getDimensions().isEmpty());
      Assert.assertFalse(cube.getMeasures().isEmpty());
      Assert.assertFalse(cube.getTimedDimensions().isEmpty());

      List<CubeFactTable> facts = cc.getAllFactTables(cube);
      Assert.assertFalse(facts.isEmpty());
      for (CubeFactTable fact : facts) {
        Assert.assertTrue(fact.getStorages().contains(CubeDDL.YODA_STORAGE));
        Assert.assertNotNull(fact.getProperties());
        Assert.assertEquals(cube.getName(), fact.getCubeName());
        Assert.assertEquals(fact.getColumns().size(),
            CubeDDL.getNobColList().size());
        // Assert.assertEquals(fact.getColumns(), CubeDDL.getNobColList());
        if (fact.getName().contains(CubeDDL.RAW_FACT_NAME)) {
          Assert.assertNull(fact.getValidColumns()); 
        } else {
          Assert.assertNotNull(fact.getValidColumns());             
        }
        if (cubesWithPISStorage.contains(cube.getName())) {
          Assert.assertTrue(fact.getStorages().contains(CubeDDL.YODA_PIE_STORAGE));
        }
      }
    }
  }

  @Test
  public void testDimPartitions() throws HiveException, IOException {
    Calendar cal = Calendar.getInstance();
    Date now = cal.getTime();
    PopulatePartitions pp = new PopulatePartitions(conf);
    pp.populateAllDimParts(new Path("file:///tmp/hive/warehouse/parts/metadata"),
        new SimpleDateFormat(UpdatePeriod.HOURLY.format()),
        now, false);
    CubeMetastoreClient cc =  CubeMetastoreClient.getInstance(conf);
    for (String dimName : cc.getAllDimensionTableNames()) {
      String storageTableName = MetastoreUtil.getDimStorageTableName(dimName,
          Storage.getPrefix(CubeDDL.YODA_STORAGE));
      Assert.assertTrue(cc.partitionExists(storageTableName,
          UpdatePeriod.HOURLY, cal.getTime()));
    }
  }

  @Test
  public void testPartitions() throws HiveException, IOException,
      ParseException, MetaException, NoSuchObjectException, TException {
    Calendar cal = Calendar.getInstance();
    Date now = cal.getTime();
    cal.add(Calendar.DAY_OF_MONTH, -2);
    Date before = cal.getTime();
    PopulatePartitions pp = new PopulatePartitions(conf);
    SimpleDateFormat format = new SimpleDateFormat(UpdatePeriod.DAILY.format());
    pp.populateCubeParts("request", before, now,
        UpdatePeriod.DAILY, new Path("file:////tmp/hive/warehouse/parts"),
        format, "all", false);

    CubeMetastoreClient cc =  CubeMetastoreClient.getInstance(conf);
    String storageTableName1 = MetastoreUtil.getFactStorageTableName(
        "request_summary1", Storage.getPrefix(CubeDDL.YODA_STORAGE));
    String storageTableName2 = MetastoreUtil.getFactStorageTableName(
        "request_summary2", Storage.getPrefix(CubeDDL.YODA_STORAGE));
    String storageTableName3 = MetastoreUtil.getFactStorageTableName(
        "request_summary3", Storage.getPrefix(CubeDDL.YODA_STORAGE));
    String storageTableName4 = MetastoreUtil.getFactStorageTableName(
        "request_raw", Storage.getPrefix(CubeDDL.YODA_STORAGE));

    cal.setTime(before);
    StringBuilder filter = new StringBuilder();
    filter.append(CubeDDL.PART_KEY_IT).append("=").append("'")
    .append(format.format(cal.getTime())).append("'");
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName1,
        filter.toString()));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName2,
        filter.toString()));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName3,
        filter.toString()));
    Assert.assertFalse(cc.partitionExistsByFilter(storageTableName4,
        filter.toString()));
    cal.add(Calendar.DAY_OF_MONTH, 1);
    filter = new StringBuilder();
    filter.append(CubeDDL.PART_KEY_IT).append("=").append("'")
    .append(format.format(cal.getTime())).append("'");
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName1,
        filter.toString()));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName2,
        filter.toString()));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName3,
        filter.toString()));
    Assert.assertFalse(cc.partitionExistsByFilter(storageTableName4,
        filter.toString()));
    cal.add(Calendar.DAY_OF_MONTH, 1);
    filter = new StringBuilder();
    filter.append(CubeDDL.PART_KEY_IT).append("=").append("'")
    .append(format.format(cal.getTime())).append("'");
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName1,
        filter.toString()));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName2,
        filter.toString()));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName3,
        filter.toString()));
    Assert.assertFalse(cc.partitionExistsByFilter(storageTableName4,
        filter.toString()));
  }

  @Test
  public void testHourlyPartitions() throws HiveException, IOException,
      ParseException, MetaException, NoSuchObjectException, TException {
    Calendar cal = Calendar.getInstance();
    Date now = cal.getTime();
    cal.add(Calendar.HOUR_OF_DAY, -2);
    Date before = cal.getTime();
    PopulatePartitions pp = new PopulatePartitions(conf);
    SimpleDateFormat format = new SimpleDateFormat(UpdatePeriod.HOURLY.format());
    pp.populateCubeParts("request", before, now,
        UpdatePeriod.HOURLY, new Path("file:////tmp/hive/warehouse/parts"),
        format, "all", false);
    CubeMetastoreClient cc =  CubeMetastoreClient.getInstance(conf);
    String storageTableName1 = MetastoreUtil.getFactStorageTableName(
        "request_summary1", Storage.getPrefix(CubeDDL.YODA_STORAGE));
    String storageTableName2 = MetastoreUtil.getFactStorageTableName(
        "request_summary2", Storage.getPrefix(CubeDDL.YODA_STORAGE));
    String storageTableName3 = MetastoreUtil.getFactStorageTableName(
        "request_summary3", Storage.getPrefix(CubeDDL.YODA_STORAGE));
    String storageTableName4 = MetastoreUtil.getFactStorageTableName(
        "request_raw", Storage.getPrefix(CubeDDL.YODA_STORAGE));

    cal.setTime(before);
    StringBuilder filter = new StringBuilder();
    filter.append(CubeDDL.PART_KEY_IT).append("=").append("'")
    .append(format.format(cal.getTime())).append("'");
    System.out.println("filter:" + filter);
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName1,
        filter.toString()));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName2,
        filter.toString()));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName3,
        filter.toString()));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName4,
        filter.toString()));
    cal.add(Calendar.HOUR_OF_DAY, 1);
    filter = new StringBuilder();
    filter.append(CubeDDL.PART_KEY_IT).append("=").append("'")
    .append(format.format(cal.getTime())).append("'");
    System.out.println("filter:" + filter);
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName1,
        filter.toString()));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName2,
        filter.toString()));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName3,
        filter.toString()));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName4,
        filter.toString()));
    cal.add(Calendar.HOUR_OF_DAY, 1);
    filter = new StringBuilder();
    filter.append(CubeDDL.PART_KEY_IT).append("=").append("'")
    .append(format.format(cal.getTime())).append("'");
    System.out.println("filter:" + filter);
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName1,
        filter.toString()));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName2,
        filter.toString()));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName3,
        filter.toString()));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName4,
        filter.toString()));
  }
}

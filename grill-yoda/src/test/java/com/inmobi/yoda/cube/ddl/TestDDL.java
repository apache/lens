package com.inmobi.yoda.cube.ddl;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.cube.metadata.*;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.thrift.TException;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.grill.api.GrillConfUtil;

public class TestDDL {

  HiveConf conf = new HiveConf(this.getClass());

  DimensionDDL dimDDL;

  @BeforeTest
  public void setup()
      throws AlreadyExistsException, HiveException, IOException {
    SessionState.start(conf);
    Hive client = Hive.get(conf);
    Database database = new Database();
    database.setName(TestDDL.class.getSimpleName());
    client.createDatabase(database);
    SessionState.get().setCurrentDatabase(TestDDL.class.getSimpleName());
    dimDDL = new DimensionDDL(conf);
    System.out.println("##setup testDDL");
  }

  @AfterTest
  public void tearDown() throws HiveException, NoSuchObjectException {
    Hive client = Hive.get(conf);
    client.dropDatabase(TestDDL.class.getSimpleName(), true, true,
        true);
    System.out.println("##teardown testDDL");
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
    Assert.assertEquals(dimTables.size(), 125);
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
  public void testAllCubes() throws HiveException, IOException, ParseException {
    CubeDDL cubeDDL = new CubeDDL(dimDDL, conf);
    cubeDDL.createAllCubes();
    CubeMetastoreClient cc =  CubeMetastoreClient.getInstance(conf);
    cc.setCurrentDatabase(TestDDL.class.getSimpleName());
    List<String> cubes = new ArrayList<String>(cc.getAllCubes().size());
    for (Cube cube : cc.getAllCubes()) {
      cubes.add(cube.getName());
    }
    // assert for some random cube table names
    Assert.assertTrue(cubes.contains("cube_request"));
    Assert.assertTrue(cubes.contains("cube_campaign"));
    Assert.assertTrue(cubes.contains("cube_user"));
    Assert.assertTrue(cubes.contains("cube_userappdistribution"));
    // campaign is not a cube table name
    Assert.assertFalse(cubes.contains("campaign"));
    List<String> factsWithPIEStorage = Arrays.asList("summary1", "summary2",
        "summary3", "cube_request_raw", "cube_impression_raw", "cube_click_raw");

    Assert.assertEquals(18, cc.getAllCubes().size());
    for (Cube cube : cc.getAllCubes()) {
      Assert.assertFalse(cube.getDimensions().isEmpty());
      Assert.assertFalse(cube.getMeasures().isEmpty());
      Assert.assertFalse(cube.getTimedDimensions().isEmpty());
      Assert.assertFalse(cc.getAllFactTables(cube).isEmpty());
    }

    CubeDimension dim = cc.getCube("cube_downloadmatch").getDimensionByName("dl_carrier_region_id");
    Assert.assertNotNull(dim);
    Assert.assertNotNull(dim.getStartTime());
    SimpleDateFormat format  = new SimpleDateFormat("yyyy-MM-dd-HH");
    Assert.assertEquals(format.format(dim.getStartTime()),
        format.format(CubeDDL.dateFormatter.parseDateTime("2012-09-25-00").toDate()));

    dim = cc.getCube("cube_request").getDimensionByName("rq_geo_type");
    Assert.assertNotNull(dim);
    Assert.assertNotNull(dim.getStartTime());
    Assert.assertEquals(format.format(dim.getStartTime()),
        format.format(CubeDDL.dateFormatter.parseDateTime("2013-01-28-10").toDate()));

    List<CubeFactTable> facts = cc.getAllFacts();
    Assert.assertEquals(24, facts.size());
    System.out.println("All Facts:" + facts);
    for (CubeFactTable fact : facts) {
      Assert.assertTrue(fact.getStorages().contains(CubeDDL.YODA_STORAGE));
      Assert.assertNotNull(fact.getProperties());
      if (fact.getName().equals("summary1") ||
          fact.getName().equals("summary2") ||
          fact.getName().equals("cube_request_raw") ||
          fact.getName().equals("cube_impression_raw") ||
          fact.getName().equals("cube_click_raw") ||
          fact.getName().equals("summary3")) {
        Assert.assertTrue(fact.getCubeNames().contains("cube_request"));
        Assert.assertTrue(fact.getCubeNames().contains("cube_impression"));
        Assert.assertTrue(fact.getCubeNames().contains("cube_click"));
      } else if (fact.getName().equals("retargetsummary1"))  {
        Assert.assertTrue(fact.getCubeNames().contains("cube_appownerretarget"));
        Assert.assertTrue(fact.getCubeNames().contains("cube_clickimpretarget"));
      } else {
        System.out.println("fact " + fact.getName() + " Cubes:" + fact.getCubeNames());
        Assert.assertEquals(fact.getCubeNames().size(), 1);
      }
      Assert.assertEquals(fact.getColumns().size(),
          CubeDDL.getNobColList().size());
      // Assert.assertEquals(fact.getColumns(), CubeDDL.getNobColList());
      if (fact.getName().contains(CubeDDL.RAW_FACT_NAME)) {
        Assert.assertNull(fact.getValidColumns()); 
        Assert.assertFalse(fact.isAggregated());
      } else {
        Assert.assertNotNull(fact.getValidColumns());             
        Assert.assertTrue(fact.isAggregated());
      }
      if (factsWithPIEStorage.contains(fact.getName())) {
        Assert.assertTrue(fact.getStorages().contains(CubeDDL.YODA_PIE_STORAGE));
      } else {
        System.out.println("fact:" + fact.getName() + "storages:" + fact.getStorages());
        Assert.assertFalse(fact.getStorages().contains(CubeDDL.YODA_PIE_STORAGE));
      }
      // storage cost validation
      for (String storage : fact.getStorages()) {
        String tableName = MetastoreUtil.getStorageTableName(
            fact.getName(), Storage.getPrefix(storage));
        Table tbl = cc.getHiveTable(tableName);
        Assert.assertEquals(Double.toString(fact.weight()),
            tbl.getParameters().get(GrillConfUtil.STORAGE_COST));
      }
    }

  }

  @Test
  public void testDimPartitions() throws HiveException, IOException {
    Calendar cal = Calendar.getInstance();
    Date now = cal.getTime();
    PopulatePartitions pp = new PopulatePartitions(conf);
    pp.populateAllDimParts(new Path("file:///tmp/hive/warehouse/parts/metadata"),
        UpdatePeriod.HOURLY.format(),
        now, false);
    CubeMetastoreClient cc =  CubeMetastoreClient.getInstance(conf);
    for (CubeDimensionTable dim : cc.getAllDimensionTables()) {
      String dimName = dim.getName();
      String storageTableName = MetastoreUtil.getDimStorageTableName(dimName,
          Storage.getPrefix(CubeDDL.YODA_STORAGE));
      Map<String, Date> timeParts = new HashMap<String, Date>();
      timeParts.put(DimensionDDL.dim_time_part_column, cal.getTime());
      Assert.assertTrue(cc.partitionExists(storageTableName,
          UpdatePeriod.HOURLY, timeParts));
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
    DateFormat format = UpdatePeriod.DAILY.format();
    pp.populateCubeParts("request", before, now,
        UpdatePeriod.DAILY, new Path("file:////tmp/hive/warehouse/parts"),
        format, "all", false);

    CubeMetastoreClient cc =  CubeMetastoreClient.getInstance(conf);
    String rawFactName = null;
    for (CubeFactTable fact : cc.getAllFactTables(cc.getCube("cube_request"))) {
      if (fact.getName().endsWith(CubeDDL.RAW_FACT_NAME)) {
        rawFactName = fact.getName();
      }
    }
    String storageTableName1 = MetastoreUtil.getFactStorageTableName(
        "summary1", Storage.getPrefix(CubeDDL.YODA_STORAGE));
    String storageTableName2 = MetastoreUtil.getFactStorageTableName(
        "summary2", Storage.getPrefix(CubeDDL.YODA_STORAGE));
    String storageTableName3 = MetastoreUtil.getFactStorageTableName(
        "summary3", Storage.getPrefix(CubeDDL.YODA_STORAGE));
    String storageTableName4 = MetastoreUtil.getFactStorageTableName(
        rawFactName, Storage.getPrefix(CubeDDL.YODA_STORAGE));

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
    String lastPart = format.format(cal.getTime());
    filter.append(CubeDDL.PART_KEY_IT).append("=").append("'")
    .append(lastPart).append("'");
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName1,
        filter.toString()));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName2,
        filter.toString()));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName3,
        filter.toString()));
    Assert.assertFalse(cc.partitionExistsByFilter(storageTableName4,
        filter.toString()));

    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName1,
        StorageConstants.getLatestPartFilter(CubeDDL.PART_KEY_IT)));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName2,
        StorageConstants.getLatestPartFilter(CubeDDL.PART_KEY_IT)));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName3,
        StorageConstants.getLatestPartFilter(CubeDDL.PART_KEY_IT)));

    List<Partition> latestParts = cc.getPartitionsByFilter(storageTableName1,
        StorageConstants.getLatestPartFilter(CubeDDL.PART_KEY_IT));
    Assert.assertEquals(latestParts.size(), 1);
  }

  @Test
  public void testHourlyPartitions() throws HiveException, IOException,
  ParseException, MetaException, NoSuchObjectException, TException {
    Calendar cal = Calendar.getInstance();
    Date now = cal.getTime();
    cal.add(Calendar.HOUR_OF_DAY, -2);
    Date before = cal.getTime();
    PopulatePartitions pp = new PopulatePartitions(conf);
    DateFormat format = UpdatePeriod.HOURLY.format();
    pp.populateCubeParts("request", before, now,
        UpdatePeriod.HOURLY, new Path("file:////tmp/hive/warehouse/parts"),
        format, "all", false);
    CubeMetastoreClient cc =  CubeMetastoreClient.getInstance(conf);
    String rawFactName = null;
    for (CubeFactTable fact : cc.getAllFactTables(cc.getCube("cube_request"))) {
      if (fact.getName().endsWith(CubeDDL.RAW_FACT_NAME)) {
        rawFactName = fact.getName();
      }
    }
    String storageTableName1 = MetastoreUtil.getFactStorageTableName(
        "summary1", Storage.getPrefix(CubeDDL.YODA_STORAGE));
    String storageTableName2 = MetastoreUtil.getFactStorageTableName(
        "summary2", Storage.getPrefix(CubeDDL.YODA_STORAGE));
    String storageTableName3 = MetastoreUtil.getFactStorageTableName(
        "summary3", Storage.getPrefix(CubeDDL.YODA_STORAGE));
    String storageTableName4 = MetastoreUtil.getFactStorageTableName(
        rawFactName, Storage.getPrefix(CubeDDL.YODA_STORAGE));

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
    String lastPart = format.format(cal.getTime());
    filter.append(CubeDDL.PART_KEY_IT).append("=").append("'")
    .append(lastPart).append("'");
    System.out.println("filter:" + filter);
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName1,
        filter.toString()));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName2,
        filter.toString()));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName3,
        filter.toString()));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName4,
        filter.toString()));

    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName1,
        StorageConstants.getLatestPartFilter(CubeDDL.PART_KEY_IT)));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName2,
        StorageConstants.getLatestPartFilter(CubeDDL.PART_KEY_IT)));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName3,
        StorageConstants.getLatestPartFilter(CubeDDL.PART_KEY_IT)));
    Assert.assertTrue(cc.partitionExistsByFilter(storageTableName4,
        StorageConstants.getLatestPartFilter(CubeDDL.PART_KEY_IT)));

    List<Partition> latestParts = cc.getPartitionsByFilter(storageTableName1,
        StorageConstants.getLatestPartFilter(CubeDDL.PART_KEY_IT));
    Assert.assertEquals(latestParts.size(), 1);
  }
}

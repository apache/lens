package com.inmobi.yoda.cube.ddl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestDimensionDDL {
  HiveConf conf = new HiveConf(this.getClass());

  @BeforeTest
  public void setup() throws AlreadyExistsException, HiveException {
    Hive client = Hive.get(conf);
    Database database = new Database();
    database.setName(TestDimensionDDL.class.getSimpleName());
    client.createDatabase(database);
    client.setCurrentDatabase(TestDimensionDDL.class.getSimpleName());    
  }

  @AfterTest
  public void tearDown() throws HiveException, NoSuchObjectException {
    Hive client = Hive.get(conf);
    client.dropDatabase(TestDimensionDDL.class.getSimpleName(), true, true,
        true);
  }

  @Test
  public void testAllDimensions() throws HiveException, IOException {
    DimensionDDL dimDDL = new DimensionDDL(conf);
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
    // network_object is not a dimension table
    Assert.assertFalse(dimTables.contains("network_object"));
  }
}

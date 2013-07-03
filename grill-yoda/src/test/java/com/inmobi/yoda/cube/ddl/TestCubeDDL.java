package com.inmobi.yoda.cube.ddl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.cube.metadata.Cube;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestCubeDDL {

  HiveConf conf = new HiveConf(this.getClass());

  DimensionDDL dimDDL;
  @BeforeTest
  public void setup() throws AlreadyExistsException, HiveException, IOException {
    Hive client = Hive.get(conf);
    Database database = new Database();
    database.setName(TestCubeDDL.class.getSimpleName());
    client.createDatabase(database);
    client.setCurrentDatabase(TestCubeDDL.class.getSimpleName());    
    dimDDL = new DimensionDDL(conf);
  }

  @AfterTest
  public void tearDown() throws HiveException, NoSuchObjectException {
    Hive client = Hive.get(conf);
    client.dropDatabase(TestCubeDDL.class.getSimpleName(), true, true,
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

    Assert.assertEquals(cc.getAllCubeNames().size(), 14);
    for (Cube cube : cc.getAllCubes()) {
      Assert.assertFalse(cube.getDimensions().isEmpty());
      Assert.assertFalse(cube.getMeasures().isEmpty());
      Assert.assertFalse(cube.getTimedDimensions().isEmpty());

      List<CubeFactTable> facts = cc.getAllFactTables(cube);
      if (cube.getName().equals("cube_downloadmatch")) {
        Assert.assertTrue(facts.isEmpty());
      } else {
        Assert.assertFalse(facts.isEmpty());
        for (CubeFactTable fact : facts) {
          Assert.assertTrue(fact.getStorages().contains(CubeDDL.YODA_STORAGE));
          Assert.assertNotNull(fact.getProperties());
          Assert.assertEquals(cube.getName(), fact.getCubeName());
          Assert.assertEquals(fact.getColumns().size(),
              CubeDDL.getNobColList().size());
          // Assert.assertEquals(fact.getColumns(), CubeDDL.getNobColList());
          Assert.assertNotNull(fact.getValidColumns()); 
        }
      }
    }
  }
}

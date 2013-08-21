package com.inmobi.yoda.cube.ddl;


import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.cube.metadata.Cube;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.*;

public class TestCubeSelectorService {
  private DimensionDDL dimDDL;
  private HiveConf conf;
  private CubeMetastoreClient metastore;

  @BeforeTest
  public void setup() throws Exception {
    conf = new HiveConf(TestCubeSelectorService.class);
    Hive client = Hive.get(conf);
    Database database = new Database();
    database.setName(TestDDL.class.getSimpleName());
    client.createDatabase(database);
    client.setCurrentDatabase(TestDDL.class.getSimpleName());
    dimDDL = new DimensionDDL(conf);
    CubeDDL cubeDDL = new CubeDDL(dimDDL, conf);
    cubeDDL.createAllCubes();
    metastore = CubeMetastoreClient.getInstance(conf);
  }

  @AfterTest
  public void tearDown() throws Exception {
    Hive.get(conf).dropDatabase(TestDDL.class.getSimpleName(), true, true,
      true);
  }

  @Test
  public void testSelectCube() throws Exception  {
    String columns[] = {"bl_billedcount", "dl_joined_count"};

    CubeSelectorService selector = CubeSelectorFactory.getSelectorSvcInstance(conf);

    Map<Cube, List<String>> selected = selector.selectCubes(Arrays.asList(columns));

    assertNotNull(selected);
    assertEquals(selected.size(), 3);

    Cube click = metastore.getCube("cube_click");
    Cube dlUnMatch = metastore.getCube("cube_downloadunmatch");
    Cube dlMatch = metastore.getCube("cube_downloadmatch");

    assertEquals(selected.keySet(), new HashSet<Cube>(Arrays.asList(click, dlMatch, dlUnMatch)));
    assertEquals(selected.get(click), Arrays.asList("bl_billedcount"));
    assertEquals(selected.get(dlUnMatch), Arrays.asList("dl_joined_count"));
    assertEquals(selected.get(dlMatch), Arrays.asList("dl_joined_count"));
  }

  @Test
  public void testMinCostSelect() throws Exception {
    CubeSelectorService selector = CubeSelectorFactory.getSelectorSvcInstance(conf);
    Map<Cube, List<String>> selected = selector.selectCubes(Arrays.asList("rq_pgreq"));
    System.out.println("## " + selected.toString());
    assertNotNull(selected);
    assertEquals(selected.size(), 1);
  }

}

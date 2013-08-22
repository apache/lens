package com.inmobi.yoda.cube.ddl;


import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.cube.metadata.*;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.mapred.TextInputFormat;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.*;

import static org.testng.Assert.*;

public class TestCubeSelectorService {
  public static final String TEST_DB = "test_cube_selector_db";
  private DimensionDDL dimDDL;
  private HiveConf conf;
  private CubeMetastoreClient metastore;

  private void createTestDim(CubeMetastoreClient client)
    throws HiveException {
    String dimName = "testDim";

    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("testdim_name", "string", "field1"));

    Map<String, List<TableReference>> dimensionReferences =
      new HashMap<String, List<TableReference>>();

    Storage hdfsStorage1 = new HDFSStorage("C1",
      TextInputFormat.class.getCanonicalName(),
      HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    Storage hdfsStorage2 = new HDFSStorage("C2",
      TextInputFormat.class.getCanonicalName(),
      HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    Map<Storage, UpdatePeriod> snapshotDumpPeriods =
      new HashMap<Storage, UpdatePeriod>();
    snapshotDumpPeriods.put(hdfsStorage1, UpdatePeriod.HOURLY);
    snapshotDumpPeriods.put(hdfsStorage2, null);

    client.createCubeDimensionTable(dimName, dimColumns, 0L,
      dimensionReferences, snapshotDumpPeriods, null);
  }

  @BeforeTest
  public void setup() throws Exception {
    conf = new HiveConf(TestCubeSelectorService.class);
    Hive client = Hive.get(conf);
    Database database = new Database();
    database.setName(TEST_DB);
    client.createDatabase(database);

    client.setCurrentDatabase(TEST_DB);
    metastore = CubeMetastoreClient.getInstance(conf);
    metastore.setCurrentDatabase(TEST_DB);

    dimDDL = new DimensionDDL(conf);
    CubeDDL cubeDDL = new CubeDDL(dimDDL, conf);
    cubeDDL.createAllCubes();
    createTestDim(metastore);
    System.out.println("##setup test cubeselector service");
  }

  @AfterTest
  public void tearDown() throws Exception {
    Hive.get(conf).dropDatabase(TEST_DB, true, true,
      true);
    System.out.println("##teardown cubeselector service");
  }

  @Test
  public void testSelectCube() throws Exception  {
    String columns[] = {"bl_billedcount", "dl_joined_count"};

    CubeSelectorService selector = CubeSelectorFactory.getSelectorSvcInstance(conf);

    Map<List<String>, List<AbstractCubeTable>> selected = selector.select(Arrays.asList(columns));

    assertNotNull(selected);
    assertEquals(selected.size(), 2);

    Cube click = metastore.getCube("cube_click");
    Cube dlUnMatch = metastore.getCube("cube_downloadunmatch");
    Cube dlMatch = metastore.getCube("cube_downloadmatch");

    System.out.println("## result " + selected.toString());
    List<AbstractCubeTable> clickList = selected.get(new ArrayList<String>(Arrays.asList("bl_billedcount")));
    assertEquals(clickList.size(), 1, "Size mistmatch:" + clickList.toString());
    assertEquals(clickList.get(0), click);

    List<AbstractCubeTable> dlList = selected.get(new ArrayList<String>(Arrays.asList("dl_joined_count")));
    assertEquals(dlList.size(), 2);
    assertEquals(new HashSet<AbstractCubeTable>(dlList),
      new HashSet<AbstractCubeTable>(Arrays.asList(dlUnMatch, dlMatch)));
  }

  @Test
  public void testSelectDimension() throws Exception {
    String columns[] = {"bl_billedcount", "testdim_name"};
    CubeSelectorService selector = CubeSelectorFactory.getSelectorSvcInstance(conf);

    Map<List<String>, List<AbstractCubeTable>> selection = selector.select(Arrays.asList(columns));

    assertNotNull(selection);
    assertEquals(selection.size(), 2);

    Cube click = metastore.getCube("cube_click");
    CubeDimensionTable testDim = metastore.getDimensionTable("testDim");

    List<AbstractCubeTable> clickList = new ArrayList<AbstractCubeTable>();
    clickList.add(click);
    List<AbstractCubeTable> dimList = new ArrayList<AbstractCubeTable>();
    dimList.add(testDim);

    List<String> clickCols = new ArrayList<String>();
    clickCols.add("bl_billedcount");

    List<String> dimCols = new ArrayList<String>();
    dimCols.add("testdim_name");

    assertEquals(selection.get(clickCols), clickList);
    assertEquals(selection.get(dimCols), dimList);
  }

}

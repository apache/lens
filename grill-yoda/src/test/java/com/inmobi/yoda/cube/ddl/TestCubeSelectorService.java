package com.inmobi.yoda.cube.ddl;


import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.cube.metadata.*;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.TextInputFormat;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.*;

public class TestCubeSelectorService {
  public static final String TEST_DB = "test_cube_selector_db";
  private DimensionDDL dimDDL;
  private HiveConf conf;
  private CubeMetastoreClient metastore;
  private CubeSelectorService selector;
  AbstractCubeTable uber1, uber2, uber3, downloadMatch, downloadUnMatch, click, billing, request, impression;

  @BeforeTest
  public void setup() throws Exception {
    conf = new HiveConf(TestCubeSelectorService.class);
    SessionState.start(conf);
    Hive client = Hive.get(conf);
    Database database = new Database();
    database.setName(TEST_DB);
    client.createDatabase(database, true);

    SessionState.get().setCurrentDatabase(TEST_DB);
    metastore = CubeMetastoreClient.getInstance(conf);
    metastore.setCurrentDatabase(TEST_DB);
    System.out.println("@@ Set current DB to " + metastore.getCurrentDatabase());
    dimDDL = new DimensionDDL(conf);
    CubeDDL cubeDDL = new CubeDDL(dimDDL, conf);
    cubeDDL.createAllCubes();
    dimDDL.createAllDimensions();
    selector = CubeSelectorFactory.getSelectorSvcInstance(conf);
    System.out.println("##setup test cubeselector service");
    
    uber1 = metastore.getCube("cube_rrcube_uber1");
    uber2 = metastore.getCube("cube_rrcube_uber2");
    uber3 = metastore.getCube("cube_rrcube_uber3");
    billing = metastore.getCube("cube_billing");
    impression = metastore.getCube("cube_impression");
    click = metastore.getCube("cube_click");
    downloadMatch = metastore.getCube("cube_downloadmatch");
    downloadUnMatch = metastore.getCube("cube_downloadunmatch");
  }

  @AfterTest
  public void tearDown() throws Exception {
    Hive.get(conf).dropDatabase(TEST_DB, true, true, true);
    System.out.println("##teardown cubeselector service");
  }
  
  
  
  private Set<AbstractCubeTable> setOf(AbstractCubeTable ... tables) {
  	Set<AbstractCubeTable> tabset = new HashSet<AbstractCubeTable>();
  	for (AbstractCubeTable t : tables) {
  		tabset.add(t);
  	}
  	return tabset;
  }
  
  private Set<String> setOf(String ... columns) {
  	Set<String> colset = new HashSet<String>();
  	for (String c : columns) {
  		colset.add(c);
  	}
  	return colset;
  }

  @Test
  public void testSelectorService() throws Exception {
    List<String> col1 = Arrays.asList("rq_time", "rq_siteid", "rq_adimp",
      "rq_mkvldadreq", "bc_click_no_pings", "bl_data_enrichment_cost","dl_joined_count");
    System.out.println("@@TEST_1: " + col1.toString());
    Map<Set<String>, Set<AbstractCubeTable>> result1 = selector.select(col1);
   
    assertEquals(result1.get(setOf("rq_time", "rq_siteid", "rq_adimp", 
    		"rq_mkvldadreq", "bc_click_no_pings", "bl_data_enrichment_cost")), setOf(uber3));
    assertEquals(result1.get(setOf("rq_time", "rq_siteid", "dl_joined_count")), 
    		setOf(downloadMatch, downloadUnMatch));
    printResult(result1);


    List<String> col2 = Arrays.asList("rq_time", "rq_siteid", "impid", 
    		"rq_adimp", "bc_click_no_pings", "bl_data_enrichment_cost", "dl_joined_count");
    System.out.println("@@TEST_2: " + col2.toString());
    Map<Set<String>, Set<AbstractCubeTable>> result2 = selector.select(col2);
    assertEquals(result2.get(setOf("rq_time", "impid", "rq_siteid", "dl_joined_count")),
    		setOf(downloadMatch, downloadUnMatch));
    assertEquals(result2.get(setOf("rq_time", "impid", "rq_siteid", "rq_adimp", "bc_click_no_pings",
    		"bl_data_enrichment_cost")), setOf(uber2));
    printResult(result2);

    List<String> col3 = Arrays.asList("rq_time", "rq_siteid", "impid", "dl_carrier_city_id");
    System.out.println("@@TEST_3: " + col3.toString());
    Map<Set<String>, Set<AbstractCubeTable>> result3 = selector.select(col3);
    assertEquals(result3.get(setOf("rq_time", "impid", "rq_siteid", "dl_carrier_city_id")),
    		setOf(downloadMatch, downloadUnMatch));
    printResult(result3);

    List<String> col4 = Arrays.asList("rq_time", "rq_siteid", "impid");
    System.out.println("@@TEST_4: " + col4.toString());
    Map<Set<String>, Set<AbstractCubeTable>> result4 = selector.select(col4);
    assertEquals(result4.get(setOf("rq_time", "impid", "rq_siteid")),
    		setOf(uber1, uber2, billing, click, impression, downloadMatch, downloadUnMatch));
    printResult(result4);
  }

  private void printResult( Map<Set<String>, Set<AbstractCubeTable>> result) {
    if (result == null || result.isEmpty()) {
      System.out.println("@@ EMPTY");
      return;
    }
    for (Set<String> key : result.keySet()) {
      System.out.print("@@ " + key.toString() + " -> [");
      for(AbstractCubeTable ct : result.get(key)) {
        System.out.print(ct.getName());
        System.out.print(",");
      }
      System.out.println("]");
    }
  }
}

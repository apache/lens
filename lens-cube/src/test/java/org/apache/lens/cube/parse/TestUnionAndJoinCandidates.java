package org.apache.lens.cube.parse;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.lens.server.api.LensServerAPITestUtil;
import org.apache.lens.server.api.error.LensException;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.apache.lens.cube.metadata.DateFactory.*;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.*;
import static org.apache.lens.cube.parse.CubeTestSetup.*;

public class TestUnionAndJoinCandidates extends TestQueryRewrite {

  private Configuration testConf;

  @BeforeTest
  public void setupDriver() throws Exception {
    testConf = LensServerAPITestUtil.getConfiguration(
        DISABLE_AUTO_JOINS, false,
        ENABLE_SELECT_TO_GROUPBY, true,
        ENABLE_GROUP_BY_TO_SELECT, true,
        DISABLE_AGGREGATE_RESOLVER, false,
        ENABLE_STORAGES_UNION, true);
  }

  @Override
  public Configuration getConf() {
    return new Configuration(testConf);
  }

  @Test
  public void testRangeCoveringCandidates() throws ParseException, LensException {
    try {
      String prefix = "union_join_ctx_";
      String cubeName = prefix + "der1";
      Configuration conf = LensServerAPITestUtil.getConfigurationWithParams(getConf(),
          //Supported storage
          CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1",
          // Storage tables
          getValidStorageTablesKey(prefix + "fact1"), "C1_" + prefix + "fact1",
          getValidStorageTablesKey(prefix + "fact2"), "C1_" + prefix + "fact2",
          getValidStorageTablesKey(prefix + "fact3"), "C1_" + prefix + "fact3",
          // Update periods
          getValidUpdatePeriodsKey(prefix + "fact1", "C1"), "DAILY",
          getValidUpdatePeriodsKey(prefix + "fact2", "C1"), "DAILY",
          getValidUpdatePeriodsKey(prefix + "fact3", "C1"), "DAILY");

      String colsSelected = prefix + "cityid , " + prefix + "zipcode , " + "sum(" + prefix + "msr1) , "
          + "sum(" + prefix + "msr2), " + "sum(" + prefix + "msr3) ";

      String whereCond = prefix + "zipcode = 'a' and " + prefix + "cityid = 'b' and " +
          "(" + TWO_MONTHS_RANGE_UPTO_DAYS + ")";
      String hqlQuery = rewrite("select " + colsSelected + " from " + cubeName + " where " + whereCond, conf);

      System.out.println(hqlQuery);

    } finally {
      getStorageToUpdatePeriodMap().clear();
    }
  }

}

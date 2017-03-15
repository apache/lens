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

package org.apache.lens.cube.parse;

import static org.apache.lens.cube.metadata.DateFactory.BEFORE_4_DAYS;
import static org.apache.lens.cube.metadata.DateFactory.BEFORE_6_DAYS;
import static org.apache.lens.cube.metadata.DateFactory.LAST_HOUR_TIME_RANGE;
import static org.apache.lens.cube.metadata.DateFactory.NOW;
import static org.apache.lens.cube.metadata.DateFactory.THIS_YEAR_RANGE;
import static org.apache.lens.cube.metadata.DateFactory.TWODAYS_BACK;
import static org.apache.lens.cube.metadata.DateFactory.TWO_DAYS_RANGE;
import static org.apache.lens.cube.metadata.DateFactory.TWO_DAYS_RANGE_BEFORE_4_DAYS;
import static org.apache.lens.cube.metadata.DateFactory.TWO_DAYS_RANGE_IT;
import static org.apache.lens.cube.metadata.DateFactory.TWO_DAYS_RANGE_TTD;
import static org.apache.lens.cube.metadata.DateFactory.TWO_MONTHS_RANGE_UPTO_HOURS;
import static org.apache.lens.cube.metadata.DateFactory.TWO_MONTHS_RANGE_UPTO_MONTH;
import static org.apache.lens.cube.metadata.DateFactory.getDateStringWithOffset;
import static org.apache.lens.cube.metadata.DateFactory.getDateWithOffset;
import static org.apache.lens.cube.metadata.DateFactory.getTimeRangeString;
import static org.apache.lens.cube.metadata.UpdatePeriod.CONTINUOUS;
import static org.apache.lens.cube.metadata.UpdatePeriod.DAILY;
import static org.apache.lens.cube.metadata.UpdatePeriod.HOURLY;
import static org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode.INCOMPLETE_PARTITION;
import static org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode.LESS_DATA;
import static org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode.MISSING_PARTITIONS;
import static org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode.NO_CANDIDATE_STORAGES;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.DISABLE_AUTO_JOINS;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.REPLACE_TIMEDIM_WITH_PART_COL;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.REWRITE_DIM_FILTER_TO_FACT_FILTER;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.getValidStorageTablesKey;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.getValidUpdatePeriodsKey;
import static org.apache.lens.cube.parse.CubeTestSetup.DERIVED_CUBE_NAME;
import static org.apache.lens.cube.parse.CubeTestSetup.TEST_CUBE_NAME;
import static org.apache.lens.cube.parse.CubeTestSetup.getDateUptoHours;
import static org.apache.lens.cube.parse.CubeTestSetup.getDbName;
import static org.apache.lens.cube.parse.CubeTestSetup.getExpectedQuery;
import static org.apache.lens.cube.parse.CubeTestSetup.getWhereForDailyAndHourly2days;
import static org.apache.lens.cube.parse.CubeTestSetup.getWhereForDailyAndHourly2daysWithTimeDim;
import static org.apache.lens.cube.parse.CubeTestSetup.getWhereForHourly2days;
import static org.apache.lens.cube.parse.CubeTestSetup.getWhereForMonthly2months;
import static org.apache.lens.cube.parse.CubeTestSetup.getWhereForMonthlyDailyAndHourly2months;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.error.NoCandidateDimAvailableException;
import org.apache.lens.cube.error.NoCandidateFactAvailableException;
import org.apache.lens.cube.metadata.BaseDimAttribute;
import org.apache.lens.cube.metadata.Cube;
import org.apache.lens.cube.metadata.CubeDimensionTable;
import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.cube.metadata.DateFactory;
import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.TimePartition;
import org.apache.lens.server.api.LensServerAPITestUtil;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestCubeSegmentationRewriter extends TestQueryRewrite {

  private Configuration conf;

  @BeforeTest
  public void setupDriver() throws Exception {
    conf = LensServerAPITestUtil.getConfiguration(
      DRIVER_SUPPORTED_STORAGES, "C0,C1,C2",
      DISABLE_AUTO_JOINS, true,
      ENABLE_SELECT_TO_GROUPBY, true,
      ENABLE_GROUP_BY_TO_SELECT, true,
      DISABLE_AGGREGATE_RESOLVER, false);
  }

  @Override
  public Configuration getConf() {
    return new Configuration(conf);
  }

  @Test
  public void test() throws ParseException, LensException {
    CubeQueryContext ctx = rewriteCtx("select cityid, segmsr1 from testcube where " + TWO_DAYS_RANGE,
      getConf());
    System.out.println(ctx);
    System.out.println(ctx.toHQL());
  }
}

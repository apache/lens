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

import static org.apache.lens.cube.metadata.DateFactory.*;
import static org.apache.lens.cube.metadata.DateUtil.*;
import static org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode.MISSING_PARTITIONS;
import static org.apache.lens.cube.parse.CubeTestSetup.*;
import static org.apache.lens.cube.parse.TestCubeRewriter.compareContains;
import static org.apache.lens.cube.parse.TestCubeRewriter.compareQueries;

import static org.apache.hadoop.hive.ql.parse.HiveParser.KW_AND;

import static org.testng.Assert.*;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lens.api.error.ErrorCollectionFactory;
import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.error.NoCandidateFactAvailableException;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ASTNode;

import org.antlr.runtime.CommonToken;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import lombok.Getter;

public class TestBaseCubeQueries extends TestQueryRewrite {
  @Getter
  private Configuration conf;
  private final String cubeName = CubeTestSetup.BASE_CUBE_NAME;

  @BeforeTest
  public void setupDriver() throws Exception {
    conf = new Configuration();
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY, true);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT, true);
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER, false);
  }

  @Test
  public void testNoCandidateFactAvailableExceptionCompareTo() throws Exception {
    //maxCause : COLUMN_NOT_FOUND, Ordinal : 9
    NoCandidateFactAvailableException ne1 =(NoCandidateFactAvailableException)
            getLensExceptionInRewrite("select dim1, test_time_dim, msr3, msr13 from basecube where "
            + TWO_DAYS_RANGE, conf);
    //maxCause : FACT_NOT_AVAILABLE_IN_RANGE, Ordinal : 1
    NoCandidateFactAvailableException ne2 = (NoCandidateFactAvailableException)
            getLensExceptionInRewrite("select dim1 from " + cubeName + " where " + LAST_YEAR_RANGE, getConf());
    assertEquals(ne1.compareTo(ne2), 8);
  }

  @Test
  public void testColumnErrors() throws Exception {
    LensException e;

    e = getLensExceptionInRewrite("select msr11 + msr2 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    e.buildLensErrorResponse(new ErrorCollectionFactory().createErrorCollection(), null, "testid");
    assertEquals(e.getErrorCode(),
      LensCubeErrorCode.NO_FACT_HAS_COLUMN.getLensErrorInfo().getErrorCode());
    assertTrue(e.getMessage().contains("msr11"), e.getMessage());
    assertTrue(e.getMessage().contains("msr2"), e.getMessage());
    // no fact has the all the dimensions queried
    e = getLensExceptionInRewrite("select dim1, test_time_dim, msr3, msr13 from basecube where "
      + TWO_DAYS_RANGE, conf);
    assertEquals(e.getErrorCode(),
        LensCubeErrorCode.NO_CANDIDATE_FACT_AVAILABLE.getLensErrorInfo().getErrorCode());
    NoCandidateFactAvailableException ne = (NoCandidateFactAvailableException) e;
    PruneCauses.BriefAndDetailedError pruneCauses = ne.getJsonMessage();
    String regexp = String.format(CandidateTablePruneCause.CandidateTablePruneCode.COLUMN_NOT_FOUND.errorFormat,
      "Column Sets: (.*?)", "queriable together");
    Matcher matcher = Pattern.compile(regexp).matcher(pruneCauses.getBrief());
    assertTrue(matcher.matches(), pruneCauses.getBrief());
    assertEquals(matcher.groupCount(), 1);
    String columnSetsStr = matcher.group(1);
    assertNotEquals(columnSetsStr.indexOf("test_time_dim"), -1, columnSetsStr);
    assertNotEquals(columnSetsStr.indexOf("msr3, msr13"), -1);

    /**
     * Verifying the BriefAndDetailedError:
     * 1. Check for missing columns(COLUMN_NOT_FOUND)
     *    and check the respective tables for each COLUMN_NOT_FOUND
     * 2. check for ELEMENT_IN_SET_PRUNED
     *
     */
    boolean columnNotFound = false;
    List<String> testTimeDimFactTables = Arrays.asList("testfact3_base", "testfact1_raw_base", "testfact3_raw_base",
      "testfact5_base", "testfact6_base", "testfact4_raw_base");
    List<String> factTablesForMeasures = Arrays.asList("testfact_deprecated", "testfact2_raw_base", "testfact2_base",
            "testfact5_raw_base");
    for (Map.Entry<String, List<CandidateTablePruneCause>> entry : pruneCauses.getDetails().entrySet()) {
      if (entry.getValue().contains(CandidateTablePruneCause.columnNotFound("test_time_dim"))) {
        columnNotFound = true;
        compareStrings(testTimeDimFactTables, entry);
      }
      if (entry.getValue().contains(CandidateTablePruneCause.columnNotFound("msr3", "msr13"))) {
        columnNotFound = true;
        compareStrings(factTablesForMeasures, entry);
      }
    }
    Assert.assertTrue(columnNotFound);
    assertEquals(pruneCauses.getDetails().get("testfact1_base"),
      Arrays.asList(new CandidateTablePruneCause(CandidateTablePruneCode.ELEMENT_IN_SET_PRUNED)));
  }

  private void compareStrings(List<String> factTablesList, Map.Entry<String, List<CandidateTablePruneCause>> entry) {
    String factTablesString = entry.getKey();
    Iterable<String> factTablesIterator = Splitter.on(',').split(factTablesString);
    for (String factTable : factTablesIterator) {
      Assert.assertTrue(factTablesList.contains(factTable), "Not selecting" + factTable + "fact table");
    }
  }

  @Test
  public void testCommonDimensions() throws Exception {
    String hqlQuery = rewrite("select dim1, SUM(msr1) from basecube" + " where " + TWO_DAYS_RANGE, conf);
    String expected =
      getExpectedQuery(cubeName, "select basecube.dim1, SUM(basecube.msr1) FROM ", null, " group by basecube.dim1",
        getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select dim1, SUM(msr1), msr2 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "select basecube.dim1, SUM(basecube.msr1), basecube.msr2 FROM ", null,
        " group by basecube.dim1", getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select dim1, roundedmsr2 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "select basecube.dim1, round(sum(basecube.msr2)/1000) FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select booleancut, msr2 from basecube" + " where " + TWO_DAYS_RANGE + " and substrexpr != 'XYZ'", conf);
    expected =
      getExpectedQuery(cubeName, "select basecube.dim1 != 'x' AND basecube.dim2 != 10 ,"
          + " sum(basecube.msr2) FROM ", null, " and substr(basecube.dim1, 3) != 'XYZ' "
          + "group by basecube.dim1 != 'x' AND basecube.dim2 != 10",
        getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select dim1, msr12 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "select basecube.dim1, sum(basecube.msr12) FROM ", null, " group by basecube.dim1",
        getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testMultiFactQueryWithNoDimensionsSelected() throws Exception {
    CubeQueryContext ctx = rewriteCtx("select roundedmsr2, msr12 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    Set<String> candidateFacts = new HashSet<String>();
    for (CandidateFact cfact : ctx.getCandidateFacts()) {
      candidateFacts.add(cfact.getName().toLowerCase());
    }
    Assert.assertTrue(candidateFacts.contains("testfact1_base"));
    Assert.assertTrue(candidateFacts.contains("testfact2_base"));
    String hqlQuery = ctx.toHQL();
    String expected1 =
      getExpectedQuery(cubeName, "select sum(basecube.msr12) as `msr12` FROM ", null,
        null, getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 =
      getExpectedQuery(cubeName, "select round(sum(basecube.msr2)/1000) as `roundedmsr2` FROM ", null,
        null, getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    String lower = hqlQuery.toLowerCase();
    assertTrue(lower.startsWith("select mq2.roundedmsr2 roundedmsr2, mq1.msr12 msr12 from ")
      || lower.startsWith("select mq1.roundedmsr2 roundedmsr2, mq2.msr12 msr12 from "), hqlQuery);
    assertTrue(lower.contains("mq1 full outer join") && lower.endsWith("mq2"), hqlQuery);
    assertFalse(lower.contains("mq2 on"), hqlQuery);
    assertFalse(lower.contains("<=>"), hqlQuery);
  }

  @Test
  public void testMoreThanTwoFactQueryWithNoDimensionsSelected() throws Exception {
    CubeQueryContext ctx = rewriteCtx("select roundedmsr2, msr14, msr12 from basecube" + " where " + TWO_DAYS_RANGE,
      conf);
    Set<String> candidateFacts = new HashSet<String>();
    for (CandidateFact cfact : ctx.getCandidateFacts()) {
      candidateFacts.add(cfact.getName().toLowerCase());
    }
    Assert.assertEquals(candidateFacts.size(), 3);
    Assert.assertTrue(candidateFacts.contains("testfact1_base"));
    Assert.assertTrue(candidateFacts.contains("testfact2_base"));
    Assert.assertTrue(candidateFacts.contains("testfact3_base"));
    String hqlQuery = ctx.toHQL();
    String expected1 = getExpectedQuery(cubeName, "select sum(basecube.msr12) as `msr12` FROM ", null, null,
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 = getExpectedQuery(cubeName, "select round(sum(basecube.msr2)/1000) as `roundedmsr2` FROM ", null,
      null, getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    String expected3 = getExpectedQuery(cubeName, "select count((basecube.msr14)) as `msr14` FROM ", null, null,
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact3_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    compareContains(expected3, hqlQuery);
    String lower = hqlQuery.toLowerCase();
    assertTrue(lower.startsWith("select mq1.roundedmsr2 roundedmsr2, mq3.msr14 msr14, mq2.msr12 msr12 from ") || lower
      .startsWith("select mq3.roundedmsr2 roundedmsr2, mq1.msr14 msr14, mq2.msr12 msr12 from ") || lower
      .startsWith("select mq2.roundedmsr2 roundedmsr2, mq3.msr14 msr14, mq1.msr12 msr12 from ") || lower
      .startsWith("select mq3.roundedmsr2 roundedmsr2, mq2.msr14 msr14, mq1.msr12 msr12 from ") || lower
      .startsWith("select mq1.roundedmsr2 roundedmsr2, mq2.msr14 msr14, mq3.msr12 msr12 from ") || lower
      .startsWith("select mq2.roundedmsr2 roundedmsr2, mq1.msr14 msr14, mq3.msr12 msr12 from "), hqlQuery);
    assertTrue(lower.contains("mq1 full outer join") && lower.endsWith("mq3"));
    assertFalse(lower.contains("mq3 on"), hqlQuery);
    assertFalse(lower.contains("mq2 on"), hqlQuery);
    assertFalse(lower.contains("<=>"), hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithSingleCommonDimension() throws Exception {
    String hqlQuery = rewrite("select dim1, roundedmsr2, msr12 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    String expected1 =
      getExpectedQuery(cubeName, "select basecube.dim1 as `dim1`, sum(basecube.msr12) as `msr12` FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 = getExpectedQuery(cubeName,
      "select basecube.dim1 as `dim1`, round(sum(basecube.msr2)/1000) as `roundedmsr2` FROM ", null,
      " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    String lower = hqlQuery.toLowerCase();
    assertTrue(
      lower.startsWith("select coalesce(mq1.dim1, mq2.dim1) dim1, mq2.roundedmsr2 roundedmsr2, mq1.msr12 msr12 from ")
        || lower.startsWith("select coalesce(mq1.dim1, mq2.dim1) dim1, mq1.roundedmsr2 roundedmsr2, mq2.msr12 msr12"
        + " from "), hqlQuery);

    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.dim1 <=> mq2.dim1"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithSingleCommonDimensionWithLightestFactFirst() throws Exception {
    Configuration tConf = new Configuration(conf);
    tConf.setBoolean(CubeQueryConfUtil.LIGHTEST_FACT_FIRST, true);
    String hqlQuery = rewrite("select dim1, roundedmsr2, msr12 from basecube" + " where " + TWO_DAYS_RANGE, tConf);
    String expected1 =
      getExpectedQuery(cubeName, "select basecube.dim1 as `dim1`, sum(basecube.msr12) as `msr12` FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 = getExpectedQuery(cubeName,
        "select basecube.dim1 as `dim1`, round(sum(basecube.msr2)/1000) as `roundedmsr2` FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    String lower = hqlQuery.toLowerCase();
    assertTrue(
      lower.startsWith("select coalesce(mq1.dim1, mq2.dim1) dim1, mq2.roundedmsr2 roundedmsr2, mq1.msr12 msr12 from ")
      || lower.startsWith("select coalesce(mq1.dim1, mq2.dim1) dim1, mq1.roundedmsr2 roundedmsr2, mq2.msr12 msr12"
        + " from "), hqlQuery);

    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.dim1 <=> mq2.dim1"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithExpressionsFromMultipleFacts() throws Exception {
    Configuration tConf = new Configuration(conf);
    tConf.setBoolean(CubeQueryConfUtil.LIGHTEST_FACT_FIRST, true);
    String hqlQuery = rewrite("select  dim1, roundedmsr2, flooredmsr12 from basecube" + " where "
            + TWO_DAYS_RANGE, tConf);
    String expected1 =
            getExpectedQuery(cubeName, "select basecube.dim1 as `dim1`, "
                            + "floor(sum(( basecube . msr12 ))) as `flooredmsr12` FROM ", null,
                    " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 = getExpectedQuery(cubeName,
            "select basecube.dim1 as `dim1`, round(sum(basecube.msr2)/1000) as `roundedmsr2` FROM ", null,
            " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    String lower = hqlQuery.toLowerCase();
    assertTrue(
            lower.startsWith("select coalesce(mq1.dim1, mq2.dim1) dim1, mq2.roundedmsr2 roundedmsr2, "
                    + "mq1.flooredmsr12 flooredmsr12 from ")
                    || lower.startsWith("select coalesce(mq1.dim1, mq2.dim1) dim1, mq1.roundedmsr2 roundedmsr2, "
                    + "mq2.flooredmsr12 flooredmsr12"
                    + " from "), hqlQuery);

    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.dim1 <=> mq2.dim1"),
            hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithSingleCommonDimensionWithColumnsSwapped() throws Exception {
    // columns in select interchanged
    String hqlQuery = rewrite("select dim1, msr12, roundedmsr2 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    String expected1 =
      getExpectedQuery(cubeName, "select basecube.dim1 as `dim1`, sum(basecube.msr12) as `msr12` FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 = getExpectedQuery(cubeName,
        "select basecube.dim1 as `dim1`, round(sum(basecube.msr2)/1000) as `roundedmsr2` FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    String lower = hqlQuery.toLowerCase();
    assertTrue(
      lower.startsWith("select coalesce(mq1.dim1, mq2.dim1) dim1, mq2.msr12 msr12, mq1.roundedmsr2 roundedmsr2 from ")
      || lower.startsWith("select coalesce(mq1.dim1, mq2.dim1) dim1, mq1.msr12 msr12, mq2.roundedmsr2 roundedmsr2"
        + " from "), hqlQuery);

    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.dim1 <=> mq2.dim1"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryInvolvingThreeFactTables() throws Exception {
    // query with 3 fact tables
    String hqlQuery = rewrite("select dim1, d_time, msr12, roundedmsr2, msr13, msr3 from basecube where "
        + TWO_DAYS_RANGE, conf);
    String expected1 =
      getExpectedQuery(cubeName, "select basecube.dim1 as `dim1`, basecube.d_time as `d_time`, "
          + "sum(basecube.msr12) as `msr12` FROM ", null, " group by basecube.dim1",
          getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 = getExpectedQuery(
        cubeName,
        "select basecube.dim1 as `dim1`, basecube.d_time as `d_time`, round(sum(basecube.msr2)/1000) "
            + "as `roundedmsr2`, max(basecube.msr3) as `msr3` FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    String expected3 =
      getExpectedQuery(cubeName, "select basecube.dim1 as `dim1`, basecube.d_time as `d_time`, "
          + "max(basecube.msr13) as `msr13` FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "c1_testfact3_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    compareContains(expected3, hqlQuery);
    assertTrue(
      hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim1, mq2.dim1, mq3.dim1) dim1, coalesce(mq1.d_time, mq2.d_time, mq3.d_time) d_time, "
            + "mq1.msr12 msr12, mq2.roundedmsr2 roundedmsr2, mq3.msr13 msr13, mq2.msr3 msr3 from ")
        || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim1, mq2.dim1, mq3.dim1) dim1, coalesce(mq1.d_time, mq2.d_time, mq3.d_time) d_time,"
            + " mq1.msr12 msr12, mq3.roundedmsr2 roundedmsr2, mq2.msr13 msr13, mq3.msr3 msr3 from ")
        || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim1, mq2.dim1, mq3.dim1) dim1, coalesce(mq1.d_time, mq2.d_time, mq3.d_time) d_time,"
            + " mq2.msr12 msr12, mq1.roundedmsr2 roundedmsr2, mq3.msr13 msr13, mq1.msr3 msr3 from ")
        || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim1, mq2.dim1, mq3.dim1) dim1, coalesce(mq1.d_time, mq2.d_time, mq3.d_time) d_time, "
            + "mq2.msr12 msr12, mq3.roundedmsr2 roundedmsr2, mq1.msr13 msr13, mq3.msr3 msr3 from ")
        || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim1, mq2.dim1, mq3.dim1) dim1, coalesce(mq1.d_time, mq2.d_time, mq3.d_time) d_time,"
            + " mq3.msr12 msr12, mq1.roundedmsr2 roundedmsr2, mq2.msr13 msr13, mq1.msr3 msr3 from ")
        || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim1, mq2.dim1, mq3.dim1) dim1, coalesce(mq1.d_time, mq2.d_time, mq3.d_time) d_time, "
            + "mq3.msr12 msr12, mq2.roundedmsr2 roundedmsr2, mq1.msr13 msr13, mq2.msr3 msr3 from "), hqlQuery);
    assertTrue(hqlQuery.toLowerCase().contains("mq1 full outer join ")
        && hqlQuery.toLowerCase().contains("mq2 on mq1.dim1 <=> mq2.dim1 and mq1.d_time <=> mq2.d_time")
        && hqlQuery.toLowerCase().endsWith("mq3 on mq2.dim1 <=> mq3.dim1 and mq2.d_time <=> mq3.d_time"), hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithTwoCommonDimensions() throws Exception {
    // query two dim attributes
    String hqlQuery = rewrite("select dim1, dim11, msr12, roundedmsr2 from basecube where " + TWO_DAYS_RANGE, conf);
    String expected1 = getExpectedQuery(cubeName,
        "select basecube.dim1 as `dim1`, basecube.dim11 as `dim11`, sum(basecube.msr12) as `msr12` FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 = getExpectedQuery(
        cubeName,
        "select basecube.dim1 as `dim1`, basecube.dim11 as `dim11`, round(sum(basecube.msr2)/1000) as `roundedmsr2` "
        + "FROM ", null, " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select coalesce(mq1.dim1, mq2.dim1) dim1, coalesce(mq1.dim11, mq2.dim11) dim11,"
        + " mq1.msr12 msr12, mq2.roundedmsr2 roundedmsr2 from ")
      || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim1, mq2.dim1) dim1, coalesce(mq1.dim11, mq2.dim11) dim11,"
        + " mq2.msr12 msr12, mq1.roundedmsr2 roundedmsr2 from "), hqlQuery);

    assertTrue(hqlQuery.contains("mq1 full outer join ")
      && hqlQuery.endsWith("mq2 on mq1.dim1 <=> mq2.dim1 AND mq1.dim11 <=> mq2.dim11"), hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithNoAggregates() throws Exception {
    // no aggregates in the query
    String hqlQuery = rewrite("select dim1, msr11, roundedmsr2 from basecube where " + TWO_DAYS_RANGE, conf);
    String expected1 =
      getExpectedQuery(cubeName, "select basecube.dim1 as `dim1`, basecube.msr11 as `msr11` FROM ", null, null,
        getWhereForHourly2days(cubeName, "C1_testfact2_raw_base"));
    String expected2 = getExpectedQuery(cubeName,
        "select basecube.dim1 as `dim1`, round(basecube.msr2/1000) as `roundedmsr2` FROM ", null, null,
        getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select coalesce(mq1.dim1, mq2.dim1) dim1, mq1.msr11 msr11, mq2.roundedmsr2 roundedmsr2 from ")
      || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim1, mq2.dim1) dim1, mq2.msr11 msr11, mq1.roundedmsr2 roundedmsr2 from "), hqlQuery);

    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.dim1 <=> mq2.dim1"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithColumnAliases() throws Exception {
    // query with aliases passed
    String hqlQuery =
      rewrite("select dim1 d1, msr12 `my msr12`, roundedmsr2 m2 from basecube where " + TWO_DAYS_RANGE, conf);
    String expected1 =
      getExpectedQuery(cubeName, "select basecube.dim1 as `expr1`, sum(basecube.msr12) as `expr2` FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 =
      getExpectedQuery(cubeName, "select basecube.dim1 as `expr1`, round(sum(basecube.msr2)/1000) as `expr3` FROM ",
        null, " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select coalesce(mq1.expr1, mq2.expr1) `d1`, mq2.expr2 `my msr12`, mq1.expr3 `m2` from ")
      || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.expr1, mq2.expr1) `d1`, mq1.expr2 `my msr12`, mq2.expr3 `m2` from "), hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.expr1 <=> mq2.expr1"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithColumnAliasesAsFunctions() throws Exception {
    String hqlQuery =
      rewrite("select dim1 d1, msr12 `sum(msr12)`, roundedmsr2 as `round(sum(msr2)/1000)` from basecube where "
        + TWO_DAYS_RANGE, conf);
    String expected1 =
      getExpectedQuery(cubeName, "select basecube.dim1 as `expr1`, sum(basecube.msr12) as `expr2` FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 =
      getExpectedQuery(cubeName, "select basecube.dim1 as `expr1`, round(sum(basecube.msr2)/1000) as `expr3` FROM ",
        null, " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.expr1, mq2.expr1) `d1`, mq2.expr2 `sum(msr12)`, mq1.expr3 `round(sum(msr2)/1000)` from ")
        || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.expr1, mq2.expr1) `d1`, mq1.expr2 `sum(msr12)`, mq2.expr3 `round(sum(msr2)/1000)` from "),
      hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.expr1 <=> mq2.expr1"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithAliasAsColumnName() throws Exception {
    String hqlQuery =
      rewrite("select dim1 d1, msr12 `my msr12`, roundedmsr2 as `msr2` from basecube where " + TWO_DAYS_RANGE, conf);

    String expected1 =
      getExpectedQuery(cubeName, "select basecube.dim1 as `expr1`, sum(basecube.msr12) as `expr2` FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 =
      getExpectedQuery(cubeName, "select basecube.dim1 as `expr1`, round(sum(basecube.msr2)/1000) as `expr3` FROM ",
        null, " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.expr1, mq2.expr1) `d1`, mq2.expr2 `my msr12`, mq1.expr3 `msr2` from ")
        || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.expr1, mq2.expr1) `d1`, mq1.expr2 `my msr12`, mq2.expr3 `msr2` from "),
      hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.expr1 <=> mq2.expr1"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithAliasAsExpressionName() throws Exception {
    String hqlQuery =
      rewrite("select dim1 d1, msr12 `my msr12`, roundedmsr2 as `roundedmsr2` from basecube where " + TWO_DAYS_RANGE,
        conf);

    String expected1 =
      getExpectedQuery(cubeName, "select basecube.dim1 as `expr1`, sum(basecube.msr12) as `expr2` FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 =
      getExpectedQuery(cubeName, "select basecube.dim1 as `expr1`, round(sum(basecube.msr2)/1000) as `expr3` FROM ",
        null, " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.expr1, mq2.expr1) `d1`, mq2.expr2 `my msr12`, mq1.expr3 `roundedmsr2` from ")
        || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.expr1, mq2.expr1) `d1`, mq1.expr2 `my msr12`, mq2.expr3 `roundedmsr2` from "),
      hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.expr1 <=> mq2.expr1"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithExprOnDimsWithoutAliases() throws Exception {
    String hqlQuery =
      rewrite("select reverse(dim1), ltrim(dim1), msr12, roundedmsr2 from basecube where " + TWO_DAYS_RANGE, conf);
    String expected1 =
      getExpectedQuery(cubeName, "select reverse(basecube.dim1) as `expr1`, ltrim(basecube.dim1)  as `expr2`,"
        + " sum(basecube.msr12) as `msr12` FROM ", null,
        " group by reverse(basecube.dim1), ltrim(basecube.dim1)",
        getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 =
      getExpectedQuery(cubeName, "select reverse(basecube.dim1) as `expr1`, ltrim(basecube.dim1)  as `expr2`,"
        + " round(sum(basecube.msr2)/1000) as `roundedmsr2` FROM ", null,
        " group by reverse(basecube.dim1), ltrim(basecube.dim1)",
        getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select coalesce(mq1.expr1, mq2.expr1) `reverse(dim1)`,"
      + " coalesce(mq1.expr2, mq2.expr2) `ltrim(dim1)`, mq2.msr12 msr12, mq1.roundedmsr2 roundedmsr2 from ")
      || hqlQuery.toLowerCase().startsWith("select coalesce(mq1.expr1, mq2.expr1) `reverse(dim1)`,"
        + " coalesce(mq1.expr2, mq2.expr2) `ltrim(dim1)`, mq1.msr12 msr12, mq2.roundedmsr2 roundedmsr2 from "),
      hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ")
      && hqlQuery.endsWith("mq2 on mq1.expr1 <=> mq2.expr1 AND mq1.expr2 <=> mq2.expr2"), hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithDirectMsr() throws Exception {
    String hqlQuery =
      rewrite("select reverse(dim1), directMsrExpr as directMsr, roundedmsr2 from basecube where " + TWO_DAYS_RANGE,
        conf);
    String expected1 =
      getExpectedQuery(cubeName, "select reverse(basecube.dim1) as `expr1`, "
        + "max(basecube.msr13) + count(basecube . msr14) as `expr2` FROM ", null,
        " group by reverse(basecube.dim1)", getWhereForDailyAndHourly2days(cubeName, "C1_testFact3_BASE"));
    String expected2 =
      getExpectedQuery(cubeName, "select reverse(basecube.dim1) as expr1, "
        + "round(sum(basecube.msr2)/1000) as `roundedmsr2` FROM ", null, " group by reverse(basecube.dim1)",
        getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select coalesce(mq1.expr1, mq2.expr1) `reverse(dim1)`, mq2.expr2 `directmsr`, mq1.roundedmsr2 roundedmsr2 "
        + "from ")
      || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.expr1, mq2.expr1) `reverse(dim1)`, mq1.expr2 `directmsr`, mq2.roundedmsr2 roundedmsr2 "
          + "from "),
      hqlQuery.toLowerCase());
    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.expr1 <=> mq2.expr1"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithNoDefaultAggregates() throws Exception {
   // query with non default aggregate
    String hqlQuery = rewrite("select dim1, avg(msr12), avg(msr2) from basecube where " + TWO_DAYS_RANGE, conf);
    String expected1 =
      getExpectedQuery(cubeName, "select basecube.dim1 as `dim1`, avg(basecube.msr12) as `expr2` FROM ", null,
        " group by basecube.dim1", getWhereForHourly2days(cubeName, "C1_testfact2_raw_base"));
    String expected2 =
      getExpectedQuery(cubeName, "select basecube.dim1 as `dim1`, avg(basecube.msr2)) as `expr3` FROM ", null,
        " group by basecube.dim1", getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select coalesce(mq1.dim1, mq2.dim1) dim1, mq2.expr2 `avg(msr12)`, mq1.expr3 `avg(msr2)` from ")
      || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim1, mq2.dim1) dim1, mq1.expr2 `avg(msr12)`, mq2.expr3 `avg(msr2)` from "), hqlQuery);

    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.dim1 <=> mq2.dim1"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithJoins() throws Exception {
    // query with join
    String hqlQuery = rewrite("select dim2chain.name, msr12, roundedmsr2 from basecube where " + TWO_DAYS_RANGE, conf);
    String expected1 = getExpectedQuery(cubeName,
        "select dim2chain.name as `name`, sum(basecube.msr12) as `msr12` FROM ", " JOIN " + getDbName()
            + "c1_testdim2tbl dim2chain ON basecube.dim2 = " + " dim2chain.id and (dim2chain.dt = 'latest') ", null,
        " group by dim2chain.name", null, getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 = getExpectedQuery(cubeName,
        "select dim2chain.name as `name`, round(sum(basecube.msr2)/1000) as `roundedmsr2` FROM ", " JOIN " + getDbName()
            + "c1_testdim2tbl dim2chain ON basecube.dim2 = " + " dim2chain.id and (dim2chain.dt = 'latest') ", null,
        " group by dim2chain.name", null, getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select coalesce(mq1.name, mq2.name) name, mq2.msr12 msr12, mq1.roundedmsr2 roundedmsr2 from ")
      || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.name, mq2.name) name, mq1.msr12 msr12, mq2.roundedmsr2 roundedmsr2 from "), hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.name <=> mq2.name"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithDenormColumn() throws Exception {
    // query with denorm variable
    String hqlQuery = rewrite("select dim2, msr13, roundedmsr2 from basecube where " + TWO_DAYS_RANGE, conf);
    String expected1 = getExpectedQuery(cubeName, "select dim2chain.id as `dim2`, max(basecube.msr13) as `msr13` FROM ",
        " JOIN " + getDbName() + "c1_testdim2tbl dim2chain ON basecube.dim12 = "
            + " dim2chain.id and (dim2chain.dt = 'latest') ", null, " group by dim2chain.id", null,
        getWhereForHourly2days(cubeName, "C1_testFact3_RAW_BASE"));
    String expected2 = getExpectedQuery(cubeName,
        "select basecube.dim2 as `dim2`, round(sum(basecube.msr2)/1000) as `roundedmsr2` FROM ", null,
        " group by basecube.dim2", getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select coalesce(mq1.dim2, mq2.dim2) dim2, mq2.msr13 msr13, mq1.roundedmsr2 roundedmsr2 from ")
      || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim2, mq2.dim2) dim2, mq1.msr13 msr13, mq2.roundedmsr2 roundedmsr2 from "), hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.dim2 <=> mq2.dim2"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithDenormColumnInWhere() throws Exception {
    // query with denorm variable
    String hqlQuery = rewrite("select dim2, msr13, roundedmsr2 from basecube where dim2 == 10 and " + TWO_DAYS_RANGE,
      conf);
    String expected1 = getExpectedQuery(cubeName, "select dim2chain.id as `dim2`, max(basecube.msr13) as `msr13` FROM ",
      " JOIN " + getDbName() + "c1_testdim2tbl dim2chain ON basecube.dim12 = "
        + " dim2chain.id and (dim2chain.dt = 'latest') ", "dim2chain.id == 10", " group by dim2chain.id", null,
      getWhereForHourly2days(cubeName, "C1_testFact3_RAW_BASE"));
    String expected2 = getExpectedQuery(cubeName,
      "select basecube.dim2 as `dim2`, round(sum(basecube.msr2)/1000) as `roundedmsr2` FROM ", "basecube.dim2 == 10",
      " group by basecube.dim2", getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select coalesce(mq1.dim2, mq2.dim2) dim2, mq2.msr13 msr13, mq1.roundedmsr2 roundedmsr2 from ")
      || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim2, mq2.dim2) dim2, mq1.msr13 msr13, mq2.roundedmsr2 roundedmsr2 from "), hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.dim2 <=> mq2.dim2"),
      hqlQuery);
  }
  @Test
  public void testMultiFactQueryWithExpressionInvolvingDenormVariable() throws Exception {
    // query with expression
    // The expression to be answered from denorm columns
    String hqlQuery =
      rewrite(
        "select booleancut, round(sum(msr2)/1000), avg(msr13 + msr14) from basecube where " + TWO_DAYS_RANGE,
        conf);
    String expected1 =
      getExpectedQuery(cubeName, "select basecube.dim1 != 'x' AND dim2chain.id != 10 as `booleancut`,"
          + " avg(basecube.msr13 + basecube.msr14) as `expr3` FROM ", " JOIN " + getDbName()
          + "c1_testdim2tbl dim2chain ON basecube.dim12 = " + " dim2chain.id and (dim2chain.dt = 'latest') ", null,
        " group by basecube.dim1 != 'x' AND dim2chain.id != 10", null,
        getWhereForHourly2days(cubeName, "C1_testfact3_raw_base"));
    String expected2 =
      getExpectedQuery(cubeName, "select basecube.dim1 != 'x' AND basecube.dim2 != 10 as `booleancut`,"
          + " round(sum(basecube.msr2)/1000) as `expr2` FROM ", null,
        " group by basecube.dim1 != 'x' AND basecube.dim2 != 10",
        getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select coalesce(mq1.booleancut, mq2.booleancut) booleancut, "
      + "mq2.expr2 `round((sum(msr2) / 1000))`, mq1.expr3 `avg((msr13 + msr14))` from ")
      || hqlQuery.toLowerCase().startsWith("select coalesce(mq1.booleancut, mq2.booleancut) booleancut, "
        + "mq1.expr2 `round((sum(msr2) / 1000))`, mq2.expr3 `avg((msr13 + msr14))` from "), hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ")
      && hqlQuery.endsWith("mq2 on mq1.booleancut <=> mq2.booleancut"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithExpressionInvolvingDenormVariableInWhereClause() throws Exception {
    // query with expression
    // The expression to be answered from denorm columns
    String hqlQuery =
      rewrite(
        "select booleancut, round(sum(msr2)/1000), avg(msr13 + msr14) from basecube where booleancut == 'true' and "
          + TWO_DAYS_RANGE, conf);
    String expected1 =
      getExpectedQuery(cubeName, "select basecube.dim1 != 'x' AND dim2chain.id != 10 as `booleancut`,"
          + " avg(basecube.msr13 + basecube.msr14) as `expr3` FROM ", " JOIN " + getDbName()
          + "c1_testdim2tbl dim2chain ON basecube.dim12 = " + " dim2chain.id and (dim2chain.dt = 'latest') ",
        "(basecube.dim1 != 'x' AND dim2chain.id != 10) == true",
        " group by basecube.dim1 != 'x' AND dim2chain.id != 10", null,
        getWhereForHourly2days(cubeName, "C1_testfact3_raw_base"));
    String expected2 =
      getExpectedQuery(cubeName, "select basecube.dim1 != 'x' AND basecube.dim2 != 10 as `booleancut`,"
          + " round(sum(basecube.msr2)/1000) as `expr2` FROM ",
        "(basecube.dim1 != 'x' AND basecube.dim2 != 10) == true",
        " group by basecube.dim1 != 'x' AND basecube.dim2 != 10",
        getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select coalesce(mq1.booleancut, mq2.booleancut) booleancut, "
      + "mq2.expr2 `round((sum(msr2) / 1000))`, mq1.expr3 `avg((msr13 + msr14))` from ")
      || hqlQuery.toLowerCase().startsWith("select coalesce(mq1.booleancut, mq2.booleancut) booleancut, "
        + "mq1.expr2 `round((sum(msr2) / 1000))`, mq2.expr3 `avg((msr13 + msr14))` from "), hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ")
        && hqlQuery.endsWith("mq2 on mq1.booleancut <=> mq2.booleancut"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithMaterializedExpressions() throws Exception {
    Configuration tconf = new Configuration(conf);
    tconf.set(CubeQueryConfUtil.getValidFactTablesKey("basecube"), "testfact5_base,testfact6_base");
    String hqlQuery =
      rewrite(
        "select booleancut, round(sum(msr2)/1000), msr13 from basecube where " + TWO_DAYS_RANGE, tconf);
    String expected1 =
      getExpectedQuery(cubeName, "select basecube.booleancut as `booleancut`,max(basecube.msr13) as `msr13` FROM ",
        null, " group by basecube.booleancut", getWhereForDailyAndHourly2days(cubeName, "C1_testfact6_base"));
    String expected2 =
      getExpectedQuery(cubeName, "select basecube.booleancut as `booleancut`,"
          + " round(sum(basecube.msr2)/1000) as `expr2` FROM ", null, " group by basecube.booleancut",
        getWhereForDailyAndHourly2days(cubeName, "C1_testfact5_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select coalesce(mq1.booleancut, mq2.booleancut) booleancut, "
      + "mq2.expr2 `round((sum(msr2) / 1000))`, mq1.msr13 msr13 from ")
      || hqlQuery.toLowerCase().startsWith("select coalesce(mq1.booleancut, mq2.booleancut) booleancut, "
        + "mq1.expr2 `round((sum(msr2) / 1000))`, mq2.msr13 msr13 from "), hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ")
        && hqlQuery.endsWith("mq2 on mq1.booleancut <=> mq2.booleancut"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryCaseWhenExpressionWithChainField() throws Exception {
    Configuration tconf = new Configuration(conf);
    String hqlQuery =
      rewrite("select sum(case when dim22 = 'x' then msr12 else 0 end) as case_expr, sum(msr1) from basecube where "
          + TWO_DAYS_RANGE, tconf);
    String expected1 =
      getExpectedQuery(cubeName, "select sum(case when basecube.dim22 = 'x' then basecube.msr12 else 0 end) as "
          + "`expr1` FROM ", null, null, getWhereForHourly2days(cubeName, "C1_testfact2_raw_base"));
    String expected2 =
      getExpectedQuery(cubeName, "select sum(basecube.msr1) as `expr2` FROM ", null, null,
        getWhereForHourly2days(cubeName, "c1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select mq2.expr1 `case_expr`, mq1.expr2 `sum(msr1)` from ")
      || hqlQuery.toLowerCase().startsWith("select mq1.expr1 `case_expr`, mq2.expr2 `sum(msr1)` from "), hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2"), hqlQuery);
  }

  @Test
  public void testMultiFactQueryCaseWhenExpression() throws Exception {
    Configuration tconf = new Configuration(conf);
    String hqlQuery =
      rewrite("select sum(case when dim13 = 'x' then msr12 else 0 end) as case_expr, sum(msr1) from basecube where "
        + TWO_DAYS_RANGE, tconf);
    String expected1 =
      getExpectedQuery(cubeName, "select sum(case when basecube.dim13 = 'x' then basecube.msr12 else 0 end) as "
        + "`expr1` FROM ", null, null, getWhereForHourly2days(cubeName, "C1_testfact2_raw_base"));
    String expected2 =
      getExpectedQuery(cubeName, "select sum(basecube.msr1) as `expr2` FROM ", null, null,
        getWhereForHourly2days(cubeName, "c1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select mq2.expr1 `case_expr`, mq1.expr2 `sum(msr1)` from ")
      || hqlQuery.toLowerCase().startsWith("select mq1.expr1 `case_expr`, mq2.expr2 `sum(msr1)` from "), hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2"), hqlQuery);
  }

  @Test
  public void testMultiFactQueryCaseWhenExpressionWithGroupby() throws Exception {
    Configuration tconf = new Configuration(conf);
    String hqlQuery =
      rewrite("select dim1, sum(case when dim13 = 'x' then msr12 else 0 end) as case_expr, sum(msr1) from basecube "
        + "where " + TWO_DAYS_RANGE, tconf);
    String expected1 =
      getExpectedQuery(cubeName, "select basecube.dim1 as `dim1`, sum(case when basecube.dim13 = 'x' then basecube"
          + ".msr12 else 0 end) as `expr2` FROM ", null, " group by basecube.dim1 ",
        getWhereForHourly2days(cubeName, "C1_testfact2_raw_base"));
    String expected2 =
      getExpectedQuery(cubeName, "select basecube.dim1 as `dim1`, sum(basecube.msr1) as `expr3` FROM ", null,
        " group by basecube.dim1 ", getWhereForHourly2days(cubeName, "c1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select coalesce(mq1.dim1, mq2.dim1) dim1, mq2.expr2 `case_expr`, mq1.expr3 `sum(msr1)` from ")
      || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim1, mq2.dim1) dim1, mq1.expr2 `case_expr`, mq2.expr3 `sum(msr1)` from "), hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.dim1 <=> mq2.dim1"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryCaseWhenExpressionWithHavingClause() throws Exception {
    Configuration tconf = new Configuration(conf);
    String hqlQuery =
      rewrite("select sum(case when dim13 = 'x' then msr12 else 0 end) as case_expr, sum(msr1) from basecube "
        + "where "
        + TWO_DAYS_RANGE + " having sum(case when dim13 = 'x' then msr12 else 0 end) > 100 and sum(msr1) > 500", tconf);
    String expected1 =
      getExpectedQuery(cubeName, "select sum(case when basecube.dim13 = 'x' then basecube.msr12 else 0 end) as "
        + "`expr1` FROM ", null, " having sum(case when basecube.dim13 = 'x' then basecube.msr12 else 0 end) > 100",
        getWhereForHourly2days(cubeName, "C1_testfact2_raw_base"));
    String expected2 =
      getExpectedQuery(cubeName, "select sum(basecube.msr1) as `expr2` FROM ", null, " having sum(basecube.msr1) > 500",
        getWhereForHourly2days(cubeName, "c1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select mq2.expr1 `case_expr`, mq1.expr2 `sum(msr1)` from ")
      || hqlQuery.toLowerCase().startsWith("select mq1.expr1 `case_expr`, mq2.expr2 `sum(msr1)` from "), hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2"), hqlQuery);
  }

  @Test
  public void testMultiFactQueryCaseWhenExpressionWithGroubyAndHavingClause() throws Exception {
    Configuration tconf = new Configuration(conf);
    String hqlQuery =
      rewrite("select dim1, sum(case when dim13 = 'x' then msr12 else 0 end) as case_expr, sum(msr1) from basecube "
        + "where "
        + TWO_DAYS_RANGE + " having sum(case when dim13 = 'x' then msr12 else 0 end) > 100 and sum(msr1) > 500", tconf);
    String expected1 =
      getExpectedQuery(cubeName, "select basecube.dim1 as `dim1`, sum(case when basecube.dim13 = 'x' then basecube"
          + ".msr12 else  0 end) as `expr2` FROM ", null,
        " group by basecube.dim1 having sum(case when basecube.dim13 = 'x' then basecube.msr12 else 0 end) > 100",
        getWhereForHourly2days(cubeName, "C1_testfact2_raw_base"));
    String expected2 =
      getExpectedQuery(cubeName, "select basecube.dim1 as `dim1`, sum(basecube.msr1) as `expr3` FROM ", null,
        " group by basecube.dim1 having sum(basecube.msr1) > 500",
        getWhereForHourly2days(cubeName, "c1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select coalesce(mq1.dim1, mq2.dim1) dim1, mq2.expr2 `case_expr`, mq1.expr3 `sum(msr1)` from ")
      || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim1, mq2.dim1) dim1, mq1.expr2 `case_expr`, mq2.expr3 `sum(msr1)` from "), hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.dim1 <=> mq2.dim1"),
      hqlQuery);
  }

  @Test
  public void testFallbackPartCol() throws Exception {
    Configuration conf = getConfWithStorages("C1");
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
    String hql, expected;
    // Prefer fact that has a storage with part col on queried time dim
    hql = rewrite("select msr12 from basecube where " + TWO_DAYS_RANGE, conf);
    expected = getExpectedQuery(BASE_CUBE_NAME, "select sum(basecube.msr12) FROM ", null, null,
      getWhereForDailyAndHourly2days(BASE_CUBE_NAME, "c1_testfact2_base"));
    compareQueries(hql, expected);

    // If going to fallback timedim, and partitions are missing, then error should be missing partition on that
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C4");
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);
    LensException exc =
      getLensExceptionInRewrite("select msr12 from basecube where " + TWO_DAYS_RANGE, conf);
    NoCandidateFactAvailableException ne = (NoCandidateFactAvailableException) exc;
    PruneCauses.BriefAndDetailedError pruneCause = ne.getJsonMessage();
    assertTrue(pruneCause.getBrief().contains("Missing partitions"));
    assertEquals(pruneCause.getDetails().get("testfact2_base").iterator().next().getCause(), MISSING_PARTITIONS);
    assertEquals(pruneCause.getDetails().get("testfact2_base").iterator().next().getMissingPartitions().size(), 1);
    assertEquals(
      pruneCause.getDetails().get("testfact2_base").iterator().next().getMissingPartitions().iterator().next(),
      "ttd:["
        + UpdatePeriod.SECONDLY.format(DateUtils.addDays(DateUtils.truncate(TWODAYS_BACK, Calendar.HOUR), -10))
        + ", " + UpdatePeriod.SECONDLY.format(DateUtils.addDays(DateUtils.truncate(NOW, Calendar.HOUR), 10))
        + ")");

    // fail on partial false. Should go to fallback column. Also testing transitivity of timedim relations
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
    hql = rewrite("select msr12 from basecube where " + TWO_DAYS_RANGE, conf);
    String dTimeWhereClause = "basecube.d_time >= '" + HIVE_QUERY_DATE_PARSER.get().format(ABSDATE_PARSER.get().parse(
      getAbsDateFormatString(getDateUptoHours(
        TWODAYS_BACK)))) + "' and "
      + "basecube.d_time < '" + HIVE_QUERY_DATE_PARSER.get().format(ABSDATE_PARSER.get().parse(
        getAbsDateFormatString(getDateUptoHours(NOW))));
    String pTimeWhereClause = "basecube.processing_time >= '"
      + HIVE_QUERY_DATE_PARSER.get().format(ABSDATE_PARSER.get().parse(
        getAbsDateFormatString(getDateUptoHours(
          DateUtils.addDays(TWODAYS_BACK, -5))))) + "' and "
        + "basecube.processing_time < '" + HIVE_QUERY_DATE_PARSER.get().format(ABSDATE_PARSER.get().parse(
          getAbsDateFormatString(getDateUptoHours(DateUtils.addDays(NOW, 5)))));
    expected = getExpectedQuery(BASE_CUBE_NAME, "select sum(basecube.msr12) FROM ", null,
        " and " + dTimeWhereClause + " and " + pTimeWhereClause,
      getWhereForDailyAndHourly2daysWithTimeDim(BASE_CUBE_NAME, "ttd",
        DateUtils.addDays(TWODAYS_BACK, -10), DateUtils.addDays(NOW, 10), "c4_testfact2_base"));
    compareQueries(hql, expected);

    // Multiple timedims in single query. test that
    CubeQueryContext ctx =
      rewriteCtx("select msr12 from basecube where " + TWO_DAYS_RANGE + " and " + TWO_DAYS_RANGE_TTD, conf);
    assertEquals(ctx.getCandidateFactSets().size(), 1);
    assertEquals(ctx.getCandidateFactSets().iterator().next().size(), 1);
    CandidateFact cfact = ctx.getCandidateFactSets().iterator().next().iterator().next();

    assertEquals(cfact.getRangeToStoragePartMap().size(), 2);
    Set<String> storages = Sets.newHashSet();
    for(Map<String, String> entry: cfact.getRangeToStorageWhereMap().values()) {
      storages.addAll(entry.keySet());
    }
    assertEquals(storages.size(), 1);
    String storage = storages.iterator().next();
    for(Map.Entry<TimeRange, Map<String, String>> entry: cfact.getRangeToStorageWhereMap().entrySet()) {
      if (entry.getKey().getPartitionColumn().equals("dt")) {
        ASTNode parsed = HQLParser.parseExpr(entry.getValue().get(storage));
        assertEquals(parsed.getToken().getType(), KW_AND);
        assertTrue(entry.getValue().get(storage).substring(((CommonToken) parsed.getToken()).getStopIndex() + 1)
          .toLowerCase().contains(dTimeWhereClause));
        assertFalse(entry.getValue().get(storage).substring(0, ((CommonToken) parsed.getToken()).getStartIndex())
          .toLowerCase().contains("and"));
      } else if (entry.getKey().getPartitionColumn().equals("ttd")) {
        assertFalse(entry.getValue().get(storage).toLowerCase().contains("and"));
      } else {
        throw new LensException("Unexpected");
      }
    }
  }
  @Test
  public void testMultiFactQueryWithHaving() throws Exception {

    String hqlQuery, expected1, expected2;
    String endSubString = "mq2 on mq1.dim1 <=> mq2.dim1 AND mq1.dim11 <=> mq2.dim11";
    String joinSubString = "mq1 full outer join ";

    // only One having clause, that too answerable from one fact
    hqlQuery = rewrite("select dim1, dim11, msr12 from basecube where " + TWO_DAYS_RANGE
      + "having roundedmsr2 > 0", conf);
    expected1 = getExpectedQuery(cubeName,
      "select basecube.dim1 as dim1, basecube.dim11 as dim11, sum(basecube.msr12) as msr12 FROM ",
      null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 = getExpectedQuery(cubeName,
      "select basecube.dim1 as dim1, basecube.dim11 as dim11 FROM ",
      null, " group by basecube.dim1, basecube.dim11 having round(sum(basecube.msr2)/1000) > 0",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));

    assertTrue(hqlQuery.toLowerCase().contains("having"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select coalesce(mq1.dim1, mq2.dim1) dim1, "
      + "coalesce(mq1.dim11, mq2.dim11) dim11, mq2.msr12 msr12 from ")
      || hqlQuery.toLowerCase().startsWith("select coalesce(mq1.dim1, mq2.dim1) dim1, coalesce(mq1.dim11, mq2.dim11) "
        + "dim11, mq1.msr12 msr12 from "), hqlQuery);
    assertTrue(hqlQuery.contains(joinSubString)
      && hqlQuery.endsWith(endSubString), hqlQuery);

    // Two having clause, one from each fact.
    hqlQuery = rewrite("select dim1, dim11, msr12, roundedmsr2 from basecube where " + TWO_DAYS_RANGE
      + "having msr12 > 2 and roundedmsr2 > 0", conf);
    expected1 = getExpectedQuery(cubeName,
      "select basecube.dim1 as dim1, basecube.dim11 as dim11, sum(basecube.msr12) as msr12 FROM ",
      null, " group by basecube.dim1, basecube.dim11 HAVING sum(basecube.msr12) > 2",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 = getExpectedQuery(cubeName,
      "select basecube.dim1 as dim1, basecube.dim11 as dim11, round(sum(basecube.msr2)/1000) as roundedmsr2 FROM ",
      null, " group by basecube.dim1, basecube.dim11 HAVING round(sum(basecube.msr2)/1000) > 0",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));

    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select coalesce(mq1.dim1, mq2.dim1) dim1, "
      + "coalesce(mq1.dim11, mq2.dim11) dim11, mq1.msr12 msr12, mq2.roundedmsr2 roundedmsr2 from ")
      || hqlQuery.toLowerCase().startsWith("select coalesce(mq1.dim1, mq2.dim1) dim1, "
        + "coalesce(mq1.dim11, mq2.dim11) dim11, mq2.msr12 msr12, mq1.roundedmsr2 roundedmsr2 from "), hqlQuery);
    assertTrue(hqlQuery.contains(joinSubString)
      && hqlQuery.endsWith(endSubString), hqlQuery);

    // Two having clauses and one complex expression in having which needs to be split over the two facts
    // And added as where clause outside
    hqlQuery = rewrite("select dim1, dim11, msr12, roundedmsr2 from basecube where " + TWO_DAYS_RANGE
      + "having flooredmsr12+roundedmsr2 <= 1000 and msr12 > 2 and roundedmsr2 > 0", conf);
    expected1 = getExpectedQuery(cubeName,
      "select basecube.dim1 as dim1, basecube.dim11 as dim11, sum(basecube.msr12) as msr12 , "
        + "floor(sum(basecube.msr12)) as alias0 FROM ",
      null, " group by basecube.dim1, basecube.dim11 HAVING sum(basecube.msr12) > 2",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));

    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select coalesce(mq1.dim1, mq2.dim1) dim1, "
      + "coalesce(mq1.dim11, mq2.dim11) dim11, mq2.msr12 msr12, mq1.roundedmsr2 roundedmsr2 from ")
      || hqlQuery.toLowerCase().startsWith("select coalesce(mq1.dim1, mq2.dim1) dim1, "
        + "coalesce(mq1.dim11, mq2.dim11) dim11, mq1.msr12 msr12, mq2.roundedmsr2 roundedmsr2 from "), hqlQuery);
    assertTrue(hqlQuery.contains(joinSubString)
      && hqlQuery.endsWith(endSubString + " WHERE ((alias0 + roundedmsr2) <= 1000)"), hqlQuery);

    // No push-down-able having clauses.
    hqlQuery = rewrite("select dim1, dim11, msr12, roundedmsr2 from basecube where " + TWO_DAYS_RANGE
      + "having flooredmsr12+roundedmsr2 <= 1000", conf);
    expected1 = getExpectedQuery(cubeName,
      "select basecube.dim1 as dim1, basecube.dim11 as dim11, sum(basecube.msr12) as msr12, "
        + "floor(sum(( basecube . msr12 ))) as `alias0` FROM ",
      null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 = getExpectedQuery(cubeName,
      "select basecube.dim1 as dim1, basecube.dim11 as dim11, round(sum(basecube.msr2)/1000) as roundedmsr2 FROM ",
      null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));

    assertFalse(hqlQuery.toLowerCase().contains("having"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select coalesce(mq1.dim1, mq2.dim1) dim1, "
      + "coalesce(mq1.dim11, mq2.dim11) dim11, mq2.msr12 msr12, mq1.roundedmsr2 roundedmsr2 from ")
      || hqlQuery.toLowerCase().startsWith("select coalesce(mq1.dim1, mq2.dim1) dim1, coalesce(mq1.dim11, mq2.dim11) "
        + "dim11, mq1.msr12 msr12, mq2.roundedmsr2 roundedmsr2 from "), hqlQuery);
    assertTrue(hqlQuery.contains(joinSubString)
      && hqlQuery.endsWith(endSubString + " WHERE ((alias0 + roundedmsr2) <= 1000)"), hqlQuery);

    // function over expression of two functions over measures
    hqlQuery = rewrite("select dim1, dim11, msr12, roundedmsr2 from basecube where " + TWO_DAYS_RANGE
      + "having round(flooredmsr12+roundedmsr2) <= 1000", conf);
    expected1 = getExpectedQuery(cubeName,
      "select basecube.dim1 as dim1, basecube.dim11 as dim11, sum(basecube.msr12) as msr12, "
        + "floor(sum(( basecube . msr12 ))) as `alias0` FROM ",
      null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 = getExpectedQuery(cubeName,
      "select basecube.dim1 as dim1, basecube.dim11 as dim11, round(sum(basecube.msr2)/1000) as roundedmsr2 FROM ",
      null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));

    assertFalse(hqlQuery.toLowerCase().contains("having"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select coalesce(mq1.dim1, mq2.dim1) dim1, "
      + "coalesce(mq1.dim11, mq2.dim11) dim11, mq2.msr12 msr12, mq1.roundedmsr2 roundedmsr2 from ")
      || hqlQuery.toLowerCase().startsWith("select coalesce(mq1.dim1, mq2.dim1) dim1, coalesce(mq1.dim11, mq2.dim11) "
        + "dim11, mq1.msr12 msr12, mq2.roundedmsr2 roundedmsr2 from "), hqlQuery);
    assertTrue(hqlQuery.contains(joinSubString)
      && hqlQuery.endsWith(endSubString + " WHERE (round((alias0 + roundedmsr2)) <= 1000)"), hqlQuery);


    // Following test cases only select dimensions, and all the measures are in having.
    // Mostly tests follow the same pattern as the above tests,
    // The extra thing to test is the inclusion of sub-expressions in select clauses.


    hqlQuery = rewrite("select dim1, dim11 from basecube where " + TWO_DAYS_RANGE
      + "having msr12 > 2 and roundedmsr2 > 0", conf);
    expected1 = getExpectedQuery(cubeName,
      "select basecube.dim1 as dim1, basecube.dim11 as dim11 FROM ",
      null, " group by basecube.dim1, basecube.dim11 HAVING sum(basecube.msr12) > 2",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 = getExpectedQuery(cubeName,
      "select basecube.dim1 as dim1, basecube.dim11 as dim11 FROM ",
      null, " group by basecube.dim1, basecube.dim11 HAVING round(sum(basecube.msr2)/1000) > 0",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    String begin = "select coalesce(mq1.dim1, mq2.dim1) dim1, coalesce(mq1.dim11, mq2.dim11) dim11 from ";
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(begin), hqlQuery);
    assertTrue(hqlQuery.contains(joinSubString) && hqlQuery.endsWith(endSubString), hqlQuery);

    hqlQuery = rewrite("select dim1, dim11 from basecube where " + TWO_DAYS_RANGE
      + "having msr12 > 2 and roundedmsr2 > 0 and msr2 > 100", conf);
    expected2 = getExpectedQuery(cubeName,
      "select basecube.dim1 as dim1, basecube.dim11 as dim11 FROM ", null,
      " group by basecube.dim1, basecube.dim11 HAVING round(sum(basecube.msr2)/1000) > 0 and sum(basecube.msr2) > 100",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(begin), hqlQuery);
    assertTrue(hqlQuery.contains(joinSubString) && hqlQuery.endsWith(endSubString), hqlQuery);

    hqlQuery = rewrite("select dim1, dim11 from basecube where " + TWO_DAYS_RANGE
      + "having flooredmsr12+roundedmsr2 <= 1000", conf);
    expected1 = getExpectedQuery(cubeName,
      "select basecube.dim1 as dim1, basecube.dim11 as dim11, "
        + "floor(sum(basecube.msr12)) as alias0 FROM ",
      null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 = getExpectedQuery(cubeName,
      "select basecube.dim1 as dim1, basecube.dim11 as dim11, round(sum(basecube.msr2/1000)) as alias1 FROM ",
      null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));

    assertFalse(hqlQuery.toLowerCase().contains("having"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(begin), hqlQuery);
    assertTrue(hqlQuery.contains(joinSubString)
      && hqlQuery.endsWith(endSubString + " WHERE ((alias0 + alias1) <= 1000)"), hqlQuery);

    hqlQuery = rewrite("select dim1, dim11 from basecube where " + TWO_DAYS_RANGE
      + "having msr12 > 2 and roundedmsr2 > 0 and flooredmsr12+roundedmsr2 <= 1000", conf);
    expected1 = getExpectedQuery(cubeName,
      "select basecube.dim1 as dim1, basecube.dim11 as dim11, "
        + "floor(sum(( basecube . msr12 ))) as `alias0` FROM ",
      null, " group by basecube.dim1, basecube.dim11 having sum(basecube.msr12) > 2",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 = getExpectedQuery(cubeName,
      "select basecube.dim1 as dim1, basecube.dim11 as dim11, round(sum(basecube.msr2)/1000) as alias1 FROM ",
      null, " group by basecube.dim1, basecube.dim11 having round(sum(basecube.msr2)/1000) > 0",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));

    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(begin), hqlQuery);
    assertTrue(hqlQuery.contains(joinSubString)
      && hqlQuery.endsWith(endSubString + " WHERE ((alias0 + alias1) <= 1000)"), hqlQuery);

    hqlQuery = rewrite("select dim1, dim11 from basecube where " + TWO_DAYS_RANGE
      + "having msr12 > 2 or roundedmsr2 > 0 or flooredmsr12+roundedmsr2 <= 1000", conf);
    expected1 = getExpectedQuery(cubeName,
      "select basecube.dim1 as dim1, basecube.dim11 as dim11, "
        + "sum(basecube.msr12) as alias0, floor(sum(basecube.msr12)) as alias2 FROM ",
      null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 = getExpectedQuery(cubeName,
      "select basecube.dim1 as dim1, basecube.dim11 as dim11, round(sum(basecube.msr2)/1000) as alias1 FROM ",
      null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    String havingToWhere = " WHERE ((alias0 > 2) or (alias1 > 0) or ((alias2 + alias1) <= 1000))";

    assertFalse(hqlQuery.toLowerCase().contains("having"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(begin), hqlQuery);
    assertTrue(hqlQuery.contains(joinSubString)
      && hqlQuery.endsWith(endSubString + havingToWhere), hqlQuery);
  }
}

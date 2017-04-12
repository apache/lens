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

import static com.google.common.collect.Sets.newHashSet;
import static org.apache.lens.cube.metadata.DateFactory.*;
import static org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode.COLUMN_NOT_FOUND;
import static org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode.PART_COL_DOES_NOT_EXIST;
import static
  org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode.STORAGE_NOT_AVAILABLE_IN_RANGE;
import static org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode.TIME_RANGE_NOT_ANSWERABLE;
import static org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode.UNSUPPORTED_STORAGE;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.lens.cube.error.NoCandidateFactAvailableException;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ParseException;

import org.joda.time.DateTime;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

public class TestTimeRangeResolver extends TestQueryRewrite {

  private final String cubeName = CubeTestSetup.TEST_CUBE_NAME;

  private Configuration conf;

  @BeforeTest
  public void setupDriver() throws Exception {
    conf = new Configuration();
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1,C2");
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, true);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY, true);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT, true);
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER, false);
  }

  @Override
  public Configuration getConf() {
    return new Configuration(conf);
  }

  @Test
  public void testFactValidity() throws ParseException, LensException, HiveException, ClassNotFoundException {
    LensException e =
      getLensExceptionInRewrite("select msr2 from " + cubeName + " where " + LAST_YEAR_RANGE,
        getConf());
    NoCandidateFactAvailableException ne = (NoCandidateFactAvailableException) e;
    PruneCauses.BriefAndDetailedError causes = ne.getJsonMessage();
    assertTrue(causes.getBrief().contains("No storages available for all of these time ranges: "
          + "[dt [2016-01-01-00:00:00,000 to 2017-01-01-00:00:00,000)]"), causes.getBrief());
    assertEquals(causes.getDetails().values().stream().flatMap(Collection::stream)
        .map(CandidateTablePruneCause::getCause).collect(Collectors.toSet()), newHashSet(COLUMN_NOT_FOUND,
      PART_COL_DOES_NOT_EXIST, UNSUPPORTED_STORAGE, STORAGE_NOT_AVAILABLE_IN_RANGE));
  }

  @Test
  public void testAbsoluteValidity() throws ParseException, HiveException, LensException {
    CubeQueryContext ctx =
      rewriteCtx("select msr12 from basecube where " + TWO_DAYS_RANGE + " or " + TWO_DAYS_RANGE_BEFORE_4_DAYS,
        getConf());
    List<CandidateTablePruneCause> causes = findPruningMessagesForStorage("c3_testfact_deprecated",
      ctx.getStoragePruningMsgs());
    assertEquals(causes.size(), 1);
    assertEquals(causes.get(0).getCause(), UNSUPPORTED_STORAGE);

    causes = findPruningMessagesForStorage("c4_testfact_deprecated",
      ctx.getStoragePruningMsgs());
    assertEquals(causes.size(), 1);
    assertEquals(causes.get(0).getCause(), UNSUPPORTED_STORAGE);

    // testfact_deprecated's validity should be in between of both ranges. So both ranges should be in the invalid list
    // That would prove that parsing of properties has gone through successfully

    causes = findPruningMessagesForStorage("c1_testfact_deprecated",
      ctx.getStoragePruningMsgs());
    assertEquals(causes.size(), 1);
    assertEquals(causes.get(0).getCause(), TIME_RANGE_NOT_ANSWERABLE);

    causes = findPruningMessagesForStorage("c2_testfact_deprecated",
      ctx.getStoragePruningMsgs());
    assertEquals(causes.size(), 1);
    assertEquals(causes.get(0).getCause(), TIME_RANGE_NOT_ANSWERABLE);
  }

  @Test
  public void testCustomNow() throws Exception {
    Configuration conf = getConf();
    DateTime dt = new DateTime(1990, 3, 23, 12, 0, 0, 0);
    conf.setLong(LensConfConstants.QUERY_CURRENT_TIME_IN_MILLIS, dt.getMillis());
    NoCandidateFactAvailableException e =
      (NoCandidateFactAvailableException)getLensExceptionInRewrite(
        "select msr12 from basecube where time_range_in(d_time, 'now.day-275days','now')", conf);
    TimeRange timeRange = e.getCubeQueryContext().getTimeRanges().get(0);
    // Month starts from zero.
    Calendar from = new GregorianCalendar(1989, 5, 21, 0, 0, 0);
    assertEquals(timeRange.getFromDate(), from.getTime());
    assertEquals(timeRange.getToDate(), dt.toDate());
  }

  /**
   *
   * @param stoargeName  storageName_factName
   * @param allStoragePruningMsgs
   * @return
   */
  private static List<CandidateTablePruneCause> findPruningMessagesForStorage(String stoargeName,
    PruneCauses<Candidate> allStoragePruningMsgs) {
    for (Candidate sc : allStoragePruningMsgs.keySet()) {
      if (sc instanceof StorageCandidate) {
        if (((StorageCandidate)sc).getName().equals(stoargeName)) {
          return allStoragePruningMsgs.get(sc);
        }
      }
    }
    return  new ArrayList<CandidateTablePruneCause>();
  }

}

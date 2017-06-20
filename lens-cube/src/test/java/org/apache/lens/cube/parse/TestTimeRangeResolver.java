/*
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
import static org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode.TIME_RANGE_NOT_ANSWERABLE;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.*;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ParseException;

import org.joda.time.DateTime;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TestTimeRangeResolver extends TestQueryRewrite {

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
    String query = "select msr2 from " + CubeTestSetup.TEST_CUBE_NAME + " where "  + LAST_YEAR_RANGE;
    LensException e = getLensExceptionInRewrite(query, getConf());
    assertEquals(e.getErrorInfo().getErrorName(), "NO_UNION_CANDIDATE_AVAILABLE");
  }

  @Test
  public void testAbsoluteValidity() throws ParseException, HiveException, LensException {
    CubeQueryContext ctx =
      rewriteCtx("select msr12 from basecube where " + TWO_DAYS_RANGE + " or " + TWO_DAYS_RANGE_BEFORE_4_DAYS,
        getConf());
    List<CandidateTablePruneCause> causes = findPruningMessagesForStorage("c3_testfact_deprecated",
      ctx.getStoragePruningMsgs());
    assertTrue(causes.isEmpty());

    causes = findPruningMessagesForStorage("c4_testfact_deprecated",
      ctx.getStoragePruningMsgs());
    assertTrue(causes.isEmpty());

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
    String query = "select msr12 from basecube where time_range_in(d_time, 'now.day-275days','now')";
    LensException e = getLensExceptionInRewrite(query, conf);
    assertEquals(e.getMessage(), "NO_CANDIDATE_FACT_AVAILABLE[Range not answerable]");
  }

  /**
   *
   * @param stoargeName  storageName_factName
   * @param allStoragePruningMsgs all pruning messages
   * @return pruning messages for storagetable
   */
  private static List<CandidateTablePruneCause> findPruningMessagesForStorage(String stoargeName,
    PruneCauses<Candidate> allStoragePruningMsgs) {
    for (Candidate sc : allStoragePruningMsgs.keySet()) {
      if (sc instanceof StorageCandidate) {
        if (((StorageCandidate)sc).getStorageTable().equals(stoargeName)) {
          return allStoragePruningMsgs.get(sc);
        }
      }
    }
    return new ArrayList<>();
  }
}

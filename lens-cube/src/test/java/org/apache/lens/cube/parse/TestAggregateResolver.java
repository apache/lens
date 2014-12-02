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

import static org.apache.lens.cube.parse.CubeTestSetup.getExpectedQuery;
import static org.apache.lens.cube.parse.CubeTestSetup.getWhereForDailyAndHourly2days;
import static org.apache.lens.cube.parse.CubeTestSetup.getWhereForHourly2days;
import static org.apache.lens.cube.parse.CubeTestSetup.twoDaysRange;
import static org.apache.lens.cube.parse.TestCubeRewriter.compareQueries;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.lens.cube.parse.CandidateFact;
import org.apache.lens.cube.parse.CubeQueryConfUtil;
import org.apache.lens.cube.parse.CubeQueryContext;
import org.apache.lens.cube.parse.CubeQueryRewriter;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestAggregateResolver extends TestQueryRewrite {

  private Configuration conf;
  private final String cubeName = CubeTestSetup.TEST_CUBE_NAME;

  @BeforeTest
  public void setupDriver() throws Exception {
    conf = new Configuration();
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1,C2");
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, true);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY, true);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT, true);
  }

  private CubeQueryContext rewrittenQuery;

  @Test
  public void testAggregateResolver() throws Exception {
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER, false);

    // pass
    String q1 = "SELECT cityid, testCube.msr2 from testCube where " + twoDaysRange;

    // pass
    String q2 = "SELECT cityid, testCube.msr2 * testCube.msr3 from testCube where " + twoDaysRange;

    // pass
    String q3 = "SELECT cityid, sum(testCube.msr2) from testCube where " + twoDaysRange;

    // pass
    String q4 = "SELECT cityid, sum(testCube.msr2) from testCube where " + twoDaysRange + " having testCube.msr2 > 100";

    // pass
    String q5 =
        "SELECT cityid, testCube.msr2 from testCube where " + twoDaysRange
            + " having testCube.msr2 + testCube.msr3 > 100";

    // pass
    String q6 =
        "SELECT cityid, testCube.msr2 from testCube where " + twoDaysRange
            + " having testCube.msr2 > 100 AND testCube.msr2 < 1000";

    // pass
    String q7 =
        "SELECT cityid, sum(testCube.msr2) from testCube where " + twoDaysRange
            + " having (testCube.msr2 > 100) OR (testcube.msr2 < 100" + " AND max(testcube.msr3) > 1000)";

    // pass
    String q8 = "SELECT cityid, sum(testCube.msr2) * max(testCube.msr3) from" + " testCube where " + twoDaysRange;

    // pass
    String q9 =
        "SELECT cityid c1, max(msr3) m3 from testCube where " + "c1 > 100 and " + twoDaysRange + " having (msr2 < 100"
            + " AND m3 > 1000)";

    String q10 = "SELECT cityid, round(testCube.msr2) from testCube where " + twoDaysRange;

    String expectedq1 =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " sum(testCube.msr2) from ", null,
            "group by testcube.cityid", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    String expectedq2 =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " sum(testCube.msr2) * max(testCube.msr3) from ", null,
            "group by testcube.cityid", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    String expectedq3 =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " sum(testCube.msr2) from ", null,
            "group by testcube.cityid", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    String expectedq4 =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " sum(testCube.msr2) from ", null,
            "group by testcube.cityid having" + " sum(testCube.msr2) > 100",
            getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    String expectedq5 =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " sum(testCube.msr2) from ", null,
            "group by testcube.cityid having" + " sum(testCube.msr2) + max(testCube.msr3) > 100",
            getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    String expectedq6 =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " sum(testCube.msr2) from ", null,
            "group by testcube.cityid having" + " sum(testCube.msr2) > 100 and sum(testCube.msr2) < 1000",
            getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    String expectedq7 =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " sum(testCube.msr2) from ", null,
            "group by testcube.cityid having" + " sum(testCube.msr2) > 100) OR (sum(testCube.msr2) < 100 AND"
                + " max(testcube.msr3) > 1000)", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    String expectedq8 =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " sum(testCube.msr2) * max(testCube.msr3) from ", null,
            "group by testcube.cityid", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    String expectedq9 =
        getExpectedQuery(cubeName, "SELECT testcube.cityid c1," + " max(testCube.msr3) m3 from ", "c1 > 100",
            "group by testcube.cityid" + " having sum(testCube.msr2) < 100 AND (m3 > 1000)",
            getWhereForDailyAndHourly2days(cubeName, "c2_testfact"));
    String expectedq10 =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " round(sum(testCube.msr2)) from ", null,
            "group by testcube.cityid", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));

    String tests[] = { q1, q2, q3, q4, q5, q6, q7, q8, q9, q10 };
    String expected[] =
        { expectedq1, expectedq2, expectedq3, expectedq4, expectedq5, expectedq6, expectedq7, expectedq8, expectedq9,
            expectedq10 };

    for (int i = 0; i < tests.length; i++) {
      String hql = rewrite(tests[i], conf);
      System.out.println("hql[" + i + "]:" + hql);
      compareQueries(expected[i], hql);
    }
    aggregateFactSelectionTests(conf);
    rawFactSelectionTests(conf);
  }

  @Test
  public void testAggregateResolverOff() throws SemanticException, ParseException {
    Configuration conf2 = new Configuration(this.conf);
    conf2.setBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER, true);

    // Test if raw fact is selected for query with no aggregate function on a
    // measure, with aggregate resolver disabled
    String query = "SELECT cityid, testCube.msr2 FROM testCube WHERE " + twoDaysRange;
    CubeQueryContext cubeql = rewriteCtx(query, conf2);
    String hQL = cubeql.toHQL();
    Assert.assertEquals(1, cubeql.getCandidateFactTables().size());
    CandidateFact candidateFact = cubeql.getCandidateFactTables().iterator().next();
    Assert.assertEquals("testFact2_raw".toLowerCase(), candidateFact.fact.getName().toLowerCase());
    String expectedQL =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " testCube.msr2 from ", null, null,
            getWhereForHourly2days("c1_testfact2_raw"));
    compareQueries(expectedQL, hQL);

    aggregateFactSelectionTests(conf2);
    rawFactSelectionTests(conf2);
  }

  private void aggregateFactSelectionTests(Configuration conf) throws SemanticException, ParseException {
    String query = "SELECT count(distinct cityid) from testcube where " + twoDaysRange;
    CubeQueryContext cubeql = rewriteCtx(query, conf);
    String hQL = cubeql.toHQL();
    String expectedQL =
        getExpectedQuery(cubeName, "SELECT count(distinct testcube.cityid)," + " from ", null, null,
            getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expectedQL, hQL);

    query = "SELECT distinct cityid from testcube where " + twoDaysRange;
    hQL = rewrite(query, conf);
    expectedQL =
        getExpectedQuery(cubeName, "SELECT distinct testcube.cityid," + " from ", null, null,
            getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expectedQL, hQL);

    // with aggregate resolver on/off, msr with its default aggregate around it
    // should pick up aggregated fact
    query = "SELECT cityid, sum(testCube.msr2) FROM testCube WHERE " + twoDaysRange;
    cubeql = rewriteCtx(query, conf);
    hQL = cubeql.toHQL();
    expectedQL =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " sum(testCube.msr2) from ", null,
            "group by testcube.cityid", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expectedQL, hQL);

    query = "SELECT cityid, sum(testCube.msr2) m2 FROM testCube WHERE " + twoDaysRange + " order by m2";
    cubeql = rewriteCtx(query, conf);
    hQL = cubeql.toHQL();
    expectedQL =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " sum(testCube.msr2) m2 from ", null,
            "group by testcube.cityid order by m2 asc", getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expectedQL, hQL);

    query = "SELECT cityid, sum(testCube.msr2) FROM testCube WHERE " + twoDaysRange + " having max(msr3) > 100";
    cubeql = rewriteCtx(query, conf);
    hQL = cubeql.toHQL();
    expectedQL =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " sum(testCube.msr2) from ", null,
            "group by testcube.cityid having max(testcube.msr3) > 100",
            getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expectedQL, hQL);
  }

  private void rawFactSelectionTests(Configuration conf) throws SemanticException, ParseException {
    // Check a query with non default aggregate function
    String query = "SELECT cityid, avg(testCube.msr2) FROM testCube WHERE " + twoDaysRange;
    CubeQueryContext cubeql = rewriteCtx(query, conf);
    String hQL = cubeql.toHQL();
    Assert.assertEquals(1, cubeql.getCandidateFactTables().size());
    CandidateFact candidateFact = cubeql.getCandidateFactTables().iterator().next();
    Assert.assertEquals("testFact2_raw".toLowerCase(), candidateFact.fact.getName().toLowerCase());
    String expectedQL =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " avg(testCube.msr2)) from ", null,
            "group by testcube.cityid", getWhereForHourly2days("c1_testfact2_raw"));
    compareQueries(expectedQL, hQL);

    // query with measure in a where clause
    query = "SELECT cityid, sum(testCube.msr2) FROM testCube WHERE testCube.msr1 < 100 and " + twoDaysRange;
    cubeql = rewriteCtx(query, conf);
    Assert.assertEquals(1, cubeql.getCandidateFactTables().size());
    candidateFact = cubeql.getCandidateFactTables().iterator().next();
    Assert.assertEquals("testFact2_raw".toLowerCase(), candidateFact.fact.getName().toLowerCase());
    hQL = cubeql.toHQL();
    expectedQL =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " sum(testCube.msr2)) from ", "testcube.msr1 < 100",
            "group by testcube.cityid", getWhereForHourly2days("c1_testfact2_raw"));
    compareQueries(expectedQL, hQL);

    query = "SELECT cityid, testCube.msr2 FROM testCube WHERE testCube.msr2 < 100 and " + twoDaysRange;
    cubeql = rewriteCtx(query, conf);
    Assert.assertEquals(1, cubeql.getCandidateFactTables().size());
    candidateFact = cubeql.getCandidateFactTables().iterator().next();
    Assert.assertEquals("testFact2_raw".toLowerCase(), candidateFact.fact.getName().toLowerCase());
    hQL = cubeql.toHQL();
    expectedQL =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " testCube.msr2 from ", "testcube.msr2 < 100", null,
            getWhereForHourly2days("c1_testfact2_raw"));
    compareQueries(expectedQL, hQL);

    query = "SELECT cityid, sum(testCube.msr2) FROM testCube WHERE " + twoDaysRange + " group by testCube.msr1";
    cubeql = rewriteCtx(query, conf);
    Assert.assertEquals(1, cubeql.getCandidateFactTables().size());
    candidateFact = cubeql.getCandidateFactTables().iterator().next();
    Assert.assertEquals("testFact2_raw".toLowerCase(), candidateFact.fact.getName().toLowerCase());
    hQL = cubeql.toHQL();
    expectedQL =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " sum(testCube.msr2)) from ", null,
            " group by testCube.msr1, testcube.cityid", getWhereForHourly2days("c1_testfact2_raw"));
    compareQueries(expectedQL, hQL);

    query = "SELECT cityid, sum(testCube.msr2) FROM testCube WHERE " + twoDaysRange + " group by testCube.msr3";
    cubeql = rewriteCtx(query, conf);
    Assert.assertEquals(1, cubeql.getCandidateFactTables().size());
    candidateFact = cubeql.getCandidateFactTables().iterator().next();
    Assert.assertEquals("testFact2_raw".toLowerCase(), candidateFact.fact.getName().toLowerCase());
    hQL = cubeql.toHQL();
    expectedQL =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " sum(testCube.msr2)) from ", null,
            " group by testCube.msr3, testcube.cityid", getWhereForHourly2days("c1_testfact2_raw"));
    compareQueries(expectedQL, hQL);

    query = "SELECT cityid, sum(testCube.msr2) FROM testCube WHERE " + twoDaysRange + " order by testCube.msr1";
    cubeql = rewriteCtx(query, conf);
    Assert.assertEquals(1, cubeql.getCandidateFactTables().size());
    candidateFact = cubeql.getCandidateFactTables().iterator().next();
    Assert.assertEquals("testFact2_raw".toLowerCase(), candidateFact.fact.getName().toLowerCase());
    hQL = cubeql.toHQL();
    expectedQL =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " sum(testCube.msr2)) from ", null,
            " group by testcube.cityid order by testcube.msr1 asc", getWhereForHourly2days("c1_testfact2_raw"));
    compareQueries(expectedQL, hQL);

    query = "SELECT cityid, sum(testCube.msr2) FROM testCube WHERE " + twoDaysRange + " order by testCube.msr3";
    cubeql = rewriteCtx(query, conf);
    Assert.assertEquals(1, cubeql.getCandidateFactTables().size());
    candidateFact = cubeql.getCandidateFactTables().iterator().next();
    Assert.assertEquals("testFact2_raw".toLowerCase(), candidateFact.fact.getName().toLowerCase());
    hQL = cubeql.toHQL();
    expectedQL =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " sum(testCube.msr2)) from ", null,
            " group by testcube.cityid order by testcube.msr3 asc", getWhereForHourly2days("c1_testfact2_raw"));
    compareQueries(expectedQL, hQL);

    query = "SELECT distinct cityid, round(testCube.msr2) from testCube where " + twoDaysRange;
    cubeql = rewriteCtx(query, conf);
    Assert.assertEquals(1, cubeql.getCandidateFactTables().size());
    candidateFact = cubeql.getCandidateFactTables().iterator().next();
    Assert.assertEquals("testFact2_raw".toLowerCase(), candidateFact.fact.getName().toLowerCase());
    hQL = cubeql.toHQL();
    expectedQL =
        getExpectedQuery(cubeName, "SELECT distinct testcube.cityid," + " round(testCube.msr2) from ", null, null,
            getWhereForHourly2days("c1_testfact2_raw"));
    compareQueries(expectedQL, hQL);

    query = "SELECT cityid, count(distinct(testCube.msr2)) from testCube where " + twoDaysRange;
    cubeql = rewriteCtx(query, conf);
    Assert.assertEquals(1, cubeql.getCandidateFactTables().size());
    candidateFact = cubeql.getCandidateFactTables().iterator().next();
    Assert.assertEquals("testFact2_raw".toLowerCase(), candidateFact.fact.getName().toLowerCase());
    hQL = cubeql.toHQL();
    expectedQL =
        getExpectedQuery(cubeName, "SELECT testcube.cityid, count(distinct testCube.msr2) from ", null,
            "group by testcube.cityid", getWhereForHourly2days("c1_testfact2_raw"));
    compareQueries(expectedQL, hQL);

    // query with no default aggregate measure
    query = "SELECT cityid, round(testCube.msr1) from testCube where " + twoDaysRange;
    cubeql = rewriteCtx(query, conf);
    Assert.assertEquals(1, cubeql.getCandidateFactTables().size());
    candidateFact = cubeql.getCandidateFactTables().iterator().next();
    Assert.assertEquals("testFact2_raw".toLowerCase(), candidateFact.fact.getName().toLowerCase());
    hQL = cubeql.toHQL();
    expectedQL =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " round(testCube.msr1) from ", null, null,
            getWhereForHourly2days("c1_testfact2_raw"));
    compareQueries(expectedQL, hQL);

    query = "SELECT distinct cityid, round(testCube.msr1) from testCube where " + twoDaysRange;
    cubeql = rewriteCtx(query, conf);
    Assert.assertEquals(1, cubeql.getCandidateFactTables().size());
    candidateFact = cubeql.getCandidateFactTables().iterator().next();
    Assert.assertEquals("testFact2_raw".toLowerCase(), candidateFact.fact.getName().toLowerCase());
    hQL = cubeql.toHQL();
    expectedQL =
        getExpectedQuery(cubeName, "SELECT distinct testcube.cityid," + " round(testCube.msr1) from ", null, null,
            getWhereForHourly2days("c1_testfact2_raw"));
    compareQueries(expectedQL, hQL);

    query = "SELECT cityid, count(distinct(testCube.msr1)) from testCube where " + twoDaysRange;
    cubeql = rewriteCtx(query, conf);
    Assert.assertEquals(1, cubeql.getCandidateFactTables().size());
    candidateFact = cubeql.getCandidateFactTables().iterator().next();
    Assert.assertEquals("testFact2_raw".toLowerCase(), candidateFact.fact.getName().toLowerCase());
    hQL = cubeql.toHQL();
    expectedQL =
        getExpectedQuery(cubeName, "SELECT testcube.cityid, count(distinct testCube.msr1) from ", null,
            "group by testcube.cityid", getWhereForHourly2days("c1_testfact2_raw"));
    compareQueries(expectedQL, hQL);

    query = "SELECT cityid, sum(testCube.msr1) from testCube where " + twoDaysRange;
    cubeql = rewriteCtx(query, conf);
    Assert.assertEquals(1, cubeql.getCandidateFactTables().size());
    candidateFact = cubeql.getCandidateFactTables().iterator().next();
    Assert.assertEquals("testFact2_raw".toLowerCase(), candidateFact.fact.getName().toLowerCase());
    hQL = cubeql.toHQL();
    expectedQL =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " sum(testCube.msr1)) from ", null,
            "group by testcube.cityid", getWhereForHourly2days("c1_testfact2_raw"));

    query = "SELECT cityid, sum(testCube.msr2) FROM testCube WHERE " + twoDaysRange + " having max(msr1) > 100";
    cubeql = rewriteCtx(query, conf);
    hQL = cubeql.toHQL();
    expectedQL =
        getExpectedQuery(cubeName, "SELECT testcube.cityid," + " sum(testCube.msr2) from ", null,
            "group by testcube.cityid having max(testcube.msr1) > 100", getWhereForHourly2days("c1_testfact2_raw"));
    compareQueries(expectedQL, hQL);
  }
}

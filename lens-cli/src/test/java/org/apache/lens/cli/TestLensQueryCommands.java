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
package org.apache.lens.cli;

import static org.testng.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.*;

import javax.ws.rs.BadRequestException;
import javax.xml.datatype.DatatypeFactory;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.metastore.*;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.cli.commands.LensCubeCommands;
import org.apache.lens.cli.commands.LensDimensionTableCommands;
import org.apache.lens.cli.commands.LensQueryCommands;
import org.apache.lens.client.LensClient;
import org.apache.lens.driver.hive.TestHiveDriver;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;

import org.testng.annotations.*;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class TestLensQueryCommands.
 */
@Slf4j
public class TestLensQueryCommands extends LensCliApplicationTest {

  /** The explain plan. */
  private static String explainPlan = "TOK_QUERY\n" + "   TOK_FROM\n" + "      TOK_TABREF\n" + "         TOK_TABNAME\n"
    + "            local_dim_table\n" + "         test_dim\n" + "   TOK_INSERT\n" + "      TOK_DESTINATION\n"
    + "         TOK_DIR\n" + "            TOK_TMP_FILE\n" + "      TOK_SELECT\n" + "         TOK_SELEXPR\n"
    + "            .\n" + "               TOK_TABLE_OR_COL\n" + "                  test_dim\n"
    + "               id\n" + "         TOK_SELEXPR\n" + "            .\n" + "               TOK_TABLE_OR_COL\n"
    + "                  test_dim\n" + "               name\n" + "      TOK_WHERE\n" + "         =\n"
    + "            .\n" + "               TOK_TABLE_OR_COL\n" + "                  test_dim\n"
    + "               dt\n" + "            'latest'";
  private File resDir;

  @BeforeClass
  public void createResultsDir() {
    resDir = new File("target/lens-results");
    assertTrue(resDir.exists() || resDir.mkdirs());
  }

  @DataProvider(name = "queryCommands")
  public Object[][] provideQueryCommands() throws Exception {
    LensQueryCommands queryCommands = setupQueryCommands(false);
    LensQueryCommands queryCommandsWithMetrics = setupQueryCommands(true);

    return new Object[][] {
      { queryCommands },
      { queryCommandsWithMetrics },
    };
  }

  private LensQueryCommands setupQueryCommands(boolean withMetrics) throws Exception {
    LensClient client = new LensClient();
    client.setConnectionParam("lens.query.enable.persistent.resultset.indriver", "false");
    if (withMetrics) {
      client.setConnectionParam("lens.query.enable.metrics.per.query", "true");
    }

    LensQueryCommands qCom = new LensQueryCommands();
    qCom.setClient(client);
    return qCom;
  }

  private void closeClientConnection(LensQueryCommands queryCommands) {
    queryCommands.getClient().closeConnection();
  }

  /**
   * Test execute sync query
   */
  @Test(dataProvider = "queryCommands")
  public void executeSyncQuery(LensQueryCommands qCom) {
    assertEquals(qCom.getAllPreparedQueries("all", "", -1, -1), "No prepared queries");
    String sql = "cube select id,name from test_dim";
    String result = qCom.executeQuery(sql, false, "testQuery2");
    assertTrue(result.contains("1\tfirst"), result);
    closeClientConnection(qCom);
  }

  /**
   * Test prepared query.
   *
   * @param qCom the q com
   * @throws Exception the exception
   */
  @Test(dataProvider = "queryCommands")
  public void preparedQuery(LensQueryCommands qCom) throws Exception {
    long submitTime = System.currentTimeMillis();
    String sql = "cube select id, name from test_dim";
    String result = qCom.getAllPreparedQueries("testPreparedName", "all", submitTime, Long.MAX_VALUE);

    assertEquals(result, "No prepared queries");
    final String qh = qCom.prepare(sql, "testPreparedName");
    result = qCom.getAllPreparedQueries("testPreparedName", "all", submitTime, System.currentTimeMillis());
    assertEquals(qh, result);

    result = qCom.getPreparedStatus(qh);
    assertTrue(result.contains("User query:cube select id, name from test_dim"));
    assertTrue(result.contains(qh));

    result = qCom.executePreparedQuery(qh, false, "testPrepQuery1");

    log.warn("XXXXXX Prepared query sync result is  " + result);
    assertTrue(result.contains("1\tfirst"));

    String handle = qCom.executePreparedQuery(qh, true, "testPrepQuery2");
    log.debug("Perpared query handle is   " + handle);
    while (!qCom.getClient().getQueryStatus(handle).finished()) {
      Thread.sleep(5000);
    }
    String status = qCom.getStatus(handle);
    log.debug("Prepared Query Status is  " + status);
    assertTrue(status.contains("Status: SUCCESSFUL"));

    //Fetch results
    result = qCom.getQueryResults(handle, null, true);
    log.debug("Prepared Query Result is  " + result);
    assertTrue(result.contains("1\tfirst"));
    //Wait for query to purge. Purger runs every second
    Thread.sleep(3000);
    //Fetch again. Should not get resultset
    result = qCom.getQueryResults(handle, null, true);
    log.debug("Prepared Query Result is  " + result);
    assertTrue(result.contains("Resultset not available for the query"), "Query is not purged yet " + handle);

    result = qCom.destroyPreparedQuery(qh);

    log.debug("destroy result is " + result);
    assertEquals("Successfully destroyed " + qh, result);
    result = qCom.getAllPreparedQueries("testPreparedName", "all", submitTime, Long.MAX_VALUE);

    assertEquals(result, "No prepared queries");

    final String qh2 = qCom.explainAndPrepare(sql, "testPrepQuery3");
    assertTrue(qh2.contains(explainPlan));
    String handles = qCom.getAllPreparedQueries("testPrepQuery3", "all", -1, Long.MAX_VALUE);
    assertFalse(handles.contains("No prepared queries"), handles);

    String handles2 = qCom.getAllPreparedQueries("testPrepQuery3", "all", -1, submitTime - 1);
    assertFalse(handles2.contains(qh), handles2);
    result = qCom.destroyPreparedQuery(handles);
    assertEquals("Successfully destroyed " + handles, result);
    closeClientConnection(qCom);
  }

  /**
   * Test fail prepared query.
   *
   * @param qCom the q com
   * @throws Exception the exception
   */
  //@Test(dataProvider = "queryCommands")
  public void failPreparedQuery(LensQueryCommands qCom) throws Exception {
    LensClient client = qCom.getClient();
    client.setConnectionParam("hive.exec.driver.run.hooks", TestHiveDriver.FailHook.class.getCanonicalName());
    String sql = "cube select id, name from test_dim";
    final String result = qCom.explainAndPrepare(sql, "testFailPrepared");
    assertTrue(result.contains("Explain FAILED:Error while processing statement: FAILED: Hive Internal Error:"
      + " java.lang.ClassNotFoundException(org.apache.lens.driver.hive.TestHiveDriver.FailHook)"));
    client.setConnectionParam("hive.exec.driver.run.hooks", "");
  }

  /**
   * Test explain query.
   *
   * @param qCom the q com
   * @throws Exception the exception
   */
  @Test(dataProvider = "queryCommands")
  public void explainQuery(LensQueryCommands qCom) throws Exception {
    String sql = "cube select id, name from test_dim";
    String result = qCom.explainQuery(sql, null);

    log.debug(result);
    assertTrue(result.contains(explainPlan));
    closeClientConnection(qCom);
  }

  /**
   * Test explain fail query.
   *
   * @param qCom the q com
   * @throws Exception the exception
   */
  //@Test(dataProvider = "queryCommands")
  public void explainFailQuery(LensQueryCommands qCom) throws Exception {
    String sql = "cube select id2, name from test_dim";
    String result = qCom.explainQuery(sql, null);

    log.debug(result);
    assertTrue(result.contains("Explain FAILED:"));

    result = qCom.explainAndPrepare(sql, "");
    assertTrue(result.contains("Explain FAILED:"));
  }

  /**
   * Test execute async query.
   *
   * @param qCom the q com
   * @throws Exception the exception
   */
  @Test(dataProvider = "queryCommands")
  public void executeAsyncQuery(LensQueryCommands qCom) throws Exception {
    System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
    String sql = "cube select id,name from test_dim";
    long submitTime = System.currentTimeMillis();
    String qh = qCom.executeQuery(sql, true, "testQuery1");
    String user = qCom.getClient().getLensStatement(new QueryHandle(UUID.fromString(qh)))
        .getQuery().getSubmittedUser();
    String result = qCom.getAllQueries("", "testQuery1", user, "", -1, Long.MAX_VALUE);
    // this is because previous query has run two query handle will be there
    assertTrue(result.contains(qh), result);
    assertTrue(result.contains("Total number of queries"));
    String[] resultSplits = result.split("\n");
    // assert on the number of queries
    assertEquals(String.valueOf(resultSplits.length - 1), resultSplits[resultSplits.length - 1].split(": ")[1]);
    assertEquals(qCom.getOrDefaultQueryHandleString(null), qh);
    QueryStatus queryStatus = qCom.getClient().getQueryStatus(qh);
    while (!queryStatus.finished()) {
      if (queryStatus.launched()) {
        String details = qCom.getDetails(null);
        assertTrue(details.contains("Driver Query:"));
      }
      Thread.sleep(1000);
      queryStatus = qCom.getClient().getQueryStatus(qh);
    }

    // Check that query name searching is 'ilike'
    String result2 = qCom.getAllQueries("", "query", "all", "", -1, Long.MAX_VALUE);
    assertTrue(result2.contains(qh), result2);
    assertTrue(qCom.getStatus(qh).contains("Status: SUCCESSFUL"));
    String details = qCom.getDetails(qh);
    assertTrue(details.contains("Driver Query:"));

    result = qCom.getQueryResults(null, null, true);
    assertTrue(result.contains("1\tfirst"));

    // Kill query is not tested as there is no deterministic way of killing a query

    result = qCom.getAllQueries("SUCCESSFUL", "", "all", "", -1, Long.MAX_VALUE);
    assertTrue(result.contains(qh), result);

    result = qCom.getAllQueries("FAILED", "", "all", "", -1, Long.MAX_VALUE);
    if (!result.contains("No queries")) {
      // Make sure valid query handles are returned
      String[] handles = StringUtils.split(result, "\n");
      for (String handle : handles) {
        if (!handle.contains("Total number of queries")) {
          QueryHandle.fromString(handle.trim());
        }
      }
    }

    String queryName = qCom.getClient().getLensStatement(new QueryHandle(UUID.fromString(qh))).getQuery()
            .getQueryName();
    assertTrue("testQuery1".equalsIgnoreCase(queryName), queryName);
    result = qCom.getAllQueries("", "", "", "", submitTime, System.currentTimeMillis());
    assertTrue(result.contains(qh), result);

    result = qCom.getAllQueries("", "fooBar", "all", "", submitTime, System.currentTimeMillis());
    assertTrue(result.contains("No queries"), result);

    result = qCom.getAllQueries("SUCCESSFUL", "", "all", "", submitTime, System.currentTimeMillis());
    assertTrue(result.contains(qh));

    result = qCom.getAllQueries("SUCCESSFUL", "", "all", "", submitTime - 5000, submitTime - 1);
    // should not give query since its not in the range
    assertFalse(result.contains(qh));

    // Filters on driver
    result = qCom.getAllQueries("SUCCESSFUL", "", "all", "hive/hive1", submitTime,
      System.currentTimeMillis());
    assertTrue(result.contains(qh));

    result = qCom.getAllQueries("SUCCESSFUL", "", "all", "DummyDriver", submitTime, System.currentTimeMillis());
    assertFalse(result.contains(qh));

    try {
      // Should fail with bad request since fromDate > toDate
      result = qCom.getAllQueries("SUCCESSFUL", "", "all", "", submitTime + 5000, submitTime);
      fail("Call should have failed with BadRequestException, instead got " + result);
    } catch (BadRequestException exc) {
      // pass
    }

    System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
    closeClientConnection(qCom);
  }

  private String readFile(String path) throws FileNotFoundException {
    return new Scanner(new File(path)).useDelimiter("\\Z").next();
  }

  /**
   * Sets the up.
   *
   * @param client the new up
   * @throws Exception the exception
   */
  @BeforeClass
  public void setup() throws Exception {
    LensClient client = new LensClient();
    LensCubeCommands command = new LensCubeCommands();
    command.setClient(client);

    log.debug("Starting to test cube commands");
    URL cubeSpec = TestLensQueryCommands.class.getClassLoader().getResource("sample-cube.xml");
    command.createCube(new File(cubeSpec.toURI()));
    TestLensDimensionCommands.createDimension();
    TestLensDimensionTableCommands.addDim1Table("dim_table", "dim_table.xml", "local");

    // Add partition
    URL dataDir = TestLensQueryCommands.class.getClassLoader().getResource("dim2-part");
    XPartition xp = new XPartition();
    xp.setFactOrDimensionTableName("dim_table");
    xp.setLocation(new Path(dataDir.toURI()).toString());
    xp.setUpdatePeriod(XUpdatePeriod.HOURLY);
    XTimePartSpec timePart = new XTimePartSpec();
    XTimePartSpecElement partElement = new XTimePartSpecElement();
    partElement.setKey("dt");
    partElement.setValue(DatatypeFactory.newInstance().newXMLGregorianCalendar(new GregorianCalendar()));
    timePart.getPartSpecElement().add(partElement);
    xp.setTimePartitionSpec(timePart);
    APIResult result = client.addPartitionToDim("dim_table", "local", xp);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  @AfterClass
  public void tearDown() {
    LensClient client = new LensClient();

    LensCubeCommands cubeCmd = new LensCubeCommands();
    cubeCmd.setClient(client);
    cubeCmd.dropCube("sample_cube");

    TestLensDimensionCommands.dropDimensions();

    LensDimensionTableCommands dimTableCmd = new LensDimensionTableCommands();
    dimTableCmd.setClient(client);
    dimTableCmd.dropDimensionTable("dim_table", true);

    // close client connection
    client.closeConnection();
  }

  /**
   * Test show persistent result set.
   *
   * @param qCom the q com
   * @throws Exception the exception
   */
  @Test(dataProvider = "queryCommands")
  public void showPersistentResultSet(LensQueryCommands qCom) throws Exception {
    System.out.println("@@PERSISTENT_RESULT_TEST-------------");
    qCom.getClient().setConnectionParam("lens.query.enable.persistent.resultset", "true");
    String query = "cube select id,name from test_dim";
    try {
      String result = qCom.executeQuery(query, false, "testQuery3");
      System.out.println("@@ RESULT " + result);
      assertNotNull(result);
      assertFalse(result.contains("Failed to get resultset"));
    } catch (Exception exc) {
      log.error("Exception not expected while getting resultset.", exc);
      fail("Exception not expected: " + exc.getMessage());
    }
    //Downlaod once
    downloadResult(qCom, qCom.getClient().getStatement().getQueryHandleString(), "testQuery3", "\"1\",\"first\"");
    //Download Again
    downloadResult(qCom, qCom.getClient().getStatement().getQueryHandleString(), "testQuery3",  "\"1\",\"first\"");
    System.out.println("@@END_PERSISTENT_RESULT_TEST-------------");
    qCom.getClient().setConnectionParam("lens.query.enable.persistent.resultset.indriver", "false");
    closeClientConnection(qCom);
  }

  private void downloadResult(LensQueryCommands qCom, String qHandle, String qName, String expected)
    throws IOException{
    assertTrue(qCom.getQueryResults(qHandle, resDir, true).contains("Saved to"));
    assertEquals(readFile(resDir.getAbsolutePath() + File.separator + qName + "-" + qHandle + ".csv").trim(),
        expected.trim());
  }


  /**
   * Test execute sync results.
   *
   * @param qCom the q com
   */
  @Test(dataProvider = "queryCommands")
  public void syncResults(LensQueryCommands qCom) {
    String sql = "cube select id,name from test_dim";
    String qh = qCom.executeQuery(sql, true, "testQuery4");
    String result = qCom.getQueryResults(qh, null, false);
    assertTrue(result.contains("1\tfirst"), result);
    closeClientConnection(qCom);
  }

  /**
   * Test purged finished result set.
   *
   * @param qCom the q com
   */
  @Test(dataProvider = "queryCommands")
  public void purgedFinishedResultSet(LensQueryCommands qCom) throws InterruptedException {
    System.out.println("@@START_FINISHED_PURGED_RESULT_TEST-------------");
    qCom.getClient().setConnectionParam("lens.query.enable.persistent.resultset", "true");
    String query = "cube select id,name from test_dim";
    String qh = qCom.executeQuery(query, true, "testQuery");
    while (!qCom.getClient().getQueryStatus(qh).finished()) {
      Thread.sleep(5000);
    }
    assertTrue(qCom.getStatus(qh).contains("Status: SUCCESSFUL"));

    String result = qCom.getQueryResults(qh, null, true);
    System.out.println("@@ RESULT " + result);
    assertNotNull(result);

    // This is to check for positive processing time
    assertFalse(result.contains("(-"));
    System.out.println("@@END_FINISHED_PURGED_RESULT_TEST-------------");
    qCom.getClient().setConnectionParam("lens.query.enable.persistent.resultset", "false");
    closeClientConnection(qCom);
  }

}

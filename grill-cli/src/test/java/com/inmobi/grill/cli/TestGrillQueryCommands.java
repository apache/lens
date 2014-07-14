package com.inmobi.grill.cli;


import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.cli.commands.GrillCubeCommands;
import com.inmobi.grill.cli.commands.GrillQueryCommands;
import com.inmobi.grill.client.GrillClient;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;

public class TestGrillQueryCommands extends GrillCliApplicationTest {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestGrillQueryCommands.class);

  private GrillClient client;

  private static String explainPlan = "TOK_QUERY\n" +
      "   TOK_FROM\n" +
      "      TOK_TABREF\n" +
      "         TOK_TABNAME\n" +
      "            local_dim_table\n" +
      "         test_dim\n" +
      "   TOK_INSERT\n" +
      "      TOK_DESTINATION\n" +
      "         TOK_DIR\n" +
      "            TOK_TMP_FILE\n" +
      "      TOK_SELECT\n" +
      "         TOK_SELEXPR\n" +
      "            .\n" +
      "               TOK_TABLE_OR_COL\n" +
      "                  test_dim\n" +
      "               id\n" +
      "         TOK_SELEXPR\n" +
      "            .\n" +
      "               TOK_TABLE_OR_COL\n" +
      "                  test_dim\n" +
      "               name\n" +
      "      TOK_WHERE\n" +
      "         =\n" +
      "            .\n" +
      "               TOK_TABLE_OR_COL\n" +
      "                  test_dim\n" +
      "               dt\n" +
      "            'latest'";

  @Test
  public void testQueryCommands() throws Exception {
    client = new GrillClient();
    client.setConnectionParam("grill.persistent.resultset.indriver", "false");
    setup(client);
    GrillQueryCommands qCom = new GrillQueryCommands();
    qCom.setClient(client);
    testExecuteSyncQuery(qCom);
    testExecuteAsyncQuery(qCom);
    testExplainQuery(qCom);
    testPreparedQuery(qCom);
  }

  private void testPreparedQuery(GrillQueryCommands qCom) throws Exception {
    String sql = "cube select id, name from test_dim";
    String result = qCom.getAllPreparedQueries();

    Assert.assertEquals("No prepared queries", result);
    String qh = qCom.prepare(sql);
    result = qCom.getAllPreparedQueries();
    Assert.assertEquals(qh, result);

    result = qCom.getPreparedStatus(qh);
    Assert.assertTrue(result.contains("User query:cube select id, name from test_dim"));
    Assert.assertTrue(result.contains(qh));

    result = qCom.executePreparedQuery(qh, false);

    LOG.warn("XXXXXX Prepared query sync result is  " + result);
    Assert.assertTrue(result.contains("1\tfirst"));

    String handle = qCom.executePreparedQuery(qh, true);
    LOG.debug("Perpared query handle is   " + handle);
    while(!client.getQueryStatus(handle).isFinished()) {
      Thread.sleep(5000);
    }
    String status = qCom.getStatus(handle);
    LOG.debug("Prepared Query Status is  " + status);
    Assert.assertTrue(status.contains("Status : SUCCESSFUL"));

    result = qCom.getQueryResults(handle);
    LOG.debug("Prepared Query Result is  " + result);
    Assert.assertTrue(result.contains("1\tfirst"));

    result = qCom.destroyPreparedQuery(qh);

    LOG.debug("destroy result is " + result);
    Assert.assertEquals("Successfully destroyed " + qh, result);

    result = qCom.explainAndPrepare(sql);
    Assert.assertTrue(result.contains(explainPlan));
    qh = qCom.getAllPreparedQueries();
    Assert.assertTrue(result.contains("Prepare handle:"+ qh));
    result = qCom.destroyPreparedQuery(qh);
    Assert.assertEquals("Successfully destroyed " + qh, result);

  }

  private void testExplainQuery(GrillQueryCommands qCom) throws Exception {
    String sql = "cube select id, name from test_dim";
    String result = qCom.explainQuery(sql, "");

    LOG.debug(result);
    Assert.assertTrue(result.contains(explainPlan));

  }

  private void testExecuteAsyncQuery(GrillQueryCommands qCom) throws Exception {
    String sql = "cube select id,name from test_dim";
    String qh = qCom.executeQuery(sql, true);
    String result = qCom.getAllQueries();
    //this is because previous query has run two query handle will be there
    Assert.assertTrue(result.contains(qh));

    while(!client.getQueryStatus(qh).isFinished()) {
      Thread.sleep(5000);
    }

    Assert.assertTrue(qCom.getStatus(qh).contains("Status : SUCCESSFUL"));

    result = qCom.getQueryResults(qh);
    Assert.assertTrue(result.contains("1\tfirst"));
    //Kill query is not tested as there is no deterministic way of killing a query
  }


  public void setup(GrillClient client) throws Exception {
    GrillCubeCommands command = new GrillCubeCommands();
    command.setClient(client);

    LOG.debug("Starting to test cube commands");
    URL cubeSpec =
        TestGrillQueryCommands.class.getClassLoader().getResource("sample-cube.xml");
    command.createCube(new File(cubeSpec.toURI()).getAbsolutePath());
    TestGrillDimensionCommands.createDimension();
    TestGrillDimensionTableCommands.addDim1Table("dim_table",
        "dim_table.xml", "dim_table_storage.xml", "local");

    URL dataFile =
        TestGrillQueryCommands.class.getClassLoader().getResource("data.txt");

    QueryHandle qh = client.executeQueryAsynch("LOAD DATA LOCAL INPATH '"
        + new File(dataFile.toURI()).getAbsolutePath()+
        "' OVERWRITE INTO TABLE local_dim_table partition(dt='latest')");

    while(!client.getQueryStatus(qh).isFinished()) {
      Thread.sleep(5000);
    }

    Assert.assertEquals(client.getQueryStatus(qh).getStatus(),QueryStatus.Status.SUCCESSFUL);
  }

  private void testExecuteSyncQuery(GrillQueryCommands qCom) {
    String sql = "cube select id,name from test_dim";
    String result = qCom.executeQuery(sql, false);
    Assert.assertTrue(result.contains("1\tfirst"), result);
  }
}

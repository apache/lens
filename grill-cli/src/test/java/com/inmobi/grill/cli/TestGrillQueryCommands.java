package com.inmobi.grill.cli;


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

public class TestGrillQueryCommands extends GrillCliApplicationTest {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestGrillQueryCommands.class);

  private static String explainPlan = "TOK_QUERY\n" +
      "   TOK_FROM\n" +
      "      TOK_TABREF\n" +
      "         TOK_TABNAME\n" +
      "            local_dim_table\n" +
      "         dim_table\n" +
      "   TOK_INSERT\n" +
      "      TOK_DESTINATION\n" +
      "         TOK_DIR\n" +
      "            TOK_TMP_FILE\n" +
      "      TOK_SELECT\n" +
      "         TOK_SELEXPR\n" +
      "            .\n" +
      "               TOK_TABLE_OR_COL\n" +
      "                  dim_table\n" +
      "               id\n" +
      "         TOK_SELEXPR\n" +
      "            .\n" +
      "               TOK_TABLE_OR_COL\n" +
      "                  dim_table\n" +
      "               name\n" +
      "      TOK_WHERE\n" +
      "         =\n" +
      "            .\n" +
      "               TOK_TABLE_OR_COL\n" +
      "                  dim_table\n" +
      "               dt\n" +
      "            'latest'";

  @Test
  public void testQueryCommands() throws Exception {
    GrillClient client = new GrillClient();
    client.setConnectionParam("grill.persistent.resultset.indriver", "false");
    setup(client);
    GrillQueryCommands qCom = new GrillQueryCommands();
    qCom.setClient(client);
    testExecuteSyncQuery(qCom);
    testExecuteAsyncQuery(qCom);
    testExplainQuery(qCom);
    testPreparedQuery(qCom);
    cleanup();
  }

  private void testPreparedQuery(GrillQueryCommands qCom) throws Exception {
    String sql = "cube select id, name from dim_table";
    String result = qCom.getAllPreparedQueries();

    Assert.assertEquals("No prepared queries", result);
    String qh = qCom.prepare(sql);
    result = qCom.getAllPreparedQueries();
    Assert.assertEquals(qh, result);

    result = qCom.getPreparedStatus(qh);
    Assert.assertTrue(result.contains("User query:cube select id, name from dim_table"));
    Assert.assertTrue(result.contains(qh));

    result = qCom.executePreparedQuery(qh, false);

    LOG.debug("Prepared query sync result is  " + result);
    Assert.assertTrue(result.contains("1\tfirst"));

    String handle = qCom.executePreparedQuery(qh, true);
    LOG.debug("Perpared query handle is   " + handle);
    String status = qCom.getStatus(handle);
    while (status.contains("RUNNING")
        || status.contains("LAUNCHED")
        || status.contains("QUEUED")
        || status.contains("NEW")) {
      Thread.sleep(5000);
      status = qCom.getStatus(handle);
    }
    status = qCom.getStatus(handle);
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

  }

  private void testExplainQuery(GrillQueryCommands qCom) throws Exception {
    String sql = "cube select id, name from dim_table";
    String result = qCom.explainQuery(sql, "");

    LOG.debug(result);
    Assert.assertTrue(result.contains(explainPlan));

  }

  private void testExecuteAsyncQuery(GrillQueryCommands qCom) throws Exception {
    String sql = "cube select id,name from dim_table";
    String qh = qCom.executeQuery(sql, true);
    String result = qCom.getAllQueries();
    //this is because previous query has run two query handle will be there
    Assert.assertTrue(result.contains(qh));
    String status = qCom.getStatus(qh);
    while (status.equals("RUNNING")
        || status.equals("LAUNCHED")
        || status.equals("QUEUED")
        || status.equals("NEW")) {
      Thread.sleep(5000);
      status = qCom.getStatus(qh);
    }

    Assert.assertTrue(qCom.getStatus(qh).contains("Status : SUCCESSFUL"));

    result = qCom.getQueryResults(qh);
    Assert.assertTrue(result.contains("1\tfirst"));
    //Kill query is not tested as there is no deterministic way of killing a query
  }

  /*@BeforeTest
  public void setup() throws Exception {
    if (System.getenv("HADOOP_HOME") == null
        || System.getenv("HADOOP_HOME").isEmpty()) {
      throw new IllegalStateException("Test cant be" +
          " run without setting hadoop home env variable");
    }
  }*/

  @AfterTest
  public void cleanup() throws Exception {
    new File("/tmp/dim_table/1.txt").delete();
    new File("/tmp/dim_table").delete();
    FileUtils.deleteDirectory(new File("/tmp/grillserver/"));
  }

  public void setup(GrillClient client) throws IOException {
    GrillCubeCommands command = new GrillCubeCommands();
    command.setClient(client);

    LOG.debug("Starting to test cube commands");
    File f = TestUtil.getPath("sample-cube.xml");
    command.createCube(f.getAbsolutePath());
    TestGrillDimensionCommands.addDim1Table("dim_table",
        "dim_table.xml", "dim_table_storage.xml", "local");
    TestGrillDimensionCommands.addPartitionToStorage("dim_table",
        "local","dim-local-part.xml");
    File file = new File("/tmp/dim_table/1.txt");
    file.getParentFile().mkdirs();
    FileWriter writer = new FileWriter(file, true);
    writer.write("1,first,this is one,11\n");
    writer.close();
  }

  private void testExecuteSyncQuery(GrillQueryCommands qCom) {
    String sql = "cube select id,name from dim_table";
    String result = qCom.executeQuery(sql, false);
    Assert.assertTrue(result.contains("1\tfirst"), result);
  }
}

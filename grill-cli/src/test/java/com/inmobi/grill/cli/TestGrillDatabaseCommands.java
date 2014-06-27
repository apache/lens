package com.inmobi.grill.cli;


import com.inmobi.grill.cli.commands.GrillDatabaseCommands;
import com.inmobi.grill.client.GrillClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestGrillDatabaseCommands extends GrillCliApplicationTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestGrillDatabaseCommands.class);

  @Test
  public void testDatabaseCommands() {
    GrillClient client = new GrillClient();
    GrillDatabaseCommands command = new GrillDatabaseCommands();
    command.setClient(client);

    String myDatabase = "my_db";
    String databaseList = command.showAllDatabases();
    Assert.assertFalse(
        databaseList.contains(myDatabase));
    String result;
    command.createDatabase(myDatabase, false);

    databaseList = command.showAllDatabases();
    Assert.assertTrue(
        databaseList.contains(myDatabase));

    result = command.switchDatabase(myDatabase);
    Assert.assertEquals(
        "Successfully switched to my_db", result);

    result = command.dropDatabase(myDatabase);
    Assert.assertEquals(
        "drop database my_db successful", result);
  }

}

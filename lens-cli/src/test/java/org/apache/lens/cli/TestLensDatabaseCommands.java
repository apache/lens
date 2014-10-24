package org.apache.lens.cli;

import org.apache.lens.cli.commands.LensDatabaseCommands;
import org.apache.lens.client.LensClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * The Class TestLensDatabaseCommands.
 */
public class TestLensDatabaseCommands extends LensCliApplicationTest {

  /** The Constant LOG. */
  private static final Logger LOG = LoggerFactory.getLogger(TestLensDatabaseCommands.class);

  /**
   * Test database commands.
   */
  @Test
  public void testDatabaseCommands() {
    LensClient client = new LensClient();
    LensDatabaseCommands command = new LensDatabaseCommands();
    command.setClient(client);

    String myDatabase = "my_db";
    String databaseList = command.showAllDatabases();
    Assert.assertFalse(databaseList.contains(myDatabase));
    String result;
    command.createDatabase(myDatabase, false);

    databaseList = command.showAllDatabases();
    Assert.assertTrue(databaseList.contains(myDatabase));

    result = command.switchDatabase(myDatabase);
    Assert.assertEquals("Successfully switched to my_db", result);

    result = command.dropDatabase(myDatabase);
    Assert.assertEquals("drop database my_db successful", result);
  }

}

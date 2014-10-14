package org.apache.lens.cli;
/*
 * #%L
 * Grill CLI
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */



import org.apache.lens.cli.commands.LensDatabaseCommands;
import org.apache.lens.client.GrillClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestLensDatabaseCommands extends GrillCliApplicationTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestLensDatabaseCommands.class);

  @Test
  public void testDatabaseCommands() {
    GrillClient client = new GrillClient();
    LensDatabaseCommands command = new LensDatabaseCommands();
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
